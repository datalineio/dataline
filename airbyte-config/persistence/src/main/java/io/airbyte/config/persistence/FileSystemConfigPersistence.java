/*
 * MIT License
 *
 * Copyright (c) 2020 Airbyte
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package io.airbyte.config.persistence;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ConfigSchema;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// we force all interaction with disk storage to be effectively single threaded.
public class FileSystemConfigPersistence implements ConfigPersistence {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileSystemConfigPersistence.class);
  private static final String CONFIG_DIR = "config";
  private static final String TMP_DIR = "tmp_storage";

  private static final Object lock = new Object();

  // root of the file system persistence
  private final Path storageRoot;
  // root for where configs are stored
  private final Path configRoot;

  public static ConfigPersistence createWithValidation(final Path storageRoot) throws IOException {
    return new ValidatingConfigPersistence(new FileSystemConfigPersistence(storageRoot));
  }

  public FileSystemConfigPersistence(final Path storageRoot) throws IOException {
    this.storageRoot = storageRoot;
    this.configRoot = storageRoot.resolve(CONFIG_DIR);
    Files.createDirectories(configRoot);
  }

  @Override
  public <T> T getConfig(final ConfigSchema configType, final String configId, final Class<T> clazz)
      throws ConfigNotFoundException, JsonValidationException, IOException {
    synchronized (lock) {
      return getConfigInternal(configType, configId, clazz);
    }
  }

  @Override
  public <T> List<T> listConfigs(ConfigSchema configType, Class<T> clazz) throws JsonValidationException, IOException {
    synchronized (lock) {
      return listConfigsInternal(configType, clazz);
    }
  }

  @Override
  public <T> void writeConfig(ConfigSchema configType, String configId, T config) throws IOException {
    synchronized (lock) {
      writeConfigInternal(configType, configId, config);
    }
  }

  private <T> void writeConfigs(ConfigSchema configType, Stream<T> configs, Path rootOverride) {
    configs.forEach(config -> {
      String configId = configType.getId(config);
      try {
        writeConfigInternal(configType, configId, config, rootOverride);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Override
  public Map<String, Stream<JsonNode>> dumpConfigs() throws IOException {
    final Map<String, Stream<JsonNode>> configs = new HashMap<>();

    final List<String> directories = listDirectories();
    for (String directory : directories) {
      final List<JsonNode> configList = listConfig(directory);
      configs.put(directory, configList.stream());
    }
    return configs;
  }

  private List<String> listDirectories() throws IOException {
    try (Stream<Path> files = Files.list(configRoot)) {
      return files.map(c -> c.getFileName().toString()).collect(Collectors.toList());
    }
  }

  private List<JsonNode> listConfig(String configType) throws IOException {
    final Path configTypePath = configRoot.resolve(configType);
    if (!Files.exists(configTypePath)) {
      return Collections.emptyList();
    }
    try (Stream<Path> files = Files.list(configTypePath)) {
      final List<String> ids = files
          .filter(p -> !p.endsWith(".json"))
          .map(p -> p.getFileName().toString().replace(".json", ""))
          .collect(Collectors.toList());

      final List<JsonNode> configs = Lists.newArrayList();
      for (String id : ids) {
        try {
          final Path configPath = configRoot.resolve(configType).resolve(String.format("%s.json", id));
          if (!Files.exists(configPath)) {
            throw new RuntimeException("Config NotFound");
          }

          final JsonNode config = Jsons.deserialize(Files.readString(configPath), JsonNode.class);
          configs.add(config);
        } catch (RuntimeException e) {
          throw new IOException(e);
        }
      }

      return configs;
    }
  }

  @Override
  public <T> void replaceAllConfigs(Map<ConfigSchema, Stream<T>> configs, boolean dryRun) throws IOException {
    final String oldConfigsDir = "config_deprecated";
    // create a new folder
    final String importDirectory = TMP_DIR + UUID.randomUUID();
    final Path rootOverride = storageRoot.resolve(importDirectory);
    Files.createDirectories(rootOverride);

    // write everything
    for (final Map.Entry<ConfigSchema, Stream<T>> config : configs.entrySet()) {
      writeConfigs(config.getKey(), config.getValue(), rootOverride);
    }

    if (dryRun) {
      FileUtils.deleteDirectory(rootOverride.toFile());
      return;
    }

    FileUtils.moveDirectory(configRoot.toFile(), storageRoot.resolve(oldConfigsDir).toFile());
    LOGGER.info("Renamed config to {} successfully", oldConfigsDir);

    FileUtils.moveDirectory(rootOverride.toFile(), configRoot.toFile());
    LOGGER.info("Renamed " + importDirectory + " to config successfully");

    FileUtils.deleteDirectory(storageRoot.resolve(oldConfigsDir).toFile());
    LOGGER.info("Deleted {}", oldConfigsDir);
  }

  private <T> T getConfigInternal(ConfigSchema configType, String configId, Class<T> clazz)
      throws ConfigNotFoundException, IOException {
    // validate file with schema
    final Path configPath = buildConfigPath(configType, configId, configRoot);
    if (!Files.exists(configPath)) {
      throw new ConfigNotFoundException(configType, configId);
    } else {
      return Jsons.deserialize(Files.readString(configPath), clazz);
    }
  }

  private <T> List<T> listConfigsInternal(ConfigSchema configType, Class<T> clazz) throws JsonValidationException, IOException {
    final Path configTypePath = buildTypePath(configType, configRoot);
    if (!Files.exists(configTypePath)) {
      return Collections.emptyList();
    }

    try (Stream<Path> files = Files.list(configTypePath)) {
      final List<String> ids = files
          .filter(p -> !p.endsWith(".json"))
          .map(p -> p.getFileName().toString().replace(".json", ""))
          .collect(Collectors.toList());

      final List<T> configs = Lists.newArrayList();
      for (String id : ids) {
        try {
          configs.add(getConfig(configType, id, clazz));
        } catch (ConfigNotFoundException e) {
          // should not happen since we just read the ids from disk.
          throw new IOException(e);
        }
      }

      return configs;
    }
  }

  private <T> void writeConfigInternal(ConfigSchema configType, String configId, T config) throws IOException {
    writeConfigInternal(configType, configId, config, configRoot);
  }

  private <T> void writeConfigInternal(ConfigSchema configType, String configId, T config, Path storageRoot) throws IOException {

    final Path configPath = buildConfigPath(configType, configId, storageRoot);
    Files.createDirectories(configPath.getParent());

    Files.writeString(configPath, Jsons.serialize(config));
  }

  private static Path buildConfigPath(ConfigSchema configType, String configId, Path storageRoot) {
    return buildTypePath(configType, storageRoot).resolve(String.format("%s.json", configId));
  }

  private static Path buildTypePath(ConfigSchema configType, Path storageRoot) {
    return storageRoot.resolve(configType.toString());
  }

}
