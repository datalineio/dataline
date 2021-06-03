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
import io.airbyte.validation.json.JsonSchemaValidator;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

// we force all interaction with disk storage to be effectively single threaded.
public class DefaultConfigPersistence implements ConfigPersistence {

  private static final String CONFIG_DIR = "config";

  private static final Object lock = new Object();

  private final JsonSchemaValidator jsonSchemaValidator;
  private final Path storageRoot;

  public DefaultConfigPersistence(final Path storageRoot) {
    this(storageRoot, new JsonSchemaValidator());
  }

  public DefaultConfigPersistence(final Path storageRoot, final JsonSchemaValidator schemaValidator) {
    this.storageRoot = storageRoot.resolve(CONFIG_DIR);
    jsonSchemaValidator = schemaValidator;
  }

  @Override
  public <T> T getConfig(final ConfigSchema configType, final UUID configId, final Class<T> clazz)
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
  public <T> void writeConfig(ConfigSchema configType, UUID configId, T config) throws JsonValidationException, IOException {
    synchronized (lock) {
      writeConfigInternal(configType, configId, config);
    }
  }

  private <T> T getConfigInternal(ConfigSchema configType, UUID configId, Class<T> clazz)
      throws ConfigNotFoundException, JsonValidationException, IOException {
    // validate file with schema
    final Path configPath = buildConfigPath(configType, configId);
    if (!Files.exists(configPath)) {
      throw new ConfigNotFoundException(configType, configId);
    }

    final T config = Jsons.deserialize(Files.readString(configPath), clazz);
    validateJson(config, configType);

    return config;
  }

  private <T> List<T> listConfigsInternal(ConfigSchema configType, Class<T> clazz) throws JsonValidationException, IOException {
    final Path configTypePath = buildTypePath(configType);
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
          configs.add(getConfig(configType, UUID.fromString(id), clazz));
        } catch (ConfigNotFoundException e) {
          // should not happen since we just read the ids from disk.
          throw new IOException(e);
        }
      }

      return configs;
    }
  }

  private <T> void writeConfigInternal(ConfigSchema configType, UUID configId, T config) throws JsonValidationException, IOException {
    // validate config with schema
    validateJson(Jsons.jsonNode(config), configType);

    final Path configPath = buildConfigPath(configType, configId);
    Files.createDirectories(configPath.getParent());

    Files.writeString(configPath, Jsons.serialize(config));
  }

  private Path buildConfigPath(ConfigSchema type, UUID configId) {
    return buildTypePath(type).resolve(String.format("%s.json", configId.toString()));
  }

  private Path buildTypePath(ConfigSchema type) {
    return storageRoot.resolve(type.toString());
  }

  private <T> void validateJson(T config, ConfigSchema configType) throws JsonValidationException {
    JsonNode schema = JsonSchemaValidator.getSchema(configType.getFile());
    jsonSchemaValidator.ensure(schema, Jsons.jsonNode(config));
  }

}
