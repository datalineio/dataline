/*
 * MIT License
 * 
 * Copyright (c) 2020 Dataline
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
package io.dataline.config.persistence;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dataline.config.ConfigSchema;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import me.andrz.jackson.JsonReferenceException;
import me.andrz.jackson.JsonReferenceProcessor;
import org.apache.commons.io.FileUtils;

// we force all interaction with disk storage to be effectively single threaded.
public class DefaultConfigPersistence implements ConfigPersistence {
  private static final Object lock = new Object();
  private static final String CONFIG_SCHEMA_ROOT = "../dataline-config/src/main/resources/json/";

  private final ObjectMapper objectMapper;
  private final JsonSchemaValidation jsonSchemaValidation;
  private final String storageRoot;

  public DefaultConfigPersistence(String storageRoot) {
    this.storageRoot = storageRoot;
    jsonSchemaValidation = new JsonSchemaValidation();
    objectMapper = new ObjectMapper();
  }

  @Override
  public <T> T getConfig(
      PersistenceConfigType persistenceConfigType, String configId, Class<T> clazz)
      throws ConfigNotFoundException, JsonValidationException {
    synchronized (lock) {
      return getConfigInternal(persistenceConfigType, configId, clazz);
    }
  }

  private <T> T getConfigInternal(
      PersistenceConfigType persistenceConfigType, String configId, Class<T> clazz)
      throws ConfigNotFoundException, JsonValidationException {
    // find file
    File configFile = getFileOrThrow(persistenceConfigType, configId);

    // validate file with schema
    validateJson(configFile, persistenceConfigType);

    // cast file to type
    return fileToPojo(configFile, clazz);
  }

  @Override
  public <T> Set<T> getConfigs(PersistenceConfigType persistenceConfigType, Class<T> clazz)
      throws JsonValidationException {
    synchronized (lock) {
      final Set<T> configs = new HashSet<>();
      for (String configId : getConfigIds(persistenceConfigType)) {
        try {
          configs.add(getConfig(persistenceConfigType, configId, clazz));
          // this should not happen, because we just looked up these ids.
        } catch (ConfigNotFoundException e) {
          throw new RuntimeException(e);
        }
      }
      return configs;
    }
  }

  @Override
  public <T> void writeConfig(
      PersistenceConfigType persistenceConfigType, String configId, T config) {
    synchronized (lock) {
      final String configPath = getConfigPath(persistenceConfigType, configId);
      ensureDirectory(getConfigDirectory(persistenceConfigType));
      try {
        objectMapper.writeValue(new File(configPath), config);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private JsonNode getSchema(PersistenceConfigType persistenceConfigType) {
    String configSchemaFilename =
        standardConfigTypeToConfigSchema(persistenceConfigType).getSchemaFilename();
    File schemaFile = new File(String.format("%s/%s", CONFIG_SCHEMA_ROOT, configSchemaFilename));
    try {
      // JsonReferenceProcessor follows $ref in json objects. Jackson does not natively support
      // this.
      final JsonReferenceProcessor jsonReferenceProcessor = new JsonReferenceProcessor();
      jsonReferenceProcessor.setMaxDepth(-1); // no max.
      return jsonReferenceProcessor.process(schemaFile);
    } catch (IOException | JsonReferenceException e) {
      throw new RuntimeException(e);
    }
  }

  private Set<Path> getFiles(PersistenceConfigType persistenceConfigType) {
    String configDirPath = getConfigDirectory(persistenceConfigType);
    try {
      return Files.list(new File(configDirPath).toPath()).collect(Collectors.toSet());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private String getConfigDirectory(PersistenceConfigType persistenceConfigType) {
    return String.format("%s/%s", storageRoot, persistenceConfigType.toString());
  }

  private String getConfigPath(PersistenceConfigType persistenceConfigType, String configId) {
    return String.format("%s/%s", getConfigDirectory(persistenceConfigType), getFilename(configId));
  }

  private Set<String> getConfigIds(PersistenceConfigType persistenceConfigType) {
    return getFiles(persistenceConfigType).stream()
        .map(path -> path.getFileName().toString().replace(".json", ""))
        .collect(Collectors.toSet());
  }

  private Optional<Path> getFile(PersistenceConfigType persistenceConfigType, String id) {
    ensureDirectory(getConfigDirectory(persistenceConfigType));
    final Path path = Paths.get(getConfigPath(persistenceConfigType, id));
    if (Files.exists(path)) {
      return Optional.of(path);
    } else {
      return Optional.empty();
    }
  }

  private String getFilename(String id) {
    return String.format("%s.json", id);
  }

  private ConfigSchema standardConfigTypeToConfigSchema(
      PersistenceConfigType persistenceConfigType) {
    switch (persistenceConfigType) {
      case STANDARD_WORKSPACE:
        return ConfigSchema.STANDARD_WORKSPACE;
      case STANDARD_SOURCE:
        return ConfigSchema.STANDARD_SOURCE;
      case SOURCE_CONNECTION_SPECIFICATION:
        return ConfigSchema.SOURCE_CONNECTION_SPECIFICATION;
      case SOURCE_CONNECTION_IMPLEMENTATION:
        return ConfigSchema.SOURCE_CONNECTION_IMPLEMENTATION;
      case STANDARD_DESTINATION:
        return ConfigSchema.STANDARD_DESTINATION;
      case DESTINATION_CONNECTION_SPECIFICATION:
        return ConfigSchema.DESTINATION_CONNECTION_SPECIFICATION;
      case DESTINATION_CONNECTION_IMPLEMENTATION:
        return ConfigSchema.DESTINATION_CONNECTION_IMPLEMENTATION;
      case STANDARD_CONNECTION_STATUS:
        return ConfigSchema.STANDARD_CONNECTION_STATUS;
      case STANDARD_DISCOVERY_OUTPUT:
        return ConfigSchema.STANDARD_DISCOVERY_OUTPUT;
      case STANDARD_SYNC:
        return ConfigSchema.STANDARD_SYNC;
      case STANDARD_SYNC_SUMMARY:
        return ConfigSchema.STANDARD_SYNC_SUMMARY;
      case STANDARD_SYNC_SCHEDULE:
        return ConfigSchema.STANDARD_SYNC_SCHEDULE;
      case STATE:
        return ConfigSchema.STATE;
      default:
        throw new RuntimeException(
            String.format(
                "No mapping from StandardConfigType to ConfigSchema for %s",
                persistenceConfigType));
    }
  }

  private void validateJson(File configFile, PersistenceConfigType persistenceConfigType)
      throws JsonValidationException {
    JsonNode configJson;
    try {
      configJson = objectMapper.readTree(configFile);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    JsonNode schema = getSchema(persistenceConfigType);
    jsonSchemaValidation.validateThrow(schema, configJson);
  }

  private File getFileOrThrow(PersistenceConfigType persistenceConfigType, String configId)
      throws ConfigNotFoundException {
    return getFile(persistenceConfigType, configId)
        .map(Path::toFile)
        .orElseThrow(
            () ->
                new ConfigNotFoundException(
                    String.format(
                        "config type: %s id: %s not found in path %s",
                        persistenceConfigType,
                        configId,
                        getConfigPath(persistenceConfigType, configId))));
  }

  private <T> T fileToPojo(File file, Class<T> clazz) {
    try {
      return objectMapper.readValue(file, clazz);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void ensureDirectory(String path) {
    try {
      FileUtils.forceMkdir(new File(path));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
