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

package io.airbyte.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Streams;
import io.airbyte.analytics.TrackingClientSingleton;
import io.airbyte.api.model.ImportRead;
import io.airbyte.api.model.ImportRead.StatusEnum;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.io.Archives;
import io.airbyte.commons.io.IOs;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.stream.MoreStreams;
import io.airbyte.commons.version.AirbyteVersion;
import io.airbyte.commons.yaml.Yamls;
import io.airbyte.config.ConfigSchema;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.config.persistence.PersistenceConstants;
import io.airbyte.scheduler.persistence.DatabaseSchema;
import io.airbyte.scheduler.persistence.JobPersistence;
import io.airbyte.validation.json.JsonSchemaValidator;
import io.airbyte.validation.json.JsonValidationException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class ConfigDumpImporter {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigDumpImporter.class);
  private static final String CONFIG_FOLDER_NAME = "airbyte_config";
  private static final String DB_FOLDER_NAME = "airbyte_db";
  private static final String VERSION_FILE_NAME = "VERSION";
  private final ConfigRepository configRepository;
  private final JsonSchemaValidator jsonSchemaValidator;
  private final JobPersistence postgresPersistence;

  public ConfigDumpImporter(ConfigRepository configRepository, JobPersistence postgresPersistence) {
    this(configRepository, postgresPersistence, new JsonSchemaValidator());
  }

  @VisibleForTesting
  public ConfigDumpImporter(ConfigRepository configRepository, JobPersistence postgresPersistence, JsonSchemaValidator jsonSchemaValidator) {
    this.jsonSchemaValidator = jsonSchemaValidator;
    this.postgresPersistence = postgresPersistence;
    this.configRepository = configRepository;
  }

  @VisibleForTesting
  public Optional<UUID> getCurrentCustomerId() {
    try {
      return Optional.of(configRepository
          .getStandardWorkspace(PersistenceConstants.DEFAULT_WORKSPACE_ID, true).getCustomerId());
    } catch (Exception e) {
      // because this is used for tracking we prefer to log instead of killing the import.
      LOGGER.error("failed to fetch current customerId.", e);
      return Optional.empty();
    }
  }

  public ImportRead importData(String targetVersion, File archive) {
    return importDataInternal(targetVersion, archive, Optional.empty());
  }

  public ImportRead importDataWithSeed(String targetVersion, File archive, Path seedPath) {
    return importDataInternal(targetVersion, archive, Optional.of(seedPath));
  }

  // seedPath - if present, merge with the import. otherwise just use the data in the import.
  private ImportRead importDataInternal(String targetVersion, File archive, Optional<Path> seedPath) {
    Preconditions.checkNotNull(seedPath);

    final Optional<UUID> previousCustomerIdOptional = getCurrentCustomerId();
    ImportRead result;
    try {
      final Path sourceRoot = Files.createTempDirectory(Path.of("/tmp"), "airbyte_archive");
      try {
        // 1. Unzip source
        Archives.extractArchive(archive.toPath(), sourceRoot);

        // 2. dry run
        try {
          checkImport(targetVersion, sourceRoot, seedPath);
        } catch (Exception e) {
          LOGGER.error("Dry run failed.", e);
          throw e;
        }

        // 3. Import Postgres content
        importDatabaseFromArchive(sourceRoot, targetVersion);

        // 4. Import Configs
        importConfigsFromArchive(sourceRoot, seedPath, false);

        // 5. Set DB version
        LOGGER.info("Setting the DB Airbyte version to : " + targetVersion);
        postgresPersistence.setVersion(targetVersion);

        // 6. check db version
        checkDBVersion(targetVersion);
        result = new ImportRead().status(StatusEnum.SUCCEEDED);
      } finally {
        FileUtils.deleteDirectory(sourceRoot.toFile());
        FileUtils.deleteQuietly(archive);
      }

      // identify this instance as the new customer id.
      TrackingClientSingleton.get().identify();
      // report that the previous customer id is now superseded by the imported one.
      previousCustomerIdOptional.ifPresent(previousCustomerId -> TrackingClientSingleton.get().alias(previousCustomerId.toString()));
    } catch (IOException | JsonValidationException | RuntimeException e) {
      LOGGER.error("Import failed", e);
      result = new ImportRead().status(StatusEnum.FAILED).reason(e.getMessage());
    }

    return result;
  }

  private void checkImport(String targetVersion, Path tempFolder, Optional<Path> seed) throws IOException, JsonValidationException {
    final Path versionFile = tempFolder.resolve(VERSION_FILE_NAME);
    final String importVersion = Files.readString(versionFile, Charset.defaultCharset())
        .replace("\n", "").strip();
    LOGGER.info(String.format("Checking Airbyte Version to import %s", importVersion));
    if (!AirbyteVersion.isCompatible(targetVersion, importVersion)) {
      throw new IOException(String
          .format("Imported VERSION (%s) is incompatible with current Airbyte version (%s).\n" +
              "Please upgrade your Airbyte Archive, see more at https://docs.airbyte.io/tutorials/upgrading-airbyte\n",
              importVersion, targetVersion));
    }
    importConfigsFromArchive(tempFolder, seed, true);
  }

  // Config
  private List<String> listDirectories(Path sourceRoot) throws IOException {
    try (Stream<Path> files = Files.list(sourceRoot.resolve(CONFIG_FOLDER_NAME))) {
      return files.map(c -> c.getFileName().toString())
          .collect(Collectors.toList());
    }
  }

  private <T> void importConfigsFromArchive(final Path sourceRoot, Optional<Path> seedPath, final boolean dryRun)
      throws IOException, JsonValidationException {
    final List<String> sourceDefinitionsToMigrate = new ArrayList<>();
    final List<String> destinationDefinitionsToMigrate = new ArrayList<>();
    final boolean[] sourceProcessed = {false};
    final boolean[] destinationProcessed = {false};
    final List<String> directories = listDirectories(sourceRoot);
    // We sort the directories cause we want to process SOURCE_CONNECTION before
    // STANDARD_SOURCE_DEFINITION and DESTINATION_CONNECTION before STANDARD_DESTINATION_DEFINITION
    // so that we can identify which definitions should not be upgraded to the latest version
    Collections.sort(directories);
    final Map<ConfigSchema, Stream<T>> data = new LinkedHashMap<>();

    final Map<ConfigSchema, Map<String, T>> seed;
    if (seedPath.isPresent()) {
      seed = getSeed(seedPath.get());
    } else {
      seed = new HashMap<>();
    }
    for (String directory : directories) {
      final Optional<ConfigSchema> configSchemaOptional = Enums.toEnum(directory.replace(".yaml", ""), ConfigSchema.class);

      if (configSchemaOptional.isEmpty()) {
        continue;
      }

      final ConfigSchema configSchema = configSchemaOptional.get();
      Stream<T> configs = readConfigsFromArchive(sourceRoot, configSchema);
      configs = streamWithAdditionalOperation(
          sourceDefinitionsToMigrate,
          destinationDefinitionsToMigrate,
          sourceProcessed,
          destinationProcessed,
          configSchema,
          configs,
          seed);
      data.put(configSchema, configs);
    }
    configRepository.replaceAllConfigs(data, dryRun);
  }

  private <T> Map<ConfigSchema, Map<String, T>> getSeed(Path seed) throws IOException {
    final List<ConfigSchema> configSchemas = Files.list(seed).map(c -> ConfigSchema.valueOf(c.getFileName().toString())).collect(Collectors.toList());
    final Map<ConfigSchema, Map<String, T>> allData = new HashMap<>();
    for (ConfigSchema configSchema : configSchemas) {
      final Map<String, T> data = readLatestSeed(seed, configSchema);
      allData.put(configSchema, data);
    }
    return allData;
  }

  private <T> Map<String, T> readLatestSeed(Path latestSeed, ConfigSchema configSchema) throws IOException {
    try (Stream<Path> files = Files.list(latestSeed.resolve(configSchema.toString()))) {
      final List<String> ids = files
          .filter(p -> !p.endsWith(".json"))
          .map(p -> p.getFileName().toString().replace(".json", ""))
          .collect(Collectors.toList());

      final Map<String, T> configData = new HashMap<>();
      for (String id : ids) {
        try {
          final Path configPath = latestSeed.resolve(configSchema.toString()).resolve(String.format("%s.json", id));
          if (!Files.exists(configPath)) {
            throw new RuntimeException("Config NotFound");
          }

          T config = Jsons.deserialize(Files.readString(configPath), configSchema.getClassName());
          configData.put(id, config);
        } catch (RuntimeException e) {
          throw new IOException(e);
        }
      }

      return configData;
    }
  }

  private <T> Stream<T> streamWithAdditionalOperation(List<String> sourceDefinitionsToMigrate,
                                                      List<String> destinationDefinitionsToMigrate,
                                                      boolean[] sourceProcessed,
                                                      boolean[] destinationProcessed,
                                                      ConfigSchema configSchema,
                                                      Stream<T> configs,
                                                      Map<ConfigSchema, Map<String, T>> latestSeeds) {
    if (configSchema == ConfigSchema.SOURCE_CONNECTION) {
      sourceProcessed[0] = true;
      configs = configs.peek(config -> sourceDefinitionsToMigrate.add(((SourceConnection) config).getSourceDefinitionId().toString()));
    } else if (configSchema == ConfigSchema.DESTINATION_CONNECTION) {
      destinationProcessed[0] = true;
      configs = configs.peek(config -> destinationDefinitionsToMigrate.add(((DestinationConnection) config).getDestinationDefinitionId().toString()));
    } else if (configSchema == ConfigSchema.STANDARD_SOURCE_DEFINITION) {
      configs = getDefinitionStream(sourceDefinitionsToMigrate, sourceProcessed[0], configSchema, configs, latestSeeds);
    } else if (configSchema == ConfigSchema.STANDARD_DESTINATION_DEFINITION) {
      configs = getDefinitionStream(destinationDefinitionsToMigrate, destinationProcessed[0], configSchema, configs, latestSeeds);
    }
    return configs;
  }

  /**
   * This method combines latest definitions, with existing definition. If a connector is being used
   * by user, it will continue to be at the same version, otherwise it will be migrated to the latest
   * version
   */
  private <T> Stream<T> getDefinitionStream(List<String> definitionsToMigrate,
                                            boolean definitionsPopulated,
                                            ConfigSchema configSchema,
                                            Stream<T> configs,
                                            Map<ConfigSchema, Map<String, T>> latestSeeds) {
    if (!definitionsPopulated) {
      throw new RuntimeException("Trying to process " + configSchema + " without populating the definitions to migrate");
    }

    return Streams.concat(configs.filter(c -> definitionsToMigrate.contains(configSchema.getId(c))),
        latestSeeds.getOrDefault(configSchema, new HashMap<>()).entrySet().stream().filter(c -> !definitionsToMigrate.contains(c.getKey()))
            .map(Entry::getValue));
  }

  private <T> Stream<T> readConfigsFromArchive(final Path storageRoot, final ConfigSchema schemaType)
      throws IOException {

    final Path configPath = buildConfigPath(storageRoot, schemaType);
    if (configPath.toFile().exists()) {
      final String configStr = Files.readString(configPath);
      final JsonNode node = Yamls.deserialize(configStr);
      return StreamSupport
          .stream(Spliterators.spliteratorUnknownSize(node.elements(), Spliterator.ORDERED), false)
          .map(element -> {
            final T config = Jsons.object(element, schemaType.getClassName());
            try {
              validateJson(config, schemaType);
              return config;
            } catch (JsonValidationException e) {
              throw new RuntimeException(e);
            }
          });

    } else {
      throw new FileNotFoundException(
          String.format("Airbyte Configuration %s was not found in the archive", schemaType));
    }
  }

  private <T> void validateJson(final T config, final ConfigSchema configType)
      throws JsonValidationException {
    JsonNode schema = JsonSchemaValidator.getSchema(configType.getFile());
    jsonSchemaValidator.ensure(schema, Jsons.jsonNode(config));
  }

  protected static Path buildConfigPath(final Path storageRoot, final ConfigSchema schemaType) {
    return storageRoot.resolve(CONFIG_FOLDER_NAME)
        .resolve(String.format("%s.yaml", schemaType.name()));
  }

  // Postgres Portion
  public void importDatabaseFromArchive(final Path storageRoot, final String airbyteVersion) throws IOException {
    try {
      final Map<DatabaseSchema, Stream<JsonNode>> data = new HashMap<>();
      for (DatabaseSchema tableType : DatabaseSchema.values()) {
        final Path tablePath = buildTablePath(storageRoot, tableType.name());
        data.put(tableType, readTableFromArchive(tableType, tablePath));
      }
      postgresPersistence.importDatabase(airbyteVersion, data);
      LOGGER.info("Successful upgrade of airbyte postgres database from archive");
    } catch (Exception e) {
      LOGGER.warn("Postgres database version upgrade failed, reverting to state previous to migration.");
      throw e;
    }
  }

  protected static Path buildTablePath(final Path storageRoot, final String tableName) {
    return storageRoot
        .resolve(DB_FOLDER_NAME)
        .resolve(String.format("%s.yaml", tableName.toUpperCase()));
  }

  private Stream<JsonNode> readTableFromArchive(final DatabaseSchema tableSchema,
                                                final Path tablePath)
      throws FileNotFoundException {
    final JsonNode schema = tableSchema.toJsonNode();
    if (schema != null) {
      return MoreStreams.toStream(Yamls.deserialize(IOs.readFile(tablePath)).elements())
          .peek(r -> {
            try {
              jsonSchemaValidator.ensure(schema, r);
            } catch (JsonValidationException e) {
              throw new IllegalArgumentException(
                  "Archived Data Schema does not match current Airbyte Data Schemas", e);
            }
          });
    } else {
      throw new FileNotFoundException(String
          .format("Airbyte Database table %s was not found in the archive", tableSchema.name()));
    }
  }

  private void checkDBVersion(final String airbyteVersion) throws IOException {
    final Optional<String> airbyteDatabaseVersion = postgresPersistence.getVersion();
    airbyteDatabaseVersion
        .ifPresent(dbversion -> AirbyteVersion.assertIsCompatible(airbyteVersion, dbversion));
  }

}
