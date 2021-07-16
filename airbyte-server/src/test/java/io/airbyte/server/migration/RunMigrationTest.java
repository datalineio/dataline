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

package io.airbyte.server.migration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.common.io.Resources;
import io.airbyte.commons.io.Archives;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.OperatorNormalization.Option;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardSync;
import io.airbyte.config.StandardSyncOperation;
import io.airbyte.config.StandardSyncOperation.OperatorType;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.config.persistence.FileSystemConfigPersistence;
import io.airbyte.db.Database;
import io.airbyte.migrate.Migrations;
import io.airbyte.scheduler.persistence.DefaultJobPersistence;
import io.airbyte.scheduler.persistence.JobPersistence;
import io.airbyte.server.RunMigration;
import io.airbyte.validation.json.JsonValidationException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RunMigrationTest {

  private static final String INITIAL_VERSION = "0.17.0-alpha";
  private static final String TARGET_VERSION = Migrations.MIGRATIONS.get(Migrations.MIGRATIONS.size() - 1).getVersion();
  private static final String DEPRECATED_SOURCE_DEFINITION_NOT_BEING_USED = "d2147be5-fa36-4936-977e-f031affa5895";
  private static final String DEPRECATED_SOURCE_DEFINITION_BEING_USED = "4eb22946-2a79-4d20-a3e6-effd234613c3";
  private List<File> resourceToBeCleanedUp;

  @BeforeEach
  public void setup() {
    resourceToBeCleanedUp = new ArrayList<>();
  }

  @AfterEach
  public void cleanup() throws IOException {
    for (File file : resourceToBeCleanedUp) {
      if (file.exists()) {
        if (file.isDirectory()) {
          FileUtils.deleteDirectory(file);
        } else {
          Files.delete(file.toPath());
        }
      }
    }
  }

  @Test
  public void testRunMigration() {
    try (StubAirbyteDB stubAirbyteDB = new StubAirbyteDB()) {
      final File file = Path
          .of(Resources.getResource("migration/03a4c904-c91d-447f-ab59-27a43b52c2fd.gz").toURI())
          .toFile();

      final Path dummyDataSource = Path.of(Resources.getResource("migration/dummy_data").toURI());

      final Path configRoot = Files.createTempDirectory(Path.of("/tmp"), "dummy_data");
      FileUtils.copyDirectory(dummyDataSource.toFile(), configRoot.toFile());
      resourceToBeCleanedUp.add(configRoot.toFile());
      final JobPersistence jobPersistence = getJobPersistence(stubAirbyteDB.getDatabase(), file, INITIAL_VERSION);
      assertPreMigrationConfigs(configRoot, jobPersistence);

      runMigration(jobPersistence, configRoot);

      assertDatabaseVersion(jobPersistence, TARGET_VERSION);
      assertPostMigrationConfigs(configRoot);
      FileUtils.deleteDirectory(configRoot.toFile());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void assertPreMigrationConfigs(Path configRoot, JobPersistence jobPersistence) throws IOException, JsonValidationException {
    assertDatabaseVersion(jobPersistence, INITIAL_VERSION);
    ConfigRepository configRepository = new ConfigRepository(FileSystemConfigPersistence.createWithValidation(configRoot));
    Map<String, StandardSourceDefinition> sourceDefinitionsBeforeMigration = configRepository.listStandardSources().stream()
        .collect(Collectors.toMap(c -> c.getSourceDefinitionId().toString(), c -> c));
    assertTrue(sourceDefinitionsBeforeMigration.containsKey(DEPRECATED_SOURCE_DEFINITION_NOT_BEING_USED));
    assertTrue(sourceDefinitionsBeforeMigration.containsKey(DEPRECATED_SOURCE_DEFINITION_BEING_USED));
  }

  private void assertDatabaseVersion(JobPersistence jobPersistence, String version) throws IOException {
    final Optional<String> versionFromDb = jobPersistence.getVersion();
    assertTrue(versionFromDb.isPresent());
    assertEquals(versionFromDb.get(), version);
  }

  private void assertPostMigrationConfigs(Path importRoot) throws IOException, JsonValidationException, ConfigNotFoundException {
    final ConfigRepository configRepository = new ConfigRepository(FileSystemConfigPersistence.createWithValidation(importRoot));
    final StandardSyncOperation standardSyncOperation = assertSyncOperations(configRepository);
    assertStandardSyncs(configRepository, standardSyncOperation);
    assertWorkspace(configRepository);
    assertSources(configRepository);
    assertDestinations(configRepository);
    assertSourceDefinitions(configRepository);
    assertDestinationDefinitions(configRepository);
  }

  private void assertSourceDefinitions(ConfigRepository configRepository) throws JsonValidationException, IOException {
    final Map<String, StandardSourceDefinition> sourceDefinitions = configRepository.listStandardSources()
        .stream()
        .collect(Collectors.toMap(c -> c.getSourceDefinitionId().toString(), c -> c));
    assertTrue(sourceDefinitions.size() >= 59);
    // the definition is not present in latest seeds so it should be deleted
    assertFalse(sourceDefinitions.containsKey(DEPRECATED_SOURCE_DEFINITION_NOT_BEING_USED));
    // the definition is not present in latest seeds but it was being used as a connection so it should
    // not be deleted
    assertTrue(sourceDefinitions.containsKey(DEPRECATED_SOURCE_DEFINITION_BEING_USED));

    final StandardSourceDefinition mysqlDefinition = sourceDefinitions.get("435bb9a5-7887-4809-aa58-28c27df0d7ad");
    assertEquals("0.2.0", mysqlDefinition.getDockerImageTag());
    assertEquals("MySQL", mysqlDefinition.getName());

    final StandardSourceDefinition postgresDefinition = sourceDefinitions.get("decd338e-5647-4c0b-adf4-da0e75f5a750");
    String[] tagBrokenAsArray = postgresDefinition.getDockerImageTag().replace(".", ",").split(",");
    assertEquals(3, tagBrokenAsArray.length);
    assertTrue(Integer.parseInt(tagBrokenAsArray[0]) >= 0);
    assertTrue(Integer.parseInt(tagBrokenAsArray[1]) >= 3);
    assertTrue(Integer.parseInt(tagBrokenAsArray[2]) >= 4);
    assertTrue(postgresDefinition.getName().contains("Postgres"));
  }

  private void assertDestinationDefinitions(ConfigRepository configRepository) throws JsonValidationException, IOException {
    final Map<String, StandardDestinationDefinition> sourceDefinitions = configRepository.listStandardDestinationDefinitions()
        .stream()
        .collect(Collectors.toMap(c -> c.getDestinationDefinitionId().toString(), c -> c));
    assertTrue(sourceDefinitions.size() >= 11);

    final StandardDestinationDefinition postgresDefinition = sourceDefinitions.get("25c5221d-dce2-4163-ade9-739ef790f503");
    assertEquals("0.2.0", postgresDefinition.getDockerImageTag());
    assertEquals(postgresDefinition.getName(), "Postgres");

    final StandardDestinationDefinition localCsvDefinition = sourceDefinitions.get("8be1cf83-fde1-477f-a4ad-318d23c9f3c6");
    assertTrue(localCsvDefinition.getName().contains("Local CSV"));
    assertEquals("0.2.0", localCsvDefinition.getDockerImageTag());

    final StandardDestinationDefinition snowflakeDefinition = sourceDefinitions.get("424892c4-daac-4491-b35d-c6688ba547ba");
    String[] tagBrokenAsArray = snowflakeDefinition.getDockerImageTag().replace(".", ",").split(",");
    assertEquals(3, tagBrokenAsArray.length);
    assertTrue(Integer.parseInt(tagBrokenAsArray[0]) >= 0);
    assertTrue(Integer.parseInt(tagBrokenAsArray[1]) >= 3);
    assertTrue(Integer.parseInt(tagBrokenAsArray[2]) >= 9);
    assertTrue(snowflakeDefinition.getName().contains("Snowflake"));
  }

  private void assertStandardSyncs(ConfigRepository configRepository,
                                   StandardSyncOperation standardSyncOperation)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    final List<StandardSync> standardSyncs = configRepository.listStandardSyncs();
    assertEquals(standardSyncs.size(), 2);
    for (StandardSync standardSync : standardSyncs) {
      if (standardSync.getConnectionId().toString().equals("a294256f-1abe-4837-925f-91602c7207b4")) {
        assertEquals(standardSync.getPrefix(), "");
        assertEquals(standardSync.getSourceId().toString(), "28ffee2b-372a-4f72-9b95-8ed56a8b99c5");
        assertEquals(standardSync.getDestinationId().toString(), "4e00862d-5484-4f50-9860-f3bbb4317397");
        assertEquals(standardSync.getOperationIds().size(), 1);
        assertEquals(standardSync.getOperationIds().get(0).toString(), standardSyncOperation.getOperationId().toString());
        assertEquals(standardSync.getName(), "default");
        assertEquals(standardSync.getStatus().value(), "active");
        assertNull(standardSync.getSchedule());
        assertTrue(standardSync.getManual());
      } else if (standardSync.getConnectionId().toString().equals("49dae3f0-158b-4737-b6e4-0eed77d4b74e")) {
        assertEquals(standardSync.getPrefix(), "");
        assertEquals(standardSync.getSourceId().toString(), "28ffee2b-372a-4f72-9b95-8ed56a8b99c5");
        assertEquals(standardSync.getDestinationId().toString(), "5434615d-a3b7-4351-bc6b-a9a695555a30");
        assertEquals(standardSync.getOperationIds().size(), 0);
        assertEquals(standardSync.getName(), "default");
        assertEquals(standardSync.getStatus().value(), "active");
        assertNull(standardSync.getSchedule());
        assertTrue(standardSync.getManual());
      } else {
        fail("Unknown sync " + standardSync.getConnectionId().toString());
      }
    }
  }

  @NotNull
  private StandardSyncOperation assertSyncOperations(ConfigRepository configRepository) throws IOException, JsonValidationException {
    final List<StandardSyncOperation> standardSyncOperations = configRepository.listStandardSyncOperations();
    assertEquals(standardSyncOperations.size(), 1);
    StandardSyncOperation standardSyncOperation = standardSyncOperations.get(0);
    assertEquals(standardSyncOperation.getName(), "default-normalization");
    assertEquals(standardSyncOperation.getOperatorType(), OperatorType.NORMALIZATION);
    assertEquals(standardSyncOperation.getOperatorNormalization().getOption(), Option.BASIC);
    assertNull(standardSyncOperation.getOperatorDbt());
    assertFalse(standardSyncOperation.getTombstone());
    return standardSyncOperation;
  }

  private void assertSources(ConfigRepository configRepository) throws JsonValidationException, IOException {
    final Map<String, SourceConnection> sources = configRepository.listSourceConnection().stream()
        .collect(Collectors.toMap(sourceConnection -> sourceConnection.getSourceId().toString(), sourceConnection -> sourceConnection));
    assertEquals(sources.size(), 2);
    final SourceConnection mysqlConnection = sources.get("28ffee2b-372a-4f72-9b95-8ed56a8b99c5");
    assertEquals(mysqlConnection.getName(), "MySQL localhost");
    assertEquals(mysqlConnection.getSourceDefinitionId().toString(), "435bb9a5-7887-4809-aa58-28c27df0d7ad");
    assertEquals(mysqlConnection.getWorkspaceId().toString(), "5ae6b09b-fdec-41af-aaf7-7d94cfc33ef6");
    assertEquals(mysqlConnection.getSourceId().toString(), "28ffee2b-372a-4f72-9b95-8ed56a8b99c5");
    assertEquals(mysqlConnection.getConfiguration().get("username").asText(), "root");
    assertEquals(mysqlConnection.getConfiguration().get("password").asText(), "password");
    assertEquals(mysqlConnection.getConfiguration().get("database").asText(), "localhost_test");
    assertEquals(mysqlConnection.getConfiguration().get("port").asInt(), 3306);
    assertEquals(mysqlConnection.getConfiguration().get("host").asText(), "host.docker.internal");
    assertTrue(sources.containsKey("e48cae1a-1f5c-42cc-9ec1-a44ff7fb4969"));

  }

  private void assertWorkspace(ConfigRepository configRepository) throws JsonValidationException, IOException {
    final List<StandardWorkspace> standardWorkspaces = configRepository.listStandardWorkspaces(true);
    assertEquals(standardWorkspaces.size(), 1);
    StandardWorkspace workspace = standardWorkspaces.get(0);
    assertEquals(workspace.getWorkspaceId().toString(), "5ae6b09b-fdec-41af-aaf7-7d94cfc33ef6");
    assertEquals(workspace.getCustomerId().toString(), "17f90b72-5ae4-40b7-bc49-d6c2943aea57");
    assertEquals(workspace.getName(), "default");
    assertEquals(workspace.getSlug(), "default");
    assertEquals(workspace.getInitialSetupComplete(), true);
    assertEquals(workspace.getAnonymousDataCollection(), false);
    assertEquals(workspace.getNews(), false);
    assertEquals(workspace.getSecurityUpdates(), false);
    assertEquals(workspace.getDisplaySetupWizard(), false);
  }

  private void assertDestinations(ConfigRepository configRepository) throws JsonValidationException, IOException {
    final List<DestinationConnection> destinationConnections = configRepository.listDestinationConnection();
    assertEquals(destinationConnections.size(), 2);
    for (DestinationConnection destination : destinationConnections) {
      if (destination.getDestinationId().toString().equals("4e00862d-5484-4f50-9860-f3bbb4317397")) {
        assertEquals(destination.getName(), "Postgres Docker");
        assertEquals(destination.getDestinationDefinitionId().toString(), "25c5221d-dce2-4163-ade9-739ef790f503");
        assertEquals(destination.getWorkspaceId().toString(), "5ae6b09b-fdec-41af-aaf7-7d94cfc33ef6");
        assertEquals(destination.getConfiguration().get("username").asText(), "postgres");
        assertEquals(destination.getConfiguration().get("password").asText(), "password");
        assertEquals(destination.getConfiguration().get("database").asText(), "postgres");
        assertEquals(destination.getConfiguration().get("schema").asText(), "public");
        assertEquals(destination.getConfiguration().get("port").asInt(), 3000);
        assertEquals(destination.getConfiguration().get("host").asText(), "localhost");
        assertNull(destination.getConfiguration().get("basic_normalization"));
      } else if (destination.getDestinationId().toString().equals("5434615d-a3b7-4351-bc6b-a9a695555a30")) {
        assertEquals(destination.getName(), "CSV");
        assertEquals(destination.getDestinationDefinitionId().toString(), "8be1cf83-fde1-477f-a4ad-318d23c9f3c6");
        assertEquals(destination.getWorkspaceId().toString(), "5ae6b09b-fdec-41af-aaf7-7d94cfc33ef6");
        assertEquals(destination.getConfiguration().get("destination_path").asText(), "csv_data");
      } else {
        fail("Unknown destination found with destination id : " + destination.getDestinationId().toString());
      }
    }
  }

  private void runMigration(JobPersistence jobPersistence, Path configRoot) throws IOException {
    try (RunMigration runMigration = new RunMigration(
        jobPersistence,
        new ConfigRepository(FileSystemConfigPersistence.createWithValidation(configRoot)),
        TARGET_VERSION,
        Path.of(System.getProperty("user.dir")).resolve("build/config_init/resources/main/config"))) {
      runMigration.run();
    }
  }

  private JobPersistence getJobPersistence(Database database, File file, String version) throws IOException {
    final DefaultJobPersistence jobPersistence = new DefaultJobPersistence(database);
    final Path tempFolder = Files.createTempDirectory(Path.of("/tmp"), "db_init");
    resourceToBeCleanedUp.add(tempFolder.toFile());

    Archives.extractArchive(file.toPath(), tempFolder);
    final DatabaseArchiver databaseArchiver = new DatabaseArchiver(jobPersistence);
    databaseArchiver.importDatabaseFromArchive(tempFolder, version);
    return jobPersistence;
  }

}
