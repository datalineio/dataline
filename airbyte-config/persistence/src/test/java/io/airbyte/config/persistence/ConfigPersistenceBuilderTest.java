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

import static io.airbyte.config.persistence.AirbyteConfigsTable.AIRBYTE_CONFIGS;
import static io.airbyte.config.persistence.AirbyteConfigsTable.AIRBYTE_CONFIGS_TABLE_SCHEMA;
import static io.airbyte.config.persistence.AirbyteConfigsTable.CONFIG_BLOB;
import static io.airbyte.config.persistence.AirbyteConfigsTable.CONFIG_ID;
import static io.airbyte.config.persistence.AirbyteConfigsTable.CONFIG_TYPE;
import static io.airbyte.config.persistence.AirbyteConfigsTable.CREATED_AT;
import static io.airbyte.config.persistence.AirbyteConfigsTable.UPDATED_AT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.resources.MoreResources;
import io.airbyte.config.ConfigSchema;
import io.airbyte.config.Configs;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.db.Database;
import io.airbyte.db.Databases;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.jooq.JSONB;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

class ConfigPersistenceBuilderTest extends BaseTest {

  private static PostgreSQLContainer<?> container;
  private static Configs configs;

  private Database database;

  @BeforeAll
  public static void dbSetup() {
    container = new PostgreSQLContainer<>("postgres:13-alpine")
        .withDatabaseName("airbyte")
        .withUsername("docker")
        .withPassword("docker");
    container.start();

    configs = mock(Configs.class);
    when(configs.getConfigDatabaseUser()).thenReturn(container.getUsername());
    when(configs.getConfigDatabasePassword()).thenReturn(container.getPassword());
    when(configs.getConfigDatabaseUrl()).thenReturn(container.getJdbcUrl());
  }

  @AfterAll
  public static void dbDown() {
    container.close();
  }

  @BeforeEach
  public void setup() throws Exception {
    database = Databases.createPostgresDatabase(container.getUsername(), container.getPassword(), container.getJdbcUrl());
    database.transaction(ctx -> ctx.execute("DROP TABLE IF EXISTS airbyte_configs"));
  }

  @AfterEach
  void tearDown() throws Exception {
    database.close();
  }

  @Test
  public void testCreateDbPersistenceWithYamlSeed() throws IOException {
    ConfigPersistence dbPersistence = new ConfigPersistenceBuilder(configs, true).getDbPersistenceWithYamlSeed();
    ConfigPersistence seedPersistence = new YamlSeedConfigPersistence();
    assertSameConfigDump(seedPersistence.dumpConfigs(), dbPersistence.dumpConfigs());
  }

  @Test
  public void testCreateDbPersistenceWithFileSeed() throws Exception {
    Path testRoot = Path.of("/tmp/cpf_test_file_seed");
    Path rootPath = Files.createTempDirectory(Files.createDirectories(testRoot), ConfigPersistenceBuilderTest.class.getName());
    ConfigPersistence seedPersistence = new FileSystemConfigPersistence(rootPath);
    writeSource(seedPersistence, SOURCE_GITHUB);
    writeDestination(seedPersistence, DESTINATION_S3);

    when(configs.getConfigRoot()).thenReturn(rootPath);

    ConfigPersistence dbPersistence = new ConfigPersistenceBuilder(configs, true).getDbPersistenceWithFileSeed();
    int dbConfigSize = (int) dbPersistence.dumpConfigs().values().stream()
        .map(stream -> stream.collect(Collectors.toList()))
        .mapToLong(Collection::size)
        .sum();
    assertEquals(2, dbConfigSize);
    assertSameConfigDump(seedPersistence.dumpConfigs(), dbPersistence.dumpConfigs());
  }

  @Test
  public void testCreateDbPersistenceWithoutSetupDatabase() throws Exception {
    // Initialize the database with one config.
    String schema = MoreResources.readResource(AIRBYTE_CONFIGS_TABLE_SCHEMA);
    Timestamp timestamp = Timestamp.from(Instant.ofEpochMilli(System.currentTimeMillis()));
    database.transaction(ctx -> {
      ctx.execute(schema);
      ctx.insertInto(AIRBYTE_CONFIGS)
          .set(CONFIG_ID, SOURCE_GITHUB.getSourceDefinitionId().toString())
          .set(CONFIG_TYPE, ConfigSchema.STANDARD_SOURCE_DEFINITION.name())
          .set(CONFIG_BLOB, JSONB.valueOf(Jsons.serialize(SOURCE_GITHUB)))
          .set(CREATED_AT, timestamp)
          .set(UPDATED_AT, timestamp)
          .execute();
      return null;
    });

    ConfigPersistence seedPersistence = spy(new YamlSeedConfigPersistence());
    // When setupDatabase is false, the createDbPersistence method does not initialize
    // the database itself, but it expects that the database has already been initialized.
    ConfigPersistence dbPersistence = new ConfigPersistenceBuilder(configs, false).getDbPersistence(seedPersistence);
    // The return persistence is not initialized by the seed persistence, and has only one config.
    verify(seedPersistence, never()).dumpConfigs();
    assertSameConfigDump(
        Map.of(ConfigSchema.STANDARD_SOURCE_DEFINITION.name(), Stream.of(Jsons.jsonNode(SOURCE_GITHUB))),
        dbPersistence.dumpConfigs());
  }

  @Test
  public void testCreateFileSystemConfigPersistence() throws Exception {
    Path testRoot = Path.of("/tmp/cpf_test_file_system");
    Path rootPath = Files.createTempDirectory(Files.createDirectories(testRoot), ConfigPersistenceBuilderTest.class.getName());
    ConfigPersistence seedPersistence = new FileSystemConfigPersistence(rootPath);
    writeSource(seedPersistence, SOURCE_GITHUB);
    writeDestination(seedPersistence, DESTINATION_S3);

    when(configs.getConfigRoot()).thenReturn(rootPath);

    ConfigPersistence filePersistence = new ConfigPersistenceBuilder(configs, false).getFileSystemPersistence();
    assertSameConfigDump(seedPersistence.dumpConfigs(), filePersistence.dumpConfigs());
  }

  /**
   * This test mimics the file -> db config persistence migration process.
   */
  @Test
  public void testMigrateFromFileToDbPersistence() throws Exception {
    Map<ConfigSchema, Stream<Object>> seedConfigs = Map.of(
        ConfigSchema.STANDARD_WORKSPACE, Stream.of(DEFAULT_WORKSPACE),
        ConfigSchema.STANDARD_SOURCE_DEFINITION, Stream.of(SOURCE_GITHUB, SOURCE_POSTGRES),
        ConfigSchema.STANDARD_DESTINATION_DEFINITION, Stream.of(DESTINATION_S3));
    StandardWorkspace extraWorkspace = new StandardWorkspace()
        .withWorkspaceId(UUID.randomUUID())
        .withName("extra")
        .withSlug("extra")
        .withEmail("mary@airbyte.io")
        .withInitialSetupComplete(true);

    // first run uses file system config persistence, and adds an extra workspace
    Path testRoot = Path.of("/tmp/cpf_test_migration");
    Path rootPath = Files.createTempDirectory(Files.createDirectories(testRoot), ConfigPersistenceBuilderTest.class.getName());
    when(configs.getConfigRoot()).thenReturn(rootPath);

    ConfigPersistence filePersistence = new ConfigPersistenceBuilder(configs, false).getFileSystemPersistence();

    filePersistence.replaceAllConfigs(seedConfigs, false);
    filePersistence.writeConfig(ConfigSchema.STANDARD_WORKSPACE, extraWorkspace.getWorkspaceId().toString(), extraWorkspace);

    // second run uses database config persistence;
    // the only difference is that useConfigDatabase is no longer overridden to false;
    // the extra workspace should be ported to this persistence
    ConfigPersistence dbPersistence = new ConfigPersistenceBuilder(configs, true).create();
    Map<String, Stream<JsonNode>> expected = Map.of(
        ConfigSchema.STANDARD_WORKSPACE.name(), Stream.of(Jsons.jsonNode(DEFAULT_WORKSPACE), Jsons.jsonNode(extraWorkspace)),
        ConfigSchema.STANDARD_SOURCE_DEFINITION.name(), Stream.of(Jsons.jsonNode(SOURCE_GITHUB), Jsons.jsonNode(SOURCE_POSTGRES)),
        ConfigSchema.STANDARD_DESTINATION_DEFINITION.name(), Stream.of(Jsons.jsonNode(DESTINATION_S3)));
    assertSameConfigDump(expected, dbPersistence.dumpConfigs());
  }

}
