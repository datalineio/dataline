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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.airbyte.db.Database;
import io.airbyte.db.Databases;
import io.airbyte.scheduler.persistence.DatabaseSchema;
import io.airbyte.scheduler.persistence.DefaultJobPersistence;
import io.airbyte.scheduler.persistence.JobPersistence;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

public class DatabaseArchiverTest {

  private static final String TEMP_PREFIX = "testDatabaseArchive";

  private PostgreSQLContainer<?> container;
  private Database database;
  private DatabaseArchiver databaseArchiver;

  @BeforeEach
  void setUp() throws IOException, InterruptedException {
    container = new PostgreSQLContainer<>("postgres:13-alpine")
        .withDatabaseName("airbyte")
        .withUsername("docker")
        .withPassword("docker");
    container.start();

    container.copyFileToContainer(MountableFile.forClasspathResource("schema.sql"), "/etc/init.sql");
    // execInContainer uses Docker's EXEC so it needs to be split up like this
    container.execInContainer("psql", "-d", "airbyte", "-U", "docker", "-a", "-f", "/etc/init.sql");

    database = Databases.createPostgresDatabase(container.getUsername(), container.getPassword(), container.getJdbcUrl());
    JobPersistence persistence = new DefaultJobPersistence(database);
    databaseArchiver = new DatabaseArchiver(persistence);
  }

  @AfterEach
  void tearDown() throws Exception {
    database.close();
    container.close();
  }

  @Test
  void testUnknownTableExport() throws Exception {
    // Create a table that is not declared in DatabaseSchema
    database.query(ctx -> {
      ctx.fetch("CREATE TABLE id_and_name(id INTEGER, name VARCHAR(200), updated_at DATE);");
      ctx.fetch(
          "INSERT INTO id_and_name (id, name, updated_at) VALUES (1,'picard', '2004-10-19'),  (2, 'crusher', '2005-10-19'), (3, 'vash', '2006-10-19');");
      return null;
    });
    final Path tempFolder = Files.createTempDirectory(TEMP_PREFIX);

    databaseArchiver.exportDatabaseToArchive(tempFolder);

    final Set<String> exportedFiles = Files.walk(tempFolder)
        .map(Path::toString)
        .map(String::toLowerCase)
        .collect(Collectors.toSet());

    assertTrue(exportedFiles.stream().anyMatch(x -> x.contains("jobs")));
    assertFalse(exportedFiles.stream().anyMatch(x -> x.contains("id_and_name")));
  }

  @Test
  void testDatabaseExportImport() throws Exception {
    database.query(ctx -> {
      ctx.fetch(
          "INSERT INTO jobs(id, scope, config_type, config, status, created_at, started_at, updated_at) VALUES "
              + "(1,'get_spec_scope', 'get_spec', '{ \"type\" : \"getSpec\" }', 'succeeded', '2004-10-19', null, '2004-10-19'), "
              + "(2,'sync_scope', 'sync', '{ \"job\" : \"sync\" }', 'running', '2005-10-19', null, '2005-10-19'), "
              + "(3,'sync_scope', 'sync', '{ \"job\" : \"sync\" }', 'pending', '2006-10-19', null, '2006-10-19');");
      return null;
    });
    final Path tempFolder = Files.createTempDirectory(TEMP_PREFIX);
    databaseArchiver.exportDatabaseToArchive(tempFolder);
    databaseArchiver.importDatabaseFromArchive(tempFolder, "test");
    // TODO check database state before/after
  }

  @Test
  void testPartialDatabaseImport() throws Exception {
    final Path tempFolder = Files.createTempDirectory(TEMP_PREFIX);
    databaseArchiver.exportDatabaseToArchive(tempFolder);
    Files.delete(DatabaseArchiver.buildTablePath(tempFolder.toRealPath(), DatabaseSchema.ATTEMPTS.name()));
    assertThrows(RuntimeException.class, () -> databaseArchiver.importDatabaseFromArchive(tempFolder, "test"));
  }

}
