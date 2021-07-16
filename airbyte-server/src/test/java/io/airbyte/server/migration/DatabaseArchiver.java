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

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.io.IOs;
import io.airbyte.commons.lang.CloseableConsumer;
import io.airbyte.commons.lang.Exceptions;
import io.airbyte.commons.stream.MoreStreams;
import io.airbyte.commons.version.AirbyteVersion;
import io.airbyte.commons.yaml.Yamls;
import io.airbyte.scheduler.persistence.DatabaseSchema;
import io.airbyte.scheduler.persistence.JobPersistence;
import io.airbyte.validation.json.JsonSchemaValidator;
import io.airbyte.validation.json.JsonValidationException;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// todo (cgardens) - kill this class. only kept around because it is hard to extricate from the
// {@link RunMigrationTest} test.
public class DatabaseArchiver {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseArchiver.class);
  private static final String DB_FOLDER_NAME = "airbyte_db";

  private final JobPersistence persistence;
  private final JsonSchemaValidator jsonSchemaValidator;

  public DatabaseArchiver(final JobPersistence persistence, final JsonSchemaValidator jsonSchemaValidator) {
    this.persistence = persistence;
    this.jsonSchemaValidator = jsonSchemaValidator;
  }

  public DatabaseArchiver(final JobPersistence persistence) {
    this(persistence, new JsonSchemaValidator());
  }

  /**
   * Serializes each internal Airbyte Database table into a single archive file stored in YAML.
   */
  public void exportDatabaseToArchive(final Path storageRoot) throws Exception {
    final Map<DatabaseSchema, Stream<JsonNode>> tables = persistence.exportDatabase();
    Files.createDirectories(storageRoot.resolve(DB_FOLDER_NAME));
    for (final DatabaseSchema tableSchema : DatabaseSchema.values()) {
      final Path tablePath = buildTablePath(storageRoot, tableSchema.name());
      if (tables.containsKey(tableSchema)) {
        writeTableToArchive(tableSchema, tablePath, tables.get(tableSchema));
      } else {
        // Create empty file
        Files.createFile(tablePath);
      }
    }
    LOGGER.debug("Successful export of airbyte database");
  }

  private void writeTableToArchive(final DatabaseSchema tableSchema, final Path tablePath, final Stream<JsonNode> tableStream) throws Exception {
    Files.createDirectories(tablePath.getParent());
    final BufferedWriter recordOutputWriter = new BufferedWriter(new FileWriter(tablePath.toFile()));
    final CloseableConsumer<JsonNode> recordConsumer = Yamls.listWriter(recordOutputWriter);
    tableStream.forEach(row -> Exceptions.toRuntime(() -> {
      jsonSchemaValidator.ensure(tableSchema.toJsonNode(), row);
      recordConsumer.accept(row);
    }));
    recordConsumer.close();
    LOGGER.debug(String.format("Successful export of airbyte table %s", tableSchema.name()));
  }

  protected static Path buildTablePath(final Path storageRoot, final String tableName) {
    return storageRoot
        .resolve(DB_FOLDER_NAME)
        .resolve(String.format("%s.yaml", tableName.toUpperCase()));
  }

  public void checkVersion(final String airbyteVersion) throws IOException {
    final Optional<String> airbyteDatabaseVersion = persistence.getVersion();
    airbyteDatabaseVersion.ifPresent(dbversion -> AirbyteVersion.assertIsCompatible(airbyteVersion, dbversion));
  }

  /**
   * Reads a YAML configuration archive file and deserialize table into the Airbyte Database. The
   * objects will be validated against the current version of Airbyte server's JSON Schema.
   */
  public void importDatabaseFromArchive(final Path storageRoot, final String airbyteVersion) throws IOException {
    final Map<DatabaseSchema, Stream<JsonNode>> data = new HashMap<>();
    for (DatabaseSchema tableType : DatabaseSchema.values()) {
      final Path tablePath = buildTablePath(storageRoot, tableType.name());
      data.put(tableType, readTableFromArchive(tableType, tablePath));
    }
    persistence.importDatabase(airbyteVersion, data);
    LOGGER.debug("Successful upgrade of airbyte database from archive");
  }

  private Stream<JsonNode> readTableFromArchive(final DatabaseSchema tableSchema, final Path tablePath) throws FileNotFoundException {
    final JsonNode schema = tableSchema.toJsonNode();
    if (schema != null) {
      return MoreStreams.toStream(Yamls.deserialize(IOs.readFile(tablePath)).elements())
          .peek(r -> {
            try {
              jsonSchemaValidator.ensure(schema, r);
            } catch (JsonValidationException e) {
              throw new IllegalArgumentException("Archived Data Schema does not match current Airbyte Data Schemas", e);
            }
          });
    } else {
      throw new FileNotFoundException(String.format("Airbyte Database table %s was not found in the archive", tableSchema.name()));
    }
  }

}
