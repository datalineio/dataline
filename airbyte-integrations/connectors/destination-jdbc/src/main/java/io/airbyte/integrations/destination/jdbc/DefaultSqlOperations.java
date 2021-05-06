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

package io.airbyte.integrations.destination.jdbc;

import io.airbyte.commons.json.Jsons;
import io.airbyte.db.jdbc.JdbcDatabase;
import io.airbyte.integrations.base.JavaBaseConstants;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultSqlOperations implements SqlOperations {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultSqlOperations.class);

  @Override
  public void createSchemaIfNotExists(JdbcDatabase database, String schemaName) throws Exception {
    database.execute(createSchemaQuery(schemaName));
  }

  private String createSchemaQuery(String schemaName) {
    return String.format("CREATE SCHEMA IF NOT EXISTS %s;\n", schemaName);
  }

  @Override
  public void createTableIfNotExists(JdbcDatabase database, String schemaName, String tableName) throws SQLException {
    database.execute(createTableQuery(schemaName, tableName));
  }

  @Override
  public String createTableQuery(String schemaName, String tableName) {
    return String.format(
        "CREATE TABLE IF NOT EXISTS %s.%s ( \n"
            + "%s VARCHAR PRIMARY KEY,\n"
            + "%s JSONB,\n"
            + "%s TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP\n"
            + ");\n",
        schemaName, tableName, JavaBaseConstants.COLUMN_NAME_AB_ID, JavaBaseConstants.COLUMN_NAME_DATA, JavaBaseConstants.COLUMN_NAME_EMITTED_AT);
  }

  @Override
  public void insertRecords(JdbcDatabase database, List<AirbyteRecordMessage> records, String schemaName, String tmpTableName) throws SQLException {
    if (records.isEmpty()) {
      return;
    }

    // todo (cgardens) - move this into a postgres version of this. this syntax is postgres-specific
    database.execute(connection -> {
      try {
        final Reader reader = writeBatchToFile(records);

        var copyManager = new CopyManager(connection.unwrap(BaseConnection.class));
        var sql = String.format("COPY %s.%s FROM stdin DELIMITER ',' CSV", schemaName, tmpTableName);
        copyManager.copyIn(sql, reader);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  private Reader writeBatchToFile(List<AirbyteRecordMessage> records) throws Exception {
    ByteArrayOutputStream byteArrayOutputStream = null;
    try {
      byteArrayOutputStream = new ByteArrayOutputStream();
      final OutputStreamWriter outputStreamWriter = new OutputStreamWriter(byteArrayOutputStream);
      final CSVPrinter csvPrinter = new CSVPrinter(outputStreamWriter, CSVFormat.DEFAULT);

      for (AirbyteRecordMessage record : records) {
        var uuid = UUID.randomUUID().toString();
        var jsonData = Jsons.serialize(record.getData());
        var emittedAt = Timestamp.from(Instant.ofEpochMilli(record.getEmittedAt()));
        csvPrinter.printRecord(uuid, jsonData, emittedAt);
      }
    } finally {
      if (byteArrayOutputStream != null) {
        byteArrayOutputStream.close();
      }
    }
    return new StringReader(byteArrayOutputStream.toString(StandardCharsets.UTF_8));
  }

  @Override
  public String truncateTableQuery(String schemaName, String tableName) {
    return String.format("TRUNCATE TABLE %s.%s;\n", schemaName, tableName);
  }

  @Override
  public String copyTableQuery(String schemaName, String srcTableName, String dstTableName) {
    return String.format("INSERT INTO %s.%s SELECT * FROM %s.%s;\n", schemaName, dstTableName, schemaName, srcTableName);
  }

  @Override
  public void executeTransaction(JdbcDatabase database, String queries) throws Exception {
    database.execute("BEGIN;\n" + queries + "COMMIT;");
  }

  @Override
  public void dropTableIfExists(JdbcDatabase database, String schemaName, String tableName) throws SQLException {
    database.execute(dropTableIfExistsQuery(schemaName, tableName));
  }

  private String dropTableIfExistsQuery(String schemaName, String tableName) {
    return String.format("DROP TABLE IF EXISTS %s.%s;\n", schemaName, tableName);
  }

  @Override
  public boolean isValidData(String data) {
    return true;
  }

}
