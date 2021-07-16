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

package io.airbyte.integrations.destination.mysql;

import io.airbyte.db.jdbc.JdbcDatabase;
import io.airbyte.integrations.base.JavaBaseConstants;
import io.airbyte.integrations.destination.jdbc.DefaultSqlOperations;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;

public class MySQLSqlOperations extends DefaultSqlOperations {

  private boolean isLocalFileEnabled = false;

  @Override
  public void executeTransaction(JdbcDatabase database, List<String> queries) throws Exception {
    database.executeWithinTransaction(queries);
  }

  @Override
  public void insertRecords(JdbcDatabase database,
                            List<AirbyteRecordMessage> records,
                            String schemaName,
                            String tmpTableName)
      throws SQLException {
    if (records.isEmpty()) {
      return;
    }

    verifyLocalFileEnabled(database);
    try {
      File tmpFile = Files.createTempFile(tmpTableName + "-", ".tmp").toFile();

      loadDataIntoTable(database, records, schemaName, tmpTableName, tmpFile);

      Files.delete(tmpFile.toPath());
    } catch (IOException e) {
      throw new SQLException(e);
    }
  }

  private void loadDataIntoTable(JdbcDatabase database,
                                 List<AirbyteRecordMessage> records,
                                 String schemaName,
                                 String tmpTableName,
                                 File tmpFile)
      throws SQLException {
    database.execute(connection -> {
      try {
        writeBatchToFile(tmpFile, records);

        String absoluteFile = "'" + tmpFile.getAbsolutePath() + "'";

        String query = String.format(
            "LOAD DATA LOCAL INFILE %s INTO TABLE %s.%s FIELDS TERMINATED BY ',' ENCLOSED BY '\"' ESCAPED BY '\\\"' LINES TERMINATED BY '\\r\\n'",
            absoluteFile, schemaName, tmpTableName);

        try (Statement stmt = connection.createStatement()) {
          stmt.execute(query);
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  void verifyLocalFileEnabled(JdbcDatabase database) throws SQLException {
    boolean localFileEnabled = isLocalFileEnabled || checkIfLocalFileIsEnabled(database);
    if (!localFileEnabled) {
      tryEnableLocalFile(database);
    }
    isLocalFileEnabled = true;
  }

  private void tryEnableLocalFile(JdbcDatabase database) throws SQLException {
    database.execute(connection -> {
      try (Statement statement = connection.createStatement()) {
        statement.execute("set global local_infile=true");
      } catch (Exception e) {
        throw new RuntimeException(
            "The DB user provided to airbyte was unable to switch on the local_infile attribute on the MySQL server. As an admin user, you will need to run \"SET GLOBAL local_infile = true\" before syncing data with Airbyte.",
            e);
      }
    });
  }

  private double getVersion(JdbcDatabase database) throws SQLException {
    List<String> value = database.resultSetQuery(connection -> connection.createStatement().executeQuery("select version()"),
        resultSet -> resultSet.getString("version()")).collect(Collectors.toList());
    return Double.parseDouble(value.get(0).substring(0, 3));
  }

  VersionCompatibility isCompatibleVersion(JdbcDatabase database) throws SQLException {
    double version = getVersion(database);
    return new VersionCompatibility(version, version >= 5.7);
  }

  @Override
  public boolean isSchemaRequired() {
    return false;
  }

  private boolean checkIfLocalFileIsEnabled(JdbcDatabase database) throws SQLException {
    List<String> value = database.resultSetQuery(connection -> connection.createStatement().executeQuery("SHOW GLOBAL VARIABLES LIKE 'local_infile'"),
        resultSet -> resultSet.getString("Value")).collect(Collectors.toList());

    return value.get(0).equalsIgnoreCase("on");
  }

  @Override
  public String createTableQuery(JdbcDatabase database, String schemaName, String tableName) {
    // MySQL requires byte information with VARCHAR. Since we are using uuid as value for the column,
    // 256 is enough
    return String.format(
        "CREATE TABLE IF NOT EXISTS %s.%s ( \n"
            + "%s VARCHAR(256) PRIMARY KEY,\n"
            + "%s JSON,\n"
            + "%s TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6)\n"
            + ");\n",
        schemaName, tableName, JavaBaseConstants.COLUMN_NAME_AB_ID, JavaBaseConstants.COLUMN_NAME_DATA, JavaBaseConstants.COLUMN_NAME_EMITTED_AT);
  }

  public static class VersionCompatibility {

    private final double version;
    private final boolean isCompatible;

    public VersionCompatibility(double version, boolean isCompatible) {
      this.version = version;
      this.isCompatible = isCompatible;
    }

    public double getVersion() {
      return version;
    }

    public boolean isCompatible() {
      return isCompatible;
    }

  }

}
