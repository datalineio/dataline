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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import io.airbyte.commons.json.Jsons;
import io.airbyte.db.Databases;
import io.airbyte.integrations.base.JavaBaseConstants;
import io.airbyte.integrations.destination.ExtendedNameTransformer;
import io.airbyte.integrations.standardtest.destination.DestinationAcceptanceTest;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import org.jooq.JSONFormat;
import org.jooq.JSONFormat.RecordFormat;
import org.jooq.SQLDialect;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MariaDBContainer;

public class MySQLDestinationAcceptanceTest extends DestinationAcceptanceTest {

  private static final JSONFormat JSON_FORMAT = new JSONFormat().recordFormat(RecordFormat.OBJECT);

  private MariaDBContainer<?> db;
  private ExtendedNameTransformer namingResolver = new MySQLNameTransformer();

  @Override
  protected String getImageName() {
    return "airbyte/destination-mysql:dev";
  }

  @Override
  protected boolean supportsDBT() {
    return true;
  }

  @Override
  protected boolean implementsNamespaces() {
    return true;
  }

  @Override
  protected JsonNode getConfig() {
    return Jsons.jsonNode(ImmutableMap.builder()
        .put("host", db.getHost())
        .put("username", db.getUsername())
        .put("password", db.getPassword())
        .put("database", db.getDatabaseName())
        .put("port", db.getFirstMappedPort())
        .build());
  }

  @Override
  protected JsonNode getFailCheckConfig() {
    return Jsons.jsonNode(ImmutableMap.builder()
        .put("host", db.getHost())
        .put("username", db.getUsername())
        .put("password", "wrong password")
        .put("database", db.getDatabaseName())
        .put("port", db.getFirstMappedPort())
        .build());
  }

  @Override
  protected String getDefaultSchema(JsonNode config) {
    if (config.get("database") == null) {
      return null;
    }
    return config.get("database").asText();
  }

  @Override
  protected List<JsonNode> retrieveRecords(TestDestinationEnv testEnv,
                                           String streamName,
                                           String namespace,
                                           JsonNode streamSchema)
      throws Exception {
    return retrieveRecordsFromTable(namingResolver.getRawTableName(streamName), namespace)
        .stream()
        .map(r -> Jsons.deserialize(r.get(JavaBaseConstants.COLUMN_NAME_DATA).asText()))
        .collect(Collectors.toList());
  }

  private List<JsonNode> retrieveRecordsFromTable(String tableName, String schemaName) throws SQLException {
    return Databases.createDatabase(
        db.getUsername(),
        db.getPassword(),
        String.format("jdbc:mysql://%s:%s/%s",
            db.getHost(),
            db.getFirstMappedPort(),
            db.getDatabaseName()),
        MySQLDestination.TEST_DRIVER_CLASS,
        SQLDialect.MYSQL).query(
            ctx -> ctx
                .fetch(String.format("SELECT * FROM %s.%s ORDER BY %s ASC;", schemaName, tableName,
                    JavaBaseConstants.COLUMN_NAME_EMITTED_AT))
                .stream()
                .map(r -> r.formatJSON(JSON_FORMAT))
                .map(Jsons::deserialize)
                .collect(Collectors.toList()));
  }

  @Override
  protected void setup(TestDestinationEnv testEnv) {
    db = new MariaDBContainer<>("mariadb:10.6.1");
    db.start();
    setLocalInFileToTrue();
    revokeAllPermissions();
    grantCorrectPermissions();
  }

  private void setLocalInFileToTrue() {
    executeQuery("set global local_infile=true");
  }

  private void revokeAllPermissions() {
    executeQuery("REVOKE ALL PRIVILEGES, GRANT OPTION FROM " + db.getUsername() + "@'%';");
  }

  private void grantCorrectPermissions() {
    executeQuery("GRANT CREATE, INSERT, SELECT, DROP ON *.* TO " + db.getUsername() + "@'%';");
  }

  private void executeQuery(String query) {
    try {
      Databases.createDatabase(
          "root",
          "test",
          String.format("jdbc:mysql://%s:%s/%s",
              db.getHost(),
              db.getFirstMappedPort(),
              db.getDatabaseName()),
          MySQLDestination.DRIVER_CLASS,
          SQLDialect.MYSQL).query(
              ctx -> ctx
                  .execute(query));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void tearDown(TestDestinationEnv testEnv) {
    db.stop();
    db.close();
  }

  @Override
  @Test
  public void testLineBreakCharacters() {
    // overrides test with a no-op until we handle full UTF-8 in the destination
  }

}
