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

package io.airbyte.integrations.source.mysql;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.string.Strings;
import io.airbyte.db.Database;
import io.airbyte.db.Databases;
import io.airbyte.integrations.source.jdbc.AbstractJdbcSource;
import io.airbyte.integrations.source.jdbc.test.JdbcStressTest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Optional;
import org.jooq.SQLDialect;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.testcontainers.containers.MariaDBContainer;

@Disabled
class MySqlStressTest extends JdbcStressTest {

  private static final String TEST_USER = "test";
  private static final String TEST_PASSWORD = "test";
  private static MariaDBContainer<?> container;

  private JsonNode config;
  private Database database;

  @BeforeAll
  static void init() throws SQLException {
    container = new MariaDBContainer<>("mariadb:10.6.1")
        .withUsername(TEST_USER)
        .withPassword(TEST_PASSWORD)
        .withEnv("MYSQL_ROOT_HOST", "%")
        .withEnv("MYSQL_ROOT_PASSWORD", TEST_PASSWORD);
    container.start();
    Connection connection = DriverManager.getConnection(container.getJdbcUrl(), "root", TEST_PASSWORD);
    connection.createStatement().execute("GRANT ALL PRIVILEGES ON *.* TO '" + TEST_USER + "'@'%';\n");
  }

  @BeforeEach
  public void setup() throws Exception {
    config = Jsons.jsonNode(ImmutableMap.builder()
        .put("host", container.getHost())
        .put("port", container.getFirstMappedPort())
        .put("database", Strings.addRandomSuffix("db", "_", 10))
        .put("username", TEST_USER)
        .put("password", TEST_PASSWORD)
        .build());

    database = Databases.createDatabase(
        config.get("username").asText(),
        config.get("password").asText(),
        String.format("jdbc:mariadb://%s:%s",
            config.get("host").asText(),
            config.get("port").asText()),
        MySqlSource.TEST_DRIVER_CLASS,
        SQLDialect.MARIADB);

    database.query(ctx -> {
      ctx.fetch("CREATE DATABASE " + config.get("database").asText());
      return null;
    });
    database.close();

    super.setup();
  }

  @AfterEach
  void tearDown() throws Exception {
    database.close();
  }

  @AfterAll
  static void cleanUp() {
    container.close();
  }

  // MySql does not support schemas in the way most dbs do. Instead we namespace by db name.
  @Override
  public Optional<String> getDefaultSchemaName() {
    return Optional.of(config.get("database").asText());
  }

  @Override
  public AbstractJdbcSource getSource() {
    return new MySqlSource();
  }

  @Override
  public String getDriverClass() {
    return MySqlSource.TEST_DRIVER_CLASS;
  }

  @Override
  public JsonNode getConfig() {
    return Jsons.clone(config);
  }

}
