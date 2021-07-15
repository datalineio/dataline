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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import io.airbyte.commons.json.Jsons;
import io.airbyte.db.Database;
import io.airbyte.db.Databases;
import io.airbyte.db.jdbc.JdbcUtils;
import io.airbyte.protocol.models.AirbyteCatalog;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.apache.commons.lang3.RandomStringUtils;
import org.jooq.SQLDialect;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.MySQLContainer;

class MySqlTest {

  private static final String TEST_USER = "test";
  private static final String TEST_PASSWORD = "test";
  private static MySQLContainer<?> container;

  private static String TABLE_NAME = "id_and_name";

  private JsonNode config;
  private Database database;

  @BeforeAll
  static void init() throws SQLException {
    container = new MySQLContainer<>("mysql:8.0")
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
        .put("database", "db_" + RandomStringUtils.randomAlphabetic(10))
        .put("username", TEST_USER)
        .put("password", TEST_PASSWORD)
        .build());

    final Database masterDatabase = Databases.createDatabase(
        config.get("username").asText(),
        config.get("password").asText(),
        String.format("jdbc:mysql://%s:%s",
            config.get("host").asText(),
            config.get("port").asText()),
        MySqlSource.DRIVER_CLASS,
        SQLDialect.MYSQL);

    masterDatabase.query(ctx -> {
      ctx.fetch("CREATE DATABASE " + config.get("database").asText());
      return null;
    });

    masterDatabase.close();

    database = Databases.createDatabase(
        config.get("username").asText(),
        config.get("password").asText(),
        String.format("jdbc:mysql://%s:%s/%s",
            config.get("host").asText(),
            config.get("port").asText(),
            config.get("database").asText()),
        MySqlSource.DRIVER_CLASS,
        SQLDialect.MYSQL);
  }

  @AfterEach
  void tearDownMySql() throws Exception {
    database.close();
  }

  @AfterAll
  static void cleanUp() {
    container.close();
  }

  @ParameterizedTest
  @ValueSource(strings = {
    "TINYINT",
    "SMALLINT",
    "MEDIUMINT",
    "INT",
    "BIGINT",
    "INT(1)",
    "INT(2)",
    "INT(3)",
    "INT(4)",
    "INT(5)",
    "INT(6)",
    "INT(7)",
    "INT(8)"
  })
  void testSmallIntTypes(String type) throws Exception {
    database.query(ctx -> {
      ctx.fetch(String.format("CREATE TABLE %s(id %s)", JdbcUtils.getFullyQualifiedTableName(null, TABLE_NAME), type));
      ctx.fetch(String.format("INSERT INTO %s(id) VALUES (10)", JdbcUtils.getFullyQualifiedTableName(null, TABLE_NAME)));
      return null;
    });

    final AirbyteCatalog catalog = new MySqlSource().discover(config);
    assertEquals("number", catalog.getStreams().get(0).getJsonSchema().get("properties").get("id").get("type").asText());
  }

}
