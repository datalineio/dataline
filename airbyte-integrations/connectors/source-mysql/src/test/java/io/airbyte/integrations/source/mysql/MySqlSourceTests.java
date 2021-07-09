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
import io.airbyte.commons.string.Strings;
import io.airbyte.db.Database;
import io.airbyte.protocol.models.AirbyteConnectionStatus;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MariaDBContainer;

public class MySqlSourceTests {

  private static final String TEST_USER = "test";
  private static final String TEST_PASSWORD = "test";
  private static MariaDBContainer<?> container;

  private JsonNode config;
  private Database database;

  @Test
  public void testSettingTimezones() throws Exception {
    // start DB
    container = new MariaDBContainer<>("mariadb:10.6.1")
        .withUsername(TEST_USER)
        .withPassword(TEST_PASSWORD)
        .withEnv("MYSQL_ROOT_HOST", "%")
        .withEnv("MYSQL_ROOT_PASSWORD", TEST_PASSWORD)
        .withEnv("TZ", "Europe/Moscow");
    container.start();
    Properties properties = new Properties();
    properties.putAll(ImmutableMap.of("user", "root", "password", TEST_PASSWORD, "serverTimezone", "Europe/Moscow"));
    DriverManager.getConnection(container.getJdbcUrl(), properties);
    final String dbName = Strings.addRandomSuffix("db", "_", 10);
    config = getConfig(container, dbName, "serverTimezone=Europe/Moscow");

    try (Connection connection = DriverManager.getConnection(container.getJdbcUrl(), properties)) {
      connection.createStatement().execute("GRANT ALL PRIVILEGES ON *.* TO '" + TEST_USER + "'@'%';\n");
      connection.createStatement().execute("CREATE DATABASE " + config.get("database").asText());
    }
    AirbyteConnectionStatus check = new MySqlSource().check(config);
    assertEquals(AirbyteConnectionStatus.Status.SUCCEEDED, check.getStatus());

    // cleanup
    container.close();
  }

  private static JsonNode getConfig(MariaDBContainer<?> dbContainer, String dbName, String jdbcParams) {
    return Jsons.jsonNode(ImmutableMap.builder()
        .put("host", dbContainer.getHost())
        .put("port", dbContainer.getFirstMappedPort())
        .put("database", dbName)
        .put("username", TEST_USER)
        .put("password", TEST_PASSWORD)
        .put("jdbc_url_params", jdbcParams)
        .build());
  }

}
