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

package io.airbyte.db;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.airbyte.commons.functional.CheckedConsumer;
import io.airbyte.commons.functional.CheckedFunction;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.resources.MoreResources;
import io.airbyte.db.jdbc.DefaultJdbcDatabase;
import io.airbyte.db.jdbc.JdbcDatabase;
import io.airbyte.db.jdbc.JdbcStreamingQueryConfiguration;
import io.airbyte.db.jdbc.JdbcUtils;
import io.airbyte.db.jdbc.StreamingJdbcDatabase;
import io.airbyte.test.utils.PostgreSQLContainerHelper;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

public class TestStreamingJdbcDatabase {

  private static final List<JsonNode> RECORDS_AS_JSON = Lists.newArrayList(
      Jsons.jsonNode(ImmutableMap.of("id", 1, "name", "picard")),
      Jsons.jsonNode(ImmutableMap.of("id", 2, "name", "crusher")),
      Jsons.jsonNode(ImmutableMap.of("id", 3, "name", "vash")));

  private static PostgreSQLContainer<?> PSQL_DB;

  private JdbcStreamingQueryConfiguration jdbcStreamingQueryConfiguration;
  private JdbcDatabase defaultJdbcDatabase;
  private JdbcDatabase streamingJdbcDatabase;

  @BeforeAll
  static void init() {
    PSQL_DB = new PostgreSQLContainer<>("postgres:13-alpine");
    PSQL_DB.start();

  }

  @BeforeEach
  void setup() throws Exception {
    jdbcStreamingQueryConfiguration = mock(JdbcStreamingQueryConfiguration.class);

    final String dbName = "db_" + RandomStringUtils.randomAlphabetic(10).toLowerCase();

    final JsonNode config = getConfig(PSQL_DB, dbName);

    final String initScriptName = "init_" + dbName.concat(".sql");
    MoreResources.writeResource(initScriptName, "CREATE DATABASE " + dbName + ";");
    PostgreSQLContainerHelper.runSqlScript(MountableFile.forClasspathResource(initScriptName), PSQL_DB);

    final BasicDataSource connectionPool = new BasicDataSource();
    connectionPool.setDriverClassName("org.postgresql.Driver");
    connectionPool.setUsername(config.get("username").asText());
    connectionPool.setPassword(config.get("password").asText());
    connectionPool.setUrl(String.format("jdbc:postgresql://%s:%s/%s",
        config.get("host").asText(),
        config.get("port").asText(),
        config.get("database").asText()));

    defaultJdbcDatabase = spy(new DefaultJdbcDatabase(connectionPool));
    streamingJdbcDatabase = new StreamingJdbcDatabase(connectionPool, defaultJdbcDatabase, jdbcStreamingQueryConfiguration);

    defaultJdbcDatabase.execute(connection -> {
      connection.createStatement().execute("CREATE TABLE id_and_name(id INTEGER, name VARCHAR(200));");
      connection.createStatement().execute("INSERT INTO id_and_name (id, name) VALUES (1,'picard'),  (2, 'crusher'), (3, 'vash');");
    });
  }

  @AfterAll
  static void cleanUp() {
    PSQL_DB.close();
  }

  @SuppressWarnings("unchecked")
  @Test
  void testExecute() throws SQLException {
    CheckedConsumer<Connection, SQLException> queryExecutor = mock(CheckedConsumer.class);
    doNothing().when(defaultJdbcDatabase).execute(queryExecutor);

    streamingJdbcDatabase.execute(queryExecutor);

    verify(defaultJdbcDatabase).execute(queryExecutor);
  }

  @SuppressWarnings("unchecked")
  @Test
  void testBufferedResultQuery() throws SQLException {
    final CheckedFunction<Connection, ResultSet, SQLException> query = mock(CheckedFunction.class);
    final CheckedFunction<ResultSet, JsonNode, SQLException> recordTransform = mock(CheckedFunction.class);
    doReturn(RECORDS_AS_JSON).when(defaultJdbcDatabase).bufferedResultSetQuery(query, recordTransform);

    final List<JsonNode> actual = streamingJdbcDatabase.bufferedResultSetQuery(
        connection -> connection.createStatement().executeQuery("SELECT * FROM id_and_name;"),
        JdbcUtils::rowToJson);

    assertEquals(RECORDS_AS_JSON, actual);
  }

  @SuppressWarnings("unchecked")
  @Test
  void testResultSetQuery() throws SQLException {
    final CheckedFunction<Connection, ResultSet, SQLException> query = mock(CheckedFunction.class);
    final CheckedFunction<ResultSet, JsonNode, SQLException> recordTransform = mock(CheckedFunction.class);
    doReturn(Stream.of(RECORDS_AS_JSON)).when(defaultJdbcDatabase).resultSetQuery(query, recordTransform);

    final Stream<JsonNode> actual = streamingJdbcDatabase.resultSetQuery(
        connection -> connection.createStatement().executeQuery("SELECT * FROM id_and_name;"),
        JdbcUtils::rowToJson);
    final List<JsonNode> actualAsList = actual.collect(Collectors.toList());
    actual.close();

    assertEquals(RECORDS_AS_JSON, actualAsList);
  }

  @SuppressWarnings("unchecked")
  @Test
  void testQuery() throws SQLException {
    final CheckedFunction<Connection, PreparedStatement, SQLException> query = mock(CheckedFunction.class);
    final CheckedFunction<ResultSet, JsonNode, SQLException> recordTransform = mock(CheckedFunction.class);
    doReturn(Stream.of(RECORDS_AS_JSON)).when(defaultJdbcDatabase).query(query, recordTransform);

    final Stream<JsonNode> actual = streamingJdbcDatabase.query(
        connection -> connection.prepareStatement("SELECT * FROM id_and_name;"),
        JdbcUtils::rowToJson);

    assertEquals(RECORDS_AS_JSON, actual.collect(Collectors.toList()));
  }

  private JsonNode getConfig(PostgreSQLContainer<?> psqlDb, String dbName) {
    return Jsons.jsonNode(ImmutableMap.builder()
        .put("host", psqlDb.getHost())
        .put("port", psqlDb.getFirstMappedPort())
        .put("database", dbName)
        .put("username", psqlDb.getUsername())
        .put("password", psqlDb.getPassword())
        .build());
  }

}
