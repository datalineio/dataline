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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.airbyte.commons.json.Jsons;
import io.airbyte.db.DataTypeUtils;
import io.airbyte.db.Databases;
import io.airbyte.db.jdbc.JdbcDatabase;
import io.airbyte.db.jdbc.JdbcUtils;
import io.airbyte.integrations.base.JavaBaseConstants;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

class SqlOperationsUtilsTest {

  private static final Instant NOW = Instant.ofEpochSecond(900);
  private static final String SCHEMA_NAME = "public";
  private static final String STREAM_NAME = "rivers";

  private PostgreSQLContainer<?> container;
  private JdbcDatabase database;
  private Supplier<UUID> uuidSupplier;

  @SuppressWarnings("unchecked")
  @BeforeEach
  void setup() {
    container = new PostgreSQLContainer<>("postgres:13-alpine");
    container.start();

    JsonNode config = createConfig();

    database = Databases.createJdbcDatabase(
        config.get("username").asText(),
        config.get("password").asText(),
        config.get("jdbc_url").asText(),
        "org.postgresql.Driver");

    uuidSupplier = mock(Supplier.class);
  }

  @Test
  void testInsertRawRecordsInSingleQuery() throws SQLException {
    final UUID RECORD1_UUID = UUID.randomUUID();
    final UUID RECORD2_UUID = UUID.randomUUID();
    when(uuidSupplier.get()).thenReturn(RECORD1_UUID).thenReturn(RECORD2_UUID);

    new DefaultSqlOperations().createTableIfNotExists(database, SCHEMA_NAME, STREAM_NAME);

    final String insertQueryComponent = String.format(
        "INSERT INTO %s.%s (%s, %s, %s) VALUES\n",
        SCHEMA_NAME,
        STREAM_NAME,
        JavaBaseConstants.COLUMN_NAME_AB_ID,
        JavaBaseConstants.COLUMN_NAME_DATA,
        JavaBaseConstants.COLUMN_NAME_EMITTED_AT);
    final String recordQueryComponent = "(?, ?::jsonb, ?),\n";

    final List<AirbyteRecordMessage> records = Lists.newArrayList(
        new AirbyteRecordMessage()
            .withStream("rivers")
            .withEmittedAt(NOW.toEpochMilli())
            .withData(Jsons.jsonNode(ImmutableMap.of("name", "rio grande", "width", 10))),
        new AirbyteRecordMessage()
            .withStream("rivers")
            .withEmittedAt(NOW.toEpochMilli())
            .withData(Jsons.jsonNode(ImmutableMap.of("name", "mississippi", "width", 20))));

    SqlOperationsUtils.insertRawRecordsInSingleQuery(insertQueryComponent, recordQueryComponent, database, records, uuidSupplier, true);

    final List<JsonNode> actualRecords = database.bufferedResultSetQuery(
        connection -> connection.createStatement().executeQuery("SELECT * FROM RIVERS"),
        JdbcUtils::rowToJson);

    final List<JsonNode> expectedRecords = Lists.newArrayList(
        Jsons.jsonNode(ImmutableMap.builder()
            .put(JavaBaseConstants.COLUMN_NAME_AB_ID, RECORD1_UUID)
            .put(JavaBaseConstants.COLUMN_NAME_DATA, records.get(0).getData())
            .put(JavaBaseConstants.COLUMN_NAME_EMITTED_AT, DataTypeUtils
                .toISO8601String(records.get(0).getEmittedAt()))
            .build()),
        Jsons.jsonNode(ImmutableMap.builder()
            .put(JavaBaseConstants.COLUMN_NAME_AB_ID, RECORD2_UUID)
            .put(JavaBaseConstants.COLUMN_NAME_DATA, records.get(1).getData())
            .put(JavaBaseConstants.COLUMN_NAME_EMITTED_AT, DataTypeUtils
                .toISO8601String(records.get(1).getEmittedAt()))
            .build()));

    actualRecords.forEach(
        r -> ((ObjectNode) r).put(JavaBaseConstants.COLUMN_NAME_DATA, Jsons.deserialize(r.get(JavaBaseConstants.COLUMN_NAME_DATA).asText())));

    assertEquals(expectedRecords, actualRecords);
  }

  private JsonNode createConfig() {
    return Jsons.jsonNode(ImmutableMap.builder()
        .put("username", container.getUsername())
        .put("password", container.getPassword())
        .put("schema", SCHEMA_NAME)
        .put("jdbc_url", String.format("jdbc:postgresql://%s:%s/%s",
            container.getHost(),
            container.getFirstMappedPort(),
            container.getDatabaseName()))
        .build());
  }

}
