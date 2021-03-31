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

package io.airbyte.integrations.source.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.resources.MoreResources;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.db.Database;
import io.airbyte.db.Databases;
import io.airbyte.protocol.models.AirbyteCatalog;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import io.airbyte.protocol.models.CatalogHelpers;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.Field;
import io.airbyte.protocol.models.Field.JsonSchemaPrimitive;
import io.airbyte.protocol.models.SyncMode;
import io.airbyte.test.utils.PostgreSQLContainerHelper;
import io.debezium.engine.ChangeEvent;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomStringUtils;
import org.jooq.SQLDialect;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

class PostgresSourceCdcTest {

  private static final String SLOT_NAME = "debezium_slot";
  private static final String STREAM_NAME = "public.id_and_name";
  private static final AirbyteCatalog CATALOG = new AirbyteCatalog().withStreams(List.of(
      CatalogHelpers.createAirbyteStream(
          STREAM_NAME,
          Field.of("id", JsonSchemaPrimitive.NUMBER),
          Field.of("name", JsonSchemaPrimitive.STRING),
          Field.of("power", JsonSchemaPrimitive.NUMBER))
          .withSupportedSyncModes(Lists.newArrayList(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL))
          .withSourceDefinedPrimaryKey(List.of(List.of("id"))),
      CatalogHelpers.createAirbyteStream(
          STREAM_NAME + "2",
          Field.of("id", JsonSchemaPrimitive.NUMBER),
          Field.of("name", JsonSchemaPrimitive.STRING),
          Field.of("power", JsonSchemaPrimitive.NUMBER))
          .withSupportedSyncModes(Lists.newArrayList(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL)),
      CatalogHelpers.createAirbyteStream(
          "public.names",
          Field.of("first_name", JsonSchemaPrimitive.STRING),
          Field.of("last_name", JsonSchemaPrimitive.STRING),
          Field.of("power", JsonSchemaPrimitive.NUMBER))
          .withSupportedSyncModes(Lists.newArrayList(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL))
          .withSourceDefinedPrimaryKey(List.of(List.of("first_name"), List.of("last_name")))));
  private static final ConfiguredAirbyteCatalog CONFIGURED_CATALOG = CatalogHelpers.toDefaultConfiguredCatalog(CATALOG);

  private static PostgreSQLContainer<?> PSQL_DB;

  private String dbName;
  private Database database;

  @BeforeAll
  static void init() {
    PSQL_DB = new PostgreSQLContainer<>("postgres:13-alpine")
        .withCopyFileToContainer(MountableFile.forClasspathResource("postgresql.conf"), "/etc/postgresql/postgresql.conf")
        .withCommand("postgres -c config_file=/etc/postgresql/postgresql.conf");
    PSQL_DB.start();
  }

  @BeforeEach
  void setup() throws Exception {
    dbName = "db_" + RandomStringUtils.randomAlphabetic(10).toLowerCase();

    final String initScriptName = "init_" + dbName.concat(".sql");
    MoreResources.writeResource(initScriptName, "CREATE DATABASE " + dbName + ";");
    PostgreSQLContainerHelper.runSqlScript(MountableFile.forClasspathResource(initScriptName), PSQL_DB);

    final JsonNode config = getConfig(PSQL_DB, dbName);
    database = getDatabaseFromConfig(config);
    database.query(ctx -> {
      // ctx.execute("SELECT pg_create_logical_replication_slot('" + SLOT_NAME + "', 'pgoutput');");

      // need to re-add record that has -infinity.
      ctx.execute("CREATE TABLE id_and_name(id NUMERIC(20, 10), name VARCHAR(200), power double precision, PRIMARY KEY (id));");
      ctx.execute("CREATE INDEX i1 ON id_and_name (id);");
      ctx.execute("begin; INSERT INTO id_and_name (id, name, power) VALUES (1,'goku', 'Infinity'),  (2, 'vegeta', 9000.1); commit;");

      return null;
    });
    database.query(ctx -> {
      ctx.execute("begin; INSERT INTO id_and_name (id, name, power) VALUES (4,'picard', '10'); commit;");

      return null;
    });
    database.query(ctx -> {
      ctx.execute("begin; UPDATE id_and_name SET name='picard2' WHERE id=4; commit;");

      return null;
    });
    // database.close();
  }

  private JsonNode getConfig(PostgreSQLContainer<?> psqlDb, String dbName) {
    System.out.println("psqlDb.getFirstMappedPort() = " + psqlDb.getFirstMappedPort());
    return Jsons.jsonNode(ImmutableMap.builder()
        .put("host", psqlDb.getHost())
        .put("port", psqlDb.getFirstMappedPort())
        .put("database", dbName)
        .put("username", psqlDb.getUsername())
        .put("password", psqlDb.getPassword())
        .put("replication_slot", SLOT_NAME)
        .build());
    // return Jsons.jsonNode(ImmutableMap.builder()
    // .put("host", "localhost")
    // .put("port", 5432)
    // .put("database", "debezium_test")
    // .put("username", "postgres")
    // .put("password", "")
    // .put("replication_slot", SLOT_NAME)
    // .build());
  }

  private Database getDatabaseFromConfig(JsonNode config) {
    return Databases.createDatabase(
        config.get("username").asText(),
        config.get("password").asText(),
        String.format("jdbc:postgresql://%s:%s/%s",
            config.get("host").asText(),
            config.get("port").asText(),
            config.get("database").asText()),
        "org.postgresql.Driver",
        SQLDialect.POSTGRES);
  }

  @Test
  public void testIt() throws Exception {
    final PostgresSource source = new PostgresSource();
    final ConfiguredAirbyteCatalog configuredCatalog =
        CONFIGURED_CATALOG.withStreams(CONFIGURED_CATALOG.getStreams()
            .stream()
            .filter(s -> s.getStream().getName().equals(STREAM_NAME))
            .collect(Collectors.toList()));
    // coerce to incremental so it uses CDC.
    configuredCatalog.getStreams().forEach(s -> s.setSyncMode(SyncMode.INCREMENTAL));

    final AutoCloseableIterator<AirbyteMessage> read = source.read(getConfig(PSQL_DB, dbName), configuredCatalog, null);

    Thread.sleep(5000);
    final JsonNode config = getConfig(PSQL_DB, dbName);
    final Database database = getDatabaseFromConfig(config);
    // database.query(ctx -> {
    // ctx.fetch(
    // "UPDATE names SET power = 10000.2 WHERE first_name = 'san';");
    // ctx.fetch(
    // "DELETE FROM names WHERE first_name = 'san';");
    // return null;
    // });
    // database.close();

    long startMillis = System.currentTimeMillis();
    final AtomicInteger i = new AtomicInteger(10);
    while (read.hasNext()) {
      database.query(ctx -> {
        ctx.execute(
            String.format("begin; INSERT INTO id_and_name (id, name, power) VALUES (%s,'goku%s', 'Infinity'),  (%s, 'vegeta%s', 9000.1); commit;",
                i.incrementAndGet(), i.incrementAndGet(), i.incrementAndGet(), i.incrementAndGet()));
        // ctx.execute(String.format("begin; UPDATE id_and_name SET name='goku%s' WHERE id=1; UPDATE
        // id_and_name SET name='picard%s' WHERE id=4; commit;", i.incrementAndGet(), i.incrementAndGet()));
        // ctx.execute(String.format("begin; UPDATE id_and_name SET name='goku%s' WHERE id=1; commit;",
        // i.incrementAndGet()));
        // ctx.execute(String.format("begin; UPDATE id_and_name SET name='picard%s' WHERE id=4; commit;",
        // i.incrementAndGet()));

        return null;
      });
      if (read.hasNext()) {
        System.out.println("it said it had next");
        System.out.println("read.next() = " + read.next());
      }
    }
  }

  @Test
  public void testConvertChangeEvent() throws IOException {
    final String stream = "names";
    final Instant emittedAt = Instant.now();
    ChangeEvent<String, String> insertChangeEvent = mockChangeEvent("insert_change_event.json");
    ChangeEvent<String, String> updateChangeEvent = mockChangeEvent("update_change_event.json");
    ChangeEvent<String, String> deleteChangeEvent = mockChangeEvent("delete_change_event.json");

    final AirbyteMessage actualInsert = PostgresSource.convertChangeEvent(insertChangeEvent, emittedAt);
    final AirbyteMessage actualUpdate = PostgresSource.convertChangeEvent(updateChangeEvent, emittedAt);
    final AirbyteMessage actualDelete = PostgresSource.convertChangeEvent(deleteChangeEvent, emittedAt);

    final AirbyteMessage expectedInsert = createAirbyteMessage(stream, emittedAt, "insert_message.json");
    final AirbyteMessage expectedUpdate = createAirbyteMessage(stream, emittedAt, "update_message.json");
    final AirbyteMessage expectedDelete = createAirbyteMessage(stream, emittedAt, "delete_message.json");

    deepCompare(expectedInsert, actualInsert);
    deepCompare(expectedUpdate, actualUpdate);
    deepCompare(expectedDelete, actualDelete);
  }

  private static ChangeEvent<String, String> mockChangeEvent(String resourceName) throws IOException {
    final ChangeEvent<String, String> mocked = mock(ChangeEvent.class);
    final String resource = MoreResources.readResource(resourceName);
    when(mocked.value()).thenReturn(resource);

    return mocked;
  }

  private static AirbyteMessage createAirbyteMessage(String stream, Instant emittedAt, String resourceName) throws IOException {
    final String data = MoreResources.readResource(resourceName);

    final AirbyteRecordMessage recordMessage = new AirbyteRecordMessage()
        .withStream(stream)
        .withData(Jsons.deserialize(data))
        .withEmittedAt(emittedAt.toEpochMilli());

    return new AirbyteMessage()
        .withType(AirbyteMessage.Type.RECORD)
        .withRecord(recordMessage);
  }

  private static void deepCompare(Object expected, Object actual) throws JsonProcessingException {
    ObjectMapper objectMapper = new ObjectMapper();
    assertEquals(objectMapper.readTree(Jsons.serialize(expected)), objectMapper.readTree(Jsons.serialize(actual)));
  }

}
