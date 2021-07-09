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

import static io.airbyte.integrations.source.jdbc.AbstractJdbcSource.CDC_DELETED_AT;
import static io.airbyte.integrations.source.jdbc.AbstractJdbcSource.CDC_LOG_FILE;
import static io.airbyte.integrations.source.jdbc.AbstractJdbcSource.CDC_LOG_POS;
import static io.airbyte.integrations.source.jdbc.AbstractJdbcSource.CDC_UPDATED_AT;
import static io.airbyte.integrations.source.mysql.MySqlSource.MYSQL_CDC_OFFSET;
import static io.airbyte.integrations.source.mysql.MySqlSource.MYSQL_DB_HISTORY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.commons.util.AutoCloseableIterators;
import io.airbyte.db.Database;
import io.airbyte.db.Databases;
import io.airbyte.db.jdbc.JdbcDatabase;
import io.airbyte.protocol.models.AirbyteCatalog;
import io.airbyte.protocol.models.AirbyteConnectionStatus;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteMessage.Type;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import io.airbyte.protocol.models.AirbyteStateMessage;
import io.airbyte.protocol.models.AirbyteStream;
import io.airbyte.protocol.models.CatalogHelpers;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.ConfiguredAirbyteStream;
import io.airbyte.protocol.models.Field;
import io.airbyte.protocol.models.JsonSchemaPrimitive;
import io.airbyte.protocol.models.SyncMode;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.jooq.SQLDialect;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MariaDBContainer;

public class CdcMySqlSourceTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(CdcMySqlSourceTest.class);

  private static final String MODELS_SCHEMA = "models_schema";
  private static final String MODELS_STREAM_NAME = "models";
  private static final Set<String> STREAM_NAMES = Sets
      .newHashSet(MODELS_STREAM_NAME);
  private static final String COL_ID = "id";
  private static final String COL_MAKE_ID = "make_id";
  private static final String COL_MODEL = "model";
  private static final String DB_NAME = MODELS_SCHEMA;

  private static final AirbyteCatalog CATALOG = new AirbyteCatalog().withStreams(List.of(
      CatalogHelpers.createAirbyteStream(
          MODELS_STREAM_NAME,
          MODELS_SCHEMA,
          Field.of(COL_ID, JsonSchemaPrimitive.NUMBER),
          Field.of(COL_MAKE_ID, JsonSchemaPrimitive.NUMBER),
          Field.of(COL_MODEL, JsonSchemaPrimitive.STRING))
          .withSupportedSyncModes(Lists.newArrayList(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL))
          .withSourceDefinedPrimaryKey(List.of(List.of(COL_ID)))));
  private static final ConfiguredAirbyteCatalog CONFIGURED_CATALOG = CatalogHelpers
      .toDefaultConfiguredCatalog(CATALOG);

  // set all streams to incremental.
  static {
    CONFIGURED_CATALOG.getStreams().forEach(s -> s.setSyncMode(SyncMode.INCREMENTAL));
  }

  private static final List<JsonNode> MODEL_RECORDS = ImmutableList.of(
      Jsons.jsonNode(ImmutableMap.of(COL_ID, 11, COL_MAKE_ID, 1, COL_MODEL, "Fiesta")),
      Jsons.jsonNode(ImmutableMap.of(COL_ID, 12, COL_MAKE_ID, 1, COL_MODEL, "Focus")),
      Jsons.jsonNode(ImmutableMap.of(COL_ID, 13, COL_MAKE_ID, 1, COL_MODEL, "Ranger")),
      Jsons.jsonNode(ImmutableMap.of(COL_ID, 14, COL_MAKE_ID, 2, COL_MODEL, "GLA")),
      Jsons.jsonNode(ImmutableMap.of(COL_ID, 15, COL_MAKE_ID, 2, COL_MODEL, "A 220")),
      Jsons.jsonNode(ImmutableMap.of(COL_ID, 16, COL_MAKE_ID, 2, COL_MODEL, "E 350")));

  private MariaDBContainer<?> container;
  private Database database;
  private MySqlSource source;
  private JsonNode config;

  @BeforeEach
  public void setup() {
    init();
    revokeAllPermissions();
    grantCorrectPermissions();
    createAndPopulateTables();
  }

  private void init() {
    container = new MariaDBContainer<>("mariadb:10.6.1");
    container.start();
    source = new MySqlSource();
    database = Databases.createDatabase(
        "root",
        "test",
        String.format("jdbc:mariadb://%s:%s",
            container.getHost(),
            container.getFirstMappedPort()),
        MySqlSource.TEST_DRIVER_CLASS,
        SQLDialect.MARIADB);

    config = Jsons.jsonNode(ImmutableMap.builder()
        .put("host", container.getHost())
        .put("port", container.getFirstMappedPort())
        .put("database", CdcMySqlSourceTest.DB_NAME)
        .put("username", container.getUsername())
        .put("password", container.getPassword())
        .put("replication_method", "CDC")
        .build());
  }

  private void revokeAllPermissions() {
    executeQuery("REVOKE ALL PRIVILEGES, GRANT OPTION FROM " + container.getUsername() + "@'%';");
  }

  private void grantCorrectPermissions() {
    executeQuery(
        "GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO "
            + container.getUsername() + "@'%';");
  }

  private void executeQuery(String query) {
    try {
      database.query(
          ctx -> ctx
              .execute(query));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void createAndPopulateTables() {
    createAndPopulateActualTable();
    createAndPopulateRandomTable();
  }

  private void createAndPopulateActualTable() {
    executeQuery("CREATE DATABASE " + MODELS_SCHEMA + ";");
    executeQuery(String
        .format("CREATE TABLE %s.%s(%s INTEGER, %s INTEGER, %s VARCHAR(200), PRIMARY KEY (%s));",
            MODELS_SCHEMA, MODELS_STREAM_NAME, COL_ID, COL_MAKE_ID, COL_MODEL, COL_ID));
    for (JsonNode recordJson : MODEL_RECORDS) {
      writeModelRecord(recordJson);
    }
  }

  /**
   * This database and table is not part of Airbyte sync. It is being created just to make sure the
   * databases not being synced by Airbyte are not causing issues with our debezium logic
   */
  private void createAndPopulateRandomTable() {
    executeQuery("CREATE DATABASE " + MODELS_SCHEMA + "_random" + ";");
    executeQuery(String
        .format("CREATE TABLE %s.%s(%s INTEGER, %s INTEGER, %s VARCHAR(200), PRIMARY KEY (%s));",
            MODELS_SCHEMA + "_random", MODELS_STREAM_NAME + "_random", COL_ID + "_random",
            COL_MAKE_ID + "_random",
            COL_MODEL + "_random", COL_ID + "_random"));
    final List<JsonNode> MODEL_RECORDS_RANDOM = ImmutableList.of(
        Jsons
            .jsonNode(ImmutableMap
                .of(COL_ID + "_random", 11000, COL_MAKE_ID + "_random", 1, COL_MODEL + "_random",
                    "Fiesta-random")),
        Jsons.jsonNode(ImmutableMap
            .of(COL_ID + "_random", 12000, COL_MAKE_ID + "_random", 1, COL_MODEL + "_random",
                "Focus-random")),
        Jsons
            .jsonNode(ImmutableMap
                .of(COL_ID + "_random", 13000, COL_MAKE_ID + "_random", 1, COL_MODEL + "_random",
                    "Ranger-random")),
        Jsons.jsonNode(ImmutableMap
            .of(COL_ID + "_random", 14000, COL_MAKE_ID + "_random", 2, COL_MODEL + "_random",
                "GLA-random")),
        Jsons.jsonNode(ImmutableMap
            .of(COL_ID + "_random", 15000, COL_MAKE_ID + "_random", 2, COL_MODEL + "_random",
                "A 220-random")),
        Jsons
            .jsonNode(ImmutableMap
                .of(COL_ID + "_random", 16000, COL_MAKE_ID + "_random", 2, COL_MODEL + "_random",
                    "E 350-random")));
    for (JsonNode recordJson : MODEL_RECORDS_RANDOM) {
      writeRecords(recordJson, MODELS_SCHEMA + "_random", MODELS_STREAM_NAME + "_random",
          COL_ID + "_random", COL_MAKE_ID + "_random", COL_MODEL + "_random");
    }
  }

  private void writeModelRecord(JsonNode recordJson) {
    writeRecords(recordJson, CdcMySqlSourceTest.MODELS_SCHEMA, MODELS_STREAM_NAME, COL_ID,
        COL_MAKE_ID,
        COL_MODEL);
  }

  private void writeRecords(
                            JsonNode recordJson,
                            String dbName,
                            String streamName,
                            String idCol,
                            String makeIdCol,
                            String modelCol) {
    executeQuery(
        String.format("INSERT INTO %s.%s (%s, %s, %s) VALUES (%s, %s, '%s');", dbName, streamName,
            idCol, makeIdCol, modelCol,
            recordJson.get(idCol).asInt(), recordJson.get(makeIdCol).asInt(),
            recordJson.get(modelCol).asText()));
  }

  @AfterEach
  public void tearDown() {
    try {
      database.close();
      container.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void fullRefreshAndCDCShouldReturnSameRecords() throws Exception {
    JsonNode record1 = Jsons.jsonNode(ImmutableMap.of(
        "id", 1,
        "bool_col", true,
        "tiny_int_one_col", true));
    ((ObjectNode) record1).put("tiny_int_two_col", (short) 80);
    JsonNode record2 = Jsons.jsonNode(ImmutableMap.of(
        "id", 2,
        "bool_col", false,
        "tiny_int_one_col", false));
    ((ObjectNode) record2).put("tiny_int_two_col", (short) 90);
    ImmutableList<JsonNode> records = ImmutableList.of(record1, record2);
    Set<JsonNode> originalData = new HashSet<>(records);
    setupForComparisonBetweenFullRefreshAndCDCSnapshot(records);

    AirbyteCatalog discover = source.discover(config);
    List<AirbyteStream> streams = discover.getStreams();

    assertEquals(streams.size(), 1);
    JsonNode jsonSchema = streams.get(0).getJsonSchema().get("properties");
    assertEquals(jsonSchema.get("id").get("type").asText(), "number");
    assertEquals(jsonSchema.get("bool_col").get("type").asText(), "boolean");
    assertEquals(jsonSchema.get("tiny_int_one_col").get("type").asText(), "boolean");
    assertEquals(jsonSchema.get("tiny_int_two_col").get("type").asText(), "number");

    AirbyteCatalog catalog = new AirbyteCatalog().withStreams(streams);
    final ConfiguredAirbyteCatalog configuredCatalog = CatalogHelpers
        .toDefaultConfiguredCatalog(catalog);
    configuredCatalog.getStreams().forEach(c -> c.setSyncMode(SyncMode.FULL_REFRESH));

    Set<JsonNode> dataFromFullRefresh = extractRecordMessages(
        AutoCloseableIterators.toListAndClose(source.read(config, configuredCatalog, null)))
            .stream()
            .map(AirbyteRecordMessage::getData).collect(Collectors.toSet());

    configuredCatalog.getStreams().forEach(c -> c.setSyncMode(SyncMode.INCREMENTAL));
    Set<JsonNode> dataFromDebeziumSnapshot = extractRecordMessages(
        AutoCloseableIterators.toListAndClose(source.read(config, configuredCatalog, null)))
            .stream()
            .map(
                airbyteRecordMessage -> {
                  JsonNode data = airbyteRecordMessage.getData();
                  removeCDCColumns((ObjectNode) data);
                  /**
                   * Debezium reads TINYINT (expect for TINYINT(1)) as IntNode while FullRefresh reads it as Short Ref
                   * : {@link io.airbyte.db.jdbc.JdbcUtils#setJsonField(java.sql.ResultSet, int, ObjectNode)} -> case
                   * TINYINT, SMALLINT -> o.put(columnName, r.getShort(i));
                   */
                  ((ObjectNode) data)
                      .put("tiny_int_two_col", (short) data.get("tiny_int_two_col").asInt());
                  return data;
                })
            .collect(Collectors.toSet());

    assertEquals(dataFromFullRefresh, originalData);
    assertEquals(dataFromFullRefresh, dataFromDebeziumSnapshot);
  }

  private void setupForComparisonBetweenFullRefreshAndCDCSnapshot(
                                                                  ImmutableList<JsonNode> data) {
    executeQuery("CREATE DATABASE " + "test_schema" + ";");
    executeQuery(String.format(
        "CREATE TABLE %s.%s(%s INTEGER, %s Boolean, %s TINYINT(1), %s TINYINT(2), PRIMARY KEY (%s));",
        "test_schema", "table_with_tiny_int", "id", "bool_col", "tiny_int_one_col",
        "tiny_int_two_col", "id"));

    executeQuery(String
        .format("INSERT INTO %s.%s (%s, %s, %s, %s) VALUES (%s, %s, %s, %s);", "test_schema",
            "table_with_tiny_int",
            "id", "bool_col", "tiny_int_one_col", "tiny_int_two_col",
            data.get(0).get("id").asInt(), data.get(0).get("bool_col").asBoolean(),
            data.get(0).get("tiny_int_one_col").asBoolean() ? 99 : -99, data.get(0).get("tiny_int_two_col").asInt()));

    executeQuery(String
        .format("INSERT INTO %s.%s (%s, %s, %s, %s) VALUES (%s, %s, %s, %s);", "test_schema",
            "table_with_tiny_int",
            "id", "bool_col", "tiny_int_one_col", "tiny_int_two_col",
            data.get(1).get("id").asInt(), data.get(1).get("bool_col").asBoolean(),
            data.get(1).get("tiny_int_one_col").asBoolean() ? 99 : -99, data.get(1).get("tiny_int_two_col").asInt()));
    ((ObjectNode) config).put("database", "test_schema");
  }

  @Test
  @DisplayName("On the first sync, produce returns records that exist in the database.")
  void testExistingData() throws Exception {
    final AutoCloseableIterator<AirbyteMessage> read = source
        .read(config, CONFIGURED_CATALOG, null);
    final List<AirbyteMessage> actualRecords = AutoCloseableIterators.toListAndClose(read);

    final Set<AirbyteRecordMessage> recordMessages = extractRecordMessages(actualRecords);
    final List<AirbyteStateMessage> stateMessages = extractStateMessages(actualRecords);

    JdbcDatabase jdbcDatabase = Databases.createJdbcDatabase(
        config.get("username").asText(),
        config.get("password").asText(),
        String.format("jdbc:mysql://%s:%s",
            config.get("host").asText(),
            config.get("port").asInt()),
        DRIVER_CLASS);

    Optional<TargetFilePosition> targetFilePosition = TargetFilePosition.targetFilePosition(jdbcDatabase);
    assertTrue(targetFilePosition.isPresent());
    /**
     * Debezium sets the binlog file name and position values for all the records fetched during
     * snapshot to the latest log position fetched via query SHOW MASTER STATUS Ref :
     * {@linkplain io.debezium.connector.mysql.SnapshotReader#readBinlogPosition(int, io.debezium.connector.mysql.SourceInfo, io.debezium.jdbc.JdbcConnection, java.util.concurrent.atomic.AtomicReference)}
     */
    recordMessages.forEach(record -> {
      assertEquals(record.getData().get(CDC_LOG_FILE).asText(),
          targetFilePosition.get().fileName);
      assertEquals(record.getData().get(CDC_LOG_POS).asInt(), targetFilePosition.get().position);
    });

    assertExpectedRecords(
        new HashSet<>(MODEL_RECORDS), recordMessages);
    assertExpectedStateMessages(stateMessages);
  }

  @Test
  @DisplayName("When a record is deleted, produces a deletion record.")
  void testDelete() throws Exception {
    final AutoCloseableIterator<AirbyteMessage> read1 = source
        .read(config, CONFIGURED_CATALOG, null);
    final List<AirbyteMessage> actualRecords1 = AutoCloseableIterators.toListAndClose(read1);
    final List<AirbyteStateMessage> stateMessages1 = extractStateMessages(actualRecords1);

    assertExpectedStateMessages(stateMessages1);

    executeQuery(String
        .format("DELETE FROM %s.%s WHERE %s = %s", MODELS_SCHEMA, MODELS_STREAM_NAME, COL_ID,
            11));

    final JsonNode state = stateMessages1.get(0).getData();
    final AutoCloseableIterator<AirbyteMessage> read2 = source
        .read(config, CONFIGURED_CATALOG, state);
    final List<AirbyteMessage> actualRecords2 = AutoCloseableIterators.toListAndClose(read2);
    final List<AirbyteRecordMessage> recordMessages2 = new ArrayList<>(
        extractRecordMessages(actualRecords2));
    final List<AirbyteStateMessage> stateMessages2 = extractStateMessages(actualRecords2);

    assertExpectedStateMessages(stateMessages2);
    assertEquals(1, recordMessages2.size());
    assertEquals(11, recordMessages2.get(0).getData().get(COL_ID).asInt());
    assertNotNull(recordMessages2.get(0).getData().get(CDC_LOG_FILE));
    assertNotNull(recordMessages2.get(0).getData().get(CDC_UPDATED_AT));
    assertNotNull(recordMessages2.get(0).getData().get(CDC_DELETED_AT));
  }

  @Test
  @DisplayName("When a record is updated, produces an update record.")
  void testUpdate() throws Exception {
    final String updatedModel = "Explorer";
    final AutoCloseableIterator<AirbyteMessage> read1 = source
        .read(config, CONFIGURED_CATALOG, null);
    final List<AirbyteMessage> actualRecords1 = AutoCloseableIterators.toListAndClose(read1);
    final List<AirbyteStateMessage> stateMessages1 = extractStateMessages(actualRecords1);

    assertExpectedStateMessages(stateMessages1);

    executeQuery(String
        .format("UPDATE %s.%s SET %s = '%s' WHERE %s = %s", MODELS_SCHEMA, MODELS_STREAM_NAME,
            COL_MODEL, updatedModel, COL_ID, 11));

    final JsonNode state = stateMessages1.get(0).getData();
    final AutoCloseableIterator<AirbyteMessage> read2 = source
        .read(config, CONFIGURED_CATALOG, state);
    final List<AirbyteMessage> actualRecords2 = AutoCloseableIterators.toListAndClose(read2);
    final List<AirbyteRecordMessage> recordMessages2 = new ArrayList<>(
        extractRecordMessages(actualRecords2));
    final List<AirbyteStateMessage> stateMessages2 = extractStateMessages(actualRecords2);

    assertExpectedStateMessages(stateMessages2);
    assertEquals(1, recordMessages2.size());
    assertEquals(11, recordMessages2.get(0).getData().get(COL_ID).asInt());
    assertEquals(updatedModel, recordMessages2.get(0).getData().get(COL_MODEL).asText());
    assertNotNull(recordMessages2.get(0).getData().get(CDC_LOG_FILE));
    assertNotNull(recordMessages2.get(0).getData().get(CDC_UPDATED_AT));
    assertTrue(recordMessages2.get(0).getData().get(CDC_DELETED_AT).isNull());
  }

  @SuppressWarnings({"BusyWait", "CodeBlock2Expr"})
  @Test
  @DisplayName("Verify that when data is inserted into the database while a sync is happening and after the first sync, it all gets replicated.")
  void testRecordsProducedDuringAndAfterSync() throws Exception {

    final int recordsToCreate = 20;
    final int[] recordsCreated = {0};
    // first batch of records. 20 created here and 6 created in setup method.
    while (recordsCreated[0] < recordsToCreate) {
      final JsonNode record =
          Jsons.jsonNode(ImmutableMap
              .of(COL_ID, 100 + recordsCreated[0], COL_MAKE_ID, 1, COL_MODEL,
                  "F-" + recordsCreated[0]));
      writeModelRecord(record);
      recordsCreated[0]++;
    }

    final AutoCloseableIterator<AirbyteMessage> firstBatchIterator = source
        .read(config, CONFIGURED_CATALOG, null);
    final List<AirbyteMessage> dataFromFirstBatch = AutoCloseableIterators
        .toListAndClose(firstBatchIterator);
    List<AirbyteStateMessage> stateAfterFirstBatch = extractStateMessages(dataFromFirstBatch);
    assertExpectedStateMessages(stateAfterFirstBatch);
    Set<AirbyteRecordMessage> recordsFromFirstBatch = extractRecordMessages(
        dataFromFirstBatch);
    assertEquals((MODEL_RECORDS.size() + 20), recordsFromFirstBatch.size());

    // second batch of records again 20 being created
    recordsCreated[0] = 0;
    while (recordsCreated[0] < recordsToCreate) {
      final JsonNode record =
          Jsons.jsonNode(ImmutableMap
              .of(COL_ID, 200 + recordsCreated[0], COL_MAKE_ID, 1, COL_MODEL,
                  "F-" + recordsCreated[0]));
      writeModelRecord(record);
      recordsCreated[0]++;
    }

    final JsonNode state = stateAfterFirstBatch.get(0).getData();
    final AutoCloseableIterator<AirbyteMessage> secondBatchIterator = source
        .read(config, CONFIGURED_CATALOG, state);
    final List<AirbyteMessage> dataFromSecondBatch = AutoCloseableIterators
        .toListAndClose(secondBatchIterator);

    List<AirbyteStateMessage> stateAfterSecondBatch = extractStateMessages(dataFromSecondBatch);
    assertExpectedStateMessages(stateAfterSecondBatch);

    Set<AirbyteRecordMessage> recordsFromSecondBatch = extractRecordMessages(
        dataFromSecondBatch);
    assertEquals(20, recordsFromSecondBatch.size(),
        "Expected 20 records to be replicated in the second sync.");

    // sometimes there can be more than one of these at the end of the snapshot and just before the
    // first incremental.
    final Set<AirbyteRecordMessage> recordsFromFirstBatchWithoutDuplicates = removeDuplicates(
        recordsFromFirstBatch);
    final Set<AirbyteRecordMessage> recordsFromSecondBatchWithoutDuplicates = removeDuplicates(
        recordsFromSecondBatch);

    final int recordsCreatedBeforeTestCount = MODEL_RECORDS.size();
    assertTrue(recordsCreatedBeforeTestCount < recordsFromFirstBatchWithoutDuplicates.size(),
        "Expected first sync to include records created while the test was running.");
    assertEquals(40 + recordsCreatedBeforeTestCount,
        recordsFromFirstBatchWithoutDuplicates.size() + recordsFromSecondBatchWithoutDuplicates
            .size());
  }

  private static Set<AirbyteRecordMessage> removeDuplicates(Set<AirbyteRecordMessage> messages) {
    final Set<JsonNode> existingDataRecordsWithoutUpdated = new HashSet<>();
    final Set<AirbyteRecordMessage> output = new HashSet<>();

    for (AirbyteRecordMessage message : messages) {
      ObjectNode node = message.getData().deepCopy();
      node.remove("_ab_cdc_updated_at");

      if (existingDataRecordsWithoutUpdated.contains(node)) {
        LOGGER.info("Removing duplicate node: " + node);
      } else {
        output.add(message);
        existingDataRecordsWithoutUpdated.add(node);
      }
    }

    return output;
  }

  @Test
  @DisplayName("When both incremental CDC and full refresh are configured for different streams in a sync, the data is replicated as expected.")
  void testCdcAndFullRefreshInSameSync() throws Exception {
    final ConfiguredAirbyteCatalog configuredCatalog = Jsons.clone(CONFIGURED_CATALOG);

    final List<JsonNode> MODEL_RECORDS_2 = ImmutableList.of(
        Jsons.jsonNode(ImmutableMap.of(COL_ID, 110, COL_MAKE_ID, 1, COL_MODEL, "Fiesta-2")),
        Jsons.jsonNode(ImmutableMap.of(COL_ID, 120, COL_MAKE_ID, 1, COL_MODEL, "Focus-2")),
        Jsons.jsonNode(ImmutableMap.of(COL_ID, 130, COL_MAKE_ID, 1, COL_MODEL, "Ranger-2")),
        Jsons.jsonNode(ImmutableMap.of(COL_ID, 140, COL_MAKE_ID, 2, COL_MODEL, "GLA-2")),
        Jsons.jsonNode(ImmutableMap.of(COL_ID, 150, COL_MAKE_ID, 2, COL_MODEL, "A 220-2")),
        Jsons.jsonNode(ImmutableMap.of(COL_ID, 160, COL_MAKE_ID, 2, COL_MODEL, "E 350-2")));

    executeQuery(String
        .format("CREATE TABLE %s.%s(%s INTEGER, %s INTEGER, %s VARCHAR(200), PRIMARY KEY (%s));",
            MODELS_SCHEMA, MODELS_STREAM_NAME + "_2", COL_ID, COL_MAKE_ID, COL_MODEL, COL_ID));

    for (JsonNode recordJson : MODEL_RECORDS_2) {
      writeRecords(recordJson, CdcMySqlSourceTest.MODELS_SCHEMA, MODELS_STREAM_NAME + "_2", COL_ID,
          COL_MAKE_ID, COL_MODEL);
    }

    ConfiguredAirbyteStream airbyteStream = new ConfiguredAirbyteStream()
        .withStream(CatalogHelpers.createAirbyteStream(
            MODELS_STREAM_NAME + "_2",
            MODELS_SCHEMA,
            Field.of(COL_ID, JsonSchemaPrimitive.NUMBER),
            Field.of(COL_MAKE_ID, JsonSchemaPrimitive.NUMBER),
            Field.of(COL_MODEL, JsonSchemaPrimitive.STRING))
            .withSupportedSyncModes(
                Lists.newArrayList(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL))
            .withSourceDefinedPrimaryKey(List.of(List.of(COL_ID))));
    airbyteStream.setSyncMode(SyncMode.FULL_REFRESH);

    List<ConfiguredAirbyteStream> streams = configuredCatalog.getStreams();
    streams.add(airbyteStream);
    configuredCatalog.withStreams(streams);

    final AutoCloseableIterator<AirbyteMessage> read1 = source
        .read(config, configuredCatalog, null);
    final List<AirbyteMessage> actualRecords1 = AutoCloseableIterators.toListAndClose(read1);

    final Set<AirbyteRecordMessage> recordMessages1 = extractRecordMessages(actualRecords1);
    final List<AirbyteStateMessage> stateMessages1 = extractStateMessages(actualRecords1);
    HashSet<String> names = new HashSet<>(STREAM_NAMES);
    names.add(MODELS_STREAM_NAME + "_2");
    assertExpectedStateMessages(stateMessages1);
    assertExpectedRecords(Streams.concat(MODEL_RECORDS_2.stream(), MODEL_RECORDS.stream())
        .collect(Collectors.toSet()),
        recordMessages1,
        Collections.singleton(MODELS_STREAM_NAME),
        names);

    final JsonNode puntoRecord = Jsons
        .jsonNode(ImmutableMap.of(COL_ID, 100, COL_MAKE_ID, 3, COL_MODEL, "Punto"));
    writeModelRecord(puntoRecord);

    final JsonNode state = extractStateMessages(actualRecords1).get(0).getData();
    final AutoCloseableIterator<AirbyteMessage> read2 = source
        .read(config, configuredCatalog, state);
    final List<AirbyteMessage> actualRecords2 = AutoCloseableIterators.toListAndClose(read2);

    final Set<AirbyteRecordMessage> recordMessages2 = extractRecordMessages(actualRecords2);
    final List<AirbyteStateMessage> stateMessages2 = extractStateMessages(actualRecords2);

    assertExpectedStateMessages(stateMessages2);
    assertExpectedRecords(
        Streams.concat(MODEL_RECORDS_2.stream(), Stream.of(puntoRecord))
            .collect(Collectors.toSet()),
        recordMessages2,
        Collections.singleton(MODELS_STREAM_NAME),
        names);
  }

  @Test
  @DisplayName("When no records exist, no records are returned.")
  void testNoData() throws Exception {

    executeQuery(String.format("DELETE FROM %s.%s", MODELS_SCHEMA, MODELS_STREAM_NAME));

    final AutoCloseableIterator<AirbyteMessage> read = source
        .read(config, CONFIGURED_CATALOG, null);
    final List<AirbyteMessage> actualRecords = AutoCloseableIterators.toListAndClose(read);

    final Set<AirbyteRecordMessage> recordMessages = extractRecordMessages(actualRecords);
    final List<AirbyteStateMessage> stateMessages = extractStateMessages(actualRecords);

    assertExpectedRecords(Collections.emptySet(), recordMessages);
    assertExpectedStateMessages(stateMessages);
  }

  @Test
  @DisplayName("When no changes have been made to the database since the previous sync, no records are returned.")
  void testNoDataOnSecondSync() throws Exception {
    final AutoCloseableIterator<AirbyteMessage> read1 = source
        .read(config, CONFIGURED_CATALOG, null);
    final List<AirbyteMessage> actualRecords1 = AutoCloseableIterators.toListAndClose(read1);
    final JsonNode state = extractStateMessages(actualRecords1).get(0).getData();

    final AutoCloseableIterator<AirbyteMessage> read2 = source
        .read(config, CONFIGURED_CATALOG, state);
    final List<AirbyteMessage> actualRecords2 = AutoCloseableIterators.toListAndClose(read2);

    final Set<AirbyteRecordMessage> recordMessages2 = extractRecordMessages(actualRecords2);
    final List<AirbyteStateMessage> stateMessages2 = extractStateMessages(actualRecords2);

    assertExpectedRecords(Collections.emptySet(), recordMessages2);
    assertExpectedStateMessages(stateMessages2);
  }

  @Test
  void testCheck() {
    final AirbyteConnectionStatus status = source.check(config);
    assertEquals(status.getStatus(), AirbyteConnectionStatus.Status.SUCCEEDED);
  }

  @Test
  void testDiscover() throws Exception {
    final AirbyteCatalog expectedCatalog = Jsons.clone(CATALOG);

    executeQuery(String
        .format("CREATE TABLE %s.%s(%s INTEGER, %s INTEGER, %s VARCHAR(200));",
            MODELS_SCHEMA, MODELS_STREAM_NAME + "_2", COL_ID, COL_MAKE_ID, COL_MODEL));

    List<AirbyteStream> streams = expectedCatalog.getStreams();
    // stream with PK
    streams.get(0).setSourceDefinedCursor(true);
    addCdcMetadataColumns(streams.get(0));

    AirbyteStream streamWithoutPK = CatalogHelpers.createAirbyteStream(
        MODELS_STREAM_NAME + "_2",
        MODELS_SCHEMA,
        Field.of(COL_ID, JsonSchemaPrimitive.NUMBER),
        Field.of(COL_MAKE_ID, JsonSchemaPrimitive.NUMBER),
        Field.of(COL_MODEL, JsonSchemaPrimitive.STRING));
    streamWithoutPK.setSourceDefinedPrimaryKey(Collections.emptyList());
    streamWithoutPK.setSupportedSyncModes(List.of(SyncMode.FULL_REFRESH));
    addCdcMetadataColumns(streamWithoutPK);

    streams.add(streamWithoutPK);
    expectedCatalog.withStreams(streams);

    final AirbyteCatalog actualCatalog = source.discover(config);

    assertEquals(
        expectedCatalog.getStreams().stream().sorted(Comparator.comparing(AirbyteStream::getName))
            .collect(Collectors.toList()),
        actualCatalog.getStreams().stream().sorted(Comparator.comparing(AirbyteStream::getName))
            .collect(Collectors.toList()));
  }

  private static AirbyteStream addCdcMetadataColumns(AirbyteStream stream) {
    ObjectNode jsonSchema = (ObjectNode) stream.getJsonSchema();
    ObjectNode properties = (ObjectNode) jsonSchema.get("properties");

    final JsonNode numberType = Jsons.jsonNode(ImmutableMap.of("type", "number"));
    final JsonNode stringType = Jsons.jsonNode(ImmutableMap.of("type", "string"));
    properties.set(CDC_LOG_FILE, stringType);
    properties.set(CDC_LOG_POS, numberType);
    properties.set(CDC_UPDATED_AT, numberType);
    properties.set(CDC_DELETED_AT, numberType);

    return stream;
  }

  private Set<AirbyteRecordMessage> extractRecordMessages(List<AirbyteMessage> messages) {
    final List<AirbyteRecordMessage> recordMessageList = messages
        .stream()
        .filter(r -> r.getType() == Type.RECORD).map(AirbyteMessage::getRecord)
        .collect(Collectors.toList());
    final Set<AirbyteRecordMessage> recordMessageSet = new HashSet<>(recordMessageList);

    assertEquals(recordMessageList.size(), recordMessageSet.size(),
        "Expected no duplicates in airbyte record message output for a single sync.");

    return recordMessageSet;
  }

  private List<AirbyteStateMessage> extractStateMessages(List<AirbyteMessage> messages) {
    return messages.stream().filter(r -> r.getType() == Type.STATE).map(AirbyteMessage::getState)
        .collect(Collectors.toList());
  }

  private static void assertExpectedStateMessages(List<AirbyteStateMessage> stateMessages) {
    // TODO: add assertion for boolean cdc is true
    assertEquals(1, stateMessages.size());
    assertNotNull(stateMessages.get(0).getData());
    assertNotNull(
        stateMessages.get(0).getData().get("cdc_state").get("state").get(MYSQL_CDC_OFFSET));
    assertNotNull(
        stateMessages.get(0).getData().get("cdc_state").get("state").get(MYSQL_DB_HISTORY));
  }

  private static void assertExpectedRecords(Set<JsonNode> expectedRecords,
                                            Set<AirbyteRecordMessage> actualRecords) {
    // assume all streams are cdc.
    assertExpectedRecords(
        expectedRecords,
        actualRecords,
        actualRecords.stream().map(AirbyteRecordMessage::getStream).collect(Collectors.toSet()));
  }

  private static void assertExpectedRecords(Set<JsonNode> expectedRecords,
                                            Set<AirbyteRecordMessage> actualRecords,
                                            Set<String> cdcStreams) {
    assertExpectedRecords(expectedRecords, actualRecords, cdcStreams, STREAM_NAMES);
  }

  private static void assertExpectedRecords(Set<JsonNode> expectedRecords,
                                            Set<AirbyteRecordMessage> actualRecords,
                                            Set<String> cdcStreams,
                                            Set<String> streamNames) {
    final Set<JsonNode> actualData = actualRecords
        .stream()
        .map(recordMessage -> {
          assertTrue(streamNames.contains(recordMessage.getStream()));
          assertNotNull(recordMessage.getEmittedAt());

          assertEquals(MODELS_SCHEMA, recordMessage.getNamespace());

          final JsonNode data = recordMessage.getData();

          if (cdcStreams.contains(recordMessage.getStream())) {
            assertNotNull(data.get(CDC_LOG_FILE));
            assertNotNull(data.get(CDC_LOG_POS));
            assertNotNull(data.get(CDC_UPDATED_AT));
          } else {
            assertNull(data.get(CDC_LOG_FILE));
            assertNull(data.get(CDC_LOG_POS));
            assertNull(data.get(CDC_UPDATED_AT));
            assertNull(data.get(CDC_DELETED_AT));
          }

          removeCDCColumns((ObjectNode) data);

          return data;
        })
        .collect(Collectors.toSet());

    assertEquals(expectedRecords, actualData);
  }

  private static void removeCDCColumns(ObjectNode data) {
    data.remove(CDC_LOG_FILE);
    data.remove(CDC_LOG_POS);
    data.remove(CDC_UPDATED_AT);
    data.remove(CDC_DELETED_AT);
  }

}
