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

import static io.airbyte.integrations.source.mysql.AirbyteFileOffsetBackingStore.initializeState;
import static io.airbyte.integrations.source.mysql.AirbyteSchemaHistoryStorage.initializeDBHistory;
import static java.util.stream.Collectors.toList;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import io.airbyte.commons.functional.CheckedConsumer;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.commons.util.AutoCloseableIterators;
import io.airbyte.commons.util.CompositeIterator;
import io.airbyte.commons.util.MoreIterators;
import io.airbyte.db.jdbc.JdbcDatabase;
import io.airbyte.integrations.base.IntegrationRunner;
import io.airbyte.integrations.base.Source;
import io.airbyte.integrations.source.jdbc.AbstractJdbcSource;
import io.airbyte.integrations.source.relationaldb.StateManager;
import io.airbyte.integrations.source.relationaldb.TableInfo;
import io.airbyte.integrations.source.relationaldb.models.CdcState;
import io.airbyte.protocol.models.AirbyteCatalog;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteMessage.Type;
import io.airbyte.protocol.models.AirbyteStateMessage;
import io.airbyte.protocol.models.AirbyteStream;
import io.airbyte.protocol.models.CommonField;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.ConfiguredAirbyteStream;
import io.airbyte.protocol.models.SyncMode;
import io.debezium.engine.ChangeEvent;
import java.sql.JDBCType;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySqlSource extends AbstractJdbcSource implements Source {

  private static final Logger LOGGER = LoggerFactory.getLogger(MySqlSource.class);

  public static final String DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";
  public static final String TEST_DRIVER_CLASS = "org.mariadb.jdbc.Driver";
  public static final String MYSQL_CDC_OFFSET = "mysql_cdc_offset";
  public static final String MYSQL_DB_HISTORY = "mysql_db_history";

  public MySqlSource() {
    super(DRIVER_CLASS, new MySqlJdbcStreamingQueryConfiguration());
  }

  private static AirbyteStream removeIncrementalWithoutPk(AirbyteStream stream) {
    if (stream.getSourceDefinedPrimaryKey().isEmpty()) {
      stream.getSupportedSyncModes().remove(SyncMode.INCREMENTAL);
    }

    return stream;
  }

  private static AirbyteStream setIncrementalToSourceDefined(AirbyteStream stream) {
    if (stream.getSupportedSyncModes().contains(SyncMode.INCREMENTAL)) {
      stream.setSourceDefinedCursor(true);
    }

    return stream;
  }

  // Note: in place mutation.
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

  @Override
  public List<CheckedConsumer<JdbcDatabase, Exception>> getCheckOperations(JsonNode config) throws Exception {
    final List<CheckedConsumer<JdbcDatabase, Exception>> checkOperations = new ArrayList<>(super.getCheckOperations(config));
    if (isCdc(config)) {
      checkOperations.add(database -> {
        List<String> log = database.resultSetQuery(connection -> {
          final String sql = "show variables where Variable_name = 'log_bin'";

          return connection.createStatement().executeQuery(sql);
        }, resultSet -> resultSet.getString("Value")).collect(toList());

        if (log.size() != 1) {
          throw new RuntimeException("Could not query the variable log_bin");
        }

        String logBin = log.get(0);
        if (!logBin.equalsIgnoreCase("ON")) {
          throw new RuntimeException("The variable log_bin should be set to ON, but it is : " + logBin);
        }
      });

      checkOperations.add(database -> {
        List<String> format = database.resultSetQuery(connection -> {
          final String sql = "show variables where Variable_name = 'binlog_format'";

          return connection.createStatement().executeQuery(sql);
        }, resultSet -> resultSet.getString("Value")).collect(toList());

        if (format.size() != 1) {
          throw new RuntimeException("Could not query the variable binlog_format");
        }

        String binlogFormat = format.get(0);
        if (!binlogFormat.equalsIgnoreCase("ROW")) {
          throw new RuntimeException("The variable binlog_format should be set to ROW, but it is : " + binlogFormat);
        }
      });
    }

    checkOperations.add(database -> {
      List<String> image = database.resultSetQuery(connection -> {
        final String sql = "show variables where Variable_name = 'binlog_row_image'";

        return connection.createStatement().executeQuery(sql);
      }, resultSet -> resultSet.getString("Value")).collect(toList());

      if (image.size() != 1) {
        throw new RuntimeException("Could not query the variable binlog_row_image");
      }

      String binlogRowImage = image.get(0);
      if (!binlogRowImage.equalsIgnoreCase("FULL")) {
        throw new RuntimeException("The variable binlog_row_image should be set to FULL, but it is : " + binlogRowImage);
      }
    });

    return checkOperations;
  }

  @Override
  public AirbyteCatalog discover(JsonNode config) throws Exception {
    AirbyteCatalog catalog = super.discover(config);

    if (isCdc(config)) {
      final List<AirbyteStream> streams = catalog.getStreams().stream()
          .map(MySqlSource::removeIncrementalWithoutPk)
          .map(MySqlSource::setIncrementalToSourceDefined)
          .map(MySqlSource::addCdcMetadataColumns)
          .collect(toList());

      catalog.setStreams(streams);
    }

    return catalog;
  }

  @Override
  public JsonNode toDatabaseConfig(JsonNode config) {
    final StringBuilder jdbc_url = new StringBuilder(String.format("jdbc:mysql://%s:%s/%s",
        config.get("host").asText(),
        config.get("port").asText(),
        config.get("database").asText()));
    // see MySqlJdbcStreamingQueryConfiguration for more context on why useCursorFetch=true is needed.
    jdbc_url.append("?useCursorFetch=true");
    if (config.get("jdbc_url_params") != null && !config.get("jdbc_url_params").asText().isEmpty()) {
      jdbc_url.append("&").append(config.get("jdbc_url_params").asText());
    }
    ImmutableMap.Builder<Object, Object> configBuilder = ImmutableMap.builder()
        .put("username", config.get("username").asText())
        .put("jdbc_url", jdbc_url.toString());

    if (config.has("password")) {
      configBuilder.put("password", config.get("password").asText());
    }

    return Jsons.jsonNode(configBuilder.build());
  }

  private static boolean isCdc(JsonNode config) {
    return config.hasNonNull("replication_method")
        && ReplicationMethod.valueOf(config.get("replication_method").asText())
            .equals(ReplicationMethod.CDC);
  }

  private static boolean shouldUseCDC(ConfiguredAirbyteCatalog catalog) {
    Optional<SyncMode> any = catalog.getStreams().stream().map(ConfiguredAirbyteStream::getSyncMode)
        .filter(syncMode -> syncMode == SyncMode.INCREMENTAL).findAny();
    return any.isPresent();
  }

  @Override
  public List<AutoCloseableIterator<AirbyteMessage>> getIncrementalIterators(JdbcDatabase database,
                                                                             ConfiguredAirbyteCatalog catalog,
                                                                             Map<String, TableInfo<CommonField<JDBCType>>> tableNameToTable,
                                                                             StateManager stateManager,
                                                                             Instant emittedAt) {
    JsonNode sourceConfig = database.getSourceConfig();
    if (isCdc(sourceConfig) && shouldUseCDC(catalog)) {
      LOGGER.info("using CDC: {}", true);
      // TODO: Figure out how to set the isCDC of stateManager to true. Its always false
      final AirbyteFileOffsetBackingStore offsetManager = initializeState(stateManager);
      AirbyteSchemaHistoryStorage schemaHistoryManager = initializeDBHistory(stateManager);
      FilteredFileDatabaseHistory.setDatabaseName(sourceConfig.get("database").asText());
      /**
       * We use 10000 as capacity cause the default queue size and batch size of debezium is :
       * {@link io.debezium.config.CommonConnectorConfig#DEFAULT_MAX_BATCH_SIZE} is 2048
       * {@link io.debezium.config.CommonConnectorConfig#DEFAULT_MAX_QUEUE_SIZE} is 8192
       */
      final LinkedBlockingQueue<ChangeEvent<String, String>> queue = new LinkedBlockingQueue<>(10000);
      final DebeziumRecordPublisher publisher = new DebeziumRecordPublisher(sourceConfig, catalog, offsetManager, schemaHistoryManager);
      publisher.start(queue);

      Optional<TargetFilePosition> targetFilePosition = TargetFilePosition
          .targetFilePosition(database);

      // handle state machine around pub/sub logic.
      final AutoCloseableIterator<ChangeEvent<String, String>> eventIterator = new DebeziumRecordIterator(
          queue,
          targetFilePosition,
          publisher::hasClosed,
          publisher::close);

      // convert to airbyte message.
      final AutoCloseableIterator<AirbyteMessage> messageIterator = AutoCloseableIterators
          .transform(
              eventIterator,
              (event) -> DebeziumEventUtils.toAirbyteMessage(event, emittedAt));

      // our goal is to get the state at the time this supplier is called (i.e. after all message records
      // have been produced)
      final Supplier<AirbyteMessage> stateMessageSupplier = () -> {
        Map<String, String> offset = offsetManager.readMap();
        String dbHistory = schemaHistoryManager.read();

        Map<String, Object> state = new HashMap<>();
        state.put(MYSQL_CDC_OFFSET, offset);
        state.put(MYSQL_DB_HISTORY, dbHistory);

        final JsonNode asJson = Jsons.jsonNode(state);

        LOGGER.info("debezium state: {}", asJson);

        CdcState cdcState = new CdcState().withState(asJson);
        stateManager.getCdcStateManager().setCdcState(cdcState);
        final AirbyteStateMessage stateMessage = stateManager.emit();
        return new AirbyteMessage().withType(Type.STATE).withState(stateMessage);

      };

      // wrap the supplier in an iterator so that we can concat it to the message iterator.
      final Iterator<AirbyteMessage> stateMessageIterator = MoreIterators
          .singletonIteratorFromSupplier(stateMessageSupplier);

      // this structure guarantees that the debezium engine will be closed, before we attempt to emit the
      // state file. we want this so that we have a guarantee that the debezium offset file (which we use
      // to produce the state file) is up-to-date.
      final CompositeIterator<AirbyteMessage> messageIteratorWithStateDecorator = AutoCloseableIterators
          .concatWithEagerClose(messageIterator,
              AutoCloseableIterators.fromIterator(stateMessageIterator));

      return Collections.singletonList(messageIteratorWithStateDecorator);
    } else {
      LOGGER.info("using CDC: {}", false);
      return super.getIncrementalIterators(database, catalog, tableNameToTable, stateManager,
          emittedAt);
    }
  }

  @Override
  public Set<String> getExcludedInternalNameSpaces() {
    return Set.of(
        "information_schema",
        "mysql",
        "performance_schema",
        "sys");
  }

  public static void main(String[] args) throws Exception {
    final Source source = new MySqlSource();
    LOGGER.info("starting source: {}", MySqlSource.class);
    new IntegrationRunner(source).run(args);
    LOGGER.info("completed source: {}", MySqlSource.class);
  }

  public enum ReplicationMethod {
    STANDARD,
    CDC
  }

}
