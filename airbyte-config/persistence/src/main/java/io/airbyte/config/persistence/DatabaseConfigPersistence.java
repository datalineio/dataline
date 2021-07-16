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

package io.airbyte.config.persistence;

import static io.airbyte.config.persistence.AirbyteConfigsTable.AIRBYTE_CONFIGS;
import static io.airbyte.config.persistence.AirbyteConfigsTable.CONFIG_BLOB;
import static io.airbyte.config.persistence.AirbyteConfigsTable.CONFIG_ID;
import static io.airbyte.config.persistence.AirbyteConfigsTable.CONFIG_TYPE;
import static io.airbyte.config.persistence.AirbyteConfigsTable.CREATED_AT;
import static io.airbyte.config.persistence.AirbyteConfigsTable.UPDATED_AT;
import static org.jooq.impl.DSL.asterisk;
import static org.jooq.impl.DSL.select;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ConfigSchema;
import io.airbyte.db.Database;
import io.airbyte.db.ExceptionWrappingDatabase;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.jooq.JSONB;
import org.jooq.Record;
import org.jooq.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabaseConfigPersistence implements ConfigPersistence {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseConfigPersistence.class);

  private final ExceptionWrappingDatabase database;

  public DatabaseConfigPersistence(Database database) {
    this.database = new ExceptionWrappingDatabase(database);
  }

  /**
   * Initialize the database by creating the {@code airbyte_configs} table.
   */
  public DatabaseConfigPersistence initialize(String schema) throws IOException {
    database.transaction(ctx -> {
      boolean hasConfigsTable = ctx.fetchExists(select()
          .from("information_schema.tables")
          .where("table_name = 'airbyte_configs'"));
      if (hasConfigsTable) {
        return null;
      }
      LOGGER.info("Config database has not been initialized");
      LOGGER.info("Creating tables with schema: {}", schema);
      ctx.execute(schema);
      return null;
    });
    return this;
  }

  /**
   * Populate the {@code airbyte_configs} table with configs from the seed persistence. Only do so if
   * the table is empty. Otherwise, we assume that it has been populated.
   */
  public DatabaseConfigPersistence loadData(ConfigPersistence seedConfigPersistence) throws IOException {
    database.transaction(ctx -> {
      boolean isInitialized = ctx.fetchExists(select().from(AIRBYTE_CONFIGS));
      if (isInitialized) {
        LOGGER.info("Config database is not empty; skipping config seeding and copying");
        return null;
      }

      LOGGER.info("Loading data to config database...");
      Map<ConfigSchema, Stream<JsonNode>> seedConfigs;
      try {
        seedConfigs = seedConfigPersistence.dumpConfigs()
            .entrySet().stream()
            .collect(Collectors.toMap(e -> ConfigSchema.valueOf(e.getKey()), Entry::getValue));
      } catch (IOException e) {
        throw new SQLException(e);
      }
      Timestamp timestamp = Timestamp.from(Instant.ofEpochMilli(System.currentTimeMillis()));
      seedConfigs.forEach((configType, value) -> value.forEach(node -> {
        Object config = Jsons.object(node, configType.getClassName());
        ctx.insertInto(AIRBYTE_CONFIGS)
            .set(CONFIG_ID, configType.getId(config))
            .set(CONFIG_TYPE, configType.name())
            .set(CONFIG_BLOB, JSONB.valueOf(Jsons.serialize(node)))
            .set(CREATED_AT, timestamp)
            .set(UPDATED_AT, timestamp)
            .execute();
      }));

      LOGGER.info("Config database data loading completed");
      return null;
    });
    return this;
  }

  @Override
  public <T> T getConfig(ConfigSchema configType, String configId, Class<T> clazz)
      throws ConfigNotFoundException, JsonValidationException, IOException {
    Result<Record> result = database.query(ctx -> ctx.select(asterisk())
        .from(AIRBYTE_CONFIGS)
        .where(CONFIG_TYPE.eq(configType.name()), CONFIG_ID.eq(configId))
        .fetch());

    if (result.isEmpty()) {
      throw new ConfigNotFoundException(configType, configId);
    } else if (result.size() > 1) {
      throw new IllegalStateException(String.format("Multiple %s configs found for ID %s: %s", configType, configId, result));
    }

    return Jsons.deserialize(result.get(0).get(CONFIG_BLOB).data(), clazz);
  }

  @Override
  public <T> List<T> listConfigs(ConfigSchema configType, Class<T> clazz) throws IOException {
    Result<Record> results = database.query(ctx -> ctx.select(asterisk())
        .from(AIRBYTE_CONFIGS)
        .where(CONFIG_TYPE.eq(configType.name()))
        .orderBy(CONFIG_TYPE, CONFIG_ID)
        .fetch());
    return results.stream()
        .map(record -> Jsons.deserialize(record.get(CONFIG_BLOB).data(), clazz))
        .collect(Collectors.toList());
  }

  @Override
  public <T> void writeConfig(ConfigSchema configType, String configId, T config) throws IOException {
    database.transaction(ctx -> {
      boolean isExistingConfig = ctx.fetchExists(select()
          .from(AIRBYTE_CONFIGS)
          .where(CONFIG_TYPE.eq(configType.name()), CONFIG_ID.eq(configId)));

      Timestamp timestamp = Timestamp.from(Instant.ofEpochMilli(System.currentTimeMillis()));

      if (isExistingConfig) {
        int updateCount = ctx.update(AIRBYTE_CONFIGS)
            .set(CONFIG_BLOB, JSONB.valueOf(Jsons.serialize(config)))
            .set(UPDATED_AT, timestamp)
            .where(CONFIG_TYPE.eq(configType.name()), CONFIG_ID.eq(configId))
            .execute();
        if (updateCount != 0 && updateCount != 1) {
          LOGGER.warn("{} config {} has been updated; updated record count: {}", configType, configId, updateCount);
        }

        return null;
      }

      int insertionCount = ctx.insertInto(AIRBYTE_CONFIGS)
          .set(CONFIG_ID, configId)
          .set(CONFIG_TYPE, configType.name())
          .set(CONFIG_BLOB, JSONB.valueOf(Jsons.serialize(config)))
          .set(CREATED_AT, timestamp)
          .set(UPDATED_AT, timestamp)
          .execute();
      if (insertionCount != 1) {
        LOGGER.warn("{} config {} has been inserted; insertion record count: {}", configType, configId, insertionCount);
      }

      return null;
    });
  }

  @Override
  public <T> void replaceAllConfigs(Map<ConfigSchema, Stream<T>> configs, boolean dryRun) throws IOException {
    if (dryRun) {
      return;
    }

    Timestamp timestamp = Timestamp.from(Instant.ofEpochMilli(System.currentTimeMillis()));

    database.transaction(ctx -> {
      ctx.truncate(AIRBYTE_CONFIGS).restartIdentity().execute();
      configs.forEach((configType, value) -> value.forEach(config -> ctx.insertInto(AIRBYTE_CONFIGS)
          .set(CONFIG_ID, configType.getId(config))
          .set(CONFIG_TYPE, configType.name())
          .set(CONFIG_BLOB, JSONB.valueOf(Jsons.serialize(config)))
          .set(CREATED_AT, timestamp)
          .set(UPDATED_AT, timestamp)
          .execute()));
      return null;
    });
  }

  @Override
  public Map<String, Stream<JsonNode>> dumpConfigs() throws IOException {
    Map<String, Result<Record>> results = database.query(ctx -> ctx.select(asterisk())
        .from(AIRBYTE_CONFIGS)
        .orderBy(CONFIG_TYPE, CONFIG_ID)
        .fetchGroups(CONFIG_TYPE));
    return results.entrySet().stream().collect(Collectors.toMap(
        Entry::getKey,
        e -> e.getValue().stream().map(r -> Jsons.deserialize(r.get(CONFIG_BLOB).data()))));
  }

}
