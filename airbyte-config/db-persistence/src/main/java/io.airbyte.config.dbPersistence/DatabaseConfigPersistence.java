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

package io.airbyte.config.dbPersistence;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ConfigSchema;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigPersistence;
import io.airbyte.db.jdbc.JdbcDatabase;
import io.airbyte.validation.json.JsonSchemaValidator;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DatabaseConfigPersistence implements ConfigPersistence {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseConfigPersistence.class);

  protected final JdbcDatabase database;
  private final JsonSchemaValidator jsonSchemaValidator;

  protected DatabaseConfigPersistence(JdbcDatabase db, JsonSchemaValidator validator) {
    database = db;
    jsonSchemaValidator = validator;
  }

  // Create a table named CONFIG with three columns: CONFIG_ID (PK UUID/string), CONFIG_TYPE (string),
  // CONFIG_DATA (JSON/string)
  public abstract void Setup() throws SQLException;

  @Override
  public <T> T getConfig(ConfigSchema configType, UUID configId, Class<T> clazz)
      throws ConfigNotFoundException, JsonValidationException, IOException {
    try {
      Optional<String> data = database.querySingle("SELECT CONFIG_DATA FROM CONFIG WHERE CONFIG_TYPE = ? AND CONFIG_ID = ?",
          r -> r.getString("CONFIG_DATA"), configType.toString(), configId);
      if (!data.isPresent()) {
        throw new ConfigNotFoundException(configType, configId);
      }

      final T config = Jsons.deserialize(data.get(), clazz);
      validateJson(config, configType);
      return config;
    } catch (SQLException e) {
      throw new IOException(String.format("Failed to get config type %s item %s.  Reason: %s", configType, configId, e.getMessage()), e);
    }
  }

  @Override
  public <T> List<T> listConfigs(ConfigSchema configType, Class<T> clazz) throws JsonValidationException, IOException {
    try {
      List<T> results = database.query(c -> {
        var stmt = c.prepareStatement("SELECT CONFIG_DATA FROM CONFIG WHERE CONFIG_TYPE = ?");
        stmt.setString(1, configType.toString());
        return stmt;
      }, r -> r.getString("CONFIG_DATA"))
          .map(s -> (T) Jsons.deserialize(s, clazz))
          .collect(Collectors.toList());
      return results;
    } catch (SQLException e) {
      throw new IOException(String.format("Failed to get config type %s listing.  Reason: %s", configType, e.getMessage()), e);
    }
  }

  @Override
  public <T> void writeConfig(ConfigSchema configType, UUID configId, T config) throws JsonValidationException, IOException {
    // validate config with schema
    validateJson(Jsons.jsonNode(config), configType);
    final String data = Jsons.serialize(config);
    try {
      database.execute(c -> writeConfigQuery(c, configType, configId, data).execute());
    } catch (SQLException e) {
      throw new IOException(String.format("Failed to write config type %s item %s.  Reason: %s", configType, configId, e.getMessage()), e);
    }
  }

  // Made abstract because what we want for this is an upsert operation, which different databases
  // handle with different syntax
  // Overrides need to return a prepared statement with all 3 data elements added
  protected abstract PreparedStatement writeConfigQuery(Connection conn, ConfigSchema configType, UUID configId, String data) throws SQLException;

  private <T> void validateJson(T config, ConfigSchema configType) throws JsonValidationException {
    JsonNode schema = JsonSchemaValidator.getSchema(configType.getFile());
    jsonSchemaValidator.ensure(schema, Jsons.jsonNode(config));
  }

}
