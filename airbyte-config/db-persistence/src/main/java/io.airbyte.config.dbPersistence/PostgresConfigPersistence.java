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
import io.airbyte.config.ConfigSchema;
import io.airbyte.db.Databases;
import io.airbyte.validation.json.JsonSchemaValidator;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;

public class PostgresConfigPersistence extends DatabaseConfigPersistence {

  public PostgresConfigPersistence(String username, String password, String connectionString) {
    this(username, password, connectionString, new JsonSchemaValidator());
  }

  public PostgresConfigPersistence(JsonNode config) {
    this(config, new JsonSchemaValidator());
  }

  public PostgresConfigPersistence(JsonNode config, JsonSchemaValidator validator) {
    this(config.get("username").asText(), config.get("password").asText(), Databases.getPostgresJdbcUrl(config), validator);
  }

  public PostgresConfigPersistence(String username, String password, String connectionString, JsonSchemaValidator validator) {
    super(Databases.createJdbcDatabase(username, password, connectionString, Databases.POSTGRES_DRIVER), validator);
  }

  @Override
  // Create a table named CONFIG with three columns: CONFIG_ID (PK UUID/string), CONFIG_TYPE (string),
  // CONFIG_DATA (JSON/string)
  public void Setup() throws SQLException {
    database.execute("CREATE TABLE CONFIG (CONFIG_ID UUID PRIMARY KEY, CONFIG_TYPE VARCHAR(32) NOT NULL, CONFIG_DATA JSONB NOT NULL)");
  }

  @Override
  protected PreparedStatement writeConfigQuery(Connection conn, ConfigSchema configType, UUID configId, String data) throws SQLException {
    var result = conn.prepareStatement(
        "INSERT INTO CONFIG (CONFIG_ID, CONFIG_TYPE, CONFIG_DATA) VALUES (?, ?, CAST(? as jsonb)) ON CONFLICT (CONFIG_ID) DO UPDATE SET CONFIG_DATA = EXCLUDED.CONFIG_DATA");
    result.setObject(1, configId);
    result.setString(2, configType.toString());
    result.setString(3, data);
    return result;
  }

}
