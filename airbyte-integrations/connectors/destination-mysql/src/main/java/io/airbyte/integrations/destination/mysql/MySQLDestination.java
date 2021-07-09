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

package io.airbyte.integrations.destination.mysql;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import io.airbyte.commons.json.Jsons;
import io.airbyte.db.Databases;
import io.airbyte.db.jdbc.JdbcDatabase;
import io.airbyte.integrations.base.Destination;
import io.airbyte.integrations.base.IntegrationRunner;
import io.airbyte.integrations.destination.jdbc.AbstractJdbcDestination;
import io.airbyte.integrations.destination.mysql.MySQLSqlOperations.VersionCompatibility;
import io.airbyte.protocol.models.AirbyteConnectionStatus;
import io.airbyte.protocol.models.AirbyteConnectionStatus.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySQLDestination extends AbstractJdbcDestination implements Destination {

  private static final Logger LOGGER = LoggerFactory.getLogger(MySQLDestination.class);

  public static final String DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";
  public static final String TEST_DRIVER_CLASS = "org.mariadb.jdbc.Driver";

  @Override
  public AirbyteConnectionStatus check(JsonNode config) {
    try (final JdbcDatabase database = getDatabase(config)) {
      MySQLSqlOperations mySQLSqlOperations = (MySQLSqlOperations) getSqlOperations();

      String outputSchema = getNamingResolver().getIdentifier(config.get("database").asText());
      attemptSQLCreateAndDropTableOperations(outputSchema, database, getNamingResolver(),
          mySQLSqlOperations);

      mySQLSqlOperations.verifyLocalFileEnabled(database);

      VersionCompatibility compatibility = mySQLSqlOperations.isCompatibleVersion(database);
      if (!compatibility.isCompatible()) {
        throw new RuntimeException(String
            .format("Your MySQL version %s is not compatible with Airbyte",
                compatibility.getVersion()));
      }

      return new AirbyteConnectionStatus().withStatus(Status.SUCCEEDED);
    } catch (Exception e) {
      LOGGER.error("Exception while checking connection: ", e);
      return new AirbyteConnectionStatus()
          .withStatus(Status.FAILED)
          .withMessage("Could not connect with provided configuration. \n" + e.getMessage());
    }
  }

  public MySQLDestination() {
    super(DRIVER_CLASS, new MySQLNameTransformer(), new MySQLSqlOperations());
  }

  @Override
  protected JdbcDatabase getDatabase(JsonNode config) {
    final JsonNode jdbcConfig = toJdbcConfig(config);

    return Databases.createJdbcDatabase(
        jdbcConfig.get("username").asText(),
        jdbcConfig.has("password") ? jdbcConfig.get("password").asText() : null,
        jdbcConfig.get("jdbc_url").asText(),
        getDriverClass(),
        "allowLoadLocalInfile=true");
  }

  @Override
  public JsonNode toJdbcConfig(JsonNode config) {
    final StringBuilder jdbcUrl = new StringBuilder(String.format("jdbc:mysql://%s:%s/%s",
        config.get("host").asText(),
        config.get("port").asText(),
        config.get("database").asText()));

    ImmutableMap.Builder<Object, Object> configBuilder = ImmutableMap.builder()
        .put("username", config.get("username").asText())
        .put("jdbc_url", jdbcUrl.toString());

    if (config.has("password")) {
      configBuilder.put("password", config.get("password").asText());
    }

    return Jsons.jsonNode(configBuilder.build());
  }

  public static void main(String[] args) throws Exception {
    final Destination destination = new MySQLDestination();
    LOGGER.info("starting destination: {}", MySQLDestination.class);
    new IntegrationRunner(destination).run(args);
    LOGGER.info("completed destination: {}", MySQLDestination.class);
  }

}
