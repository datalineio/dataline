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

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.db.jdbc.DefaultJdbcDatabase;
import io.airbyte.db.jdbc.JdbcDatabase;
import io.airbyte.db.jdbc.JdbcStreamingQueryConfiguration;
import io.airbyte.db.jdbc.StreamingJdbcDatabase;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.commons.dbcp2.BasicDataSource;
import org.jooq.SQLDialect;

public class Databases {

  public static final String POSTGRES_DRIVER = "org.postgresql.Driver";
  public static final String REDSHIFT_DRIVER = "com.amazon.redshift.jdbc.Driver";
  public static final String MSSQL_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";

  public static Database createPostgresDatabase(String username, String password, String jdbcConnectionString) {
    return createDatabase(username, password, jdbcConnectionString, POSTGRES_DRIVER, SQLDialect.POSTGRES);
  }

  public static JdbcDatabase createRedshiftDatabase(String username, String password, String jdbcConnectionString) {
    return createJdbcDatabase(username, password, jdbcConnectionString, REDSHIFT_DRIVER);
  }

  public static Database createSqlServerDatabase(String username, String password, String jdbcConnectionString) {
    return createDatabase(username, password, jdbcConnectionString, MSSQL_DRIVER, SQLDialect.DEFAULT);
  }

  public static Database createDatabase(final String username,
                                        final String password,
                                        final String jdbcConnectionString,
                                        final String driverClassName,
                                        final SQLDialect dialect) {
    final BasicDataSource connectionPool = createBasicDataSource(username, password, jdbcConnectionString, driverClassName);

    return new Database(connectionPool, dialect);
  }

  public static JdbcDatabase createJdbcDatabase(final String username,
                                                final String password,
                                                final String jdbcConnectionString,
                                                final String driverClassName) {
    final BasicDataSource connectionPool = createBasicDataSource(username, password, jdbcConnectionString, driverClassName);

    return new DefaultJdbcDatabase(connectionPool);
  }

  public static JdbcDatabase createJdbcDatabase(final String username,
                                                final String password,
                                                final String jdbcConnectionString,
                                                final String driverClassName,
                                                final String connectionProperties) {
    final BasicDataSource connectionPool =
        createBasicDataSource(username, password, jdbcConnectionString, driverClassName, Optional.of(connectionProperties));

    return new DefaultJdbcDatabase(connectionPool);
  }

  public static JdbcDatabase createStreamingJdbcDatabase(final String username,
                                                         final String password,
                                                         final String jdbcConnectionString,
                                                         final String driverClassName,
                                                         final JdbcStreamingQueryConfiguration jdbcStreamingQuery) {
    final BasicDataSource connectionPool = createBasicDataSource(username, password, jdbcConnectionString, driverClassName);

    final JdbcDatabase defaultJdbcDatabase = createJdbcDatabase(username, password, jdbcConnectionString, driverClassName);
    return new StreamingJdbcDatabase(connectionPool, defaultJdbcDatabase, jdbcStreamingQuery);
  }

  private static BasicDataSource createBasicDataSource(final String username,
                                                       final String password,
                                                       final String jdbcConnectionString,
                                                       final String driverClassName) {
    return createBasicDataSource(username, password, jdbcConnectionString, driverClassName,
        Optional.empty());
  }

  private static BasicDataSource createBasicDataSource(final String username,
                                                       final String password,
                                                       final String jdbcConnectionString,
                                                       final String driverClassName,
                                                       final Optional<String> connectionProperties) {
    final BasicDataSource connectionPool = new BasicDataSource();
    connectionPool.setDriverClassName(driverClassName);
    connectionPool.setUsername(username);
    connectionPool.setPassword(password);
    connectionPool.setUrl(jdbcConnectionString);
    connectionProperties.ifPresent(connectionPool::setConnectionProperties);
    return connectionPool;
  }

  public static String getPostgresJdbcUrl(JsonNode config) {
    List<String> additionalParameters = new ArrayList<>();

    final StringBuilder jdbcUrl = new StringBuilder(String.format("jdbc:postgresql://%s:%s/%s?",
        config.get("host").asText(),
        config.get("port").asText(),
        config.get("database").asText()));

    if (config.has("ssl") && config.get("ssl").asBoolean()) {
      additionalParameters.add("ssl=true");
      additionalParameters.add("sslmode=require");
    }

    if (!additionalParameters.isEmpty()) {
      additionalParameters.forEach(x -> jdbcUrl.append(x).append("&"));
    }
    return jdbcUrl.toString();
  }

}
