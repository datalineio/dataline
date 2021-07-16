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

import static org.jooq.impl.DSL.select;

import io.airbyte.commons.lang.Exceptions;
import io.airbyte.db.jdbc.DefaultJdbcDatabase;
import io.airbyte.db.jdbc.JdbcDatabase;
import io.airbyte.db.jdbc.JdbcStreamingQueryConfiguration;
import io.airbyte.db.jdbc.StreamingJdbcDatabase;
import java.util.Optional;
import java.util.function.Function;
import org.apache.commons.dbcp2.BasicDataSource;
import org.jooq.SQLDialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Databases {

  private static final Logger LOGGER = LoggerFactory.getLogger(Databases.class);

  // The Job Database is initialized by SQL script, which writes a server UUID at the end.
  // So this database is ready when the server UUID record is present.
  public static final Function<Database, Boolean> IS_JOB_DATABASE_READY = database -> {
    try {
      Optional<String> uuid = ServerUuid.get(database);
      return uuid.isPresent();
    } catch (Exception e) {
      return false;
    }
  };
  public static final Function<Database, Boolean> IS_CONFIG_DATABASE_CONNECTED = database -> {
    try {
      LOGGER.info("Testing config database connection...");
      return database.query(ctx -> ctx.fetchExists(select().from("information_schema.tables")));
    } catch (Exception e) {
      return false;
    }
  };
  public static final Function<Database, Boolean> IS_CONFIG_DATABASE_LOADED_WITH_DATA = database -> {
    try {
      LOGGER.info("Testing if airbyte_configs has been created...");
      return database.query(ctx -> ctx.fetchExists(select().from("airbyte_configs")));
    } catch (Exception e) {
      return false;
    }
  };

  public static Database createPostgresDatabase(String username, String password, String jdbcConnectionString) {
    return createDatabase(username, password, jdbcConnectionString, "org.postgresql.Driver", SQLDialect.POSTGRES);
  }

  public static Database createPostgresDatabaseWithRetry(String username,
                                                         String password,
                                                         String jdbcConnectionString,
                                                         Function<Database, Boolean> isDbReady) {
    Database database = null;

    while (database == null) {
      LOGGER.warn("Waiting for database to become available...");

      try {
        database = createPostgresDatabase(username, password, jdbcConnectionString);
        if (!isDbReady.apply(database)) {
          LOGGER.info("Database is not ready yet. Please wait a moment, it might still be initializing...");
          database = null;
          Exceptions.toRuntime(() -> Thread.sleep(5000));
        }
      } catch (Exception e) {
        // Ignore the exception because this likely means that the database server is still initializing.
        LOGGER.warn("Ignoring exception while trying to request database:", e);
        database = null;
        Exceptions.toRuntime(() -> Thread.sleep(5000));
      }
    }

    LOGGER.info("Database available!");
    return database;
  }

  public static JdbcDatabase createRedshiftDatabase(String username, String password, String jdbcConnectionString) {
    return createJdbcDatabase(username, password, jdbcConnectionString, "com.amazon.redshift.jdbc.Driver");
  }

  public static Database createMySqlDatabase(String username, String password, String jdbcConnectionString) {
    return createDatabase(username, password, jdbcConnectionString, "com.mysql.cj.jdbc.Driver", SQLDialect.MYSQL);
  }

  public static Database createSqlServerDatabase(String username, String password, String jdbcConnectionString) {
    return createDatabase(username, password, jdbcConnectionString, "com.microsoft.sqlserver.jdbc.SQLServerDriver", SQLDialect.DEFAULT);
  }

  public static Database createOracleDatabase(String username, String password, String jdbcConnectionString) {
    return createDatabase(username, password, jdbcConnectionString, "oracle.jdbc.OracleDriver", SQLDialect.DEFAULT);
  }

  public static Database createDatabase(final String username,
                                        final String password,
                                        final String jdbcConnectionString,
                                        final String driverClassName,
                                        final SQLDialect dialect) {
    final BasicDataSource connectionPool = createBasicDataSource(username, password, jdbcConnectionString, driverClassName);

    return new Database(connectionPool, dialect);
  }

  public static Database createDatabase(final String username,
                                        final String password,
                                        final String jdbcConnectionString,
                                        final String driverClassName,
                                        final SQLDialect dialect,
                                        final String connectionProperties) {
    final BasicDataSource connectionPool =
        createBasicDataSource(username, password, jdbcConnectionString, driverClassName, Optional.ofNullable(connectionProperties));

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
        createBasicDataSource(username, password, jdbcConnectionString, driverClassName, Optional.ofNullable(connectionProperties));

    return new DefaultJdbcDatabase(connectionPool);
  }

  public static JdbcDatabase createStreamingJdbcDatabase(final String username,
                                                         final String password,
                                                         final String jdbcConnectionString,
                                                         final String driverClassName,
                                                         final JdbcStreamingQueryConfiguration jdbcStreamingQuery,
                                                         final String connectionProperties) {
    final BasicDataSource connectionPool =
        createBasicDataSource(username, password, jdbcConnectionString, driverClassName, Optional.ofNullable(connectionProperties));

    final JdbcDatabase defaultJdbcDatabase =
        createJdbcDatabase(username, password, jdbcConnectionString, driverClassName, connectionProperties);
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

}
