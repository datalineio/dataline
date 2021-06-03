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

package io.airbyte.db.jdbc;

import io.airbyte.commons.functional.CheckedConsumer;
import io.airbyte.commons.functional.CheckedFunction;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Database object for interacting with a JDBC connection.
 */
public interface JdbcDatabase extends AutoCloseable {

  /**
   * Execute a database query.
   *
   * @param query the query to execute against the database.
   * @throws SQLException SQL related exceptions.
   */
  void execute(CheckedConsumer<Connection, SQLException> query) throws SQLException;

  default void execute(String sql) throws SQLException {
    execute(connection -> connection.createStatement().execute(sql));
  }

  default void executeParameterized(String sql, String... params) throws SQLException {
    execute(connection -> {
      final PreparedStatement stmt = connection.prepareStatement(sql, params);
      stmt.execute();
    });
  }

  default void executeWithinTransaction(List<String> queries) throws SQLException {
    execute(connection -> {
      connection.setAutoCommit(false);
      for (String s : queries) {
        connection.createStatement().execute(s);
      }
      connection.commit();
      connection.setAutoCommit(true);
    });
  }

  /**
   * Use a connection to create a {@link ResultSet} and map it into a list. The entire
   * {@link ResultSet} will be buffered in memory before the list is returned. The caller does not
   * need to worry about closing any database resources.
   *
   * @param query execute a query using a {@link Connection} to get a {@link ResultSet}.
   * @param recordTransform transform each record of that result set into the desired type. do NOT
   *        just pass the {@link ResultSet} through. it is a stateful object will not be accessible if
   *        returned from recordTransform.
   * @param <T> type that each record will be mapped to.
   * @return Result of the query mapped to a list.
   * @throws SQLException SQL related exceptions.
   */
  <T> List<T> bufferedResultSetQuery(CheckedFunction<Connection, ResultSet, SQLException> query,
                                     CheckedFunction<ResultSet, T, SQLException> recordTransform)
      throws SQLException;

  /**
   * Use a connection to create a {@link ResultSet} and map it into a stream. You CANNOT assume that
   * data will be returned from this method before the entire {@link ResultSet} is buffered in memory.
   * Review the implementation of the database's JDBC driver or use the StreamingJdbcDriver if you
   * need this guarantee. The caller should close the returned stream to release the database
   * connection.
   *
   * @param query execute a query using a {@link Connection} to get a {@link ResultSet}.
   * @param recordTransform transform each record of that result set into the desired type. do NOT
   *        just pass the {@link ResultSet} through. it is a stateful object will not be accessible if
   *        returned from recordTransform.
   * @param <T> type that each record will be mapped to.
   * @return Result of the query mapped to a stream.
   * @throws SQLException SQL related exceptions.
   */
  <T> Stream<T> resultSetQuery(CheckedFunction<Connection, ResultSet, SQLException> query,
                               CheckedFunction<ResultSet, T, SQLException> recordTransform)
      throws SQLException;

  /**
   * Use a connection to create a {@link PreparedStatement} and map it into a stream. You CANNOT
   * assume that data will be returned from this method before the entire {@link ResultSet} is
   * buffered in memory. Review the implementation of the database's JDBC driver or use the
   * StreamingJdbcDriver if you need this guarantee. The caller should close the returned stream to
   * release the database connection.
   *
   * @param statementCreator create a {@link PreparedStatement} from a {@link Connection}.
   * @param recordTransform transform each record of that result set into the desired type. do NOT
   *        just pass the {@link ResultSet} through. it is a stateful object will not be accessible if
   *        returned from recordTransform.
   * @param <T> type that each record will be mapped to.
   * @return Result of the query mapped to a stream.
   * @throws SQLException SQL related exceptions.
   */
  <T> Stream<T> query(CheckedFunction<Connection, PreparedStatement, SQLException> statementCreator,
                      CheckedFunction<ResultSet, T, SQLException> recordTransform)
      throws SQLException;

  default <T> Optional<T> querySingle(String sql, CheckedFunction<ResultSet, T, SQLException> recordTransform, String... params)
      throws SQLException {
    return query(c -> {
      var stmt = c.prepareStatement(sql);
      int i = 1;
      for (String param : params) {
        stmt.setString(i, param);
        ++i;
      }
      return stmt;
    }, recordTransform).findFirst();
  }

  default <T> Optional<T> querySingle(String sql, CheckedFunction<ResultSet, T, SQLException> recordTransform, Object... params)
      throws SQLException {
    return query(c -> {
      var stmt = c.prepareStatement(sql);
      int i = 1;
      for (Object param : params) {
        stmt.setObject(i, param);
        ++i;
      }
      return stmt;
    }, recordTransform).findFirst();
  }

}
