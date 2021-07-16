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

package io.airbyte.db.bigquery;

import io.airbyte.db.ContextQueryFunction;
import io.airbyte.db.Database;
import io.airbyte.db.Databases;
import java.sql.SQLException;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DefaultDSLContext;

/**
 * This class is a temporary and will be removed as part of the issue @TODO #4547
 */
public class TempBigQueryJoolDatabaseImpl extends Database {

  private final BigQueryDatabase realDatabase;

  public TempBigQueryJoolDatabaseImpl(final String projectId, final String jsonCreds) {
    super(null, null);
    realDatabase = Databases.createBigQueryDatabase(projectId, jsonCreds);
  }

  @Override
  public <T> T query(ContextQueryFunction<T> transform) throws SQLException {
    return transform.query(new FakeDefaultDSLContext(realDatabase));
  }

  @Override
  public <T> T transaction(ContextQueryFunction<T> transform) throws SQLException {
    return transform.query(new FakeDefaultDSLContext(realDatabase));
  }

  @Override
  public void close() throws Exception {
    realDatabase.close();
  }

  public BigQueryDatabase getRealDatabase() {
    return realDatabase;
  }

  private static class FakeDefaultDSLContext extends DefaultDSLContext {

    private final BigQueryDatabase database;

    public FakeDefaultDSLContext(BigQueryDatabase database) {
      super((SQLDialect) null);
      this.database = database;
    }

    @Override
    public Result<Record> fetch(String sql) throws DataAccessException {
      try {
        database.execute(sql);
      } catch (SQLException e) {
        throw new DataAccessException(e.getMessage());
      }
      return null;
    }

  }

}
