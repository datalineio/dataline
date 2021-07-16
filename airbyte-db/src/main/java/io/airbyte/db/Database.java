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

import java.io.Closeable;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

/**
 * Database object for interacting with a Jooq connection.
 */
public class Database implements AutoCloseable {

  private final DataSource ds;
  private final SQLDialect dialect;

  public Database(final DataSource ds, final SQLDialect dialect) {
    this.ds = ds;
    this.dialect = dialect;
  }

  public <T> T query(ContextQueryFunction<T> transform) throws SQLException {
    return transform.query(DSL.using(ds, dialect));
  }

  public <T> T transaction(ContextQueryFunction<T> transform) throws SQLException {
    return DSL.using(ds, dialect).transactionResult(configuration -> transform.query(DSL.using(configuration)));
  }

  @Override
  public void close() throws Exception {
    // Just a safety in case we are using a datasource implementation that requires closing.
    // BasicDataSource from apache does since it also provides a pooling mechanism to reuse connections.

    if (ds instanceof AutoCloseable) {
      ((AutoCloseable) ds).close();
    }
    if (ds instanceof Closeable) {
      ((Closeable) ds).close();
    }
  }

}
