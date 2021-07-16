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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.commons.functional.CheckedFunction;
import io.airbyte.commons.json.Jsons;
import io.airbyte.db.DataTypeUtils;
import io.airbyte.protocol.models.JsonSchemaPrimitive;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.Collections;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.xml.bind.DatatypeConverter;

public class JdbcUtils {

  /**
   * Map records returned in a result set.
   *
   * @param resultSet the result set
   * @param mapper function to make each record of the result set
   * @param <T> type that each record will be mapped to
   * @return stream of records that the result set is mapped to.
   */
  public static <T> Stream<T> toStream(ResultSet resultSet, CheckedFunction<ResultSet, T, SQLException> mapper) {
    return StreamSupport.stream(new Spliterators.AbstractSpliterator<>(Long.MAX_VALUE, Spliterator.ORDERED) {

      @Override
      public boolean tryAdvance(Consumer<? super T> action) {
        try {
          if (!resultSet.next()) {
            return false;
          }
          action.accept(mapper.apply(resultSet));
          return true;
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }

    }, false);
  }

  /**
   * Collect each record of a ResultSet into a list of JsonNode.
   *
   * @param resultSet the result set
   * @return Stream of JsonNode.
   * @throws SQLException exceptions throws when parsing the ResultSet.
   */
  public static Stream<JsonNode> toJsonStream(ResultSet resultSet) throws SQLException {
    return toStream(resultSet, JdbcUtils::rowToJson);
  }

  public static JsonNode rowToJson(ResultSet r) throws SQLException {
    // the first call communicates with the database. after that the result is cached.
    final int columnCount = r.getMetaData().getColumnCount();
    final ObjectNode jsonNode = (ObjectNode) Jsons.jsonNode(Collections.emptyMap());

    for (int i = 1; i <= columnCount; i++) {
      // attempt to access the column. this allows us to know if it is null before we do type-specific
      // parsing. if it is null, we can move on. while awkward, this seems to be the agreed upon way of
      // checking for null values with jdbc.
      r.getObject(i);
      if (r.wasNull()) {
        continue;
      }

      // convert to java types that will convert into reasonable json.
      setJsonField(r, i, jsonNode);
    }

    return jsonNode;
  }

  private static JDBCType safeGetJdbcType(int columnTypeInt) {
    try {
      return JDBCType.valueOf(columnTypeInt);
    } catch (Exception e) {
      return JDBCType.VARCHAR;
    }
  }

  private static void setJsonField(ResultSet r, int i, ObjectNode o) throws SQLException {
    final int columnTypeInt = r.getMetaData().getColumnType(i);
    final String columnName = r.getMetaData().getColumnName(i);
    final JDBCType columnType = safeGetJdbcType(columnTypeInt);

    // https://www.cis.upenn.edu/~bcpierce/courses/629/jdkdocs/guide/jdbc/getstart/mapping.doc.html
    switch (columnType) {
      case BIT, BOOLEAN -> o.put(columnName, r.getBoolean(i));
      case TINYINT, SMALLINT -> o.put(columnName, r.getShort(i));
      case INTEGER -> putInteger(o, columnName, r, i);
      case BIGINT -> o.put(columnName, DataTypeUtils.returnNullIfInvalid(() -> r.getLong(i)));
      case FLOAT, DOUBLE -> o.put(columnName, DataTypeUtils
          .returnNullIfInvalid(() -> r.getDouble(i), Double::isFinite));
      case REAL -> o.put(columnName, DataTypeUtils.returnNullIfInvalid(() -> r.getFloat(i), Float::isFinite));
      case NUMERIC, DECIMAL -> o.put(columnName, DataTypeUtils.returnNullIfInvalid(() -> r.getBigDecimal(i)));
      case CHAR, VARCHAR, LONGVARCHAR -> o.put(columnName, r.getString(i));
      case DATE -> o.put(columnName, DataTypeUtils.toISO8601String(r.getDate(i)));
      case TIME -> o.put(columnName, DataTypeUtils.toISO8601String(r.getTime(i)));
      case TIMESTAMP -> {
        // https://www.cis.upenn.edu/~bcpierce/courses/629/jdkdocs/guide/jdbc/getstart/mapping.doc.html
        final Timestamp t = r.getTimestamp(i);
        java.util.Date d = new java.util.Date(t.getTime() + (t.getNanos() / 1000000));
        o.put(columnName, DataTypeUtils.toISO8601String(d));
      }
      case BLOB, BINARY, VARBINARY, LONGVARBINARY -> o.put(columnName, r.getBytes(i));
      default -> o.put(columnName, r.getString(i));
    }
  }

  /**
   * In some sources Integer might have value larger than {@link Integer#MAX_VALUE}. E.q. MySQL has
   * unsigned Integer type, which can contain value 3428724653. If we fail to cast Integer value, we
   * will try to cast Long.
   */
  private static void putInteger(ObjectNode node, String columnName, ResultSet resultSet, int index) {
    try {
      node.put(columnName, resultSet.getInt(index));
    } catch (SQLException e) {
      node.put(columnName, DataTypeUtils.returnNullIfInvalid(() -> resultSet.getLong(index)));
    }
  }

  // todo (cgardens) - move generic date helpers to commons.

  public static void setStatementField(PreparedStatement preparedStatement,
                                       int parameterIndex,
                                       JDBCType cursorFieldType,
                                       String value)
      throws SQLException {
    switch (cursorFieldType) {
      // parse time, and timestamp the same way. this seems to not cause an problems and allows us
      // to treat them all as ISO8601. if this causes any problems down the line, we can adjust.
      // Parsing TIME as a TIMESTAMP might potentially break for ClickHouse cause it doesn't expect TIME
      // value in the following format
      case TIME, TIMESTAMP -> {
        try {
          preparedStatement.setTimestamp(parameterIndex, Timestamp.from(
              DataTypeUtils.DATE_FORMAT.parse(value).toInstant()));
        } catch (ParseException e) {
          throw new RuntimeException(e);
        }
      }

      case DATE -> {
        try {
          Timestamp from = Timestamp.from(DataTypeUtils.DATE_FORMAT.parse(value).toInstant());
          preparedStatement.setDate(parameterIndex, new Date(from.getTime()));
        } catch (ParseException e) {
          throw new RuntimeException(e);
        }
      }
      // todo (cgardens) - currently we do not support bit because it requires special handling in the
      // prepared statement.
      // see
      // https://www.postgresql-archive.org/Problems-with-BIT-datatype-and-preparedStatment-td5733533.html.
      // case BIT -> preparedStatement.setString(parameterIndex, value);
      case BOOLEAN -> preparedStatement.setBoolean(parameterIndex, Boolean.parseBoolean(value));
      case TINYINT, SMALLINT -> preparedStatement.setShort(parameterIndex, Short.parseShort(value));
      case INTEGER -> preparedStatement.setInt(parameterIndex, Integer.parseInt(value));
      case BIGINT -> preparedStatement.setLong(parameterIndex, Long.parseLong(value));
      case FLOAT, DOUBLE -> preparedStatement.setDouble(parameterIndex, Double.parseDouble(value));
      case REAL -> preparedStatement.setFloat(parameterIndex, Float.parseFloat(value));
      case NUMERIC, DECIMAL -> preparedStatement.setBigDecimal(parameterIndex, new BigDecimal(value));
      case CHAR, NCHAR, NVARCHAR, VARCHAR, LONGVARCHAR -> preparedStatement.setString(parameterIndex, value);
      case BINARY, BLOB -> preparedStatement.setBytes(parameterIndex, DatatypeConverter.parseHexBinary(value));
      // since cursor are expected to be comparable, handle cursor typing strictly and error on
      // unrecognized types
      default -> throw new IllegalArgumentException(String.format("%s is not supported.", cursorFieldType));
    }
  }

  // the switch statement intentionally has duplicates so that its structure matches the type switch
  // statement above.

  @SuppressWarnings("DuplicateBranchesInSwitch")
  public static JsonSchemaPrimitive getType(JDBCType jdbcType) {
    return switch (jdbcType) {
      case BIT, BOOLEAN -> JsonSchemaPrimitive.BOOLEAN;
      case TINYINT, SMALLINT -> JsonSchemaPrimitive.NUMBER;
      case INTEGER -> JsonSchemaPrimitive.NUMBER;
      case BIGINT -> JsonSchemaPrimitive.NUMBER;
      case FLOAT, DOUBLE -> JsonSchemaPrimitive.NUMBER;
      case REAL -> JsonSchemaPrimitive.NUMBER;
      case NUMERIC, DECIMAL -> JsonSchemaPrimitive.NUMBER;
      case CHAR, NCHAR, NVARCHAR, VARCHAR, LONGVARCHAR -> JsonSchemaPrimitive.STRING;
      case DATE -> JsonSchemaPrimitive.STRING;
      case TIME -> JsonSchemaPrimitive.STRING;
      case TIMESTAMP -> JsonSchemaPrimitive.STRING;
      case BLOB, BINARY, VARBINARY, LONGVARBINARY -> JsonSchemaPrimitive.STRING;
      // since column types aren't necessarily meaningful to Airbyte, liberally convert all unrecgonised
      // types to String
      default -> JsonSchemaPrimitive.STRING;
    };
  }

}
