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

package io.airbyte.integrations.source.jdbc;

import io.airbyte.db.DataTypeUtils;
import io.airbyte.protocol.models.JsonSchemaPrimitive;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.List;
import java.util.StringJoiner;
import javax.xml.bind.DatatypeConverter;

public class SourceJdbcUtils {

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
          preparedStatement.setTimestamp(parameterIndex, Timestamp
              .from(DataTypeUtils.DATE_FORMAT.parse(value).toInstant()));
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

  /**
   * Given a database connection and identifiers, adds db-specific quoting to each identifier.
   *
   * @param connection database connection
   * @param identifiers identifiers to quote
   * @return quoted identifiers
   * @throws SQLException throws if there are any issues fulling the quoting metadata from the db.
   */
  public static String enquoteIdentifierList(Connection connection, List<String> identifiers) throws SQLException {
    final StringJoiner joiner = new StringJoiner(",");
    for (String col : identifiers) {
      String s = enquoteIdentifier(connection, col);
      joiner.add(s);
    }
    return joiner.toString();
  }

  /**
   * Given a database connection and identifier, adds db-specific quoting.
   *
   * @param connection database connection
   * @param identifier identifier to quote
   * @return quoted identifier
   * @throws SQLException throws if there are any issues fulling the quoting metadata from the db.
   */
  public static String enquoteIdentifier(Connection connection, String identifier) throws SQLException {
    final String identifierQuoteString = connection.getMetaData().getIdentifierQuoteString();

    return identifierQuoteString + identifier + identifierQuoteString;
  }

  /**
   * Create a fully qualified table name (including schema). e.g. public.my_table
   *
   * @param schemaName name of schema, if exists (CAN BE NULL)
   * @param tableName name of the table
   * @return fully qualified table name
   */
  public static String getFullyQualifiedTableName(String schemaName, String tableName) {
    return schemaName != null ? schemaName + "." + tableName : tableName;
  }

  /**
   * Create a fully qualified table name (including schema) with db-specific quoted syntax. e.g.
   * "public"."my_table"
   *
   * @param connection connection to jdbc database (gives access to proper quotes)
   * @param schemaName name of schema, if exists (CAN BE NULL)
   * @param tableName name of the table
   * @return fully qualified table name, using db-specific quoted syntax
   * @throws SQLException throws if fails to pull correct quote character.
   */
  public static String getFullyQualifiedTableNameWithQuoting(Connection connection, String schemaName, String tableName) throws SQLException {
    final String quotedTableName = enquoteIdentifier(connection, tableName);
    return schemaName != null ? enquoteIdentifier(connection, schemaName) + "." + quotedTableName : quotedTableName;
  }

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
