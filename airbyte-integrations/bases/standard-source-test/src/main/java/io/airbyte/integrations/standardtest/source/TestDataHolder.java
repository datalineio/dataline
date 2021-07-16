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

package io.airbyte.integrations.standardtest.source;

import io.airbyte.protocol.models.ConfiguredAirbyteStream;
import io.airbyte.protocol.models.JsonSchemaPrimitive;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestDataHolder {

  private static final String DEFAULT_CREATE_TABLE_SQL = "CREATE TABLE %1$s(%2$s INTEGER PRIMARY KEY, %3$s %4$s)";
  private static final String DEFAULT_INSERT_SQL = "INSERT INTO %1$s VALUES (%2$s, %3$s)";

  private final String sourceType;
  private final JsonSchemaPrimitive airbyteType;
  private final List<String> values;
  private final List<String> expectedValues;
  private final String createTablePatternSql;
  private final String insertPatternSql;
  private final String fullSourceDataType;
  private String nameSpace;
  private long testNumber;
  private String idColumnName;
  private String testColumnName;

  TestDataHolder(String sourceType,
                 JsonSchemaPrimitive airbyteType,
                 List<String> values,
                 List<String> expectedValues,
                 String createTablePatternSql,
                 String insertPatternSql,
                 String fullSourceDataType) {
    this.sourceType = sourceType;
    this.airbyteType = airbyteType;
    this.values = values;
    this.expectedValues = expectedValues;
    this.createTablePatternSql = createTablePatternSql;
    this.insertPatternSql = insertPatternSql;
    this.fullSourceDataType = fullSourceDataType;
  }

  /**
   * The builder allows to setup any comprehensive data type test.
   *
   * @return builder for setup comprehensive test
   */
  public static TestDataHolderBuilder builder() {
    return new TestDataHolderBuilder();
  }

  public static class TestDataHolderBuilder {

    private String sourceType;
    private JsonSchemaPrimitive airbyteType;
    private final List<String> values = new ArrayList<>();
    private final List<String> expectedValues = new ArrayList<>();
    private String createTablePatternSql;
    private String insertPatternSql;
    private String fullSourceDataType;

    TestDataHolderBuilder() {
      this.createTablePatternSql = DEFAULT_CREATE_TABLE_SQL;
      this.insertPatternSql = DEFAULT_INSERT_SQL;
    }

    /**
     * The name of the source data type. Duplicates by name will be tested independently from each
     * others. Note that this name will be used for connector setup and table creation. If source syntax
     * requires more details (E.g. "varchar" type requires length "varchar(50)"), you can additionally
     * set custom data type syntax by {@link TestDataHolderBuilder#fullSourceDataType(String)} method.
     *
     * @param sourceType source data type name
     * @return builder
     */
    public TestDataHolderBuilder sourceType(String sourceType) {
      this.sourceType = sourceType;
      if (fullSourceDataType == null)
        fullSourceDataType = sourceType;
      return this;
    }

    /**
     * corresponding Airbyte data type. It requires for proper configuration
     * {@link ConfiguredAirbyteStream}
     *
     * @param airbyteType Airbyte data type
     * @return builder
     */
    public TestDataHolderBuilder airbyteType(JsonSchemaPrimitive airbyteType) {
      this.airbyteType = airbyteType;
      return this;
    }

    /**
     * Set custom the create table script pattern. Use it if you source uses untypical table creation
     * sql. Default patter described {@link #DEFAULT_CREATE_TABLE_SQL} Note! The patter should contain
     * four String place holders for the: - namespace.table name (as one placeholder together) - id
     * column name - test column name - test column data type
     *
     * @param createTablePatternSql creation table sql pattern
     * @return builder
     */
    public TestDataHolderBuilder createTablePatternSql(String createTablePatternSql) {
      this.createTablePatternSql = createTablePatternSql;
      return this;
    }

    /**
     * Set custom the insert record script pattern. Use it if you source uses untypical insert record
     * sql. Default patter described {@link #DEFAULT_INSERT_SQL} Note! The patter should contains two
     * String place holders for the table name and value.
     *
     * @param insertPatternSql creation table sql pattern
     * @return builder
     */
    public TestDataHolderBuilder insertPatternSql(String insertPatternSql) {
      this.insertPatternSql = insertPatternSql;
      return this;
    }

    /**
     * Allows to set extended data type for the table creation. E.g. The "varchar" type requires in
     * MySQL requires length. In this case fullSourceDataType will be "varchar(50)".
     *
     * @param fullSourceDataType actual string for the column data type description
     * @return builder
     */
    public TestDataHolderBuilder fullSourceDataType(String fullSourceDataType) {
      this.fullSourceDataType = fullSourceDataType;
      return this;
    }

    /**
     * Adds value(s) to the scope of a corresponding test. The values will be inserted into the created
     * table. Note! The value will be inserted into the insert script without any transformations. Make
     * sure that the value is in line with the source syntax.
     *
     * @param insertValue test value
     * @return builder
     */
    public TestDataHolderBuilder addInsertValues(String... insertValue) {
      this.values.addAll(Arrays.asList(insertValue));
      return this;
    }

    /**
     * Adds expected value(s) to the test scope. If you add at least one value, it will check that all
     * values are provided by corresponding streamer.
     *
     * @param expectedValue value which should be provided by a streamer
     * @return builder
     */
    public TestDataHolderBuilder addExpectedValues(String... expectedValue) {
      this.expectedValues.addAll(Arrays.asList(expectedValue));
      return this;
    }

    /**
     * Add NULL value to the expected value list. If you need to add only one value and it's NULL, you
     * have to use this method instead of {@link #addExpectedValues(String...)}
     *
     * @return builder
     */
    public TestDataHolderBuilder addNullExpectedValue() {
      this.expectedValues.add(null);
      return this;
    }

    public TestDataHolder build() {
      return new TestDataHolder(sourceType, airbyteType, values, expectedValues, createTablePatternSql, insertPatternSql, fullSourceDataType);
    }

  }

  void setNameSpace(String nameSpace) {
    this.nameSpace = nameSpace;
  }

  void setTestNumber(long testNumber) {
    this.testNumber = testNumber;
  }

  void setIdColumnName(String idColumnName) {
    this.idColumnName = idColumnName;
  }

  void setTestColumnName(String testColumnName) {
    this.testColumnName = testColumnName;
  }

  public String getSourceType() {
    return sourceType;
  }

  public JsonSchemaPrimitive getAirbyteType() {
    return airbyteType;
  }

  public List<String> getExpectedValues() {
    return expectedValues;
  }

  public List<String> getValues() {
    return values;
  }

  public String getNameWithTestPrefix() {
    return nameSpace + "_" + testNumber + "_" + sourceType;
  }

  public String getCreateSqlQuery() {
    return String.format(createTablePatternSql, (nameSpace != null ? nameSpace + "." : "") + getNameWithTestPrefix(), idColumnName, testColumnName,
        fullSourceDataType);
  }

  public List<String> getInsertSqlQueries() {
    List<String> insertSqls = new ArrayList<>();
    int rowId = 1;
    for (String value : values) {
      insertSqls.add(String.format(insertPatternSql, (nameSpace != null ? nameSpace + "." : "") + getNameWithTestPrefix(), rowId++, value));
    }
    return insertSqls;
  }

}
