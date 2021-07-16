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

package io.airbyte.integrations.source.bigquery;

import static io.airbyte.integrations.source.bigquery.BigQuerySource.CONFIG_CREDS;
import static io.airbyte.integrations.source.bigquery.BigQuerySource.CONFIG_DATASET_ID;
import static io.airbyte.integrations.source.bigquery.BigQuerySource.CONFIG_DATASET_LOCATION;
import static io.airbyte.integrations.source.bigquery.BigQuerySource.CONFIG_PROJECT_ID;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.common.collect.ImmutableMap;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.string.Strings;
import io.airbyte.db.Database;
import io.airbyte.db.bigquery.TempBigQueryJoolDatabaseImpl;
import io.airbyte.integrations.standardtest.source.SourceComprehensiveTest;
import io.airbyte.integrations.standardtest.source.TestDataHolder;
import io.airbyte.integrations.standardtest.source.TestDestinationEnv;
import io.airbyte.protocol.models.JsonSchemaPrimitive;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
public class BigQuerySourceComprehensiveTest extends SourceComprehensiveTest {

  private static final Path CREDENTIALS_PATH = Path.of("secrets/credentials.json");
  private static final String CREATE_SQL_PATTERN = "CREATE TABLE %1$s(%2$s NUMERIC(29), %3$s %4$s)";

  private TempBigQueryJoolDatabaseImpl database;
  private Dataset dataset;
  private JsonNode config;

  @Override
  protected String getImageName() {
    return "airbyte/source-bigquery:dev";
  }

  @Override
  protected JsonNode getConfig() throws Exception {
    return config;
  }

  @Override
  protected void tearDown(TestDestinationEnv testEnv) throws Exception {

  }

  @Override
  protected Database setupDatabase() throws Exception {
    if (!Files.exists(CREDENTIALS_PATH)) {
      throw new IllegalStateException(
          "Must provide path to a big query credentials file. By default {module-root}/" + CREDENTIALS_PATH
              + ". Override by setting setting path with the CREDENTIALS_PATH constant.");
    }

    final String credentialsJsonString = new String(Files.readAllBytes(CREDENTIALS_PATH));

    final JsonNode credentialsJson = Jsons.deserialize(credentialsJsonString);
    final String projectId = credentialsJson.get(CONFIG_PROJECT_ID).asText();
    final String datasetLocation = "US";

    final String datasetId = Strings.addRandomSuffix("airbyte_tests_compr", "_", 8);

    config = Jsons.jsonNode(ImmutableMap.builder()
        .put(CONFIG_PROJECT_ID, projectId)
        .put(CONFIG_CREDS, credentialsJsonString)
        .put(CONFIG_DATASET_ID, datasetId)
        .put(CONFIG_DATASET_LOCATION, datasetLocation)
        .build());

    database = new TempBigQueryJoolDatabaseImpl(config.get(CONFIG_PROJECT_ID).asText(), credentialsJsonString);

    final DatasetInfo datasetInfo =
        DatasetInfo.newBuilder(config.get(CONFIG_DATASET_ID).asText()).setLocation(config.get(CONFIG_DATASET_LOCATION).asText()).build();
    dataset = database.getRealDatabase().getBigQuery().create(datasetInfo);

    return database;
  }

  @Override
  protected void initTests() {
    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("int64")
            .airbyteType(JsonSchemaPrimitive.NUMBER)
            .createTablePatternSql(CREATE_SQL_PATTERN)
            .addInsertValues("null", "-128", "127", "9223372036854775807", "-9223372036854775808")
            .addExpectedValues(null, "-128", "127", "9223372036854775807", "-9223372036854775808")
            .build());

    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("int")
            .airbyteType(JsonSchemaPrimitive.NUMBER)
            .createTablePatternSql(CREATE_SQL_PATTERN)
            .addInsertValues("null", "-128", "127")
            .addExpectedValues(null, "-128", "127")
            .build());

    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("smallint")
            .airbyteType(JsonSchemaPrimitive.NUMBER)
            .createTablePatternSql(CREATE_SQL_PATTERN)
            .addInsertValues("null", "-128", "127")
            .addExpectedValues(null, "-128", "127")
            .build());

    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("integer")
            .airbyteType(JsonSchemaPrimitive.NUMBER)
            .createTablePatternSql(CREATE_SQL_PATTERN)
            .addInsertValues("null", "-128", "127")
            .addExpectedValues(null, "-128", "127")
            .build());

    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("bigint")
            .airbyteType(JsonSchemaPrimitive.NUMBER)
            .createTablePatternSql(CREATE_SQL_PATTERN)
            .addInsertValues("null", "-128", "127")
            .addExpectedValues(null, "-128", "127")
            .build());

    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("tinyint")
            .airbyteType(JsonSchemaPrimitive.NUMBER)
            .createTablePatternSql(CREATE_SQL_PATTERN)
            .addInsertValues("null", "-128", "127")
            .addExpectedValues(null, "-128", "127")
            .build());

    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("byteint")
            .airbyteType(JsonSchemaPrimitive.NUMBER)
            .createTablePatternSql(CREATE_SQL_PATTERN)
            .addInsertValues("null", "-128", "127")
            .addExpectedValues(null, "-128", "127")
            .build());

    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("numeric")
            .fullSourceDataType("numeric(29,9)")
            .airbyteType(JsonSchemaPrimitive.NUMBER)
            .createTablePatternSql(CREATE_SQL_PATTERN)
            .addInsertValues("null", "-128", "127", "999999999999999999", "-999999999999999999", "0.123456789", "-0.123456789")
            .addExpectedValues(null, "-128", "127", "999999999999999999", "-999999999999999999", "0.123456789", "-0.123456789")
            .build());

    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("bignumeric")
            .fullSourceDataType("bignumeric(76,38)")
            .airbyteType(JsonSchemaPrimitive.NUMBER)
            .createTablePatternSql(CREATE_SQL_PATTERN)
            .addInsertValues("null", "-128", "127", "999999999999999999", "-999999999999999999", "0.123456789", "-0.123456789")
            .addExpectedValues(null, "-128", "127", "999999999999999999", "-999999999999999999", "0.123456789", "-0.123456789")
            .build());

    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("decimal")
            .fullSourceDataType("decimal(29,9)")
            .airbyteType(JsonSchemaPrimitive.NUMBER)
            .createTablePatternSql(CREATE_SQL_PATTERN)
            .addInsertValues("null", "-128", "127", "999999999999999999", "-999999999999999999", "0.123456789", "-0.123456789")
            .addExpectedValues(null, "-128", "127", "999999999999999999", "-999999999999999999", "0.123456789", "-0.123456789")
            .build());

    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("bigdecimal")
            .fullSourceDataType("bigdecimal(76,38)")
            .airbyteType(JsonSchemaPrimitive.NUMBER)
            .createTablePatternSql(CREATE_SQL_PATTERN)
            .addInsertValues("null", "-128", "127", "999999999999999999", "-999999999999999999", "0.123456789", "-0.123456789")
            .addExpectedValues(null, "-128", "127", "999999999999999999", "-999999999999999999", "0.123456789", "-0.123456789")
            .build());

    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("float64")
            .airbyteType(JsonSchemaPrimitive.NUMBER)
            .createTablePatternSql(CREATE_SQL_PATTERN)
            .addInsertValues("null", "-128", "127", "0.123456789", "-0.123456789")
            .addExpectedValues(null, "-128.0", "127.0", "0.123456789", "-0.123456789")
            .build());

    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("bool")
            .airbyteType(JsonSchemaPrimitive.BOOLEAN)
            .createTablePatternSql(CREATE_SQL_PATTERN)
            .addInsertValues("true", "false", "null")
            .addExpectedValues("true", "false", null)
            .build());

    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("bytes")
            .airbyteType(JsonSchemaPrimitive.STRING)
            .createTablePatternSql(CREATE_SQL_PATTERN)
            .addInsertValues("FROM_BASE64(\"test\")", "null")
            .addExpectedValues("test", null)
            .build());

    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("date")
            .airbyteType(JsonSchemaPrimitive.STRING)
            .createTablePatternSql(CREATE_SQL_PATTERN)
            .addInsertValues("date('2021-10-20')", "date('9999-12-31')", "date('0001-01-01')", "null")
            .addExpectedValues("2021-10-20T00:00:00Z", "9999-12-31T00:00:00Z", "0001-01-01T00:00:00Z", null)
            .build());

    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("datetime")
            .airbyteType(JsonSchemaPrimitive.STRING)
            .createTablePatternSql(CREATE_SQL_PATTERN)
            .addInsertValues("datetime('2021-10-20 11:22:33')", "datetime('9999-12-31 11:22:33')", "datetime('0001-01-01 11:22:33')", "null")
            .addExpectedValues("2021-10-20T11:22:33Z", "9999-12-31T11:22:33Z", "0001-01-01T11:22:33Z", null)
            .build());

    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("timestamp")
            .airbyteType(JsonSchemaPrimitive.STRING)
            .createTablePatternSql(CREATE_SQL_PATTERN)
            .addInsertValues("timestamp('2021-10-20 11:22:33')", "null")
            .addExpectedValues("2021-10-20T11:22:33Z", null)
            .build());

    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("geography")
            .airbyteType(JsonSchemaPrimitive.STRING)
            .createTablePatternSql(CREATE_SQL_PATTERN)
            .addInsertValues("ST_GEOGFROMTEXT('POINT(1 2)')", "null")
            .addExpectedValues("POINT(1 2)", null)
            .build());

    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("string")
            .airbyteType(JsonSchemaPrimitive.STRING)
            .createTablePatternSql(CREATE_SQL_PATTERN)
            .addInsertValues("'qwe'", "'йцу'", "null")
            .addExpectedValues("qwe", "йцу", null)
            .build());

    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("struct")
            .fullSourceDataType("STRUCT<course STRING,id INT64>")
            .airbyteType(JsonSchemaPrimitive.STRING)
            .createTablePatternSql(CREATE_SQL_PATTERN)
            .addInsertValues("STRUCT(\"B.A\",12)", "null")
            .build());

    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("time")
            .airbyteType(JsonSchemaPrimitive.STRING)
            .createTablePatternSql(CREATE_SQL_PATTERN)
            .addInsertValues("TIME(15, 30, 00)", "null")
            .addExpectedValues("15:30:00", null)
            .build());

    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("array")
            .fullSourceDataType("array<String>")
            .airbyteType(JsonSchemaPrimitive.STRING)
            .createTablePatternSql(CREATE_SQL_PATTERN)
            .addInsertValues("['a', 'b']")
            .addExpectedValues("[{\"test_column\":\"a\"},{\"test_column\":\"b\"}]")
            .build());

    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("struct")
            .fullSourceDataType("STRUCT<frst String, sec int64, obbj STRUCT<id_col int64, mega_obbj STRUCT<last_col time>>>")
            .airbyteType(JsonSchemaPrimitive.STRING)
            .createTablePatternSql(CREATE_SQL_PATTERN)
            .addInsertValues("STRUCT('s' as frst, 1 as sec, STRUCT(555 as id_col, STRUCT(TIME(15, 30, 00) as time) as mega_obbj) as obbj)")
            .addExpectedValues("{\"frst\":\"s\",\"sec\":1,\"obbj\":{\"id_col\":555,\"mega_obbj\":{\"last_col\":\"15:30:00\"}}}")
            .build());
  }

  @Override
  protected String getNameSpace() {
    return dataset.getDatasetId().getDataset();
  }

  @AfterAll
  public void cleanTestInstance() {
    database.getRealDatabase().cleanDataSet(getNameSpace());
  }

}
