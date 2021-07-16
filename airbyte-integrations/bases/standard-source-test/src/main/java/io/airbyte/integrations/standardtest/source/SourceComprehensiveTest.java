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

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import io.airbyte.db.Database;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteMessage.Type;
import io.airbyte.protocol.models.CatalogHelpers;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.ConfiguredAirbyteStream;
import io.airbyte.protocol.models.DestinationSyncMode;
import io.airbyte.protocol.models.Field;
import io.airbyte.protocol.models.JsonSchemaPrimitive;
import io.airbyte.protocol.models.SyncMode;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SourceComprehensiveTest extends SourceAbstractTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(SourceComprehensiveTest.class);

  private final List<TestDataHolder> testDataHolders = new ArrayList<>();

  /**
   * The column name will be used for a PK column in the test tables. Override it if default name is
   * not valid for your source.
   *
   * @return Id column name
   */
  protected String getIdColumnName() {
    return "id";
  }

  /**
   * The column name will be used for a test column in the test tables. Override it if default name is
   * not valid for your source.
   *
   * @return Test column name
   */
  protected String getTestColumnName() {
    return "test_column";
  }

  /**
   * Setup the test database. All tables and data described in the registered tests will be put there.
   *
   * @return configured test database
   * @throws Exception - might throw any exception during initialization.
   */
  protected abstract Database setupDatabase() throws Exception;

  /**
   * Put all required tests here using method {@link #addDataTypeTestData(TestDataHolder)}
   */
  protected abstract void initTests();

  @Override
  protected void setupEnvironment(TestDestinationEnv environment) throws Exception {
    setupDatabaseInternal();
  }

  /**
   * Provide a source namespace. It's allocated place for table creation. It also known ask "Database
   * Schema" or "Dataset"
   *
   * @return source name space
   */
  protected abstract String getNameSpace();

  /**
   * The test checks that connector can fetch prepared data without failure.
   */
  @Test
  public void testDataTypes() throws Exception {
    ConfiguredAirbyteCatalog catalog = getConfiguredCatalog();
    List<AirbyteMessage> allMessages = runRead(catalog);
    final List<AirbyteMessage> recordMessages = allMessages.stream().filter(m -> m.getType() == Type.RECORD).collect(Collectors.toList());
    Map<String, List<String>> expectedValues = new HashMap<>();
    testDataHolders.forEach(testDataHolder -> {
      if (!testDataHolder.getExpectedValues().isEmpty())
        expectedValues.put(testDataHolder.getNameWithTestPrefix(), testDataHolder.getExpectedValues());
    });

    recordMessages.forEach(msg -> {
      String streamName = msg.getRecord().getStream();
      List<String> expectedValuesForStream = expectedValues.get(streamName);
      if (expectedValuesForStream != null) {
        String value = getValueFromJsonNode(msg.getRecord().getData().get(getTestColumnName()));
        assertTrue(expectedValuesForStream.contains(value),
            "Returned value '" + value + "' by streamer " + streamName + " should be in the expected list: " + expectedValuesForStream);
        expectedValuesForStream.remove(value);
      }
    });

    expectedValues.forEach((streamName, values) -> assertTrue(values.isEmpty(),
        "The streamer " + streamName + " should return all expected values. Missing values: " + values));
  }

  private String getValueFromJsonNode(JsonNode jsonNode) {
    if (jsonNode != null) {
      String nodeText = jsonNode.asText();
      String nodeString = jsonNode.toString();
      String value = (nodeText != null && !nodeText.equals("") ? nodeText : nodeString);
      value = (value != null && value.equals("null") ? null : value);
      return value;
    } else
      return null;
  }

  /**
   * Creates all tables and insert data described in the registered data type tests.
   *
   * @throws Exception might raise exception if configuration goes wrong or tables creation/insert
   *         scripts failed.
   */
  private void setupDatabaseInternal() throws Exception {
    Database database = setupDatabase();

    initTests();

    for (TestDataHolder test : testDataHolders) {
      database.query(ctx -> {
        ctx.fetch(test.getCreateSqlQuery());
        LOGGER.debug("Table " + test.getNameWithTestPrefix() + " is created.");
        test.getInsertSqlQueries().forEach(ctx::fetch);
        return null;
      });
    }

    database.close();
  }

  /**
   * Configures streams for all registered data type tests.
   *
   * @return configured catalog
   */
  private ConfiguredAirbyteCatalog getConfiguredCatalog() throws Exception {
    final JsonNode config = getConfig();

    return new ConfiguredAirbyteCatalog().withStreams(
        testDataHolders
            .stream()
            .map(test -> new ConfiguredAirbyteStream()
                .withSyncMode(SyncMode.INCREMENTAL)
                .withCursorField(Lists.newArrayList(getIdColumnName()))
                .withDestinationSyncMode(DestinationSyncMode.APPEND)
                .withStream(CatalogHelpers.createAirbyteStream(
                    String.format("%s", test.getNameWithTestPrefix()),
                    String.format("%s", getNameSpace()),
                    Field.of(getIdColumnName(), JsonSchemaPrimitive.NUMBER),
                    Field.of(getTestColumnName(), test.getAirbyteType()))
                    .withSourceDefinedCursor(true)
                    .withSourceDefinedPrimaryKey(List.of(List.of(getIdColumnName())))
                    .withSupportedSyncModes(
                        Lists.newArrayList(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL))))
            .collect(Collectors.toList()));
  }

  /**
   * Register your test in the run scope. For each test will be created a table with one column of
   * specified type. Note! If you register more than one test with the same type name, they will be
   * run as independent tests with own streams.
   *
   * @param test comprehensive data type test
   */
  public void addDataTypeTestData(TestDataHolder test) {
    testDataHolders.add(test);
    test.setTestNumber(testDataHolders.stream().filter(t -> t.getSourceType().equals(test.getSourceType())).count());
    test.setNameSpace(getNameSpace());
    test.setIdColumnName(getIdColumnName());
    test.setTestColumnName(getTestColumnName());
  }

  private String formatCollection(Collection<String> collection) {
    return collection.stream().map(s -> "`" + s + "`").collect(Collectors.joining(", "));
  }

  /**
   * Builds a table with all registered test cases with values using Markdown syntax (can be used in
   * the github).
   *
   * @return formatted list of test cases
   */
  public String getMarkdownTestTable() {
    StringBuilder table = new StringBuilder()
        .append("|**Data Type**|**Insert values**|**Expected values**|**Comment**|**Common test result**|\n")
        .append("|----|----|----|----|----|\n");

    testDataHolders.forEach(test -> table.append(String.format("| %s | %s | %s | %s | %s |\n",
        test.getSourceType(),
        formatCollection(test.getValues()),
        formatCollection(test.getExpectedValues()),
        "",
        "Ok")));
    return table.toString();
  }

  protected void printMarkdownTestTable() {
    LOGGER.info(getMarkdownTestTable());
  }

}
