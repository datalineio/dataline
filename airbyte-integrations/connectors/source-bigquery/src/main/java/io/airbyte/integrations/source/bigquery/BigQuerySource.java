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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.Table;
import com.google.common.collect.ImmutableMap;
import io.airbyte.commons.functional.CheckedConsumer;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.commons.util.AutoCloseableIterators;
import io.airbyte.db.Databases;
import io.airbyte.db.SqlDatabase;
import io.airbyte.db.bigquery.BigQueryDatabase;
import io.airbyte.db.bigquery.BigQueryUtils;
import io.airbyte.integrations.base.IntegrationRunner;
import io.airbyte.integrations.base.Source;
import io.airbyte.integrations.source.relationaldb.AbstractRelationalDbSource;
import io.airbyte.integrations.source.relationaldb.TableInfo;
import io.airbyte.protocol.models.CommonField;
import io.airbyte.protocol.models.JsonSchemaPrimitive;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQuerySource extends AbstractRelationalDbSource<StandardSQLTypeName, BigQueryDatabase> implements Source {

  private static final Logger LOGGER = LoggerFactory.getLogger(BigQuerySource.class);

  public static final String CONFIG_DATASET_ID = "dataset_id";
  public static final String CONFIG_PROJECT_ID = "project_id";
  public static final String CONFIG_DATASET_LOCATION = "dataset_location";
  public static final String CONFIG_CREDS = "credentials_json";

  private String quote = "";
  private JsonNode dbConfig;

  @Override
  public JsonNode toDatabaseConfig(JsonNode config) {
    return Jsons.jsonNode(ImmutableMap.builder()
        .put(CONFIG_PROJECT_ID, config.get(CONFIG_PROJECT_ID).asText())
        .put(CONFIG_CREDS, config.get(CONFIG_CREDS).asText())
        .put(CONFIG_DATASET_ID, config.get(CONFIG_DATASET_ID).asText())
        .build());
  }

  @Override
  protected BigQueryDatabase createDatabase(JsonNode config) {
    dbConfig = Jsons.clone(config);
    return Databases.createBigQueryDatabase(config.get(CONFIG_PROJECT_ID).asText(), config.get(CONFIG_CREDS).asText());
  }

  @Override
  public List<CheckedConsumer<BigQueryDatabase, Exception>> getCheckOperations(JsonNode config) {
    List<CheckedConsumer<BigQueryDatabase, Exception>> checkList = new ArrayList<>();
    checkList.add(database -> {
      if (database.query("select 1").count() < 1)
        throw new Exception("Unable to execute any query on the source!");
      else
        LOGGER.info("The source passed the basic query test!");
    });

    checkList.add(database -> {
      if (isDatasetConfigured(database)) {
        database.query(String.format("select 1 from %s where 1=0",
            getFullTableName(getConfigDatasetId(database), "INFORMATION_SCHEMA.TABLES")));
        LOGGER.info("The source passed the Dataset query test!");
      } else {
        LOGGER.info("The Dataset query test is skipped due to not configured datasetId!");
      }
    });

    return checkList;
  }

  @Override
  protected JsonSchemaPrimitive getType(StandardSQLTypeName columnType) {
    return BigQueryUtils.getType(columnType);
  }

  @Override
  public Set<String> getExcludedInternalNameSpaces() {
    return Collections.emptySet();
  }

  @Override
  protected List<TableInfo<CommonField<StandardSQLTypeName>>> discoverInternal(BigQueryDatabase database) {
    String projectId = dbConfig.get(CONFIG_PROJECT_ID).asText();
    String datasetId = getConfigDatasetId(database);
    List<Table> tables =
        (isDatasetConfigured(database) ? database.getDatasetTables(getConfigDatasetId(database)) : database.getProjectTables(projectId));
    List<TableInfo<CommonField<StandardSQLTypeName>>> result = new ArrayList<>();
    tables.stream().map(table -> TableInfo.<CommonField<StandardSQLTypeName>>builder()
        .nameSpace(datasetId)
        .name(table.getTableId().getTable())
        .fields(Objects.requireNonNull(table.getDefinition().getSchema()).getFields().stream()
            .map(f -> {
              StandardSQLTypeName standardType = f.getType().getStandardType();
              return new CommonField<>(f.getName(), standardType);
            })
            .collect(Collectors.toList()))
        .build())
        .forEach(result::add);
    return result;
  }

  @Override
  protected Map<String, List<String>> discoverPrimaryKeys(BigQueryDatabase database, List<TableInfo<CommonField<StandardSQLTypeName>>> tableInfos) {
    return Collections.emptyMap();
  }

  @Override
  protected String getQuoteString() {
    return quote;
  }

  @Override
  public AutoCloseableIterator<JsonNode> queryTableIncremental(BigQueryDatabase database,
                                                               List<String> columnNames,
                                                               String schemaName,
                                                               String tableName,
                                                               String cursorField,
                                                               StandardSQLTypeName cursorFieldType,
                                                               String cursor) {
    return queryTableWithParams(database, String.format("SELECT %s FROM %s WHERE %s > ?",
        enquoteIdentifierList(columnNames),
        getFullTableName(schemaName, tableName),
        cursorField),
        BigQueryUtils.getQueryParameter(cursorFieldType, cursor));
  }

  private AutoCloseableIterator<JsonNode> queryTableWithParams(BigQueryDatabase database, String sqlQuery, QueryParameterValue... params) {
    return AutoCloseableIterators.lazyIterator(() -> {
      try {
        final Stream<JsonNode> stream = database.query(sqlQuery, params);
        return AutoCloseableIterators.fromStream(stream);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  private boolean isDatasetConfigured(SqlDatabase database) {
    return database.getSourceConfig().hasNonNull(CONFIG_DATASET_ID);
  }

  private String getConfigDatasetId(SqlDatabase database) {
    return (isDatasetConfigured(database) ? database.getSourceConfig().get(CONFIG_DATASET_ID).asText() : null);
  }

  public static void main(String[] args) throws Exception {
    final Source source = new BigQuerySource();
    LOGGER.info("starting source: {}", BigQuerySource.class);
    new IntegrationRunner(source).run(args);
    LOGGER.info("completed source: {}", BigQuerySource.class);
  }

}
