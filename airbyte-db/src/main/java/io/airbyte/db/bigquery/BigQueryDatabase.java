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

import static java.util.Objects.isNull;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.api.gax.retrying.RetrySettings;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.Table;
import com.google.common.base.Charsets;
import com.google.common.collect.Streams;
import io.airbyte.db.SqlDatabase;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

public class BigQueryDatabase extends SqlDatabase {

  private static final Logger LOGGER = LoggerFactory.getLogger(BigQueryDatabase.class);

  private final BigQuery bigQuery;

  public BigQueryDatabase(String projectId, String jsonCreds) {
    try {
      BigQueryOptions.Builder bigQueryBuilder = BigQueryOptions.newBuilder();
      ServiceAccountCredentials credentials = null;
      if (jsonCreds != null && !jsonCreds.isEmpty()) {
        credentials = ServiceAccountCredentials
            .fromStream(new ByteArrayInputStream(jsonCreds.getBytes(Charsets.UTF_8)));
      }
      bigQuery = bigQueryBuilder
          .setProjectId(projectId)
          .setCredentials(!isNull(credentials) ? credentials : ServiceAccountCredentials.getApplicationDefault())
          .setRetrySettings(RetrySettings
              .newBuilder()
              .setMaxAttempts(10)
              .setRetryDelayMultiplier(1.5)
              .setTotalTimeout(Duration.ofMinutes(60))
              .build())
          .build()
          .getService();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void execute(String sql) throws SQLException {
    final ImmutablePair<Job, String> result = executeQuery(bigQuery, getQueryConfig(sql, Collections.emptyList()));
    if (result.getLeft() == null) {
      throw new SQLException("BigQuery request is failed with error: " + result.getRight() + ". SQL: " + sql);
    }
    LOGGER.info("BigQuery successfully finished execution SQL: " + sql);
  }

  public Stream<JsonNode> query(String sql) throws Exception {
    return query(sql, Collections.emptyList());
  }

  public Stream<JsonNode> query(String sql, QueryParameterValue... params) throws Exception {
    return query(sql, (params == null ? Collections.emptyList() : Arrays.asList(params)));
  }

  @Override
  public Stream<JsonNode> query(String sql, String... params) throws Exception {
    List<QueryParameterValue> parameterValueList;
    if (params == null)
      parameterValueList = Collections.emptyList();
    else
      parameterValueList = Arrays.stream(params).map(param -> QueryParameterValue.newBuilder().setValue(param).setType(
          StandardSQLTypeName.STRING).build()).collect(Collectors.toList());

    return query(sql, parameterValueList);
  }

  public Stream<JsonNode> query(String sql, List<QueryParameterValue> params) throws Exception {
    final ImmutablePair<Job, String> result = executeQuery(bigQuery, getQueryConfig(sql, params));

    if (result.getLeft() != null) {
      FieldList fieldList = result.getLeft().getQueryResults().getSchema().getFields();
      return Streams.stream(result.getLeft().getQueryResults().iterateAll())
          .map(fieldValues -> BigQueryUtils.rowToJson(fieldValues, fieldList));
    } else
      throw new Exception(
          "Failed to execute query " + sql + (params != null && !params.isEmpty() ? " with params " + params : "") + ". Error: " + result.getRight());
  }

  @Override
  public void close() throws Exception {
    /**
     * The BigQuery doesn't require connection close. It will be done automatically.
     */
  }

  public QueryJobConfiguration getQueryConfig(String sql, List<QueryParameterValue> params) {
    return QueryJobConfiguration
        .newBuilder(sql)
        .setUseLegacySql(false)
        .setPositionalParameters(params)
        .build();
  }

  public ImmutablePair<Job, String> executeQuery(BigQuery bigquery, QueryJobConfiguration queryConfig) {
    final JobId jobId = JobId.of(UUID.randomUUID().toString());
    final Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
    return executeQuery(queryJob);
  }

  /**
   * Returns full information about all tables from entire project
   *
   * @param projectId BigQuery project id
   * @return List of BigQuery tables
   */
  public List<Table> getProjectTables(String projectId) {
    List<Table> tableList = new ArrayList<>();
    bigQuery.listDatasets(projectId)
        .iterateAll()
        .forEach(dataset -> bigQuery.listTables(dataset.getDatasetId())
            .iterateAll()
            .forEach(table -> tableList.add(bigQuery.getTable(table.getTableId()))));
    return tableList;
  }

  /**
   * Returns full information about all tables from specific Dataset
   *
   * @param datasetId BigQuery dataset id
   * @return List of BigQuery tables
   */
  public List<Table> getDatasetTables(String datasetId) {
    List<Table> tableList = new ArrayList<>();
    bigQuery.listTables(datasetId)
        .iterateAll()
        .forEach(table -> tableList.add(bigQuery.getTable(table.getTableId())));
    return tableList;
  }

  public BigQuery getBigQuery() {
    return bigQuery;
  }

  public void cleanDataSet(String dataSetId) {
    // allows deletion of a dataset that has contents
    final BigQuery.DatasetDeleteOption option = BigQuery.DatasetDeleteOption.deleteContents();

    final boolean success = bigQuery.delete(dataSetId, option);
    if (success) {
      LOGGER.info("BQ Dataset " + dataSetId + " deleted...");
    } else {
      LOGGER.info("BQ Dataset cleanup for " + dataSetId + " failed!");
    }
  }

  private ImmutablePair<Job, String> executeQuery(Job queryJob) {
    final Job completedJob = waitForQuery(queryJob);
    if (completedJob == null) {
      throw new RuntimeException("Job no longer exists");
    } else if (completedJob.getStatus().getError() != null) {
      // You can also look at queryJob.getStatus().getExecutionErrors() for all
      // errors, not just the latest one.
      return ImmutablePair.of(null, (completedJob.getStatus().getError().toString()));
    }

    return ImmutablePair.of(completedJob, null);
  }

  private Job waitForQuery(Job queryJob) {
    try {
      return queryJob.waitFor();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
