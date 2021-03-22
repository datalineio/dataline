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

package io.airbyte.integrations.destination.redshift;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airbyte.db.Databases;
import io.airbyte.db.jdbc.JdbcDatabase;
import io.airbyte.db.jdbc.JdbcUtils;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import io.airbyte.protocol.models.SyncMode;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@Disabled("Disabled on automatic execution as the tests requires specific credentials to run."
    + "Intended to be a lighter-weight complement to the Docker integration tests. Fill in the appropriate blank variables to run.")
public class RedshiftCopierTest {

  // Fill in the empty variables before running.
  private static final String S3_KEY_ID = "";
  private static final String S3_KEY = "";
  private static final String REDSHIFT_CONNECTION_STRING = "";
  private static final String REDSHIFT_USER = "";
  private static final String REDSHIFT_PASS = "";

  private static final String S3_REGION = "us-west-2";
  private static final String TEST_BUCKET = "redshift-copier-test";
  private static final String RUN_FOLDER = "test-folder";
  private static final String SCHEMA_NAME = "public";
  private static final String STREAM_NAME = "redshift_copier_test";
  private static final String RAW_TABLE_NAME = new RedshiftSQLNameTransformer().getRawTableName(STREAM_NAME);

  private static final ObjectMapper mapper = new ObjectMapper();

  private static AmazonS3 s3Client;
  private static JdbcDatabase redshiftDb;

  @BeforeAll
  public static void setUp() {
    var awsCreds = new BasicAWSCredentials(S3_KEY_ID, S3_KEY);
    s3Client = AmazonS3ClientBuilder.standard()
        .withCredentials(new AWSStaticCredentialsProvider(awsCreds)).withRegion(S3_REGION)
        .build();

    if (!s3Client.doesBucketExistV2(TEST_BUCKET)) {
      s3Client.createBucket(TEST_BUCKET);
    }

    redshiftDb = Databases.createRedshiftDatabase(REDSHIFT_USER, REDSHIFT_PASS, REDSHIFT_CONNECTION_STRING);
  }

  @BeforeEach
  public void reset() throws SQLException {
    var sqlOp = new RedshiftSqlOperations();
    sqlOp.dropTableIfExists(redshiftDb, SCHEMA_NAME, RAW_TABLE_NAME);
  }

  @AfterAll
  public static void tearDown() {
    if (!s3Client.doesBucketExistV2(TEST_BUCKET)) {
      s3Client.deleteBucket(TEST_BUCKET);
    }
  }

  @Test
  @DisplayName("Should wipe table before appending new data")
  public void fullSyncTest() throws IOException, SQLException {
    var copier = new RedshiftCopier(TEST_BUCKET, RUN_FOLDER, SyncMode.FULL_REFRESH, SCHEMA_NAME, STREAM_NAME, s3Client, redshiftDb, S3_KEY_ID,
        S3_KEY, S3_REGION);

    AirbyteRecordMessage msg = getAirbyteRecordMessage();

    copier.copy(msg);
    copier.close(false);

    assertFalse(s3Client.doesObjectExist(TEST_BUCKET, RUN_FOLDER + "/" + STREAM_NAME));

    var recordList = redshiftDb.bufferedResultSetQuery(
        connection -> {
          final String sql = "SELECT * FROM " + RAW_TABLE_NAME + ";";
          return connection.prepareStatement(sql).executeQuery();
        },
        JdbcUtils::rowToJson);

    assertEquals(1, recordList.size());
  }

  @Test
  @DisplayName("Should append data without wiping table")
  public void incrementalTest() throws IOException, SQLException {

    var sqlOp = new RedshiftSqlOperations();
    sqlOp.createTableIfNotExists(redshiftDb, SCHEMA_NAME, RAW_TABLE_NAME);
    sqlOp.insertRecords(redshiftDb, List.of(getAirbyteRecordMessage()).stream(), SCHEMA_NAME, RAW_TABLE_NAME);

    var copier = new RedshiftCopier(TEST_BUCKET, RUN_FOLDER, SyncMode.INCREMENTAL, SCHEMA_NAME, STREAM_NAME, s3Client, redshiftDb, S3_KEY_ID,
        S3_KEY, S3_REGION);

    AirbyteRecordMessage msg = getAirbyteRecordMessage();

    copier.copy(msg);
    copier.close(false);

    assertFalse(s3Client.doesObjectExist(TEST_BUCKET, RUN_FOLDER + "/" + STREAM_NAME));

    var recordList = redshiftDb.bufferedResultSetQuery(
        connection -> {
          final String sql = "SELECT * FROM " + RAW_TABLE_NAME + ";";
          return connection.prepareStatement(sql).executeQuery();
        },
        JdbcUtils::rowToJson);

    System.out.println(recordList);
    assertEquals(2, recordList.size());
  }

  private AirbyteRecordMessage getAirbyteRecordMessage() {
    var data = mapper.createObjectNode();
    data.put("field1", "testValue");

    var msg = new AirbyteRecordMessage();
    msg.setStream(STREAM_NAME);
    msg.setData(data);
    msg.setEmittedAt(System.currentTimeMillis());
    return msg;
  }

  @DisplayName("When sending larger amounts of data")
  public class loadTest {

    public void sendMediumTableTest() {}

    public void sendLargeTableTest() {}

  }

}
