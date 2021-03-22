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

import alex.mojaki.s3upload.MultiPartOutputStream;
import alex.mojaki.s3upload.StreamTransferManager;
import com.amazonaws.services.s3.AmazonS3;
import com.google.common.annotations.VisibleForTesting;
import io.airbyte.commons.json.Jsons;
import io.airbyte.db.jdbc.JdbcDatabase;
import io.airbyte.integrations.destination.NamingConventionTransformer;
import io.airbyte.integrations.destination.jdbc.SqlOperations;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import io.airbyte.protocol.models.SyncMode;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedshiftCopier {

  private static final Logger LOGGER = LoggerFactory.getLogger(RedshiftCopier.class);
  private static final NamingConventionTransformer NAMING_RESOLVER = new RedshiftSQLNameTransformer();

  private static final int DEFAULT_UPLOAD_THREADS = 10; // The S3 cli uses 10 threads by default.
  private static final int DEFAULT_QUEUE_CAPACITY = DEFAULT_UPLOAD_THREADS;
  // The smallest part size is 5MB. An S3 upload can be maximally formed of 10,000 parts. This gives
  // us an upper limit of 10,000 * 10 / 1000 = 100 GB
  // per table with a 10MB part size limit.
  private static final int PART_SIZE_MB = 10;

  private final String runFolder;
  private final SyncMode syncMode;
  private final String schemaName;
  private final String streamName;
  private final AmazonS3 s3Client;
  private final JdbcDatabase database;
  private final String s3KeyId;
  private final String s3Key;
  private final String s3Region;

  private final StreamTransferManager multipartUploadManager;
  private final MultiPartOutputStream outputStream;
  private final CSVPrinter csvPrinter;

  public RedshiftCopier(String runFolder,
                        SyncMode syncMode,
                        String schema,
                        String streamName,
                        AmazonS3 client,
                        JdbcDatabase database,
                        String s3KeyId,
                        String s3key,
                        String s3Region)
      throws IOException {
    this.runFolder = runFolder;
    this.syncMode = syncMode;
    this.schemaName = schema;
    this.streamName = streamName;
    this.s3Client = client;
    this.database = database;
    this.s3KeyId = s3KeyId;
    this.s3Key = s3key;
    this.s3Region = s3Region;

    // The stream transfer manager lets us greedily stream into S3. The native AWS SDK does not
    // have support for streaming multipart uploads;
    // The alternative is first writing the entire output to disk before loading into S3. This is not
    // feasible with large tables.
    // Data is chunked into parts. A part is sent off to a queue to be uploaded once it has reached it's
    // configured part size.
    // Memory consumption is queue capacity * part size = 10 * 10 = 100 MB at current configurations.
    this.multipartUploadManager =
        new StreamTransferManager(RedshiftCopyDestination.DEFAULT_AIRBYTE_STAGING_S3_BUCKET, getPath(runFolder, streamName), client)
            .numUploadThreads(DEFAULT_UPLOAD_THREADS)
            .queueCapacity(DEFAULT_QUEUE_CAPACITY)
            .partSize(PART_SIZE_MB);
    // We only need one output stream as we only have one input stream. This is reasonably performant.
    // See the above comment.
    this.outputStream = multipartUploadManager.getMultiPartOutputStreams().get(0);

    var writer = new PrintWriter(outputStream, true, StandardCharsets.UTF_8);
    this.csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT);
  }

  public void copy(AirbyteRecordMessage message) throws IOException {
    var id = UUID.randomUUID();
    var data = Jsons.serialize(message.getData());
    var emittedAt = Timestamp.from(Instant.ofEpochMilli(message.getEmittedAt()));
    csvPrinter.printRecord(id, data, emittedAt);
  }

  public void close(boolean hasFailed) throws IOException {
    // TODO (dchia): revisit error handling here
    if (hasFailed) {
      multipartUploadManager.abort();
      return;
    }
    closeS3WriteStreamAndUpload();

    var tmpTableName = NAMING_RESOLVER.getTmpTableName(streamName);
    var sqlOp = new RedshiftSqlOperations();
    try {
      createTmpTableAndCopyS3FileInto(tmpTableName, sqlOp);
      mergeIntoDestTableIncrementalOrFullRefresh(tmpTableName, sqlOp);
      removeS3FileAndDropTmpTable(tmpTableName, sqlOp);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @VisibleForTesting
  static String getPath(String runFolder, String key) {
    return String.join("/", runFolder, key);
  }

  @VisibleForTesting
  static String getFullS3Path(String runFolder, String key) {
    return String.join("/", "s3:/", RedshiftCopyDestination.DEFAULT_AIRBYTE_STAGING_S3_BUCKET, runFolder, key);
  }

  private void closeS3WriteStreamAndUpload() throws IOException {
    LOGGER.info("Uploading remaining data for {} stream.", streamName);
    csvPrinter.close();
    outputStream.close();
    multipartUploadManager.complete();
    LOGGER.info("All data for {} stream uploaded.", streamName);
  }

  private void createTmpTableAndCopyS3FileInto(String tmpTableName, RedshiftSqlOperations sqlOp) throws SQLException {
    LOGGER.info("Preparing tmp table in destination for stream {}. tmp table name: {}.", streamName, tmpTableName);
    sqlOp.createTableIfNotExists(database, schemaName, tmpTableName);
    LOGGER.info("Starting copy to tmp table {} in destination for stream {} .", tmpTableName, streamName);
    sqlOp.copyS3CsvFileIntoTable(database, getFullS3Path(runFolder, streamName), schemaName, tmpTableName, s3KeyId, s3Key, s3Region);
    LOGGER.info("Copy to tmp table {} in destination for stream {} complete.", tmpTableName, streamName);
  }

  private void mergeIntoDestTableIncrementalOrFullRefresh(String tmpTableName, SqlOperations sqlOp) throws Exception {
    LOGGER.info("Preparing tmp table {} in destination.", tmpTableName);
    var destTableName = NAMING_RESOLVER.getRawTableName(streamName);
    sqlOp.createTableIfNotExists(database, schemaName, destTableName);
    LOGGER.info("Tmp table {} in destination prepared.", tmpTableName);

    LOGGER.info("Preparing to merge tmp table {} to dest table {} in destination.", tmpTableName, destTableName);
    var queries = new StringBuilder();
    if (syncMode.equals(SyncMode.FULL_REFRESH)) {
      queries.append(sqlOp.truncateTableQuery(schemaName, destTableName));
      LOGGER.info("FULL_REFRESH detected. Dest table {} truncated.", destTableName);
    }
    queries.append(sqlOp.copyTableQuery(schemaName, tmpTableName, destTableName));
    database.execute(queries.toString());
    LOGGER.info("Merge into dest table {} complete.", destTableName);
  }

  private void removeS3FileAndDropTmpTable(String tmpTableName, SqlOperations sqlOp) throws Exception {
    var s3StagingFile = getPath(runFolder, streamName);
    LOGGER.info("Begin cleaning s3 staging file {}.", s3StagingFile);
    s3Client.deleteObject(RedshiftCopyDestination.DEFAULT_AIRBYTE_STAGING_S3_BUCKET, getPath(runFolder, streamName));
    LOGGER.info("S3 staging file {} cleaned.", s3StagingFile);

    LOGGER.info("Begin cleaning {} tmp table in destination.", tmpTableName);
    sqlOp.dropTableIfExists(database, schemaName, tmpTableName);
    LOGGER.info("{} tmp table in destination cleaned.", tmpTableName);
  }

  public static void main(String[] args) throws IOException {
  }

}
