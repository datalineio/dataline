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

package io.airbyte.integrations.destination.jdbc.copy.s3;

import alex.mojaki.s3upload.MultiPartOutputStream;
import alex.mojaki.s3upload.StreamTransferManager;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import io.airbyte.db.jdbc.JdbcDatabase;
import io.airbyte.integrations.destination.ExtendedNameTransformer;
import io.airbyte.integrations.destination.jdbc.SqlOperations;
import io.airbyte.integrations.destination.jdbc.copy.StreamCopier;
import io.airbyte.protocol.models.DestinationSyncMode;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.UUID;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class S3StreamCopier implements StreamCopier {

  private static final Logger LOGGER = LoggerFactory.getLogger(S3StreamCopier.class);

  private static final int DEFAULT_UPLOAD_THREADS = 5; // The S3 cli uses 10 threads by defaultd.
  private static final int DEFAULT_QUEUE_CAPACITY = DEFAULT_UPLOAD_THREADS;
  // The smallest part size is 5MB. An S3 upload can be maximally formed of 10,000 parts. This gives
  // us an upper limit of 10,000 * 10 / 1000 = 100 GB per table with a 10MB part size limit.
  // WARNING: Too large a part size can cause potential OOM errors.
  public static final int DEFAULT_PART_SIZE_MB = 10;

  private final String s3StagingFile;
  private final AmazonS3 s3Client;
  private final S3Config s3Config;
  private final StreamTransferManager multipartUploadManager;
  private final MultiPartOutputStream outputStream;
  private final CSVPrinter csvPrinter;
  private final String tmpTableName;
  private final DestinationSyncMode destSyncMode;
  private final String schemaName;
  private final String streamName;
  private final JdbcDatabase db;
  private final ExtendedNameTransformer nameTransformer;
  private final SqlOperations sqlOperations;

  public S3StreamCopier(String stagingFolder,
                        DestinationSyncMode destSyncMode,
                        String schema,
                        String streamName,
                        AmazonS3 client,
                        JdbcDatabase db,
                        S3Config s3Config,
                        ExtendedNameTransformer nameTransformer,
                        SqlOperations sqlOperations) {
    this.destSyncMode = destSyncMode;
    this.schemaName = schema;
    this.streamName = streamName;
    this.db = db;
    this.nameTransformer = nameTransformer;
    this.sqlOperations = sqlOperations;
    this.tmpTableName = nameTransformer.getTmpTableName(streamName);
    this.s3Client = client;
    this.s3Config = s3Config;

    this.s3StagingFile = String.join("/", stagingFolder, schemaName, streamName);
    LOGGER.info("S3 upload part size: {} MB", s3Config.getPartSize());
    // The stream transfer manager lets us greedily stream into S3. The native AWS SDK does not
    // have support for streaming multipart uploads;
    // The alternative is first writing the entire output to disk before loading into S3. This is not
    // feasible with large tables.
    // Data is chunked into parts. A part is sent off to a queue to be uploaded once it has reached it's
    // configured part size.
    // Memory consumption is queue capacity * part size = 10 * 10 = 100 MB at current configurations.
    this.multipartUploadManager =
        new StreamTransferManager(s3Config.getBucketName(), s3StagingFile, client)
            .numUploadThreads(DEFAULT_UPLOAD_THREADS)
            .queueCapacity(DEFAULT_QUEUE_CAPACITY)
            .partSize(s3Config.getPartSize());
    // We only need one output stream as we only have one input stream. This is reasonably performant.
    // See the above comment.
    this.outputStream = multipartUploadManager.getMultiPartOutputStreams().get(0);

    var writer = new PrintWriter(outputStream, true, StandardCharsets.UTF_8);
    try {
      this.csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void write(UUID id, String jsonDataString, Timestamp emittedAt) throws Exception {
    csvPrinter.printRecord(id, jsonDataString, emittedAt);
  }

  @Override
  public void closeStagingUploader(boolean hasFailed) throws Exception {
    if (hasFailed) {
      multipartUploadManager.abort();
    }
    closeAndWaitForUpload();
  }

  @Override
  public void createDestinationSchema() throws Exception {
    LOGGER.info("Creating schema in destination if it doesn't exist: {}", schemaName);
    sqlOperations.createSchemaIfNotExists(db, schemaName);
  }

  @Override
  public void createTemporaryTable() throws Exception {
    LOGGER.info("Preparing tmp table in destination for stream: {}, schema: {}, tmp table name: {}.", streamName, schemaName, tmpTableName);
    sqlOperations.createTableIfNotExists(db, schemaName, tmpTableName);
  }

  @Override
  public void copyStagingFileToTemporaryTable() throws Exception {
    LOGGER.info("Starting copy to tmp table: {} in destination for stream: {}, schema: {}, .", tmpTableName, streamName, schemaName);
    copyS3CsvFileIntoTable(db, getFullS3Path(s3Config.getBucketName(), s3StagingFile), schemaName, tmpTableName, s3Config);
    LOGGER.info("Copy to tmp table {} in destination for stream {} complete.", tmpTableName, streamName);
  }

  @Override
  public String createDestinationTable() throws Exception {
    var destTableName = nameTransformer.getRawTableName(streamName);
    LOGGER.info("Preparing table {} in destination.", destTableName);
    sqlOperations.createTableIfNotExists(db, schemaName, destTableName);
    LOGGER.info("Table {} in destination prepared.", tmpTableName);

    return destTableName;
  }

  @Override
  public String generateMergeStatement(String destTableName) throws Exception {
    LOGGER.info("Preparing to merge tmp table {} to dest table: {}, schema: {}, in destination.", tmpTableName, destTableName, schemaName);
    var queries = new StringBuilder();
    if (destSyncMode.equals(DestinationSyncMode.OVERWRITE)) {
      queries.append(sqlOperations.truncateTableQuery(schemaName, destTableName));
      LOGGER.info("Destination OVERWRITE mode detected. Dest table: {}, schema: {}, truncated.", destTableName, schemaName);
    }
    queries.append(sqlOperations.copyTableQuery(schemaName, tmpTableName, destTableName));
    return queries.toString();
  }

  @Override
  public void removeFileAndDropTmpTable() throws Exception {
    LOGGER.info("Begin cleaning s3 staging file {}.", s3StagingFile);
    if (s3Client.doesObjectExist(s3Config.getBucketName(), s3StagingFile)) {
      s3Client.deleteObject(s3Config.getBucketName(), s3StagingFile);
    }
    LOGGER.info("S3 staging file {} cleaned.", s3StagingFile);

    LOGGER.info("Begin cleaning {} tmp table in destination.", tmpTableName);
    sqlOperations.dropTableIfExists(db, schemaName, tmpTableName);
    LOGGER.info("{} tmp table in destination cleaned.", tmpTableName);
  }

  private static String getFullS3Path(String s3BucketName, String s3StagingFile) {
    return String.join("/", "s3:/", s3BucketName, s3StagingFile);
  }

  /**
   * Closes the printers/outputstreams and waits for any buffered uploads to complete.
   */
  private void closeAndWaitForUpload() throws IOException {
    LOGGER.info("Uploading remaining data for {} stream.", streamName);
    csvPrinter.close();
    outputStream.close();
    multipartUploadManager.complete();
    LOGGER.info("All data for {} stream uploaded.", streamName);
  }

  public static void attemptWriteToPersistence(S3Config s3Config) {
    final String outputTableName = "_airbyte_connection_test_" + UUID.randomUUID().toString().replaceAll("-", "");
    attemptWriteAndDeleteS3Object(s3Config, outputTableName);
  }

  private static void attemptWriteAndDeleteS3Object(S3Config s3Config, String outputTableName) {
    var s3 = getAmazonS3(s3Config);
    var s3Bucket = s3Config.getBucketName();
    s3.putObject(s3Bucket, outputTableName, "check-content");
    s3.deleteObject(s3Bucket, outputTableName);
  }

  public static AmazonS3 getAmazonS3(S3Config s3Config) {
    var accessKeyId = s3Config.getAccessKeyId();
    var secretAccessKey = s3Config.getSecretAccessKey();
    var awsCreds = new BasicAWSCredentials(accessKeyId, secretAccessKey);
    return AmazonS3ClientBuilder.standard()
        .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
        .withRegion(s3Config.getRegion())
        .build();
  }

  public abstract void copyS3CsvFileIntoTable(JdbcDatabase database,
                                              String s3FileLocation,
                                              String schema,
                                              String tableName,
                                              S3Config s3Config)
      throws SQLException;

}
