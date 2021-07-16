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

package io.airbyte.integrations.destination.jdbc.copy.gcs;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import io.airbyte.db.jdbc.JdbcDatabase;
import io.airbyte.integrations.destination.ExtendedNameTransformer;
import io.airbyte.integrations.destination.jdbc.SqlOperations;
import io.airbyte.integrations.destination.jdbc.copy.StreamCopier;
import io.airbyte.protocol.models.DestinationSyncMode;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.UUID;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.client.builder.AwsClientBuilder;


public abstract class GcsStreamCopier implements StreamCopier {

  private static final Logger LOGGER = LoggerFactory.getLogger(GcsStreamCopier.class);

  private final String gcsStagingFile;
  private final Storage storageClient;
  private final GcsConfig gcsConfig;
  private final WriteChannel channel;
  private final CSVPrinter csvPrinter;
  private final String tmpTableName;
  private final DestinationSyncMode destSyncMode;
  private final String schemaName;
  private final String streamName;
  private final JdbcDatabase db;
  private final ExtendedNameTransformer nameTransformer;
  private final SqlOperations sqlOperations;

  public GcsStreamCopier(String stagingFolder,
                         DestinationSyncMode destSyncMode,
                         String schema,
                         String streamName,
                         Storage storageClient,
                         JdbcDatabase db,
                         GcsConfig gcsConfig,
                         ExtendedNameTransformer nameTransformer,
                         SqlOperations sqlOperations) {
    this.destSyncMode = destSyncMode;
    this.schemaName = schema;
    this.streamName = streamName;
    this.db = db;
    this.nameTransformer = nameTransformer;
    this.sqlOperations = sqlOperations;
    this.tmpTableName = nameTransformer.getTmpTableName(streamName);
    this.storageClient = storageClient;
    this.gcsConfig = gcsConfig;

    this.gcsStagingFile = String.join("/", stagingFolder, schemaName, streamName);

    var blobId = BlobId.of(gcsConfig.getBucketName(), gcsStagingFile);
    var blobInfo = BlobInfo.newBuilder(blobId).build();
    var blob = storageClient.create(blobInfo);
    this.channel = blob.writer();
    OutputStream outputStream = Channels.newOutputStream(channel);

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
    LOGGER.info("Uploading remaining data for {} stream.", streamName);
    csvPrinter.close();
    channel.close();
    LOGGER.info("All data for {} stream uploaded.", streamName);
  }

  @Override
  public void copyStagingFileToTemporaryTable() throws Exception {
    LOGGER.info("Starting copy to tmp table: {} in destination for stream: {}, schema: {}.", tmpTableName, streamName, schemaName);
    copyGcsCsvFileIntoTable(db, getFullGcsPath(gcsConfig.getBucketName(), gcsStagingFile), schemaName, tmpTableName, gcsConfig);
    LOGGER.info("Copy to tmp table {} in destination for stream {} complete.", tmpTableName, streamName);
  }

  @Override
  public void removeFileAndDropTmpTable() throws Exception {
    LOGGER.info("Begin cleaning gcs staging file {}.", gcsStagingFile);
    var blobId = BlobId.of(gcsConfig.getBucketName(), gcsStagingFile);
    if (storageClient.get(blobId).exists()) {
      storageClient.delete(blobId);
    }
    LOGGER.info("GCS staging file {} cleaned.", gcsStagingFile);

    LOGGER.info("Begin cleaning {} tmp table in destination.", tmpTableName);
    sqlOperations.dropTableIfExists(db, schemaName, tmpTableName);
    LOGGER.info("{} tmp table in destination cleaned.", tmpTableName);
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
      queries.append(sqlOperations.truncateTableQuery(db, schemaName, destTableName));
      LOGGER.info("Destination OVERWRITE mode detected. Dest table: {}, schema: {}, will be truncated.", destTableName, schemaName);
    }
    queries.append(sqlOperations.copyTableQuery(db, schemaName, tmpTableName, destTableName));
    return queries.toString();
  }

  private static String getFullGcsPath(String bucketName, String stagingFile) {
    // this is intentionally gcs:/ not gcs:// since the join adds the additional slash
    return String.join("/", "gcs:/", bucketName, stagingFile);
  }

  public static void attemptWriteToPersistence(GcsConfig gcsConfig) throws IOException {
    final String outputTableName = "_airbyte_connection_test_" + UUID.randomUUID().toString().replaceAll("-", "");
    attemptWriteAndDeleteGcsObject(gcsConfig, outputTableName);
  }

  public static void attemptGcsWriteAndDelete(GcsConfig gcsConfig) throws IOException {
      final String outputTableName = "_airbyte_connection_test_" + UUID.randomUUID().toString().replaceAll("-", "");
      attemptWriteAndDeleteGcsObject(gcsConfig, outputTableName);
  }

  private static void attemptWriteAndDeleteGcsObject(GcsConfig gcsConfig, String outputTableName) throws IOException {
      var storage = getStorageClient(gcsConfig);
      // var blobId = BlobId.of(gcsConfig.getBucketName(), "check-content/" + outputTableName);
      // var blobInfo = BlobInfo.newBuilder(blobId).build();

      // storage.create(blobInfo, "".getBytes());
      // storage.delete(blobId);
      var gcsBucket = gcsConfig.getBucketName();

      storage.putObject(gcsBucket, outputTableName, "check-content");
      storage.deleteObject(gcsBucket, outputTableName);
  }

  public static AmazonS3 getStorageClient(GcsConfig gcsConfig) throws IOException {
    var region = gcsConfig.getRegion();
    var accessKeyId = gcsConfig.getAccessKeyId();
    var secretAccessKey = gcsConfig.getSecretAccessKey();

    var awsCreds = new BasicAWSCredentials(accessKeyId, secretAccessKey);
    return AmazonS3ClientBuilder.standard()
        .withEndpointConfiguration(
          new AwsClientBuilder.EndpointConfiguration(
          "https://storage.googleapis.com", region))
        .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
        .build();
    }

  public abstract void copyGcsCsvFileIntoTable(JdbcDatabase database,
                                               String gcsFileLocation,
                                               String schema,
                                               String tableName,
                                               GcsConfig gcsConfig)
      throws SQLException;

}
