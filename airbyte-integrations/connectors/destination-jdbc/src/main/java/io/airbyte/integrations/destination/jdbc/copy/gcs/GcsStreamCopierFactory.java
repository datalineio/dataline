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
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import io.airbyte.db.jdbc.JdbcDatabase;
import io.airbyte.integrations.base.AirbyteStreamNameNamespacePair;
import io.airbyte.integrations.destination.ExtendedNameTransformer;
import io.airbyte.integrations.destination.jdbc.SqlOperations;
import io.airbyte.integrations.destination.jdbc.copy.StreamCopier;
import io.airbyte.integrations.destination.jdbc.copy.StreamCopierFactory;
import io.airbyte.protocol.models.AirbyteStream;
import io.airbyte.protocol.models.DestinationSyncMode;
import java.io.ByteArrayInputStream;
import java.io.InputStream;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.client.builder.AwsClientBuilder;

public abstract class GcsStreamCopierFactory implements StreamCopierFactory<GcsConfig> {

  /**
   * Used by the copy consumer.
   */
  @Override
  public StreamCopier create(String configuredSchema,
                             GcsConfig gcsConfig,
                             String stagingFolder,
                             DestinationSyncMode syncMode,
                             AirbyteStream stream,
                             ExtendedNameTransformer nameTransformer,
                             JdbcDatabase db,
                             SqlOperations sqlOperations) {
    try {
      var pair = AirbyteStreamNameNamespacePair.fromAirbyteSteam(stream);
      var schema = getSchema(stream, configuredSchema, nameTransformer);

      var region = gcsConfig.getRegion();
      var accessKeyId = gcsConfig.getAccessKeyId();
      var secretAccessKey = gcsConfig.getSecretAccessKey();
      
      // InputStream credentialsInputStream = new ByteArrayInputStream(gcsConfig.getCredentialsJson().getBytes());
      // GoogleCredentials credentials = GoogleCredentials.fromStream(credentialsInputStream);
      // Storage storageClient = StorageOptions.newBuilder()
      //     .setCredentials(credentials)
      //     .setProjectId(gcsConfig.getProjectId())
      //     .build()
      //     .getService();

      var awsCreds = new BasicAWSCredentials(accessKeyId, secretAccessKey);
      AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
          .withEndpointConfiguration(
            new AwsClientBuilder.EndpointConfiguration(
            "https://storage.googleapis.com", region))
          .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
          .build();

      return create(stagingFolder, syncMode, schema, pair.getName(), //storageClient
      s3Client, db, gcsConfig, nameTransformer, sqlOperations);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * For specific copier suppliers to implement.
   */
  public abstract StreamCopier create(String stagingFolder,
                                      DestinationSyncMode syncMode,
                                      String schema,
                                      String streamName,
                                      AmazonS3 s3Client,
                                      JdbcDatabase db,
                                      GcsConfig gcsConfig,
                                      ExtendedNameTransformer nameTransformer,
                                      SqlOperations sqlOperations)
      throws Exception;

  private String getSchema(AirbyteStream stream, String configuredSchema, ExtendedNameTransformer nameTransformer) {
    if (stream.getNamespace() != null) {
      return nameTransformer.convertStreamName(stream.getNamespace());
    } else {
      return nameTransformer.convertStreamName(configuredSchema);
    }
  }

}
