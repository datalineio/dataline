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

package io.airbyte.integrations.destination.s3;

import com.fasterxml.jackson.databind.JsonNode;

public class GcsDestinationConfig {

  private final String bucketName;
  private final String bucketPath;
  private final String bucketRegion;
  private final String accessKeyId;
  private final String secretAccessKey;
  private final S3FormatConfig formatConfig;

  public GcsDestinationConfig(
                             String bucketName,
                             String bucketPath,
                             String bucketRegion,
                             String accessKeyId,
                             String secretAccessKey,
                             S3FormatConfig formatConfig) {
    this.bucketName = bucketName;
    this.bucketPath = bucketPath;
    this.bucketRegion = bucketRegion;
    this.accessKeyId = accessKeyId;
    this.secretAccessKey = secretAccessKey;
    this.formatConfig = formatConfig;
  }

  public static GcsDestinationConfig getGcsDestinationConfig(JsonNode config) {
    return new GcsDestinationConfig(
        config.get("gcs_bucket_name").asText(),
        config.get("gcs_bucket_path").asText(),
        config.get("gcs_bucket_region").asText(),
        config.get("access_key_id").asText(),
        config.get("secret_access_key").asText(),
        S3FormatConfigs.getS3FormatConfig(config));
  }

  public String getBucketName() {
    return bucketName;
  }

  public String getBucketPath() {
    return bucketPath;
  }

  public String getBucketRegion() {
    return bucketRegion;
  }

  public String getAccessKeyId() {
    return accessKeyId;
  }

  public String getSecretAccessKey() {
    return secretAccessKey;
  }
  
  public S3FormatConfig getFormatConfig() {
    return formatConfig;
  }

}
