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

package io.airbyte.integrations.destination.gcs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.resources.MoreResources;
import io.airbyte.integrations.destination.gcs.credential.GcsCredentialConfig;
import io.airbyte.integrations.destination.gcs.credential.GcsHmacKeyCredentialConfig;
import io.airbyte.integrations.destination.s3.S3FormatConfig;
import io.airbyte.integrations.destination.s3.avro.S3AvroFormatConfig;
import java.io.IOException;
import org.junit.jupiter.api.Test;

class GcsDestinationConfigTest {

  @Test
  public void testGetGcsDestinationConfig() throws IOException {
    JsonNode configJson = Jsons.deserialize(MoreResources.readResource("test_config.json"));

    GcsDestinationConfig config = GcsDestinationConfig.getGcsDestinationConfig(configJson);
    assertEquals("test_bucket", config.getBucketName());
    assertEquals("test_path", config.getBucketPath());
    assertEquals("us-west1", config.getBucketRegion());

    GcsCredentialConfig credentialConfig = config.getCredentialConfig();
    assertTrue(credentialConfig instanceof GcsHmacKeyCredentialConfig);

    GcsHmacKeyCredentialConfig hmacKeyConfig = (GcsHmacKeyCredentialConfig) credentialConfig;
    assertEquals("test_access_id", hmacKeyConfig.getHmacKeyAccessId());
    assertEquals("test_secret", hmacKeyConfig.getHmacKeySecret());

    S3FormatConfig formatConfig = config.getFormatConfig();
    assertTrue(formatConfig instanceof S3AvroFormatConfig);

    S3AvroFormatConfig avroFormatConfig = (S3AvroFormatConfig) formatConfig;
    assertEquals("deflate-5", avroFormatConfig.getCodecFactory().toString());
  }

}
