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

package io.airbyte.integrations.destination.gcs.credential;

import com.fasterxml.jackson.databind.JsonNode;

public class GcsHmacKeyCredentialConfig implements GcsCredentialConfig {

  private final String hmacKeyAccessId;
  private final String hmacKeySecret;

  public GcsHmacKeyCredentialConfig(JsonNode credentialConfig) {
    this.hmacKeyAccessId = credentialConfig.get("hmac_key_access_id").asText();
    this.hmacKeySecret = credentialConfig.get("hmac_key_secret").asText();
  }

  public String getHmacKeyAccessId() {
    return hmacKeyAccessId;
  }

  public String getHmacKeySecret() {
    return hmacKeySecret;
  }

  @Override
  public GcsCredential getCredentialType() {
    return GcsCredential.HMAC_KEY;
  }

}
