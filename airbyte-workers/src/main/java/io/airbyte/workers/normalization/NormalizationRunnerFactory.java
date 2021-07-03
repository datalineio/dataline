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

package io.airbyte.workers.normalization;

import com.google.common.collect.ImmutableMap;
import io.airbyte.workers.process.ProcessFactory;
import java.util.Map;

public class NormalizationRunnerFactory {

  private static final Map<String, DefaultNormalizationRunner.DestinationType> NORMALIZATION_MAPPING =
      ImmutableMap.<String, DefaultNormalizationRunner.DestinationType>builder()
          .put("airbyte/destination-bigquery", DefaultNormalizationRunner.DestinationType.BIGQUERY)
          .put("airbyte/destination-postgres", DefaultNormalizationRunner.DestinationType.POSTGRES)
          .put("airbyte/destination-redshift", DefaultNormalizationRunner.DestinationType.REDSHIFT)
          .put("airbyte/destination-snowflake", DefaultNormalizationRunner.DestinationType.SNOWFLAKE)
          .put("airbyte/destination-oracle", DefaultNormalizationRunner.DestinationType.ORACLE)
          .build();

  public static NormalizationRunner create(String imageName, ProcessFactory processFactory) {

    final String imageNameWithoutTag = imageName.split(":")[0];

    if (NORMALIZATION_MAPPING.containsKey(imageNameWithoutTag)) {
      return new DefaultNormalizationRunner(NORMALIZATION_MAPPING.get(imageNameWithoutTag), processFactory);
    } else {
      throw new IllegalStateException(
          String.format("Requested normalization for %s, but it is not included in the normalization mapping.", imageName));
    }
  }

}
