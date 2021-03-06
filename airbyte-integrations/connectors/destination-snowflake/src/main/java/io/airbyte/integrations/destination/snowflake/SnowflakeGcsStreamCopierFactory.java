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

package io.airbyte.integrations.destination.snowflake;

import com.google.cloud.storage.Storage;
import io.airbyte.db.jdbc.JdbcDatabase;
import io.airbyte.integrations.destination.ExtendedNameTransformer;
import io.airbyte.integrations.destination.jdbc.SqlOperations;
import io.airbyte.integrations.destination.jdbc.copy.StreamCopier;
import io.airbyte.integrations.destination.jdbc.copy.gcs.GcsConfig;
import io.airbyte.integrations.destination.jdbc.copy.gcs.GcsStreamCopierFactory;
import io.airbyte.protocol.models.DestinationSyncMode;

public class SnowflakeGcsStreamCopierFactory extends GcsStreamCopierFactory {

  @Override
  public StreamCopier create(String stagingFolder,
                             DestinationSyncMode syncMode,
                             String schema,
                             String streamName,
                             Storage storageClient,
                             JdbcDatabase db,
                             GcsConfig gcsConfig,
                             ExtendedNameTransformer nameTransformer,
                             SqlOperations sqlOperations)
      throws Exception {
    return new SnowflakeGcsStreamCopier(
        stagingFolder,
        syncMode,
        schema,
        streamName,
        storageClient,
        db,
        gcsConfig,
        nameTransformer,
        sqlOperations);
  }

}
