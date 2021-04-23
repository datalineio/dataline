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

package io.airbyte.integrations.destination.jdbc.copy;

import java.sql.Timestamp;
import java.util.UUID;

/**
 * StreamCopier is responsible for writing to a staging persistence and providing methods to remove
 * the staged data.
 */
public interface StreamCopier {

  /**
   * Writes a value to a staging file for the stream.
   */
  void write(UUID id, String jsonDataString, Timestamp emittedAt) throws Exception;

  /**
   * Closes the writer for the stream and loads the data into a temporary table while generating the
   * merge SQL statement.
   *
   * @return the SQL queries necessary to merge or the empty string if there was a failure
   */
  String copyToTmpTableAndPrepMergeToFinalTable(boolean hasFailed) throws Exception;

  /**
   * Cleans up the copier by removing the staging file and dropping the temporary table after
   * completion or failure.
   */
  void removeFileAndDropTmpTable() throws Exception;

}
