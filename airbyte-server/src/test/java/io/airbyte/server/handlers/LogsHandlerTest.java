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

package io.airbyte.server.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.api.model.LogType;
import io.airbyte.api.model.LogsRequestBody;
import io.airbyte.config.Configs;
import io.airbyte.config.helpers.LogHelpers;
import java.io.File;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class LogsHandlerTest {

  @Test
  public void testServerLogs() {
    final Configs configs = mock(Configs.class);
    when(configs.getWorkspaceRoot()).thenReturn(Path.of("/workspace"));

    final File expected = Path.of(String.format("/workspace/server/logs/%s", LogHelpers.LOG_FILENAME)).toFile();
    final File actual = new LogsHandler().getLogs(configs, new LogsRequestBody().logType(LogType.SERVER.SERVER));

    assertEquals(expected, actual);
  }

  @Test
  public void testSchedulerLogs() {
    final Configs configs = mock(Configs.class);
    when(configs.getWorkspaceRoot()).thenReturn(Path.of("/workspace"));

    final File expected = Path.of(String.format("/workspace/scheduler/logs/%s", LogHelpers.LOG_FILENAME)).toFile();
    final File actual = new LogsHandler().getLogs(configs, new LogsRequestBody().logType(LogType.SERVER));

    assertEquals(expected, actual);
  }

}
