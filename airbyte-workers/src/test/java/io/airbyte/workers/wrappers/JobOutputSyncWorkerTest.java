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

package io.airbyte.workers.wrappers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.commons.json.Jsons;
import io.airbyte.config.JobOutput;
import io.airbyte.config.StandardSyncInput;
import io.airbyte.config.StandardSyncOutput;
import io.airbyte.config.State;
import io.airbyte.workers.JobStatus;
import io.airbyte.workers.OutputAndStatus;
import io.airbyte.workers.SyncWorker;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

public class JobOutputSyncWorkerTest {

  @Test
  public void test() {
    final StandardSyncInput input = mock(StandardSyncInput.class);
    final Path jobRoot = Path.of("fakeroot");
    final SyncWorker syncWorker = mock(SyncWorker.class);

    final StandardSyncOutput output = new StandardSyncOutput().withState(new State().withState(Jsons.jsonNode(ImmutableMap.of("checkpoint", "10"))));

    when(syncWorker.run(input, jobRoot)).thenReturn(new OutputAndStatus<>(JobStatus.SUCCEEDED, output));
    final OutputAndStatus<JobOutput> run = new JobOutputSyncWorker(syncWorker).run(input, jobRoot);

    final JobOutput expected = new JobOutput().withOutputType(JobOutput.OutputType.SYNC).withSync(output);
    assertEquals(JobStatus.SUCCEEDED, run.getStatus());
    assertTrue(run.getOutput().isPresent());
    assertEquals(expected, run.getOutput().get());
  }

}
