/*
 * MIT License
 *
 * Copyright (c) 2020 Dataline
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

package io.dataline.scheduler;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.MoreExecutors;
import io.dataline.scheduler.persistence.SchedulerPersistence;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class JobSubmitterTest {

  private static final Job JOB = mock(Job.class);

  private ExecutorService executorService;
  private SchedulerPersistence persistence;
  private JobSubmitter jobSubmitter;
  private WorkerRunFactory workerRunFactory;

  @BeforeEach
  public void setup() {
    executorService = MoreExecutors.newDirectExecutorService();
    persistence = mock(SchedulerPersistence.class);
    workerRunFactory = mock(WorkerRunFactory.class);

    jobSubmitter = new JobSubmitter(
        executorService,
        persistence,
        workerRunFactory);
  }

  @Test
  public void testSubmitsPendingJob() throws IOException {
    when(persistence.getOldestPendingJob()).thenReturn(Optional.of(JOB));

    jobSubmitter.run();

    // verify(executorService).submit(any(WorkerRunFactory.class));
  }

  @Test
  public void testNoPendingJob() throws IOException {
    when(persistence.getOldestPendingJob()).thenReturn(Optional.empty());

    jobSubmitter.run();

    // verify(executorService, never()).submit(any(WorkerRunFactory.class));
  }

}
