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

package io.airbyte.workers;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.config.StandardSyncInput;
import io.airbyte.config.StandardTapConfig;
import io.airbyte.config.StandardTargetConfig;
import io.airbyte.scheduler.models.IntegrationLauncherConfig;
import io.airbyte.scheduler.models.JobRunConfig;
import io.airbyte.workers.process.AirbyteIntegrationLauncher;
import io.airbyte.workers.process.IntegrationLauncher;
import io.airbyte.workers.process.ProcessBuilderFactory;
import io.airbyte.workers.protocols.airbyte.HeartbeatMonitor;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.util.TriConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public class WorkerUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(WorkerUtils.class);

  public static void gentleClose(final Process process, final long timeout, final TimeUnit timeUnit) {
    if (process == null) {
      return;
    }

    try {
      process.waitFor(timeout, timeUnit);
    } catch (InterruptedException e) {
      LOGGER.error("Exception while while waiting for process to finish", e);
    }
    if (process.isAlive()) {
      forceShutdown(process, 1, TimeUnit.MINUTES);
    }
  }

  /**
   * As long as the the heartbeatMonitor detects a heartbeat, the process will be allowed to continue.
   * This method checks the heartbeat once every minute. Once there is no heartbeat detected, if the
   * process has ended, then the method returns. If the process is still running it is given a grace
   * period of the timeout arguments passed into the method. Once those expire the process is killed
   * forcibly.
   *
   * @param process - process to monitor.
   * @param heartbeatMonitor - tracks if the heart is still beating for the given process.
   * @param gracePeriodMagnitude - grace period to give the process to die after its heart stops
   *        beating.
   * @param gracePeriodTimeUnit - grace period to give the process to die after its heart stops
   *        beating.
   */
  public static void gentleCloseWithHeartbeat(final Process process,
                                              final HeartbeatMonitor heartbeatMonitor,
                                              final long gracePeriodMagnitude,
                                              final TimeUnit gracePeriodTimeUnit) {

  }

  @VisibleForTesting
  static void gentleCloseWithHeartbeat(final Process process,
                                       final HeartbeatMonitor heartbeatMonitor,
                                       final long gracefulShutdownMagnitude,
                                       final TimeUnit gracefulShutdownTimeUnit,
                                       final long checkHeartbeatMagnitude,
                                       final TimeUnit checkHeartbeatTimeUnit,
                                       final long forcedShutdownMagnitude,
                                       final TimeUnit forcedShutdownTimeUnit,
                                       final TriConsumer<Process, Long, TimeUnit> forceShutdown) {
    while (process.isAlive() && heartbeatMonitor.isBeating()) {
      try {
        process.waitFor(checkHeartbeatMagnitude, checkHeartbeatTimeUnit);
      } catch (InterruptedException e) {
        LOGGER.error("Exception while while waiting for process to finish", e);
      }
    }

    if (process.isAlive()) {
      try {
        process.waitFor(gracefulShutdownMagnitude, gracefulShutdownTimeUnit);
      } catch (InterruptedException e) {
        LOGGER.error("Exception during grace period for process to finish", e);
      }
    }

    // if we have closed gracefully exit...
    if (!process.isAlive()) {
      return;
    }

    forceShutdown.accept(process, forcedShutdownMagnitude, forcedShutdownTimeUnit);
  }

  @VisibleForTesting
  static void forceShutdown(Process process, long lastChanceMagnitude, TimeUnit lastChanceTimeUnit) {
    // otherwise we forcibly kill the process.
    LOGGER.warn("Process is taking too long to finish. Killing it");
    process.destroy();
    try {
      process.waitFor(lastChanceMagnitude, lastChanceTimeUnit);
    } catch (InterruptedException e) {
      LOGGER.error("Exception while while killing the process", e);
    }
    if (process.isAlive()) {
      LOGGER.warn("Couldn't kill the process. You might have a zombie ({})", process.info().commandLine());
    }
  }

  public static void closeProcess(Process process) {
    closeProcess(process, 1, TimeUnit.MINUTES);
  }

  public static void closeProcess(Process process, int duration, TimeUnit timeUnit) {
    if (process == null) {
      return;
    }
    try {
      process.destroy();
      process.waitFor(duration, timeUnit);
      if (process.isAlive()) {
        process.destroyForcibly();
      }
    } catch (InterruptedException e) {
      LOGGER.error("Exception when closing process.", e);
    }
  }

  public static void wait(Process process) {
    try {
      process.waitFor();
    } catch (InterruptedException e) {
      LOGGER.error("Exception while while waiting for process to finish", e);
    }
  }

  public static void cancelProcess(Process process) {
    closeProcess(process, 10, TimeUnit.SECONDS);
  }

  /**
   * Translates a StandardSyncInput into a StandardTapConfig. StandardTapConfig is a subset of
   * StandardSyncInput.
   */
  public static StandardTapConfig syncToTapConfig(StandardSyncInput sync) {
    return new StandardTapConfig()
        .withSourceConnectionConfiguration(sync.getSourceConfiguration())
        .withCatalog(sync.getCatalog())
        .withState(sync.getState());
  }

  /**
   * Translates a StandardSyncInput into a StandardTargetConfig. StandardTargetConfig is a subset of
   * StandardSyncInput.
   */
  public static StandardTargetConfig syncToTargetConfig(StandardSyncInput sync) {
    return new StandardTargetConfig()
        .withDestinationConnectionConfiguration(sync.getDestinationConfiguration())
        .withCatalog(sync.getCatalog())
        .withState(sync.getState());
  }

  // todo (cgardens) - there are 2 sources of truth for job path. we need to reduce this down to one,
  // once we are fully on temporal.
  public static Path getJobRoot(Path workspaceRoot, IntegrationLauncherConfig launcherConfig) {
    return getJobRoot(workspaceRoot, launcherConfig.getJobId(), Math.toIntExact(launcherConfig.getAttemptId()));
  }

  public static Path getJobRoot(Path workspaceRoot, JobRunConfig jobRunConfig) {
    return getJobRoot(workspaceRoot, jobRunConfig.getJobId(), jobRunConfig.getAttemptId());
  }

  public static Path getLogPath(Path jobRoot) {
    return jobRoot.resolve(WorkerConstants.LOG_FILENAME);
  }

  public static Path getJobRoot(Path workspaceRoot, String jobId, long attemptId) {
    return getJobRoot(workspaceRoot, jobId, Math.toIntExact(attemptId));
  }

  public static Path getJobRoot(Path workspaceRoot, String jobId, int attemptId) {
    return workspaceRoot
        .resolve(String.valueOf(jobId))
        .resolve(String.valueOf(attemptId));
  }

  public static void setJobMdc(Path jobRoot, String jobId) {
    MDC.put("job_id", jobId);
    MDC.put("job_root", jobRoot.toString());
    MDC.put("job_log_filename", WorkerConstants.LOG_FILENAME);
  }

  // todo (cgardens) can we get this down to just passing pbf and docker image and not job id and
  // attempt
  public static IntegrationLauncher getIntegrationLauncher(IntegrationLauncherConfig config, ProcessBuilderFactory pbf) {
    return new AirbyteIntegrationLauncher(config.getJobId(), Math.toIntExact(config.getAttemptId()), config.getDockerImage(), pbf);
  }

}
