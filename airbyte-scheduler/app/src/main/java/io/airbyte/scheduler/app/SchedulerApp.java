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

package io.airbyte.scheduler.app;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.airbyte.analytics.TrackingClientSingleton;
import io.airbyte.commons.concurrency.GracefulShutdownHandler;
import io.airbyte.commons.version.AirbyteVersion;
import io.airbyte.config.Configs;
import io.airbyte.config.EnvConfigs;
import io.airbyte.config.helpers.LogClientSingleton;
import io.airbyte.config.persistence.ConfigPersistence;
import io.airbyte.config.persistence.ConfigPersistenceBuilder;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.db.Database;
import io.airbyte.db.Databases;
import io.airbyte.scheduler.app.worker_run.TemporalWorkerRunFactory;
import io.airbyte.scheduler.models.Job;
import io.airbyte.scheduler.models.JobStatus;
import io.airbyte.scheduler.persistence.DefaultJobPersistence;
import io.airbyte.scheduler.persistence.JobNotifier;
import io.airbyte.scheduler.persistence.JobPersistence;
import io.airbyte.scheduler.persistence.job_tracker.JobTracker;
import io.airbyte.workers.process.DockerProcessFactory;
import io.airbyte.workers.process.KubeProcessFactory;
import io.airbyte.workers.process.ProcessFactory;
import io.airbyte.workers.process.WorkerHeartbeatServer;
import io.airbyte.workers.temporal.TemporalClient;
import io.airbyte.workers.temporal.TemporalPool;
import io.airbyte.workers.temporal.TemporalUtils;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.util.Config;
import io.temporal.serviceclient.WorkflowServiceStubs;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * The SchedulerApp is responsible for finding new scheduled jobs that need to be run and to launch
 * them. The current implementation uses a thread pool on the scheduler's machine to launch the
 * jobs. One thread is reserved for the job submitter, which is responsible for finding and
 * launching new jobs.
 */
public class SchedulerApp {

  private static final Logger LOGGER = LoggerFactory.getLogger(SchedulerApp.class);

  private static final long GRACEFUL_SHUTDOWN_SECONDS = 30;
  private static final int MAX_WORKERS = 4;
  private static final Duration SCHEDULING_DELAY = Duration.ofSeconds(5);
  private static final Duration CLEANING_DELAY = Duration.ofHours(2);
  private static final ThreadFactory THREAD_FACTORY = new ThreadFactoryBuilder().setNameFormat("worker-%d").build();
  private static final int KUBE_HEARTBEAT_PORT = 9000;

  private final Path workspaceRoot;
  private final ProcessFactory processFactory;
  private final JobPersistence jobPersistence;
  private final ConfigRepository configRepository;
  private final JobCleaner jobCleaner;
  private final JobNotifier jobNotifier;
  private final TemporalClient temporalClient;
  private final WorkflowServiceStubs temporalService;

  public SchedulerApp(Path workspaceRoot,
                      ProcessFactory processFactory,
                      JobPersistence jobPersistence,
                      ConfigRepository configRepository,
                      JobCleaner jobCleaner,
                      JobNotifier jobNotifier,
                      TemporalClient temporalClient,
                      WorkflowServiceStubs temporalService) {
    this.workspaceRoot = workspaceRoot;
    this.processFactory = processFactory;
    this.jobPersistence = jobPersistence;
    this.configRepository = configRepository;
    this.jobCleaner = jobCleaner;
    this.jobNotifier = jobNotifier;
    this.temporalClient = temporalClient;
    this.temporalService = temporalService;
  }

  public void start() throws IOException {
    final TemporalPool temporalPool = new TemporalPool(temporalService, workspaceRoot, processFactory);
    temporalPool.run();

    final ExecutorService workerThreadPool = Executors.newFixedThreadPool(MAX_WORKERS, THREAD_FACTORY);
    final ScheduledExecutorService scheduledPool = Executors.newSingleThreadScheduledExecutor();
    final TemporalWorkerRunFactory temporalWorkerRunFactory = new TemporalWorkerRunFactory(temporalClient, workspaceRoot);
    final JobRetrier jobRetrier = new JobRetrier(jobPersistence, Instant::now, jobNotifier);
    final JobScheduler jobScheduler = new JobScheduler(jobPersistence, configRepository);
    final JobSubmitter jobSubmitter = new JobSubmitter(
        workerThreadPool,
        jobPersistence,
        temporalWorkerRunFactory,
        new JobTracker(configRepository, jobPersistence));

    Map<String, String> mdc = MDC.getCopyOfContextMap();

    // We cancel jobs that where running before the restart. They are not being monitored by the worker
    // anymore.
    cleanupZombies(jobPersistence, jobNotifier);

    scheduledPool.scheduleWithFixedDelay(
        () -> {
          MDC.setContextMap(mdc);
          jobRetrier.run();
          jobScheduler.run();
          jobSubmitter.run();
        },
        0L,
        SCHEDULING_DELAY.toSeconds(),
        TimeUnit.SECONDS);

    scheduledPool.scheduleWithFixedDelay(
        () -> {
          MDC.setContextMap(mdc);
          jobCleaner.run();
          jobPersistence.purgeJobHistory();
        },
        CLEANING_DELAY.toSeconds(),
        CLEANING_DELAY.toSeconds(),
        TimeUnit.SECONDS);

    Runtime.getRuntime().addShutdownHook(new GracefulShutdownHandler(Duration.ofSeconds(GRACEFUL_SHUTDOWN_SECONDS), workerThreadPool, scheduledPool));
  }

  private void cleanupZombies(JobPersistence jobPersistence, JobNotifier jobNotifier) throws IOException {
    for (Job zombieJob : jobPersistence.listJobsWithStatus(JobStatus.RUNNING)) {
      jobNotifier.failJob("zombie job was cancelled", zombieJob);
      jobPersistence.cancelJob(zombieJob.getId());
    }
  }

  private static ProcessFactory getProcessBuilderFactory(Configs configs) throws IOException {
    if (configs.getWorkerEnvironment() == Configs.WorkerEnvironment.KUBERNETES) {
      final ApiClient officialClient = Config.defaultClient();
      final KubernetesClient fabricClient = new DefaultKubernetesClient();
      final BlockingQueue<Integer> workerPorts = new LinkedBlockingDeque<>(configs.getTemporalWorkerPorts());
      final String localIp = InetAddress.getLocalHost().getHostAddress();
      final String kubeHeartbeatUrl = localIp + ":" + KUBE_HEARTBEAT_PORT;
      LOGGER.info("Using Kubernetes namespace: {}", configs.getKubeNamespace());
      return new KubeProcessFactory(configs.getKubeNamespace(), officialClient, fabricClient, kubeHeartbeatUrl, workerPorts);
    } else {
      return new DockerProcessFactory(
          configs.getWorkspaceRoot(),
          configs.getWorkspaceDockerMount(),
          configs.getLocalDockerMount(),
          configs.getDockerNetwork());
    }
  }

  public static void main(String[] args) throws IOException, InterruptedException {

    final Configs configs = new EnvConfigs();

    MDC.put(LogClientSingleton.WORKSPACE_MDC_KEY, LogClientSingleton.getSchedulerLogsRoot(configs).toString());

    final Path workspaceRoot = configs.getWorkspaceRoot();
    LOGGER.info("workspaceRoot = " + workspaceRoot);

    final String temporalHost = configs.getTemporalHost();
    LOGGER.info("temporalHost = " + temporalHost);

    LOGGER.info("Creating Job DB connection pool...");
    final Database jobDatabase = Databases.createPostgresDatabaseWithRetry(
        configs.getDatabaseUser(),
        configs.getDatabasePassword(),
        configs.getDatabaseUrl(),
        Databases.IS_JOB_DATABASE_READY);

    final ProcessFactory processFactory = getProcessBuilderFactory(configs);

    final JobPersistence jobPersistence = new DefaultJobPersistence(jobDatabase);
    final ConfigPersistence configPersistence = ConfigPersistenceBuilder.getDbPersistence(configs);
    final ConfigRepository configRepository = new ConfigRepository(configPersistence);
    final JobCleaner jobCleaner = new JobCleaner(
        configs.getWorkspaceRetentionConfig(),
        workspaceRoot,
        jobPersistence);
    final JobNotifier jobNotifier = new JobNotifier(configs.getWebappUrl(), configRepository);

    if (configs.getWorkerEnvironment() == Configs.WorkerEnvironment.KUBERNETES) {
      Map<String, String> mdc = MDC.getCopyOfContextMap();
      Executors.newSingleThreadExecutor().submit(
          () -> {
            MDC.setContextMap(mdc);
            try {
              new WorkerHeartbeatServer(KUBE_HEARTBEAT_PORT).start();
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          });
    }

    TrackingClientSingleton.initialize(
        configs.getTrackingStrategy(),
        configs.getAirbyteRole(),
        configs.getAirbyteVersion(),
        configRepository);

    Optional<String> airbyteDatabaseVersion = jobPersistence.getVersion();
    int loopCount = 0;
    while ((airbyteDatabaseVersion.isEmpty() || !AirbyteVersion.isCompatible(configs.getAirbyteVersion(), airbyteDatabaseVersion.get()))
        && loopCount < 300) {
      LOGGER.warn("Waiting for Server to start...");
      TimeUnit.SECONDS.sleep(1);
      airbyteDatabaseVersion = jobPersistence.getVersion();
      loopCount++;
    }
    if (airbyteDatabaseVersion.isPresent()) {
      AirbyteVersion.assertIsCompatible(configs.getAirbyteVersion(), airbyteDatabaseVersion.get());
    } else {
      throw new IllegalStateException("Unable to retrieve Airbyte Version, aborting...");
    }

    final WorkflowServiceStubs temporalService = TemporalUtils.createTemporalService(temporalHost);
    final TemporalClient temporalClient = TemporalClient.production(temporalHost, workspaceRoot);

    LOGGER.info("Launching scheduler...");
    new SchedulerApp(workspaceRoot, processFactory, jobPersistence, configRepository, jobCleaner, jobNotifier, temporalClient, temporalService)
        .start();
  }

}
