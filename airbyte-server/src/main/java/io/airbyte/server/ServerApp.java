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

package io.airbyte.server;

import io.airbyte.analytics.TrackingClientSingleton;
import io.airbyte.commons.io.FileTtlManager;
import io.airbyte.commons.resources.MoreResources;
import io.airbyte.commons.version.AirbyteVersion;
import io.airbyte.config.Configs;
import io.airbyte.config.Configs.WorkerEnvironment;
import io.airbyte.config.EnvConfigs;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.helpers.LogClientSingleton;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigPersistence;
import io.airbyte.config.persistence.ConfigPersistenceBuilder;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.config.persistence.PersistenceConstants;
import io.airbyte.db.Database;
import io.airbyte.db.Databases;
import io.airbyte.scheduler.client.DefaultSchedulerJobClient;
import io.airbyte.scheduler.client.DefaultSynchronousSchedulerClient;
import io.airbyte.scheduler.client.SpecCachingSynchronousSchedulerClient;
import io.airbyte.scheduler.persistence.DefaultJobCreator;
import io.airbyte.scheduler.persistence.DefaultJobPersistence;
import io.airbyte.scheduler.persistence.JobPersistence;
import io.airbyte.scheduler.persistence.job_tracker.JobTracker;
import io.airbyte.server.apis.ConfigurationApi;
import io.airbyte.server.errors.InvalidInputExceptionMapper;
import io.airbyte.server.errors.InvalidJsonExceptionMapper;
import io.airbyte.server.errors.InvalidJsonInputExceptionMapper;
import io.airbyte.server.errors.KnownExceptionMapper;
import io.airbyte.server.errors.NotFoundExceptionMapper;
import io.airbyte.server.errors.UncaughtExceptionMapper;
import io.airbyte.server.version_mismatch.VersionMismatchServer;
import io.airbyte.validation.json.JsonValidationException;
import io.airbyte.workers.temporal.TemporalClient;
import io.airbyte.workers.temporal.TemporalUtils;
import io.temporal.serviceclient.WorkflowServiceStubs;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseFilter;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.jackson.internal.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import org.glassfish.jersey.process.internal.RequestScoped;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public class ServerApp {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServerApp.class);
  private static final int PORT = 8001;
  /**
   * We can't support automatic migration for kube before this version because we had a bug in kube
   * which would cause airbyte db to erase state upon termination, as a result the automatic migration
   * wouldn't run
   */
  private static final AirbyteVersion KUBE_SUPPORT_FOR_AUTOMATIC_MIGRATION = new AirbyteVersion("0.26.5-alpha");
  private final ConfigRepository configRepository;
  private final JobPersistence jobPersistence;
  private final Configs configs;
  private final Set<ContainerRequestFilter> requestFilters;
  private final Set<ContainerResponseFilter> responseFilters;

  public ServerApp(final ConfigRepository configRepository,
                   final JobPersistence jobPersistence,
                   final Configs configs,
                   final Set<ContainerRequestFilter> requestFilters,
                   final Set<ContainerResponseFilter> responseFilters) {
    this.configRepository = configRepository;
    this.jobPersistence = jobPersistence;
    this.configs = configs;
    this.requestFilters = requestFilters;
    this.responseFilters = responseFilters;
  }

  public void start() throws Exception {
    TrackingClientSingleton.get().identify();

    Server server = new Server(PORT);

    ServletContextHandler handler = new ServletContextHandler();

    Map<String, String> mdc = MDC.getCopyOfContextMap();

    ConfigurationApiFactory.setSchedulerJobClient(new DefaultSchedulerJobClient(jobPersistence, new DefaultJobCreator(jobPersistence)));
    final JobTracker jobTracker = new JobTracker(configRepository, jobPersistence);
    final WorkflowServiceStubs temporalService = TemporalUtils.createTemporalService(configs.getTemporalHost());
    final TemporalClient temporalClient = TemporalClient.production(configs.getTemporalHost(), configs.getWorkspaceRoot());

    ConfigurationApiFactory
        .setSynchronousSchedulerClient(new SpecCachingSynchronousSchedulerClient(new DefaultSynchronousSchedulerClient(temporalClient, jobTracker)));
    ConfigurationApiFactory.setTemporalService(temporalService);
    ConfigurationApiFactory.setConfigRepository(configRepository);
    ConfigurationApiFactory.setJobPersistence(jobPersistence);
    ConfigurationApiFactory.setConfigs(configs);
    ConfigurationApiFactory.setArchiveTtlManager(new FileTtlManager(10, TimeUnit.MINUTES, 10));
    ConfigurationApiFactory.setMdc(mdc);

    ResourceConfig rc =
        new ResourceConfig()
            // add filters
            .registerInstances(requestFilters)
            .registerInstances(responseFilters)
            // request logging
            .register(new RequestLogger(mdc))
            // api
            .register(ConfigurationApi.class)
            .register(
                new AbstractBinder() {

                  @Override
                  public void configure() {
                    bindFactory(ConfigurationApiFactory.class)
                        .to(ConfigurationApi.class)
                        .in(RequestScoped.class);
                  }

                })
            // exception handling
            .register(InvalidInputExceptionMapper.class)
            .register(InvalidJsonExceptionMapper.class)
            .register(InvalidJsonInputExceptionMapper.class)
            .register(KnownExceptionMapper.class)
            .register(UncaughtExceptionMapper.class)
            .register(NotFoundExceptionMapper.class)
            // needed so that the custom json exception mappers don't get overridden
            // https://stackoverflow.com/questions/35669774/jersey-custom-exception-mapper-for-invalid-json-string
            .register(JacksonJaxbJsonProvider.class);

    ServletHolder configServlet = new ServletHolder(new ServletContainer(rc));

    handler.addServlet(configServlet, "/api/*");

    server.setHandler(handler);

    server.start();
    final String banner = MoreResources.readResource("banner/banner.txt");
    LOGGER.info(banner + String.format("Version: %s\n", configs.getAirbyteVersion()));
    server.join();
  }

  private static void setCustomerIdIfNotSet(final ConfigRepository configRepository) throws InterruptedException {
    StandardWorkspace workspace = null;

    // retry until the workspace is available / waits for file config initialization
    while (workspace == null) {
      try {
        workspace = configRepository.getStandardWorkspace(PersistenceConstants.DEFAULT_WORKSPACE_ID, true);

        if (workspace.getCustomerId() == null) {
          final UUID customerId = UUID.randomUUID();
          LOGGER.info("customerId not set for workspace. Setting it to " + customerId);
          workspace.setCustomerId(customerId);

          configRepository.writeStandardWorkspace(workspace);
        } else {
          LOGGER.info("customerId already set for workspace: " + workspace.getCustomerId());
        }
      } catch (ConfigNotFoundException e) {
        LOGGER.error("Could not find workspace with id: " + PersistenceConstants.DEFAULT_WORKSPACE_ID, e);
        Thread.sleep(1000);
      } catch (JsonValidationException | IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static void runServer(final Set<ContainerRequestFilter> requestFilters,
                               final Set<ContainerResponseFilter> responseFilters)
      throws Exception {
    final Configs configs = new EnvConfigs();

    MDC.put(LogClientSingleton.WORKSPACE_MDC_KEY, LogClientSingleton.getServerLogsRoot(configs).toString());

    LOGGER.info("Creating config repository...");
    final ConfigPersistence configPersistence = ConfigPersistenceBuilder.getAndInitializeDbPersistence(configs);
    final ConfigRepository configRepository = new ConfigRepository(configPersistence);

    // hack: upon installation we need to assign a random customerId so that when
    // tracking we can associate all action with the correct anonymous id.
    setCustomerIdIfNotSet(configRepository);

    TrackingClientSingleton.initialize(
        configs.getTrackingStrategy(),
        configs.getAirbyteRole(),
        configs.getAirbyteVersion(),
        configRepository);

    LOGGER.info("Creating Scheduler persistence...");
    final Database jobDatabase = Databases.createPostgresDatabaseWithRetry(
        configs.getDatabaseUser(),
        configs.getDatabasePassword(),
        configs.getDatabaseUrl(),
        Databases.IS_JOB_DATABASE_READY);
    final JobPersistence jobPersistence = new DefaultJobPersistence(jobDatabase);

    final String airbyteVersion = configs.getAirbyteVersion();
    if (jobPersistence.getVersion().isEmpty()) {
      LOGGER.info(String.format("Setting Database version to %s...", airbyteVersion));
      jobPersistence.setVersion(airbyteVersion);
    }

    Optional<String> airbyteDatabaseVersion = jobPersistence.getVersion();
    if (airbyteDatabaseVersion.isPresent() && isDatabaseVersionBehindAppVersion(airbyteVersion, airbyteDatabaseVersion.get())) {
      boolean isKubernetes = configs.getWorkerEnvironment() == WorkerEnvironment.KUBERNETES;
      boolean versionSupportsAutoMigrate =
          new AirbyteVersion(airbyteDatabaseVersion.get()).patchVersionCompareTo(KUBE_SUPPORT_FOR_AUTOMATIC_MIGRATION) >= 0;
      if (!isKubernetes || versionSupportsAutoMigrate) {
        runAutomaticMigration(configRepository, jobPersistence, airbyteVersion, airbyteDatabaseVersion.get());
        // After migration, upgrade the DB version
        airbyteDatabaseVersion = jobPersistence.getVersion();
      } else {
        LOGGER.info("Can not run automatic migration for Airbyte on KUBERNETES before version " + KUBE_SUPPORT_FOR_AUTOMATIC_MIGRATION.getVersion());
      }
    }

    if (airbyteDatabaseVersion.isPresent() && AirbyteVersion.isCompatible(airbyteVersion, airbyteDatabaseVersion.get())) {
      LOGGER.info("Starting server...");
      new ServerApp(configRepository, jobPersistence, configs, requestFilters, responseFilters).start();
    } else {
      LOGGER.info("Start serving version mismatch errors. Automatic migration either failed or didn't run");
      new VersionMismatchServer(airbyteVersion, airbyteDatabaseVersion.get(), PORT).start();
    }
  }

  public static void main(String[] args) throws Exception {
    runServer(Collections.emptySet(), Set.of(new CorsFilter()));
  }

  /**
   * Ideally when automatic migration runs, we should make sure that we acquire a lock on database and
   * no other operation is allowed
   */
  private static void runAutomaticMigration(ConfigRepository configRepository,
                                            JobPersistence jobPersistence,
                                            String airbyteVersion,
                                            String airbyteDatabaseVersion) {
    LOGGER.info("Running Automatic Migration from version : " + airbyteDatabaseVersion + " to version : " + airbyteVersion);
    final Path latestSeedsPath = Path.of(System.getProperty("user.dir")).resolve("latest_seeds");
    LOGGER.info("Last seeds dir: {}", latestSeedsPath);
    try (RunMigration runMigration = new RunMigration(airbyteDatabaseVersion,
        jobPersistence, configRepository, airbyteVersion, latestSeedsPath)) {
      runMigration.run();
    } catch (Exception e) {
      LOGGER.error("Automatic Migration failed ", e);
    }
  }

  public static boolean isDatabaseVersionBehindAppVersion(String airbyteVersion, String airbyteDatabaseVersion) {
    boolean bothVersionsCompatible = AirbyteVersion.isCompatible(airbyteVersion, airbyteDatabaseVersion);
    if (bothVersionsCompatible) {
      return false;
    }

    AirbyteVersion serverVersion = new AirbyteVersion(airbyteVersion);
    AirbyteVersion databaseVersion = new AirbyteVersion(airbyteDatabaseVersion);

    if (databaseVersion.getMajorVersion().compareTo(serverVersion.getMajorVersion()) < 0) {
      return true;
    }

    return databaseVersion.getMinorVersion().compareTo(serverVersion.getMinorVersion()) < 0;
  }

}
