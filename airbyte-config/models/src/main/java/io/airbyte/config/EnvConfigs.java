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

package io.airbyte.config;

import com.google.common.base.Preconditions;
import io.airbyte.config.helpers.LogClientSingleton;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnvConfigs implements Configs {

  private static final Logger LOGGER = LoggerFactory.getLogger(EnvConfigs.class);

  public static final String AIRBYTE_ROLE = "AIRBYTE_ROLE";
  public static final String AIRBYTE_VERSION = "AIRBYTE_VERSION";
  public static final String WORKER_ENVIRONMENT = "WORKER_ENVIRONMENT";
  public static final String WORKSPACE_ROOT = "WORKSPACE_ROOT";
  public static final String WORKSPACE_DOCKER_MOUNT = "WORKSPACE_DOCKER_MOUNT";
  public static final String LOCAL_ROOT = "LOCAL_ROOT";
  public static final String LOCAL_DOCKER_MOUNT = "LOCAL_DOCKER_MOUNT";
  public static final String CONFIG_ROOT = "CONFIG_ROOT";
  public static final String DOCKER_NETWORK = "DOCKER_NETWORK";
  public static final String TRACKING_STRATEGY = "TRACKING_STRATEGY";
  public static final String DATABASE_USER = "DATABASE_USER";
  public static final String DATABASE_PASSWORD = "DATABASE_PASSWORD";
  public static final String DATABASE_URL = "DATABASE_URL";
  public static final String CONFIG_DATABASE_USER = "CONFIG_DATABASE_USER";
  public static final String CONFIG_DATABASE_PASSWORD = "CONFIG_DATABASE_PASSWORD";
  public static final String CONFIG_DATABASE_URL = "CONFIG_DATABASE_URL";
  public static final String WEBAPP_URL = "WEBAPP_URL";
  private static final String MINIMUM_WORKSPACE_RETENTION_DAYS = "MINIMUM_WORKSPACE_RETENTION_DAYS";
  private static final String MAXIMUM_WORKSPACE_RETENTION_DAYS = "MAXIMUM_WORKSPACE_RETENTION_DAYS";
  private static final String MAXIMUM_WORKSPACE_SIZE_MB = "MAXIMUM_WORKSPACE_SIZE_MB";
  private static final String TEMPORAL_HOST = "TEMPORAL_HOST";
  private static final String TEMPORAL_WORKER_PORTS = "TEMPORAL_WORKER_PORTS";
  private static final String KUBE_NAMESPACE = "KUBE_NAMESPACE";
  private static final String RESOURCE_CPU_REQUEST = "RESOURCE_CPU_REQUEST";
  private static final String RESOURCE_CPU_LIMIT = "RESOURCE_CPU_LIMIT";
  private static final String RESOURCE_MEMORY_REQUEST = "RESOURCE_MEMORY_REQUEST";
  private static final String RESOURCE_MEMORY_LIMIT = "RESOURCE_MEMORY_LIMIT";
  private static final String DEFAULT_KUBE_NAMESPACE = "default";
  private static final String DEFAULT_RESOURCE_REQUIREMENT_CPU = null;
  private static final String DEFAULT_RESOURCE_REQUIREMENT_MEMORY = null;
  private static final long DEFAULT_MINIMUM_WORKSPACE_RETENTION_DAYS = 1;
  private static final long DEFAULT_MAXIMUM_WORKSPACE_RETENTION_DAYS = 60;
  private static final long DEFAULT_MAXIMUM_WORKSPACE_SIZE_MB = 5000;

  public static final String DEFAULT_NETWORK = "host";

  private final Function<String, String> getEnv;

  public EnvConfigs() {
    this(System::getenv);
  }

  EnvConfigs(final Function<String, String> getEnv) {
    this.getEnv = getEnv;
  }

  @Override
  public String getAirbyteRole() {
    return getEnv(AIRBYTE_ROLE);
  }

  @Override
  public String getAirbyteVersion() {
    return getEnsureEnv(AIRBYTE_VERSION);
  }

  @Override
  public String getAirbyteVersionOrWarning() {
    return Optional.ofNullable(getEnv(AIRBYTE_VERSION)).orElse("version not set");
  }

  @Override
  public Path getConfigRoot() {
    return getPath(CONFIG_ROOT);
  }

  @Override
  public Path getWorkspaceRoot() {
    return getPath(WORKSPACE_ROOT);
  }

  @Override
  public Path getLocalRoot() {
    return getPath(LOCAL_ROOT);
  }

  @Override
  public String getDatabaseUser() {
    return getEnsureEnv(DATABASE_USER);
  }

  @Override
  public String getDatabasePassword() {
    return getEnsureEnv(DATABASE_PASSWORD);
  }

  @Override
  public String getDatabaseUrl() {
    return getEnsureEnv(DATABASE_URL);
  }

  @Override
  public String getConfigDatabaseUser() {
    // Default to reuse the job database
    return getEnvOrDefault(CONFIG_DATABASE_USER, getDatabaseUser());
  }

  @Override
  public String getConfigDatabasePassword() {
    // Default to reuse the job database
    return getEnvOrDefault(CONFIG_DATABASE_PASSWORD, getDatabasePassword());
  }

  @Override
  public String getConfigDatabaseUrl() {
    // Default to reuse the job database
    return getEnvOrDefault(CONFIG_DATABASE_URL, getDatabaseUrl());
  }

  @Override
  public String getWebappUrl() {
    return getEnsureEnv(WEBAPP_URL);
  }

  @Override
  public String getWorkspaceDockerMount() {
    return getEnvOrDefault(WORKSPACE_DOCKER_MOUNT, getWorkspaceRoot().toString());
  }

  @Override
  public String getLocalDockerMount() {
    return getEnvOrDefault(LOCAL_DOCKER_MOUNT, getLocalRoot().toString());
  }

  @Override
  public String getDockerNetwork() {
    return getEnvOrDefault(DOCKER_NETWORK, DEFAULT_NETWORK);
  }

  @Override
  public TrackingStrategy getTrackingStrategy() {
    return getEnvOrDefault(TRACKING_STRATEGY, TrackingStrategy.LOGGING, s -> {
      try {
        return TrackingStrategy.valueOf(s.toUpperCase());
      } catch (IllegalArgumentException e) {
        LOGGER.info(s + " not recognized, defaulting to " + TrackingStrategy.LOGGING);
        return TrackingStrategy.LOGGING;
      }
    });
  }

  @Override
  public WorkerEnvironment getWorkerEnvironment() {
    return getEnvOrDefault(WORKER_ENVIRONMENT, WorkerEnvironment.DOCKER, s -> WorkerEnvironment.valueOf(s.toUpperCase()));
  }

  @Override
  public WorkspaceRetentionConfig getWorkspaceRetentionConfig() {
    long minDays = getEnvOrDefault(MINIMUM_WORKSPACE_RETENTION_DAYS, DEFAULT_MINIMUM_WORKSPACE_RETENTION_DAYS);
    long maxDays = getEnvOrDefault(MAXIMUM_WORKSPACE_RETENTION_DAYS, DEFAULT_MAXIMUM_WORKSPACE_RETENTION_DAYS);
    long maxSizeMb = getEnvOrDefault(MAXIMUM_WORKSPACE_SIZE_MB, DEFAULT_MAXIMUM_WORKSPACE_SIZE_MB);

    return new WorkspaceRetentionConfig(minDays, maxDays, maxSizeMb);
  }

  @Override
  public String getTemporalHost() {
    return getEnvOrDefault(TEMPORAL_HOST, "airbyte-temporal:7233");
  }

  @Override
  public Set<Integer> getTemporalWorkerPorts() {
    return Arrays.stream(getEnvOrDefault(TEMPORAL_WORKER_PORTS, "").split(","))
        .map(Integer::valueOf)
        .collect(Collectors.toSet());
  }

  @Override
  public String getKubeNamespace() {
    return getEnvOrDefault(KUBE_NAMESPACE, DEFAULT_KUBE_NAMESPACE);
  }

  @Override
  public String getCpuRequest() {
    return getEnvOrDefault(RESOURCE_CPU_REQUEST, DEFAULT_RESOURCE_REQUIREMENT_CPU);
  }

  @Override
  public String getCpuLimit() {
    return getEnvOrDefault(RESOURCE_CPU_LIMIT, DEFAULT_RESOURCE_REQUIREMENT_CPU);
  }

  @Override
  public String getMemoryRequest() {
    return getEnvOrDefault(RESOURCE_MEMORY_REQUEST, DEFAULT_RESOURCE_REQUIREMENT_MEMORY);
  }

  @Override
  public String getMemoryLimit() {
    return getEnvOrDefault(RESOURCE_MEMORY_LIMIT, DEFAULT_RESOURCE_REQUIREMENT_MEMORY);
  }

  @Override
  public String getS3LogBucket() {
    return getEnvOrDefault(LogClientSingleton.S3_LOG_BUCKET, "");
  }

  @Override
  public String getS3LogBucketRegion() {
    return getEnvOrDefault(LogClientSingleton.S3_LOG_BUCKET_REGION, "");
  }

  @Override
  public String getAwsAccessKey() {
    return getEnvOrDefault(LogClientSingleton.AWS_ACCESS_KEY_ID, "");
  }

  @Override
  public String getAwsSecretAccessKey() {
    return getEnvOrDefault(LogClientSingleton.AWS_SECRET_ACCESS_KEY, "");
  }

  @Override
  public String getS3MinioEndpoint() {
    return getEnvOrDefault(LogClientSingleton.S3_MINIO_ENDPOINT, "");
  }

  @Override
  public String getGcpStorageBucket() {
    return getEnvOrDefault(LogClientSingleton.GCP_STORAGE_BUCKET, "");
  }

  @Override
  public String getGoogleApplicationCredentials() {
    return getEnvOrDefault(LogClientSingleton.GOOGLE_APPLICATION_CREDENTIALS, "");
  }

  private String getEnvOrDefault(String key, String defaultValue) {
    return getEnvOrDefault(key, defaultValue, Function.identity());
  }

  private long getEnvOrDefault(String key, long defaultValue) {
    return getEnvOrDefault(key, defaultValue, Long::parseLong);
  }

  private <T> T getEnvOrDefault(String key, T defaultValue, Function<String, T> parser) {
    final String value = getEnv.apply(key);
    if (value != null && !value.isEmpty()) {
      return parser.apply(value);
    } else {
      LOGGER.info(key + " not found or empty, defaulting to " + defaultValue);
      return defaultValue;
    }
  }

  private String getEnv(final String name) {
    return getEnv.apply(name);
  }

  private String getEnsureEnv(final String name) {
    final String value = getEnv(name);
    Preconditions.checkArgument(value != null, "'%s' environment variable cannot be null", name);

    return value;
  }

  private Path getPath(final String name) {
    final String value = getEnv.apply(name);
    if (value == null) {
      throw new IllegalArgumentException("Env variable not defined: " + name);
    }
    return Path.of(value);
  }

}
