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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import io.airbyte.commons.io.LineGobbler;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.OperatorDbt;
import io.airbyte.config.ResourceRequirements;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.workers.WorkerConstants;
import io.airbyte.workers.WorkerException;
import io.airbyte.workers.WorkerUtils;
import io.airbyte.workers.process.ProcessFactory;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultNormalizationRunner implements NormalizationRunner {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultNormalizationRunner.class);

  public static final String NORMALIZATION_IMAGE_NAME = "airbyte/normalization:0.1.34";

  private final DestinationType destinationType;
  private final ProcessFactory processFactory;

  private Process process = null;

  public enum DestinationType {
    BIGQUERY,
    POSTGRES,
    REDSHIFT,
    SNOWFLAKE,
    ORACLE
  }

  public DefaultNormalizationRunner(final DestinationType destinationType, final ProcessFactory processFactory) {
    this.destinationType = destinationType;
    this.processFactory = processFactory;
  }

  @Override
  public boolean configureDbt(String jobId,
                              int attempt,
                              Path jobRoot,
                              JsonNode config,
                              ResourceRequirements resourceRequirements,
                              OperatorDbt dbtConfig)
      throws Exception {
    final Map<String, String> files = ImmutableMap.of(
        WorkerConstants.DESTINATION_CONFIG_JSON_FILENAME, Jsons.serialize(config));
    final String gitRepoUrl = dbtConfig.getGitRepoUrl();
    if (Strings.isNullOrEmpty(gitRepoUrl)) {
      throw new WorkerException("Git Repo Url is required");
    }
    final String gitRepoBranch = dbtConfig.getGitRepoBranch();
    if (Strings.isNullOrEmpty(gitRepoBranch)) {
      return runProcess(jobId, attempt, jobRoot, files, resourceRequirements, "configure-dbt",
          "--integration-type", destinationType.toString().toLowerCase(),
          "--config", WorkerConstants.DESTINATION_CONFIG_JSON_FILENAME,
          "--git-repo", gitRepoUrl);
    } else {
      return runProcess(jobId, attempt, jobRoot, files, resourceRequirements, "configure-dbt",
          "--integration-type", destinationType.toString().toLowerCase(),
          "--config", WorkerConstants.DESTINATION_CONFIG_JSON_FILENAME,
          "--git-repo", gitRepoUrl,
          "--git-branch", gitRepoBranch);
    }
  }

  @Override
  public boolean normalize(String jobId,
                           int attempt,
                           Path jobRoot,
                           JsonNode config,
                           ConfiguredAirbyteCatalog catalog,
                           ResourceRequirements resourceRequirements)
      throws Exception {
    final Map<String, String> files = ImmutableMap.of(
        WorkerConstants.DESTINATION_CONFIG_JSON_FILENAME, Jsons.serialize(config),
        WorkerConstants.DESTINATION_CATALOG_JSON_FILENAME, Jsons.serialize(catalog));

    return runProcess(jobId, attempt, jobRoot, files, resourceRequirements, "run",
        "--integration-type", destinationType.toString().toLowerCase(),
        "--config", WorkerConstants.DESTINATION_CONFIG_JSON_FILENAME,
        "--catalog", WorkerConstants.DESTINATION_CATALOG_JSON_FILENAME);
  }

  private boolean runProcess(String jobId,
                             int attempt,
                             Path jobRoot,
                             Map<String, String> files,
                             ResourceRequirements resourceRequirements,
                             final String... args)
      throws Exception {
    try {
      process = processFactory.create(jobId, attempt, jobRoot, NORMALIZATION_IMAGE_NAME, false, files, null, resourceRequirements, args);

      LineGobbler.gobble(process.getInputStream(), LOGGER::info);
      LineGobbler.gobble(process.getErrorStream(), LOGGER::error);

      WorkerUtils.wait(process);

      return process.exitValue() == 0;
    } catch (Exception e) {
      // make sure we kill the process on failure to avoid zombies.
      if (process != null) {
        WorkerUtils.cancelProcess(process);
      }
      throw e;
    }
  }

  @Override
  public void close() throws Exception {
    if (process == null) {
      return;
    }

    LOGGER.debug("Closing normalization process");
    WorkerUtils.gentleClose(process, 1, TimeUnit.MINUTES);
    if (process.isAlive() || process.exitValue() != 0) {
      throw new WorkerException("Normalization process wasn't successful");
    }
  }

  @VisibleForTesting
  DestinationType getDestinationType() {
    return destinationType;
  }

}
