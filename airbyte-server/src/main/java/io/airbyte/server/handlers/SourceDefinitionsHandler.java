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

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.api.model.*;
import io.airbyte.commons.docker.DockerUtils;
import io.airbyte.commons.resources.MoreResources;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.airbyte.scheduler.client.CachingSynchronousSchedulerClient;
import io.airbyte.scheduler.client.SynchronousResponse;
import io.airbyte.server.converters.JobConverter;
import io.airbyte.server.converters.SpecFetcher;
import io.airbyte.server.errors.BadObjectSchemaKnownException;
import io.airbyte.server.errors.InternalServerKnownException;
import io.airbyte.server.services.AirbyteGithubStore;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class SourceDefinitionsHandler {

  private final SpecFetcher specFetcher;
  private final ConfigRepository configRepository;
  private final Supplier<UUID> uuidSupplier;
  private final AirbyteGithubStore githubStore;
  private final CachingSynchronousSchedulerClient schedulerSynchronousClient;

  public SourceDefinitionsHandler(
                                  final ConfigRepository configRepository,
                                  final SpecFetcher specFetcher,
                                  final CachingSynchronousSchedulerClient schedulerSynchronousClient) {
    this(configRepository, specFetcher, UUID::randomUUID, schedulerSynchronousClient, AirbyteGithubStore.production());
  }

  public SourceDefinitionsHandler(
                                  final ConfigRepository configRepository,
                                  final SpecFetcher specFetcher,
                                  final Supplier<UUID> uuidSupplier,
                                  final CachingSynchronousSchedulerClient schedulerSynchronousClient,
                                  final AirbyteGithubStore githubStore) {
    this.configRepository = configRepository;
    this.specFetcher = specFetcher;
    this.uuidSupplier = uuidSupplier;
    this.schedulerSynchronousClient = schedulerSynchronousClient;
    this.githubStore = githubStore;
  }

  @VisibleForTesting
  static SourceDefinitionRead buildSourceDefinitionRead(StandardSourceDefinition standardSourceDefinition) {
    try {
      return new SourceDefinitionRead()
          .sourceDefinitionId(standardSourceDefinition.getSourceDefinitionId())
          .name(standardSourceDefinition.getName())
          .dockerRepository(standardSourceDefinition.getDockerRepository())
          .dockerImageTag(standardSourceDefinition.getDockerImageTag())
          .documentationUrl(new URI(standardSourceDefinition.getDocumentationUrl()))
          .icon(loadIcon(standardSourceDefinition.getIcon()));
    } catch (URISyntaxException | NullPointerException e) {
      throw new InternalServerKnownException("Unable to process retrieved latest source definitions list", e);
    }
  }

  public SourceDefinitionReadList listSourceDefinitions() throws IOException, JsonValidationException {
    return toSourceDefinitionReadList(configRepository.listStandardSources());
  }

  private static SourceDefinitionReadList toSourceDefinitionReadList(List<StandardSourceDefinition> defs) {
    final List<SourceDefinitionRead> reads = defs.stream()
        .map(SourceDefinitionsHandler::buildSourceDefinitionRead)
        .collect(Collectors.toList());
    return new SourceDefinitionReadList().sourceDefinitions(reads);
  }

  public SourceDefinitionReadList listLatestSourceDefinitions() {
    return toSourceDefinitionReadList(getLatestSources());
  }

  private List<StandardSourceDefinition> getLatestSources() {
    try {
      return githubStore.getLatestSources();
    } catch (InterruptedException e) {
      throw new InternalServerKnownException("Request to retrieve latest destination definitions failed", e);
    }
  }

  public SourceDefinitionRead getSourceDefinition(SourceDefinitionIdRequestBody sourceDefinitionIdRequestBody)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    return buildSourceDefinitionRead(configRepository.getStandardSourceDefinition(sourceDefinitionIdRequestBody.getSourceDefinitionId()));
  }

  public SourceDefinitionReadWithJobInfo createSourceDefinition(SourceDefinitionCreate sourceDefinitionCreate)
      throws JsonValidationException, IOException {

    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withSourceDefinitionId(uuidSupplier.get())
        .withDockerRepository(sourceDefinitionCreate.getDockerRepository())
        .withDockerImageTag(sourceDefinitionCreate.getDockerImageTag())
        .withDocumentationUrl(sourceDefinitionCreate.getDocumentationUrl().toString())
        .withName(sourceDefinitionCreate.getName())
        .withIcon(sourceDefinitionCreate.getIcon());

    return saveSourceDefinition(sourceDefinition);
  }

  public SourceDefinitionReadWithJobInfo updateSourceDefinition(SourceDefinitionUpdate sourceDefinitionUpdate)
      throws ConfigNotFoundException, IOException, JsonValidationException {

    final StandardSourceDefinition currentSourceDefinition =
        configRepository.getStandardSourceDefinition(sourceDefinitionUpdate.getSourceDefinitionId());
    currentSourceDefinition.setDockerImageTag(sourceDefinitionUpdate.getDockerImageTag());

    return saveSourceDefinition(currentSourceDefinition);
  }

  private SourceDefinitionReadWithJobInfo saveSourceDefinition(StandardSourceDefinition definition) throws IOException, JsonValidationException {
    // Validates that the docker image exists and can generate a compatible spec by running a getSpec
    // job on the provided image and checking that getOutput() worked. If it succeeds, then writes the
    // config.
    SynchronousResponse<ConnectorSpecification> response = specFetcher.executeWithResponse(
        DockerUtils.getTaggedImageName(definition.getDockerRepository(), definition.getDockerImageTag()));
    boolean validationSucceeded = (response != null && response.isSuccess() && response.getOutput() != null);
    if (validationSucceeded) {
      configRepository.writeStandardSource(definition);
      // we want to re-fetch the spec for updated definitions.
      schedulerSynchronousClient.resetCache();
    }

    // When an error occurs, your destination isn't echoed back to you, indicating it failed to save.
    return new SourceDefinitionReadWithJobInfo()
        .sourceDefinitionRead(validationSucceeded ? buildSourceDefinitionRead(definition) : null)
        .exception(validationSucceeded ? null
            : new BadObjectSchemaKnownException(
                String.format("Error validating docker image %s:%s - %s - see job logs for details.",
                    definition.getDockerRepository(),
                    definition.getDockerImageTag(),
                    getFailureReason(response)))
                        .asKnownExceptionInfo())
        .jobInfo(JobConverter.getSynchronousJobRead(response));
  }

  private String getFailureReason(SynchronousResponse<ConnectorSpecification> response) {
    if (response == null) {
      return "Get Spec job returned null response";
    } else if (!response.isSuccess()) {
      return "Get Spec job failed.";
    } else if (response.getOutput() == null) {
      return "Get Spec job returned null spec.";
    }
    return "No failure";
  }

  public static String loadIcon(String name) {
    try {
      return name == null ? null : MoreResources.readResource("icons/" + name);
    } catch (Exception e) {
      return null;
    }
  }

}
