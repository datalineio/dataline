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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import io.airbyte.api.model.ConnectionRead;
import io.airbyte.api.model.SourceCreate;
import io.airbyte.api.model.SourceIdRequestBody;
import io.airbyte.api.model.SourceRead;
import io.airbyte.api.model.SourceReadList;
import io.airbyte.api.model.SourceUpdate;
import io.airbyte.api.model.WorkspaceIdRequestBody;
import io.airbyte.commons.docker.DockerUtils;
import io.airbyte.config.ConfigSchema;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.airbyte.server.converters.ConfigurationUpdate;
import io.airbyte.server.converters.JsonSecretsProcessor;
import io.airbyte.server.converters.SpecFetcher;
import io.airbyte.validation.json.JsonSchemaValidator;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

public class SourceHandler {

  private final Supplier<UUID> uuidGenerator;
  private final ConfigRepository configRepository;
  private final JsonSchemaValidator validator;
  private final SpecFetcher specFetcher;
  private final ConnectionsHandler connectionsHandler;
  private final JsonSecretsProcessor secretsProcessor;
  private final ConfigurationUpdate configurationUpdate;

  SourceHandler(final ConfigRepository configRepository,
                final JsonSchemaValidator integrationSchemaValidation,
                final SpecFetcher specFetcher,
                final ConnectionsHandler connectionsHandler,
                final Supplier<UUID> uuidGenerator,
                final JsonSecretsProcessor secretsProcessor,
                final ConfigurationUpdate configurationUpdate) {
    this.configRepository = configRepository;
    this.validator = integrationSchemaValidation;
    this.specFetcher = specFetcher;
    this.connectionsHandler = connectionsHandler;
    this.uuidGenerator = uuidGenerator;
    this.secretsProcessor = secretsProcessor;
    this.configurationUpdate = configurationUpdate;
  }

  public SourceHandler(final ConfigRepository configRepository,
                       final JsonSchemaValidator integrationSchemaValidation,
                       final SpecFetcher specFetcher,
                       final ConnectionsHandler connectionsHandler) {
    this(
        configRepository,
        integrationSchemaValidation,
        specFetcher,
        connectionsHandler,
        UUID::randomUUID,
        new JsonSecretsProcessor(),
        new ConfigurationUpdate(configRepository, specFetcher));
  }

  public SourceRead createSource(SourceCreate sourceCreate) throws ConfigNotFoundException, IOException, JsonValidationException {
    // validate configuration
    ConnectorSpecification spec = getSpecFromSourceDefinitionId(sourceCreate.getSourceDefinitionId());
    validateSource(spec, sourceCreate.getConnectionConfiguration());

    // persist
    final UUID sourceId = uuidGenerator.get();
    persistSourceConnection(
        sourceCreate.getName() != null ? sourceCreate.getName() : "default",
        sourceCreate.getSourceDefinitionId(),
        sourceCreate.getWorkspaceId(),
        sourceId,
        false,
        sourceCreate.getConnectionConfiguration());

    // read configuration from db
    return buildSourceRead(sourceId, spec);
  }

  public SourceRead updateSource(SourceUpdate sourceUpdate) throws ConfigNotFoundException, IOException, JsonValidationException {
    final SourceConnection updatedSource = configurationUpdate.source(sourceUpdate.getSourceId(), sourceUpdate.getConnectionConfiguration());
    ConnectorSpecification spec = getSpecFromSourceId(updatedSource.getSourceId());
    validateSource(spec, sourceUpdate.getConnectionConfiguration());

    // persist
    persistSourceConnection(
        updatedSource.getName(),
        updatedSource.getSourceDefinitionId(),
        updatedSource.getWorkspaceId(),
        updatedSource.getSourceId(),
        updatedSource.getTombstone(),
        updatedSource.getConfiguration());

    // read configuration from db
    return buildSourceRead(sourceUpdate.getSourceId(), spec);
  }

  public SourceRead getSource(SourceIdRequestBody sourceIdRequestBody) throws JsonValidationException, IOException, ConfigNotFoundException {
    final UUID sourceId = sourceIdRequestBody.getSourceId();
    final SourceConnection sourceConnection = configRepository.getSourceConnection(sourceId);

    if (sourceConnection.getTombstone()) {
      throw new ConfigNotFoundException(ConfigSchema.SOURCE_CONNECTION, sourceId);
    }

    return buildSourceRead(sourceId);
  }

  public SourceReadList listSourcesForWorkspace(WorkspaceIdRequestBody workspaceIdRequestBody)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    final List<SourceRead> reads = Lists.newArrayList();

    for (SourceConnection sci : configRepository.listSourceConnection()) {
      if (!sci.getWorkspaceId().equals(workspaceIdRequestBody.getWorkspaceId())) {
        continue;
      }
      if (sci.getTombstone()) {
        continue;
      }

      reads.add(buildSourceRead(sci.getSourceId()));
    }

    return new SourceReadList().sources(reads);
  }

  public void deleteSource(SourceIdRequestBody sourceIdRequestBody) throws JsonValidationException, IOException, ConfigNotFoundException {
    // get existing source
    final SourceRead source = buildSourceRead(sourceIdRequestBody.getSourceId());
    deleteSource(source);
  }

  public void deleteSource(SourceRead source)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    // "delete" all connections associated with source as well.
    // Delete connections first in case it it fails in the middle, source will still be visible
    final WorkspaceIdRequestBody workspaceIdRequestBody = new WorkspaceIdRequestBody().workspaceId(source.getWorkspaceId());
    for (ConnectionRead connectionRead : connectionsHandler.listConnectionsForWorkspace(workspaceIdRequestBody).getConnections()) {
      if (!connectionRead.getSourceId().equals(source.getSourceId())) {
        continue;
      }

      connectionsHandler.deleteConnection(connectionRead);
    }

    // persist
    persistSourceConnection(
        source.getName(),
        source.getSourceDefinitionId(),
        source.getWorkspaceId(),
        source.getSourceId(),
        true,
        source.getConnectionConfiguration());
  }

  private SourceRead buildSourceRead(UUID sourceId) throws ConfigNotFoundException, IOException, JsonValidationException {
    // read configuration from db
    final StandardSourceDefinition sourceDef = configRepository.getSourceDefinitionFromSource(sourceId);
    final String imageName = DockerUtils.getTaggedImageName(sourceDef.getDockerRepository(), sourceDef.getDockerImageTag());
    final ConnectorSpecification spec = specFetcher.execute(imageName);
    return buildSourceRead(sourceId, spec);
  }

  private SourceRead buildSourceRead(UUID sourceId, ConnectorSpecification spec)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    // read configuration from db
    final SourceConnection sourceConnection = configRepository.getSourceConnection(sourceId);
    final StandardSourceDefinition standardSourceDefinition = configRepository.getStandardSourceDefinition(sourceConnection.getSourceDefinitionId());
    final JsonNode sanitizedConfig = secretsProcessor.maskSecrets(sourceConnection.getConfiguration(), spec.getConnectionSpecification());
    sourceConnection.setConfiguration(sanitizedConfig);
    return toSourceRead(sourceConnection, standardSourceDefinition);
  }

  private void validateSource(ConnectorSpecification spec, JsonNode implementationJson) throws JsonValidationException {
    validator.ensure(spec.getConnectionSpecification(), implementationJson);
  }

  private ConnectorSpecification getSpecFromSourceId(UUID sourceId) throws IOException, JsonValidationException, ConfigNotFoundException {
    final SourceConnection source = configRepository.getSourceConnection(sourceId);
    return getSpecFromSourceDefinitionId(source.getSourceDefinitionId());
  }

  private ConnectorSpecification getSpecFromSourceDefinitionId(UUID sourceDefId)
      throws IOException, JsonValidationException, ConfigNotFoundException {
    final StandardSourceDefinition sourceDef = configRepository.getStandardSourceDefinition(sourceDefId);
    final String imageName = DockerUtils.getTaggedImageName(sourceDef.getDockerRepository(), sourceDef.getDockerImageTag());
    return specFetcher.execute(imageName);
  }

  private void persistSourceConnection(final String name,
                                       final UUID sourceDefinitionId,
                                       final UUID workspaceId,
                                       final UUID sourceId,
                                       final boolean tombstone,
                                       final JsonNode configurationJson)
      throws JsonValidationException, IOException {
    final SourceConnection sourceConnection = new SourceConnection()
        .withName(name)
        .withSourceDefinitionId(sourceDefinitionId)
        .withWorkspaceId(workspaceId)
        .withSourceId(sourceId)
        .withTombstone(tombstone)
        .withConfiguration(configurationJson);

    configRepository.writeSourceConnection(sourceConnection);
  }

  private SourceRead toSourceRead(final SourceConnection sourceConnection,
                                  final StandardSourceDefinition standardSourceDefinition) {
    return new SourceRead()
        .sourceDefinitionId(standardSourceDefinition.getSourceDefinitionId())
        .sourceName(standardSourceDefinition.getName())
        .sourceId(sourceConnection.getSourceId())
        .workspaceId(sourceConnection.getWorkspaceId())
        .sourceDefinitionId(sourceConnection.getSourceDefinitionId())
        .connectionConfiguration(sourceConnection.getConfiguration())
        .name(sourceConnection.getName());
  }

}
