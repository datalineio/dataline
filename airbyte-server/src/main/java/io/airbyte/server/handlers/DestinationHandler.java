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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import io.airbyte.api.model.ConnectionRead;
import io.airbyte.api.model.DestinationCreate;
import io.airbyte.api.model.DestinationIdRequestBody;
import io.airbyte.api.model.DestinationRead;
import io.airbyte.api.model.DestinationReadList;
import io.airbyte.api.model.DestinationUpdate;
import io.airbyte.api.model.WorkspaceIdRequestBody;
import io.airbyte.commons.docker.DockerUtils;
import io.airbyte.config.ConfigSchema;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.StandardDestinationDefinition;
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

public class DestinationHandler {

  private final ConnectionsHandler connectionsHandler;
  private final SpecFetcher specFetcher;
  private final Supplier<UUID> uuidGenerator;
  private final ConfigRepository configRepository;
  private final JsonSchemaValidator validator;
  private final JsonSecretsProcessor secretProcessor;
  private final ConfigurationUpdate configurationUpdate;

  @VisibleForTesting
  DestinationHandler(final ConfigRepository configRepository,
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
    this.secretProcessor = secretsProcessor;
    this.configurationUpdate = configurationUpdate;
  }

  public DestinationHandler(final ConfigRepository configRepository,
                            final JsonSchemaValidator integrationSchemaValidation,
                            final SpecFetcher specFetcher,
                            final ConnectionsHandler connectionsHandler) {
    this(
        configRepository,
        integrationSchemaValidation,
        specFetcher,
        connectionsHandler, UUID::randomUUID,
        new JsonSecretsProcessor(),
        new ConfigurationUpdate(configRepository, specFetcher));
  }

  public DestinationRead createDestination(final DestinationCreate destinationCreate)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    // validate configuration
    final ConnectorSpecification spec = getSpec(destinationCreate.getDestinationDefinitionId());
    validateDestination(spec, destinationCreate.getConnectionConfiguration());

    // persist
    final UUID destinationId = uuidGenerator.get();
    persistDestinationConnection(
        destinationCreate.getName() != null ? destinationCreate.getName() : "default",
        destinationCreate.getDestinationDefinitionId(),
        destinationCreate.getWorkspaceId(),
        destinationId,
        destinationCreate.getConnectionConfiguration(),
        false);

    // read configuration from db
    return buildDestinationRead(destinationId, spec);
  }

  public void deleteDestination(final DestinationIdRequestBody destinationIdRequestBody)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    // get existing implementation
    final DestinationRead destination = buildDestinationRead(destinationIdRequestBody.getDestinationId());

    deleteDestination(destination);
  }

  public void deleteDestination(final DestinationRead destination)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    // disable all connections associated with this destination
    // Delete connections first in case it it fails in the middle, destination will still be visible
    final WorkspaceIdRequestBody workspaceIdRequestBody = new WorkspaceIdRequestBody().workspaceId(destination.getWorkspaceId());
    for (ConnectionRead connectionRead : connectionsHandler.listConnectionsForWorkspace(workspaceIdRequestBody).getConnections()) {
      if (!connectionRead.getDestinationId().equals(destination.getDestinationId())) {
        continue;
      }

      connectionsHandler.deleteConnection(connectionRead);
    }

    // persist
    persistDestinationConnection(
        destination.getName(),
        destination.getDestinationDefinitionId(),
        destination.getWorkspaceId(),
        destination.getDestinationId(),
        destination.getConnectionConfiguration(),
        true);
  }

  public DestinationRead updateDestination(final DestinationUpdate destinationUpdate)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    // get existing implementation
    final DestinationConnection updatedDestination = configurationUpdate
        .destination(destinationUpdate.getDestinationId(), destinationUpdate.getConnectionConfiguration());

    final ConnectorSpecification spec = getSpec(updatedDestination.getDestinationDefinitionId());

    // validate configuration
    validateDestination(spec, updatedDestination.getConfiguration());

    // persist
    persistDestinationConnection(
        updatedDestination.getName(),
        updatedDestination.getDestinationDefinitionId(),
        updatedDestination.getWorkspaceId(),
        updatedDestination.getDestinationId(),
        updatedDestination.getConfiguration(),
        updatedDestination.getTombstone());

    // read configuration from db
    return buildDestinationRead(destinationUpdate.getDestinationId(), spec);
  }

  public DestinationRead getDestination(DestinationIdRequestBody destinationIdRequestBody)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    final UUID destinationId = destinationIdRequestBody.getDestinationId();
    final DestinationConnection dci = configRepository.getDestinationConnection(destinationId);

    if (dci.getTombstone()) {
      throw new ConfigNotFoundException(ConfigSchema.DESTINATION_CONNECTION, destinationId);
    }

    return buildDestinationRead(destinationIdRequestBody.getDestinationId());
  }

  public DestinationReadList listDestinationsForWorkspace(WorkspaceIdRequestBody workspaceIdRequestBody)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    final List<DestinationRead> reads = Lists.newArrayList();

    for (DestinationConnection dci : configRepository.listDestinationConnection()) {
      if (!dci.getWorkspaceId().equals(workspaceIdRequestBody.getWorkspaceId())) {
        continue;
      }

      if (dci.getTombstone()) {
        continue;
      }

      reads.add(buildDestinationRead(dci.getDestinationId()));
    }

    return new DestinationReadList().destinations(reads);
  }

  private void validateDestination(final ConnectorSpecification spec, final JsonNode configuration) throws JsonValidationException {
    validator.ensure(spec.getConnectionSpecification(), configuration);
  }

  private ConnectorSpecification getSpec(UUID destinationDefinitionId)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    final StandardDestinationDefinition destinationDef = configRepository.getStandardDestinationDefinition(destinationDefinitionId);
    final String imageName = DockerUtils.getTaggedImageName(destinationDef.getDockerRepository(), destinationDef.getDockerImageTag());
    return specFetcher.execute(imageName);
  }

  private void persistDestinationConnection(final String name,
                                            final UUID destinationDefinitionId,
                                            final UUID workspaceId,
                                            final UUID destinationId,
                                            final JsonNode configurationJson,
                                            final boolean tombstone)
      throws JsonValidationException, IOException {
    final DestinationConnection destinationConnection = new DestinationConnection()
        .withName(name)
        .withDestinationDefinitionId(destinationDefinitionId)
        .withWorkspaceId(workspaceId)
        .withDestinationId(destinationId)
        .withConfiguration(configurationJson)
        .withTombstone(tombstone);

    configRepository.writeDestinationConnection(destinationConnection);
  }

  private DestinationRead buildDestinationRead(final UUID destinationId) throws JsonValidationException, IOException, ConfigNotFoundException {
    final ConnectorSpecification spec = getSpec(configRepository.getDestinationConnection(destinationId).getDestinationDefinitionId());
    return buildDestinationRead(destinationId, spec);
  }

  private DestinationRead buildDestinationRead(final UUID destinationId, ConnectorSpecification spec)
      throws ConfigNotFoundException, IOException, JsonValidationException {

    // remove secrets from config before returning the read
    final DestinationConnection dci = configRepository.getDestinationConnection(destinationId);
    dci.setConfiguration(secretProcessor.maskSecrets(dci.getConfiguration(), spec.getConnectionSpecification()));

    final StandardDestinationDefinition standardDestinationDefinition =
        configRepository.getStandardDestinationDefinition(dci.getDestinationDefinitionId());
    return buildDestinationRead(dci, standardDestinationDefinition);
  }

  private DestinationRead buildDestinationRead(final DestinationConnection destinationConnection,
                                               final StandardDestinationDefinition standardDestinationDefinition) {
    return new DestinationRead()
        .destinationDefinitionId(standardDestinationDefinition.getDestinationDefinitionId())
        .destinationId(destinationConnection.getDestinationId())
        .workspaceId(destinationConnection.getWorkspaceId())
        .destinationDefinitionId(destinationConnection.getDestinationDefinitionId())
        .connectionConfiguration(destinationConnection.getConfiguration())
        .name(destinationConnection.getName())
        .destinationName(standardDestinationDefinition.getName());
  }

}
