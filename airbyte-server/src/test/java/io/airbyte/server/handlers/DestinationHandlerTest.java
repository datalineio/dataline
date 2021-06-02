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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import io.airbyte.api.model.*;
import io.airbyte.commons.docker.DockerUtils;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSync;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.airbyte.server.converters.ConfigurationUpdate;
import io.airbyte.server.converters.JsonSecretsProcessor;
import io.airbyte.server.converters.SpecFetcher;
import io.airbyte.server.helpers.ConnectionHelpers;
import io.airbyte.server.helpers.ConnectorSpecificationHelpers;
import io.airbyte.server.helpers.DestinationHelpers;
import io.airbyte.validation.json.JsonSchemaValidator;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DestinationHandlerTest {

  private ConfigRepository configRepository;
  private StandardDestinationDefinition standardDestinationDefinition;
  private DestinationDefinitionSpecificationRead destinationDefinitionSpecificationRead;
  private DestinationDefinitionIdRequestBody destinationDefinitionIdRequestBody;
  private UUID workspaceUUID;
  private List<DestinationConnection> destinationConnections;
  private DestinationConnection destinationConnection;
  private DestinationHandler destinationHandler;
  private ConnectionsHandler connectionsHandler;
  private SpecFetcher specFetcher;
  private ConfigurationUpdate configurationUpdate;
  private JsonSchemaValidator validator;
  private Supplier<UUID> uuidGenerator;
  private JsonSecretsProcessor secretsProcessor;
  private ConnectorSpecification connectorSpecification;
  private String imageName;

  @SuppressWarnings("unchecked")
  @BeforeEach
  void setUp() throws IOException {
    configRepository = mock(ConfigRepository.class);
    validator = mock(JsonSchemaValidator.class);
    uuidGenerator = mock(Supplier.class);
    connectionsHandler = mock(ConnectionsHandler.class);
    specFetcher = mock(SpecFetcher.class);
    configurationUpdate = mock(ConfigurationUpdate.class);
    secretsProcessor = mock(JsonSecretsProcessor.class);

    standardDestinationDefinition = new StandardDestinationDefinition()
        .withDestinationDefinitionId(UUID.randomUUID())
        .withName("db2")
        .withDockerRepository("thebestrepo")
        .withDockerImageTag("thelatesttag")
        .withDocumentationUrl("https://wikipedia.org");;
    imageName =
        DockerUtils.getTaggedImageName(standardDestinationDefinition.getDockerRepository(), standardDestinationDefinition.getDockerImageTag());

    destinationDefinitionIdRequestBody =
        new DestinationDefinitionIdRequestBody().destinationDefinitionId(standardDestinationDefinition.getDestinationDefinitionId());
    connectorSpecification = ConnectorSpecificationHelpers.generateConnectorSpecification();
    destinationDefinitionSpecificationRead = new DestinationDefinitionSpecificationRead()
        .connectionSpecification(connectorSpecification.getConnectionSpecification())
        .destinationDefinitionId(standardDestinationDefinition.getDestinationDefinitionId())
        .documentationUrl(connectorSpecification.getDocumentationUrl().toString());

    workspaceUUID =  UUID.randomUUID();
    destinationConnection = DestinationHelpers.generateDestination(standardDestinationDefinition.getDestinationDefinitionId());
    destinationConnections = DestinationHelpers.generateDestinations(standardDestinationDefinition.getDestinationDefinitionId(), workspaceUUID, 20);
    destinationHandler =
        new DestinationHandler(configRepository, validator, specFetcher, connectionsHandler, uuidGenerator, secretsProcessor, configurationUpdate);
  }

  @Test
  void testCreateDestination() throws JsonValidationException, ConfigNotFoundException, IOException {
    when(uuidGenerator.get())
        .thenReturn(destinationConnection.getDestinationId());
    when(configRepository.getDestinationConnection(destinationConnection.getDestinationId()))
        .thenReturn(destinationConnection);
    when(configRepository.getStandardDestinationDefinition(standardDestinationDefinition.getDestinationDefinitionId()))
        .thenReturn(standardDestinationDefinition);
    when(specFetcher.execute(imageName)).thenReturn(connectorSpecification);
    when(secretsProcessor.maskSecrets(destinationConnection.getConfiguration(), destinationDefinitionSpecificationRead.getConnectionSpecification()))
        .thenReturn(destinationConnection.getConfiguration());

    final DestinationCreate destinationCreate = new DestinationCreate()
        .name(destinationConnection.getName())
        .workspaceId(destinationConnection.getWorkspaceId())
        .destinationDefinitionId(standardDestinationDefinition.getDestinationDefinitionId())
        .connectionConfiguration(DestinationHelpers.getTestDestinationJson());

    final DestinationRead actualDestinationRead =
        destinationHandler.createDestination(destinationCreate);

    final DestinationRead expectedDestinationRead = new DestinationRead()
        .name(destinationConnection.getName())
        .destinationDefinitionId(standardDestinationDefinition.getDestinationDefinitionId())
        .workspaceId(destinationConnection.getWorkspaceId())
        .destinationId(destinationConnection.getDestinationId())
        .connectionConfiguration(DestinationHelpers.getTestDestinationJson())
        .destinationName(standardDestinationDefinition.getName());

    assertEquals(expectedDestinationRead, actualDestinationRead);

    verify(validator).ensure(destinationDefinitionSpecificationRead.getConnectionSpecification(), destinationConnection.getConfiguration());
    verify(configRepository).writeDestinationConnection(destinationConnection);
    verify(secretsProcessor)
        .maskSecrets(destinationConnection.getConfiguration(), destinationDefinitionSpecificationRead.getConnectionSpecification());
  }

  @Test
  void testDeleteDestination() throws JsonValidationException, ConfigNotFoundException, IOException {
    final JsonNode newConfiguration = destinationConnection.getConfiguration();
    ((ObjectNode) newConfiguration).put("apiKey", "987-xyz");

    final DestinationConnection expectedDestinationConnection = Jsons.clone(destinationConnection).withTombstone(true);
    final DestinationIdRequestBody destinationId = new DestinationIdRequestBody().destinationId(destinationConnection.getDestinationId());
    final StandardSync standardSync = ConnectionHelpers.generateSyncWithDestinationId(destinationConnection.getDestinationId());
    final ConnectionRead connectionRead = ConnectionHelpers.generateExpectedConnectionRead(standardSync);
    final ConnectionReadList connectionReadList = new ConnectionReadList().connections(Collections.singletonList(connectionRead));
    final WorkspaceIdRequestBody workspaceIdRequestBody = new WorkspaceIdRequestBody().workspaceId(destinationConnection.getWorkspaceId());

    when(configRepository.getDestinationConnection(destinationConnection.getDestinationId()))
        .thenReturn(destinationConnection)
        .thenReturn(expectedDestinationConnection);
    when(configRepository.getStandardDestinationDefinition(standardDestinationDefinition.getDestinationDefinitionId()))
        .thenReturn(standardDestinationDefinition);
    when(specFetcher.execute(imageName)).thenReturn(connectorSpecification);
    when(connectionsHandler.listConnectionsForWorkspace(workspaceIdRequestBody)).thenReturn(connectionReadList);

    destinationHandler.deleteDestination(destinationId);

    verify(configRepository).writeDestinationConnection(expectedDestinationConnection);
    verify(connectionsHandler).listConnectionsForWorkspace(workspaceIdRequestBody);
    verify(connectionsHandler).deleteConnection(connectionRead);
  }

  @Test
  void testUpdateDestination() throws JsonValidationException, ConfigNotFoundException, IOException {
    final JsonNode newConfiguration = destinationConnection.getConfiguration();

    ((ObjectNode) newConfiguration).put("apiKey", "987-xyz");

    final DestinationConnection expectedDestinationConnection = Jsons.clone(destinationConnection)
        .withConfiguration(newConfiguration)
        .withTombstone(false);
    final DestinationUpdate destinationUpdate = new DestinationUpdate()
        .destinationId(destinationConnection.getDestinationId())
        .name(destinationConnection.getName())
        .connectionConfiguration(newConfiguration);

    when(configRepository.getDestinationConnection(destinationConnection.getDestinationId()))
        .thenReturn(destinationConnection)
        .thenReturn(expectedDestinationConnection);
    when(specFetcher.execute(imageName)).thenReturn(connectorSpecification);
    when(configRepository.getStandardDestinationDefinition(standardDestinationDefinition.getDestinationDefinitionId()))
        .thenReturn(standardDestinationDefinition);
    when(secretsProcessor
        .copySecrets(destinationConnection.getConfiguration(), newConfiguration, destinationDefinitionSpecificationRead.getConnectionSpecification()))
            .thenReturn(newConfiguration);
    when(secretsProcessor.maskSecrets(newConfiguration, destinationDefinitionSpecificationRead.getConnectionSpecification()))
        .thenReturn(newConfiguration);
    when(configurationUpdate.destination(destinationConnection.getDestinationId(), newConfiguration)).thenReturn(expectedDestinationConnection);

    final DestinationRead actualDestinationRead = destinationHandler.updateDestination(destinationUpdate);

    DestinationRead expectedDestinationRead = new DestinationRead()
        .name(destinationConnection.getName())
        .destinationDefinitionId(standardDestinationDefinition.getDestinationDefinitionId())
        .workspaceId(destinationConnection.getWorkspaceId())
        .destinationId(destinationConnection.getDestinationId())
        .connectionConfiguration(newConfiguration)
        .destinationName(standardDestinationDefinition.getName());

    assertEquals(expectedDestinationRead, actualDestinationRead);

    verify(configRepository).writeDestinationConnection(expectedDestinationConnection);
    verify(validator).ensure(destinationDefinitionSpecificationRead.getConnectionSpecification(), destinationConnection.getConfiguration());
    verify(secretsProcessor).maskSecrets(newConfiguration, destinationDefinitionSpecificationRead.getConnectionSpecification());
  }

  @Test
  void testGetDestination() throws JsonValidationException, ConfigNotFoundException, IOException {
    DestinationRead expectedDestinationRead = new DestinationRead()
        .name(destinationConnection.getName())
        .destinationDefinitionId(standardDestinationDefinition.getDestinationDefinitionId())
        .workspaceId(destinationConnection.getWorkspaceId())
        .destinationId(destinationConnection.getDestinationId())
        .connectionConfiguration(destinationConnection.getConfiguration())
        .destinationName(standardDestinationDefinition.getName());
    final DestinationIdRequestBody destinationIdRequestBody =
        new DestinationIdRequestBody().destinationId(expectedDestinationRead.getDestinationId());

    when(secretsProcessor.maskSecrets(destinationConnection.getConfiguration(), destinationDefinitionSpecificationRead.getConnectionSpecification()))
        .thenReturn(destinationConnection.getConfiguration());
    when(configRepository.getDestinationConnection(destinationConnection.getDestinationId())).thenReturn(destinationConnection);
    when(configRepository.getStandardDestinationDefinition(standardDestinationDefinition.getDestinationDefinitionId()))
        .thenReturn(standardDestinationDefinition);
    when(specFetcher.execute(imageName)).thenReturn(connectorSpecification);

    final DestinationRead actualDestinationRead = destinationHandler.getDestination(destinationIdRequestBody);

    assertEquals(expectedDestinationRead, actualDestinationRead);
    verify(secretsProcessor)
        .maskSecrets(destinationConnection.getConfiguration(), destinationDefinitionSpecificationRead.getConnectionSpecification());
  }

  @Test
  void testGetDeletedDestination() throws JsonValidationException, ConfigNotFoundException, IOException {
    final UUID destinationId = destinationConnection.getDestinationId();
    final DestinationIdRequestBody destinationIdRequestBody = new DestinationIdRequestBody().destinationId(destinationId);
    final DestinationConnection deleted = DestinationHelpers.generateDestination(destinationId, true);

    when(configRepository.getDestinationConnection(destinationId))
        .thenReturn(deleted);

    assertThrows(ConfigNotFoundException.class, () -> destinationHandler.getDestination(destinationIdRequestBody));
  }

  @Test
  void testListDestinationForWorkspace() throws JsonValidationException, ConfigNotFoundException, IOException {
    DestinationRead expectedDestinationRead = new DestinationRead()
        .name(destinationConnection.getName())
        .destinationDefinitionId(standardDestinationDefinition.getDestinationDefinitionId())
        .workspaceId(destinationConnection.getWorkspaceId())
        .destinationId(destinationConnection.getDestinationId())
        .connectionConfiguration(destinationConnection.getConfiguration())
        .destinationName(standardDestinationDefinition.getName());
    final WorkspaceIdRequestBody workspaceIdRequestBody = new WorkspaceIdRequestBody().workspaceId(destinationConnection.getWorkspaceId());

    when(configRepository.getDestinationConnection(destinationConnection.getDestinationId())).thenReturn(destinationConnection);
    when(configRepository.listDestinationConnection()).thenReturn(Lists.newArrayList(destinationConnection));
    when(specFetcher.execute(imageName)).thenReturn(connectorSpecification);
    when(configRepository.getStandardDestinationDefinition(standardDestinationDefinition.getDestinationDefinitionId()))
        .thenReturn(standardDestinationDefinition);
    when(secretsProcessor.maskSecrets(destinationConnection.getConfiguration(), destinationDefinitionSpecificationRead.getConnectionSpecification()))
        .thenReturn(destinationConnection.getConfiguration());

    final DestinationReadList actualDestinationRead = destinationHandler.listDestinationsForWorkspace(workspaceIdRequestBody);

    assertEquals(expectedDestinationRead, actualDestinationRead.getDestinations().get(0));
    verify(secretsProcessor)
        .maskSecrets(destinationConnection.getConfiguration(), destinationDefinitionSpecificationRead.getConnectionSpecification());
  }

  @Test
  void testPaginateDestinationForWorkspace() throws JsonValidationException, ConfigNotFoundException, IOException {
    List<DestinationRead> expectedDestinationReads = destinationConnections.stream().map(connection -> new DestinationRead()
            .name(connection.getName())
            .destinationDefinitionId(standardDestinationDefinition.getDestinationDefinitionId())
            .workspaceId(connection.getWorkspaceId())
            .destinationId(connection.getDestinationId())
            .connectionConfiguration(connection.getConfiguration())
            .destinationName(standardDestinationDefinition.getName())).collect(Collectors.toList());

    final WorkspaceIdRequestBody workspaceIdRequestBody = new WorkspaceIdRequestBody().workspaceId(workspaceUUID);
    when(configRepository.listDestinationConnection()).thenReturn(destinationConnections);
    when(specFetcher.execute(imageName)).thenReturn(connectorSpecification);
    when(configRepository.getStandardDestinationDefinition(standardDestinationDefinition.getDestinationDefinitionId()))
            .thenReturn(standardDestinationDefinition);

    for (DestinationConnection destinationConnection: destinationConnections
    ) {
      when(configRepository.getDestinationConnection(destinationConnection.getDestinationId())).thenReturn(destinationConnection);
      when(secretsProcessor.maskSecrets(destinationConnection.getConfiguration(), destinationDefinitionSpecificationRead.getConnectionSpecification()))
              .thenReturn(destinationConnection.getConfiguration());
    }

    Integer offset = 0;
    Integer limit = 10;
    DestinationReadList actualDestinationRead = destinationHandler.paginateDestinationsForWorkspace(workspaceIdRequestBody, limit, offset);
    assertEquals(expectedDestinationReads.subList(offset, limit), actualDestinationRead.getDestinations());

    offset = 2;
    limit = 10;
    actualDestinationRead = destinationHandler.paginateDestinationsForWorkspace(workspaceIdRequestBody, limit, offset);
    assertEquals(expectedDestinationReads.subList(offset, offset + limit), actualDestinationRead.getDestinations());

    offset = 20;
    limit = 10;
    actualDestinationRead = destinationHandler.paginateDestinationsForWorkspace(workspaceIdRequestBody, limit, offset);
    assertEquals(new ArrayList<>(), actualDestinationRead.getDestinations());

    verify(secretsProcessor, times(20)).maskSecrets(destinationConnection.getConfiguration(), destinationDefinitionSpecificationRead.getConnectionSpecification());
  }

}

