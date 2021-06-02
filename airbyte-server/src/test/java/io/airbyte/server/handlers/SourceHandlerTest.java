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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import io.airbyte.api.model.ConnectionRead;
import io.airbyte.api.model.ConnectionReadList;
import io.airbyte.api.model.SourceCreate;
import io.airbyte.api.model.SourceDefinitionSpecificationRead;
import io.airbyte.api.model.SourceIdRequestBody;
import io.airbyte.api.model.SourceRead;
import io.airbyte.api.model.SourceReadList;
import io.airbyte.api.model.SourceUpdate;
import io.airbyte.api.model.WorkspaceIdRequestBody;
import io.airbyte.commons.docker.DockerUtils;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardSync;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.airbyte.server.converters.ConfigurationUpdate;
import io.airbyte.server.converters.JsonSecretsProcessor;
import io.airbyte.server.converters.SpecFetcher;
import io.airbyte.server.helpers.ConnectionHelpers;
import io.airbyte.server.helpers.ConnectorSpecificationHelpers;
import io.airbyte.server.helpers.SourceHelpers;
import io.airbyte.validation.json.JsonSchemaValidator;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.jooq.meta.derby.sys.Sys;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SourceHandlerTest {

  private ConfigRepository configRepository;
  private StandardSourceDefinition standardSourceDefinition;
  private SourceDefinitionSpecificationRead sourceDefinitionSpecificationRead;
  private SourceConnection sourceConnection;
  private List<SourceConnection> sourceConnections;
  private UUID workspaceUUID;
  private SourceHandler sourceHandler;
  private JsonSchemaValidator validator;
  private ConnectionsHandler connectionsHandler;
  private SpecFetcher specFetcher;
  private ConfigurationUpdate configurationUpdate;
  private Supplier<UUID> uuidGenerator;
  private JsonSecretsProcessor secretsProcessor;
  private ConnectorSpecification connectorSpecification;
  private String imageName;

  @SuppressWarnings("unchecked")
  @BeforeEach
  void setUp() throws IOException {
    configRepository = mock(ConfigRepository.class);
    validator = mock(JsonSchemaValidator.class);
    connectionsHandler = mock(ConnectionsHandler.class);
    specFetcher = mock(SpecFetcher.class);
    configurationUpdate = mock(ConfigurationUpdate.class);
    uuidGenerator = mock(Supplier.class);
    secretsProcessor = mock(JsonSecretsProcessor.class);

    standardSourceDefinition = new StandardSourceDefinition()
        .withSourceDefinitionId(UUID.randomUUID())
        .withName("marketo")
        .withDockerRepository("thebestrepo")
        .withDockerImageTag("thelatesttag")
        .withDocumentationUrl("https://wikipedia.org");
    imageName = DockerUtils.getTaggedImageName(standardSourceDefinition.getDockerRepository(), standardSourceDefinition.getDockerImageTag());
    connectorSpecification = ConnectorSpecificationHelpers.generateConnectorSpecification();
    sourceDefinitionSpecificationRead = new SourceDefinitionSpecificationRead()
        .sourceDefinitionId(standardSourceDefinition.getSourceDefinitionId())
        .connectionSpecification(connectorSpecification.getConnectionSpecification())
        .documentationUrl(connectorSpecification.getDocumentationUrl().toString());

    workspaceUUID =  UUID.randomUUID();
    sourceConnection = SourceHelpers.generateSource(standardSourceDefinition.getSourceDefinitionId());
    sourceConnections = SourceHelpers.generateSources(standardSourceDefinition.getSourceDefinitionId(), workspaceUUID,20);

    sourceHandler =
        new SourceHandler(configRepository, validator, specFetcher, connectionsHandler, uuidGenerator, secretsProcessor, configurationUpdate);
  }

  @Test
  void testCreateSource() throws JsonValidationException, ConfigNotFoundException, IOException {
    final SourceCreate sourceCreate = new SourceCreate()
        .name(sourceConnection.getName())
        .workspaceId(sourceConnection.getWorkspaceId())
        .sourceDefinitionId(standardSourceDefinition.getSourceDefinitionId())
        .connectionConfiguration(sourceConnection.getConfiguration());

    when(uuidGenerator.get()).thenReturn(sourceConnection.getSourceId());
    when(configRepository.getSourceConnection(sourceConnection.getSourceId())).thenReturn(sourceConnection);
    when(specFetcher.execute(imageName)).thenReturn(connectorSpecification);
    when(configRepository.getStandardSourceDefinition(sourceDefinitionSpecificationRead.getSourceDefinitionId()))
        .thenReturn(standardSourceDefinition);
    when(secretsProcessor.maskSecrets(sourceCreate.getConnectionConfiguration(), sourceDefinitionSpecificationRead.getConnectionSpecification()))
        .thenReturn(sourceCreate.getConnectionConfiguration());

    final SourceRead actualSourceRead = sourceHandler.createSource(sourceCreate);

    final SourceRead expectedSourceRead = SourceHelpers.getSourceRead(sourceConnection, standardSourceDefinition)
        .connectionConfiguration(sourceConnection.getConfiguration());

    assertEquals(expectedSourceRead, actualSourceRead);

    verify(secretsProcessor).maskSecrets(sourceCreate.getConnectionConfiguration(), sourceDefinitionSpecificationRead.getConnectionSpecification());
    verify(configRepository).writeSourceConnection(sourceConnection);
    verify(validator).ensure(sourceDefinitionSpecificationRead.getConnectionSpecification(), sourceConnection.getConfiguration());
  }

  @Test
  void testUpdateSource() throws JsonValidationException, ConfigNotFoundException, IOException {
    final JsonNode newConfiguration = sourceConnection.getConfiguration();
    ((ObjectNode) newConfiguration).put("apiKey", "987-xyz");

    final SourceConnection expectedSourceConnection = Jsons.clone(sourceConnection)
        .withConfiguration(newConfiguration)
        .withTombstone(false);

    final SourceUpdate sourceUpdate = new SourceUpdate()
        .name(sourceConnection.getName())
        .sourceId(sourceConnection.getSourceId())
        .connectionConfiguration(newConfiguration);

    when(secretsProcessor
        .copySecrets(sourceConnection.getConfiguration(), newConfiguration, sourceDefinitionSpecificationRead.getConnectionSpecification()))
            .thenReturn(newConfiguration);
    when(secretsProcessor.maskSecrets(newConfiguration, sourceDefinitionSpecificationRead.getConnectionSpecification())).thenReturn(newConfiguration);

    when(configRepository.getStandardSourceDefinition(sourceDefinitionSpecificationRead.getSourceDefinitionId()))
        .thenReturn(standardSourceDefinition);
    when(configRepository.getSourceDefinitionFromSource(sourceConnection.getSourceId())).thenReturn(standardSourceDefinition);
    when(configRepository.getSourceConnection(sourceConnection.getSourceId()))
        .thenReturn(sourceConnection)
        .thenReturn(expectedSourceConnection);
    when(specFetcher.execute(imageName)).thenReturn(connectorSpecification);
    when(configurationUpdate.source(sourceConnection.getSourceId(), newConfiguration)).thenReturn(expectedSourceConnection);

    final SourceRead actualSourceRead = sourceHandler.updateSource(sourceUpdate);
    final SourceRead expectedSourceRead =
        SourceHelpers.getSourceRead(sourceConnection, standardSourceDefinition).connectionConfiguration(newConfiguration);

    assertEquals(expectedSourceRead, actualSourceRead);

    verify(secretsProcessor).maskSecrets(newConfiguration, sourceDefinitionSpecificationRead.getConnectionSpecification());
    verify(configRepository).writeSourceConnection(expectedSourceConnection);
    verify(validator).ensure(sourceDefinitionSpecificationRead.getConnectionSpecification(), newConfiguration);
  }

  @Test
  void testGetSource() throws JsonValidationException, ConfigNotFoundException, IOException {
    final SourceRead expectedSourceRead = SourceHelpers.getSourceRead(sourceConnection, standardSourceDefinition);
    final SourceIdRequestBody sourceIdRequestBody = new SourceIdRequestBody().sourceId(expectedSourceRead.getSourceId());

    when(configRepository.getSourceConnection(sourceConnection.getSourceId())).thenReturn(sourceConnection);
    when(configRepository.getStandardSourceDefinition(sourceDefinitionSpecificationRead.getSourceDefinitionId()))
        .thenReturn(standardSourceDefinition);
    when(configRepository.getSourceDefinitionFromSource(sourceConnection.getSourceId())).thenReturn(standardSourceDefinition);
    when(specFetcher.execute(imageName)).thenReturn(connectorSpecification);
    when(secretsProcessor.maskSecrets(sourceConnection.getConfiguration(), sourceDefinitionSpecificationRead.getConnectionSpecification()))
        .thenReturn(sourceConnection.getConfiguration());

    final SourceRead actualSourceRead = sourceHandler.getSource(sourceIdRequestBody);

    assertEquals(expectedSourceRead, actualSourceRead);
    verify(secretsProcessor).maskSecrets(sourceConnection.getConfiguration(), sourceDefinitionSpecificationRead.getConnectionSpecification());
  }

  @Test
  void testGetDeletedSource() throws JsonValidationException, ConfigNotFoundException, IOException {
    final UUID sourceId = sourceConnection.getSourceId();
    final SourceIdRequestBody sourceIdRequestBody = new SourceIdRequestBody().sourceId(sourceId);
    final SourceConnection deleted = SourceHelpers.generateSource(sourceId, true);

    when(configRepository.getSourceConnection(sourceId))
        .thenReturn(deleted);

    assertThrows(ConfigNotFoundException.class, () -> sourceHandler.getSource(sourceIdRequestBody));
  }

  @Test
  void testListSourcesForWorkspace() throws JsonValidationException, ConfigNotFoundException, IOException {
    final SourceRead expectedSourceRead = SourceHelpers.getSourceRead(sourceConnection, standardSourceDefinition);
    final WorkspaceIdRequestBody workspaceIdRequestBody = new WorkspaceIdRequestBody().workspaceId(sourceConnection.getWorkspaceId());

    when(configRepository.getSourceConnection(sourceConnection.getSourceId())).thenReturn(sourceConnection);
    when(configRepository.listSourceConnection()).thenReturn(Lists.newArrayList(sourceConnection));
    when(configRepository.getStandardSourceDefinition(sourceDefinitionSpecificationRead.getSourceDefinitionId()))
        .thenReturn(standardSourceDefinition);
    when(configRepository.getSourceDefinitionFromSource(sourceConnection.getSourceId())).thenReturn(standardSourceDefinition);
    when(specFetcher.execute(imageName)).thenReturn(connectorSpecification);
    when(secretsProcessor.maskSecrets(sourceConnection.getConfiguration(), sourceDefinitionSpecificationRead.getConnectionSpecification()))
        .thenReturn(sourceConnection.getConfiguration());

    final SourceReadList actualSourceReadList = sourceHandler.listSourcesForWorkspace(workspaceIdRequestBody);

    assertEquals(expectedSourceRead, actualSourceReadList.getSources().get(0));
    verify(secretsProcessor).maskSecrets(sourceConnection.getConfiguration(), sourceDefinitionSpecificationRead.getConnectionSpecification());
  }

  @Test
  void testPaginateSourcesForWorkspace() throws JsonValidationException, ConfigNotFoundException, IOException {
    final List<SourceRead> expectedSourceRead =  sourceConnections.stream().map(connection -> SourceHelpers.getSourceRead(connection, standardSourceDefinition)).collect(Collectors.toList());
    final WorkspaceIdRequestBody workspaceIdRequestBody = new WorkspaceIdRequestBody().workspaceId(workspaceUUID);

    when(specFetcher.execute(imageName)).thenReturn(connectorSpecification);
    when(configRepository.listSourceConnection()).thenReturn(sourceConnections);
    when(configRepository.getStandardSourceDefinition(sourceDefinitionSpecificationRead.getSourceDefinitionId()))
        .thenReturn(standardSourceDefinition);

    for (SourceConnection sourceConnection: sourceConnections
         ) {
      when(configRepository.getSourceDefinitionFromSource(sourceConnection.getSourceId())).thenReturn(standardSourceDefinition);
      when(configRepository.getSourceConnection(sourceConnection.getSourceId())).thenReturn(sourceConnection);
      when(secretsProcessor.maskSecrets(sourceConnection.getConfiguration(), sourceDefinitionSpecificationRead.getConnectionSpecification()))
              .thenReturn(sourceConnection.getConfiguration());
    }

    Integer offset = 0;
    Integer limit = 10;
    SourceReadList actualSourceReadList = sourceHandler.paginateSourcesForWorkspace(workspaceIdRequestBody, limit, offset);
    assertEquals(expectedSourceRead.subList(offset, limit), actualSourceReadList.getSources());

    offset = 2;
    limit = 10;
    actualSourceReadList = sourceHandler.paginateSourcesForWorkspace(workspaceIdRequestBody, limit, offset);
    assertEquals(expectedSourceRead.subList(offset, offset + limit), actualSourceReadList.getSources());

    offset = 20;
    limit = 10;
    actualSourceReadList = sourceHandler.paginateSourcesForWorkspace(workspaceIdRequestBody, limit, offset);
    assertEquals(new ArrayList<>(), actualSourceReadList.getSources());

    verify(secretsProcessor, times(20)).maskSecrets(sourceConnection.getConfiguration(), sourceDefinitionSpecificationRead.getConnectionSpecification());
  }

  @Test
  void testDeleteSource() throws JsonValidationException, ConfigNotFoundException, IOException {
    final JsonNode newConfiguration = sourceConnection.getConfiguration();
    ((ObjectNode) newConfiguration).put("apiKey", "987-xyz");

    final SourceConnection expectedSourceConnection = Jsons.clone(sourceConnection).withTombstone(true);

    final SourceIdRequestBody sourceIdRequestBody = new SourceIdRequestBody().sourceId(sourceConnection.getSourceId());
    final StandardSync standardSync = ConnectionHelpers.generateSyncWithSourceId(sourceConnection.getSourceId());
    final ConnectionRead connectionRead = ConnectionHelpers.generateExpectedConnectionRead(standardSync);
    final ConnectionReadList connectionReadList = new ConnectionReadList().connections(Collections.singletonList(connectionRead));
    final WorkspaceIdRequestBody workspaceIdRequestBody = new WorkspaceIdRequestBody().workspaceId(sourceConnection.getWorkspaceId());

    when(configRepository.getSourceConnection(sourceConnection.getSourceId()))
        .thenReturn(sourceConnection)
        .thenReturn(expectedSourceConnection);
    when(configRepository.getStandardSourceDefinition(sourceDefinitionSpecificationRead.getSourceDefinitionId()))
        .thenReturn(standardSourceDefinition);
    when(configRepository.getSourceDefinitionFromSource(sourceConnection.getSourceId())).thenReturn(standardSourceDefinition);
    when(specFetcher.execute(imageName)).thenReturn(connectorSpecification);
    when(connectionsHandler.listConnectionsForWorkspace(workspaceIdRequestBody)).thenReturn(connectionReadList);
    when(secretsProcessor.maskSecrets(sourceConnection.getConfiguration(), sourceDefinitionSpecificationRead.getConnectionSpecification()))
        .thenReturn(sourceConnection.getConfiguration());

    sourceHandler.deleteSource(sourceIdRequestBody);

    verify(configRepository).writeSourceConnection(expectedSourceConnection);
    verify(connectionsHandler).listConnectionsForWorkspace(workspaceIdRequestBody);
    verify(connectionsHandler).deleteConnection(connectionRead);
  }

}
