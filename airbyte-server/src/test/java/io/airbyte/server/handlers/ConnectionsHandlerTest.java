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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

import com.google.common.collect.Lists;
import io.airbyte.api.model.*;
import io.airbyte.commons.enums.Enums;
import io.airbyte.config.DataType;
import io.airbyte.config.Schedule;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardSync;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.server.helpers.ConnectionHelpers;
import io.airbyte.server.helpers.SourceHelpers;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConnectionsHandlerTest {

  private ConfigRepository configRepository;
  private Supplier<UUID> uuidGenerator;

  private StandardSync standardSync;
  private List<StandardSync> standardSyncs;
  private ConnectionsHandler connectionsHandler;
  private SourceConnection source;

  @SuppressWarnings("unchecked")
  @BeforeEach
  void setUp() throws IOException {
    configRepository = mock(ConfigRepository.class);
    uuidGenerator = mock(Supplier.class);

    source = SourceHelpers.generateSource(UUID.randomUUID());
    standardSync = ConnectionHelpers.generateSyncWithSourceId(source.getSourceId());
    standardSyncs = ConnectionHelpers.generateSyncsWithSourceId(source.getSourceId(), 20);

    connectionsHandler = new ConnectionsHandler(configRepository, uuidGenerator);
  }

  @Test
  void testCreateConnection() throws JsonValidationException, ConfigNotFoundException, IOException {
    when(uuidGenerator.get()).thenReturn(standardSync.getConnectionId());

    when(configRepository.getStandardSync(standardSync.getConnectionId())).thenReturn(standardSync);

    final AirbyteCatalog catalog = ConnectionHelpers.generateBasicApiCatalog();

    final ConnectionCreate connectionCreate = new ConnectionCreate()
        .sourceId(standardSync.getSourceId())
        .destinationId(standardSync.getDestinationId())
        .operationIds(standardSync.getOperationIds())
        .name("presto to hudi")
        .prefix("presto_to_hudi")
        .status(ConnectionStatus.ACTIVE)
        .schedule(ConnectionHelpers.generateBasicConnectionSchedule())
        .syncCatalog(catalog);

    final ConnectionRead actualConnectionRead = connectionsHandler.createConnection(connectionCreate);

    final ConnectionRead expectedConnectionRead = ConnectionHelpers.generateExpectedConnectionRead(standardSync);

    assertEquals(expectedConnectionRead, actualConnectionRead);

    verify(configRepository).writeStandardSync(standardSync);
  }

  @Test
  void testUpdateConnection() throws JsonValidationException, ConfigNotFoundException, IOException {
    final AirbyteCatalog catalog = ConnectionHelpers.generateBasicApiCatalog();
    catalog.getStreams().get(0).getStream().setName("azkaban_users");
    catalog.getStreams().get(0).getConfig().setAliasName("azkaban_users");

    final ConnectionUpdate connectionUpdate = new ConnectionUpdate()
        .prefix(standardSync.getPrefix())
        .connectionId(standardSync.getConnectionId())
        .status(ConnectionStatus.INACTIVE)
        .schedule(null)
        .syncCatalog(catalog);

    final ConfiguredAirbyteCatalog configuredCatalog = ConnectionHelpers.generateBasicConfiguredAirbyteCatalog();
    configuredCatalog.getStreams().get(0).getStream().withName("azkaban_users");

    final StandardSync updatedStandardSync = new StandardSync()
        .withConnectionId(standardSync.getConnectionId())
        .withName("presto to hudi")
        .withPrefix("presto_to_hudi")
        .withSourceId(standardSync.getSourceId())
        .withDestinationId(standardSync.getDestinationId())
        .withStatus(StandardSync.Status.INACTIVE)
        .withCatalog(configuredCatalog)
        .withManual(true);

    when(configRepository.getStandardSync(standardSync.getConnectionId()))
        .thenReturn(standardSync)
        .thenReturn(updatedStandardSync);

    final ConnectionRead actualConnectionRead = connectionsHandler.updateConnection(connectionUpdate);

    final ConnectionRead expectedConnectionRead = ConnectionHelpers.generateExpectedConnectionRead(
        standardSync.getConnectionId(),
        standardSync.getSourceId(),
        standardSync.getDestinationId(),
        standardSync.getOperationIds())
        .schedule(null)
        .syncCatalog(catalog)
        .status(ConnectionStatus.INACTIVE);

    assertEquals(expectedConnectionRead, actualConnectionRead);

    verify(configRepository).writeStandardSync(updatedStandardSync);
  }

  @Test
  void testGetConnection() throws JsonValidationException, ConfigNotFoundException, IOException {
    when(configRepository.getStandardSync(standardSync.getConnectionId()))
        .thenReturn(standardSync);

    final ConnectionIdRequestBody connectionIdRequestBody = new ConnectionIdRequestBody().connectionId(standardSync.getConnectionId());
    final ConnectionRead actualConnectionRead = connectionsHandler.getConnection(connectionIdRequestBody);

    assertEquals(ConnectionHelpers.generateExpectedConnectionRead(standardSync), actualConnectionRead);
  }

  @Test
  void testListConnectionsForWorkspace() throws JsonValidationException, ConfigNotFoundException, IOException {
    when(configRepository.listStandardSyncs())
        .thenReturn(Lists.newArrayList(standardSync));
    when(configRepository.getSourceConnection(source.getSourceId()))
        .thenReturn(source);
    when(configRepository.getStandardSync(standardSync.getConnectionId()))
        .thenReturn(standardSync);

    final WorkspaceIdRequestBody workspaceIdRequestBody = new WorkspaceIdRequestBody().workspaceId(source.getWorkspaceId());
    final ConnectionReadList actualConnectionReadList = connectionsHandler.listConnectionsForWorkspace(workspaceIdRequestBody);

    assertEquals(
        ConnectionHelpers.generateExpectedConnectionRead(standardSync),
        actualConnectionReadList.getConnections().get(0));
  }

  @Test
  void testPaginateConnectionsForWorkspace() throws JsonValidationException, ConfigNotFoundException, IOException {
    when(configRepository.listStandardSyncs())
            .thenReturn(standardSyncs);
    when(configRepository.getSourceConnection(source.getSourceId()))
            .thenReturn(source);

    for (StandardSync standardSync: standardSyncs
    ) {
      when(configRepository.getStandardSync(standardSync.getConnectionId()))
              .thenReturn(standardSync);
    }

    final WorkspaceIdRequestBody workspaceIdRequestBody = new WorkspaceIdRequestBody().workspaceId(source.getWorkspaceId());

    Integer offset = 0;
    Integer limit = 10;
    ConnectionReadList actualConnectionReadList = connectionsHandler.paginateConnectionsForWorkspace(workspaceIdRequestBody, limit, offset);
    assertEquals(
            ConnectionHelpers.generateExpectedConnectionReads(standardSyncs).subList(offset, limit),
            actualConnectionReadList.getConnections());

    offset = 2;
    limit = 10;
    actualConnectionReadList = connectionsHandler.paginateConnectionsForWorkspace(workspaceIdRequestBody, limit, offset);
    assertEquals(
            ConnectionHelpers.generateExpectedConnectionReads(standardSyncs).subList(offset, offset + limit),
            actualConnectionReadList.getConnections());

    offset = 20;
    limit = 10;
    actualConnectionReadList = connectionsHandler.paginateConnectionsForWorkspace(workspaceIdRequestBody, limit, offset);
    assertEquals(
            new ArrayList<>(),
            actualConnectionReadList.getConnections());
  }


  @Test
  void testDeleteConnection() throws JsonValidationException, IOException, ConfigNotFoundException {
    final ConnectionIdRequestBody connectionIdRequestBody = new ConnectionIdRequestBody().connectionId(standardSync.getConnectionId());

    final ConnectionRead connectionRead = ConnectionHelpers.generateExpectedConnectionRead(
        standardSync.getConnectionId(),
        standardSync.getSourceId(),
        standardSync.getDestinationId(),
        standardSync.getOperationIds());

    final ConnectionUpdate expectedConnectionUpdate = new ConnectionUpdate()
        .prefix(connectionRead.getPrefix())
        .connectionId(connectionRead.getConnectionId())
        .operationIds(connectionRead.getOperationIds())
        .status(ConnectionStatus.DEPRECATED)
        .syncCatalog(connectionRead.getSyncCatalog())
        .schedule(connectionRead.getSchedule());

    final ConnectionsHandler spiedConnectionsHandler = spy(connectionsHandler);
    doReturn(connectionRead).when(spiedConnectionsHandler).getConnection(connectionIdRequestBody);
    doReturn(null).when(spiedConnectionsHandler).updateConnection(expectedConnectionUpdate);

    spiedConnectionsHandler.deleteConnection(connectionIdRequestBody);

    verify(spiedConnectionsHandler).getConnection(connectionIdRequestBody);
    verify(spiedConnectionsHandler).updateConnection(expectedConnectionUpdate);
  }

  @Test
  void testEnumConversion() {
    assertTrue(Enums.isCompatible(ConnectionStatus.class, StandardSync.Status.class));
    assertTrue(Enums.isCompatible(io.airbyte.config.SyncMode.class, SyncMode.class));
    assertTrue(Enums.isCompatible(StandardSync.Status.class, ConnectionStatus.class));
    assertTrue(Enums.isCompatible(ConnectionSchedule.TimeUnitEnum.class, Schedule.TimeUnit.class));
    assertTrue(Enums.isCompatible(io.airbyte.api.model.DataType.class, DataType.class));
    assertTrue(Enums.isCompatible(DataType.class, io.airbyte.api.model.DataType.class));
  }

}
