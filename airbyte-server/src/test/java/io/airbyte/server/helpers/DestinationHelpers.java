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

package io.airbyte.server.helpers;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.api.model.DestinationRead;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardDestinationDefinition;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class DestinationHelpers {

  public static JsonNode getTestDestinationJson() throws IOException {
    final Path path =
        Paths.get("../airbyte-server/src/test/resources/json/TestImplementation.json");

    return Jsons.deserialize(Files.readString(path));
  }

  public static DestinationConnection generateDestination(UUID destinationDefinitionId) throws IOException {
    return generateDestination(destinationDefinitionId, false);
  }

  public static DestinationConnection generateDestination(UUID destinationDefinitionId, boolean tombstone) throws IOException {
    final UUID workspaceId = UUID.randomUUID();
    final UUID destinationId = UUID.randomUUID();

    final JsonNode implementationJson = getTestDestinationJson();

    return new DestinationConnection()
        .withName("my db2 instance")
        .withWorkspaceId(workspaceId)
        .withDestinationDefinitionId(destinationDefinitionId)
        .withDestinationId(destinationId)
        .withConfiguration(implementationJson)
        .withTombstone(tombstone);
  }

  public static List<DestinationConnection> generateDestinations(UUID destinationDefinitionId, UUID workspaceId, int destinationsCount) throws IOException {
    List<DestinationConnection> destinationConnections = new ArrayList<DestinationConnection>(destinationsCount);
    for (int i=0;i < destinationsCount;i++) {
      final UUID destinationId = UUID.randomUUID();

      final JsonNode implementationJson = getTestDestinationJson();

      destinationConnections.add(new DestinationConnection()
              .withName("my db2 instance")
              .withWorkspaceId(workspaceId)
              .withDestinationDefinitionId(destinationDefinitionId)
              .withDestinationId(destinationId)
              .withConfiguration(implementationJson)
              .withTombstone(false));
      }

    return destinationConnections;

  }

  public static DestinationRead getDestinationRead(DestinationConnection destination, StandardDestinationDefinition standardDestinationDefinition) {
    return new DestinationRead()
        .destinationDefinitionId(standardDestinationDefinition.getDestinationDefinitionId())
        .workspaceId(destination.getWorkspaceId())
        .destinationDefinitionId(destination.getDestinationDefinitionId())
        .destinationId(destination.getDestinationId())
        .connectionConfiguration(destination.getConfiguration())
        .name(destination.getName())
        .destinationName(standardDestinationDefinition.getName());
  }

}
