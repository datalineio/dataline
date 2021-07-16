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

package io.airbyte.config.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.config.ConfigSchema;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardSync;
import io.airbyte.config.StandardWorkspace;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

public class YamlSeedConfigPersistenceTest {

  private static final YamlSeedConfigPersistence PERSISTENCE;

  static {
    try {
      PERSISTENCE = new YamlSeedConfigPersistence();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testGetConfig() throws Exception {
    // workspace
    StandardWorkspace defaultWorkspace = PERSISTENCE
        .getConfig(ConfigSchema.STANDARD_WORKSPACE, PersistenceConstants.DEFAULT_WORKSPACE_ID.toString(), StandardWorkspace.class);
    assertEquals(PersistenceConstants.DEFAULT_WORKSPACE_ID, defaultWorkspace.getWorkspaceId());
    assertEquals("default", defaultWorkspace.getName());
    assertEquals("default", defaultWorkspace.getSlug());
    assertEquals(false, defaultWorkspace.getInitialSetupComplete());
    assertEquals(true, defaultWorkspace.getDisplaySetupWizard());
    assertEquals(false, defaultWorkspace.getTombstone());

    // source
    String mySqlSourceId = "435bb9a5-7887-4809-aa58-28c27df0d7ad";
    StandardSourceDefinition mysqlSource = PERSISTENCE
        .getConfig(ConfigSchema.STANDARD_SOURCE_DEFINITION, mySqlSourceId, StandardSourceDefinition.class);
    assertEquals(mySqlSourceId, mysqlSource.getSourceDefinitionId().toString());
    assertEquals("MySQL", mysqlSource.getName());
    assertEquals("airbyte/source-mysql", mysqlSource.getDockerRepository());
    assertEquals("https://docs.airbyte.io/integrations/sources/mysql", mysqlSource.getDocumentationUrl());
    assertEquals("mysql.svg", mysqlSource.getIcon());

    // destination
    String s3DestinationId = "4816b78f-1489-44c1-9060-4b19d5fa9362";
    StandardDestinationDefinition s3Destination = PERSISTENCE
        .getConfig(ConfigSchema.STANDARD_DESTINATION_DEFINITION, s3DestinationId, StandardDestinationDefinition.class);
    assertEquals(s3DestinationId, s3Destination.getDestinationDefinitionId().toString());
    assertEquals("S3", s3Destination.getName());
    assertEquals("airbyte/destination-s3", s3Destination.getDockerRepository());
    assertEquals("https://docs.airbyte.io/integrations/destinations/s3", s3Destination.getDocumentationUrl());
  }

  @Test
  public void testGetInvalidConfig() {
    assertThrows(UnsupportedOperationException.class,
        () -> PERSISTENCE.getConfig(ConfigSchema.STANDARD_SYNC, "invalid_id", StandardSync.class));
    assertThrows(ConfigNotFoundException.class,
        () -> PERSISTENCE.getConfig(ConfigSchema.STANDARD_WORKSPACE, "invalid_id", StandardWorkspace.class));
  }

  @Test
  public void testDumpConfigs() {
    Map<String, Stream<JsonNode>> allSeedConfigs = PERSISTENCE.dumpConfigs();
    assertEquals(3, allSeedConfigs.size());
    assertTrue(allSeedConfigs.get(ConfigSchema.STANDARD_WORKSPACE.name()).count() > 0);
    assertTrue(allSeedConfigs.get(ConfigSchema.STANDARD_SOURCE_DEFINITION.name()).count() > 0);
    assertTrue(allSeedConfigs.get(ConfigSchema.STANDARD_DESTINATION_DEFINITION.name()).count() > 0);
  }

  @Test
  public void testWriteMethods() {
    assertThrows(UnsupportedOperationException.class, () -> PERSISTENCE.writeConfig(ConfigSchema.STANDARD_WORKSPACE, "id", new Object()));
    assertThrows(UnsupportedOperationException.class, () -> PERSISTENCE.replaceAllConfigs(Collections.emptyMap(), false));
  }

}
