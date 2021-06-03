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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.config.ConfigSchema;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConfigRepositoryTest {

  private ConfigPersistence configPersistence;
  private ConfigRepository configRepository;

  @BeforeEach
  void setup() {
    configPersistence = mock(ConfigPersistence.class);
    configRepository = new ConfigRepository(configPersistence);
  }

  @Test
  void testWorkspaceWithNullTombstone() throws ConfigNotFoundException, IOException, JsonValidationException {
    assertReturnsWorkspace(new StandardWorkspace().withWorkspaceId(PersistenceConstants.DEFAULT_WORKSPACE_ID));
  }

  @Test
  void testWorkspaceWithFalseTombstone() throws ConfigNotFoundException, IOException, JsonValidationException {
    assertReturnsWorkspace(new StandardWorkspace().withWorkspaceId(PersistenceConstants.DEFAULT_WORKSPACE_ID).withTombstone(false));
  }

  @Test
  void testWorkspaceWithTrueTombstone() throws ConfigNotFoundException, IOException, JsonValidationException {
    assertReturnsWorkspace(new StandardWorkspace().withWorkspaceId(PersistenceConstants.DEFAULT_WORKSPACE_ID).withTombstone(true));
  }

  void assertReturnsWorkspace(StandardWorkspace workspace) throws ConfigNotFoundException, IOException, JsonValidationException {
    when(configPersistence.getConfig(ConfigSchema.STANDARD_WORKSPACE, PersistenceConstants.DEFAULT_WORKSPACE_ID, StandardWorkspace.class))
        .thenReturn(workspace);

    assertEquals(workspace, configRepository.getStandardWorkspace(PersistenceConstants.DEFAULT_WORKSPACE_ID, true));
  }

}
