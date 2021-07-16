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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.analytics.TrackingClient;
import io.airbyte.api.model.ImportRead;
import io.airbyte.api.model.ImportRead.StatusEnum;
import io.airbyte.commons.io.FileTtlManager;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.config.persistence.PersistenceConstants;
import io.airbyte.server.ConfigDumpExporter;
import io.airbyte.server.ConfigDumpImporter;
import io.airbyte.validation.json.JsonValidationException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ArchiveHandlerTest {

  private static final String VERSION = "test-version";

  private ConfigRepository configRepository;
  private ArchiveHandler archiveHandler;
  private FileTtlManager fileTtlManager;
  private ConfigDumpExporter configDumpExporter;
  private ConfigDumpImporter configDumpImporter;
  private TrackingClient trackingClient;

  @BeforeEach
  void setUp() {
    configRepository = mock(ConfigRepository.class);
    fileTtlManager = mock(FileTtlManager.class);
    configDumpExporter = mock(ConfigDumpExporter.class);
    configDumpImporter = mock(ConfigDumpImporter.class);
    trackingClient = mock(TrackingClient.class);
    archiveHandler = new ArchiveHandler(
        VERSION,
        configRepository,
        fileTtlManager,
        configDumpExporter,
        configDumpImporter,
        trackingClient);
  }

  @Test
  void testExport() throws IOException {
    final File file = Files.createTempFile(Path.of("/tmp"), "dump_file", "dump_file").toFile();
    when(configDumpExporter.dump()).thenReturn(file);

    assertEquals(file, archiveHandler.exportData());

    verify(configDumpExporter).dump();
    verify(fileTtlManager).register(file.toPath());
  }

  @Test
  void testImport() throws IOException, JsonValidationException, ConfigNotFoundException {
    final File file = Files.createTempFile(Path.of("/tmp"), "dump_file", "dump_file").toFile();
    final UUID customerId = UUID.randomUUID();
    when(configRepository.getStandardWorkspace(PersistenceConstants.DEFAULT_WORKSPACE_ID, true))
        .thenReturn(new StandardWorkspace().withCustomerId(customerId));

    assertEquals(new ImportRead().status(StatusEnum.SUCCEEDED), archiveHandler.importData(file));

    // make sure it cleans up the file.
    assertFalse(Files.exists(file.toPath()));

    verify(trackingClient).identify();
    verify(trackingClient).alias(customerId.toString());
    verify(configDumpImporter).importData(VERSION, file);
  }

}
