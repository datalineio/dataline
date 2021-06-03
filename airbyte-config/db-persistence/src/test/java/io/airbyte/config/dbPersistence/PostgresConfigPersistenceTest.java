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

package io.airbyte.config.dbPersistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.google.common.collect.Sets;
import io.airbyte.config.ConfigSchema;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardSync;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.validation.json.JsonSchemaValidator;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

public class PostgresConfigPersistenceTest {

  public static final UUID UUID_1 = new UUID(0, 1);
  public static final StandardSourceDefinition SOURCE_1 = new StandardSourceDefinition();

  static {
    SOURCE_1.withSourceDefinitionId(UUID_1)
        .withName("apache storm");
  }

  public static final UUID UUID_2 = new UUID(0, 2);
  public static final StandardSourceDefinition SOURCE_2 = new StandardSourceDefinition();

  static {
    SOURCE_2.withSourceDefinitionId(UUID_2)
        .withName("apache storm");
  }

  private JsonSchemaValidator schemaValidator;

  private PostgresConfigPersistence configPersistence;

  private static final String IMAGE_NAME = "postgres:13-alpine";
  private PostgreSQLContainer<?> db;

  @BeforeEach
  void setUp() throws SQLException {
    schemaValidator = mock(JsonSchemaValidator.class);
    db = new PostgreSQLContainer<>(IMAGE_NAME);
    db.start();
    configPersistence = new PostgresConfigPersistence(db.getUsername(), db.getPassword(), db.getJdbcUrl(), schemaValidator);
    configPersistence.Setup();
  }

  @AfterEach
  void tearDown() {
    db.stop();
    db.close();
  }

  @Test
  void testReadWriteConfig() throws IOException, JsonValidationException, ConfigNotFoundException {
    configPersistence.writeConfig(ConfigSchema.STANDARD_SOURCE_DEFINITION, UUID_1, SOURCE_1);

    assertEquals(
        SOURCE_1,
        configPersistence.getConfig(
            ConfigSchema.STANDARD_SOURCE_DEFINITION,
            UUID_1,
            StandardSourceDefinition.class));
  }

  @Test
  void testListConfigs() throws JsonValidationException, IOException {
    configPersistence.writeConfig(ConfigSchema.STANDARD_SOURCE_DEFINITION, UUID_1, SOURCE_1);
    configPersistence.writeConfig(ConfigSchema.STANDARD_SOURCE_DEFINITION, UUID_2, SOURCE_2);

    assertEquals(
        Sets.newHashSet(SOURCE_1, SOURCE_2),
        Sets.newHashSet(configPersistence.listConfigs(ConfigSchema.STANDARD_SOURCE_DEFINITION, StandardSourceDefinition.class)));
  }

  @Test
  void writeConfigWithJsonSchemaRef() throws JsonValidationException, IOException, ConfigNotFoundException {
    final StandardSync standardSync = new StandardSync()
        .withName("sync")
        .withPrefix("sync")
        .withConnectionId(UUID_1)
        .withSourceId(UUID.randomUUID())
        .withDestinationId(UUID.randomUUID())
        .withOperationIds(List.of(UUID.randomUUID()));

    configPersistence.writeConfig(ConfigSchema.STANDARD_SYNC, UUID_1, standardSync);

    assertEquals(
        standardSync,
        configPersistence.getConfig(ConfigSchema.STANDARD_SYNC, UUID_1, StandardSync.class));
  }

  @Test
  void writeConfigInvalidConfig() throws JsonValidationException {
    StandardSourceDefinition standardSourceDefinition = SOURCE_1.withName(null);

    doThrow(new JsonValidationException("error")).when(schemaValidator).ensure(any(), any());

    assertThrows(JsonValidationException.class, () -> configPersistence.writeConfig(
        ConfigSchema.STANDARD_SOURCE_DEFINITION,
        UUID_1,
        standardSourceDefinition));
  }

}
