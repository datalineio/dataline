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

package io.airbyte.integrations.base;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.airbyte.commons.io.IOs;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.util.AutoCloseableIterators;
import io.airbyte.commons.util.MoreIterators;
import io.airbyte.protocol.models.AirbyteCatalog;
import io.airbyte.protocol.models.AirbyteConnectionStatus;
import io.airbyte.protocol.models.AirbyteConnectionStatus.Status;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteMessage.Type;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import io.airbyte.protocol.models.AirbyteStateMessage;
import io.airbyte.protocol.models.AirbyteStream;
import io.airbyte.protocol.models.CatalogHelpers;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.airbyte.validation.json.JsonSchemaValidator;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

class IntegrationRunnerTest {

  private static final String CONFIG_FILE_NAME = "config.json";
  private static final String CONFIGURED_CATALOG_FILE_NAME = "configured_catalog.json";
  private static final String STATE_FILE_NAME = "state.json";

  private static final String[] ARGS = new String[] {"args"};

  private static final String CONFIG_STRING = "{ \"username\": \"airbyte\" }";
  private static final JsonNode CONFIG = Jsons.deserialize(CONFIG_STRING);
  private static final String STREAM_NAME = "users";
  private static final Long EMITTED_AT = Instant.now().toEpochMilli();
  private static final Path TEST_ROOT = Path.of("/tmp/airbyte_tests");

  private static final AirbyteCatalog CATALOG = new AirbyteCatalog().withStreams(Lists.newArrayList(new AirbyteStream().withName(STREAM_NAME)));
  private static final ConfiguredAirbyteCatalog CONFIGURED_CATALOG = CatalogHelpers.toDefaultConfiguredCatalog(CATALOG);
  private static final JsonNode STATE = Jsons.jsonNode(ImmutableMap.of("checkpoint", "05/08/1945"));

  private IntegrationCliParser cliParser;
  private Consumer<AirbyteMessage> stdoutConsumer;
  private Destination destination;
  private Source source;
  private Path configPath;
  private Path configuredCatalogPath;
  private Path statePath;

  @SuppressWarnings("unchecked")
  @BeforeEach
  void setup() throws IOException {
    cliParser = mock(IntegrationCliParser.class);
    stdoutConsumer = Mockito.mock(Consumer.class);
    destination = mock(Destination.class);
    source = mock(Source.class);
    Path configDir = Files.createTempDirectory(Files.createDirectories(TEST_ROOT), "test");

    configPath = IOs.writeFile(configDir, CONFIG_FILE_NAME, CONFIG_STRING);
    configuredCatalogPath = IOs.writeFile(configDir, CONFIGURED_CATALOG_FILE_NAME, Jsons.serialize(CONFIGURED_CATALOG));
    statePath = IOs.writeFile(configDir, STATE_FILE_NAME, Jsons.serialize(STATE));
  }

  @Test
  void testSpecSource() throws Exception {
    final IntegrationConfig intConfig = IntegrationConfig.spec();
    final ConnectorSpecification output = new ConnectorSpecification().withDocumentationUrl(new URI("https://docs.airbyte.io/"));

    when(cliParser.parse(ARGS)).thenReturn(intConfig);
    when(source.spec()).thenReturn(output);

    new IntegrationRunner(cliParser, stdoutConsumer, null, source).run(ARGS);

    verify(source).spec();
    verify(stdoutConsumer).accept(new AirbyteMessage().withType(Type.SPEC).withSpec(output));
  }

  @Test
  void testSpecDestination() throws Exception {
    final IntegrationConfig intConfig = IntegrationConfig.spec();
    final ConnectorSpecification output = new ConnectorSpecification().withDocumentationUrl(new URI("https://docs.airbyte.io/"));

    when(cliParser.parse(ARGS)).thenReturn(intConfig);
    when(destination.spec()).thenReturn(output);

    new IntegrationRunner(cliParser, stdoutConsumer, destination, null).run(ARGS);

    verify(destination).spec();
    verify(stdoutConsumer).accept(new AirbyteMessage().withType(Type.SPEC).withSpec(output));
  }

  @Test
  void testCheckSource() throws Exception {
    final IntegrationConfig intConfig = IntegrationConfig.check(configPath);
    final AirbyteConnectionStatus output = new AirbyteConnectionStatus().withStatus(Status.FAILED).withMessage("it failed");

    when(cliParser.parse(ARGS)).thenReturn(intConfig);
    when(source.check(CONFIG)).thenReturn(output);

    final ConnectorSpecification expectedConnSpec = mock(ConnectorSpecification.class);
    when(source.spec()).thenReturn(expectedConnSpec);
    when(expectedConnSpec.getConnectionSpecification()).thenReturn(CONFIG);
    JsonSchemaValidator jsonSchemaValidator = mock(JsonSchemaValidator.class);
    new IntegrationRunner(cliParser, stdoutConsumer, null, source, jsonSchemaValidator).run(ARGS);

    verify(source).check(CONFIG);
    verify(stdoutConsumer).accept(new AirbyteMessage().withType(Type.CONNECTION_STATUS).withConnectionStatus(output));
    verify(jsonSchemaValidator).validate(any(), any());
  }

  @Test
  void testCheckDestination() throws Exception {
    final IntegrationConfig intConfig = IntegrationConfig.check(configPath);
    final AirbyteConnectionStatus output = new AirbyteConnectionStatus().withStatus(Status.FAILED).withMessage("it failed");

    when(cliParser.parse(ARGS)).thenReturn(intConfig);
    when(destination.check(CONFIG)).thenReturn(output);

    final ConnectorSpecification expectedConnSpec = mock(ConnectorSpecification.class);
    when(destination.spec()).thenReturn(expectedConnSpec);
    when(expectedConnSpec.getConnectionSpecification()).thenReturn(CONFIG);

    JsonSchemaValidator jsonSchemaValidator = mock(JsonSchemaValidator.class);

    new IntegrationRunner(cliParser, stdoutConsumer, destination, null, jsonSchemaValidator).run(ARGS);

    verify(destination).check(CONFIG);
    verify(stdoutConsumer).accept(new AirbyteMessage().withType(Type.CONNECTION_STATUS).withConnectionStatus(output));
    verify(jsonSchemaValidator).validate(any(), any());
  }

  @Test
  void testDiscover() throws Exception {
    final IntegrationConfig intConfig = IntegrationConfig.discover(configPath);
    final AirbyteCatalog output = new AirbyteCatalog()
        .withStreams(Lists.newArrayList(new AirbyteStream().withName("oceans")));

    when(cliParser.parse(ARGS)).thenReturn(intConfig);
    when(source.discover(CONFIG)).thenReturn(output);

    final ConnectorSpecification expectedConnSpec = mock(ConnectorSpecification.class);
    when(source.spec()).thenReturn(expectedConnSpec);
    when(expectedConnSpec.getConnectionSpecification()).thenReturn(CONFIG);

    JsonSchemaValidator jsonSchemaValidator = mock(JsonSchemaValidator.class);
    new IntegrationRunner(cliParser, stdoutConsumer, null, source, jsonSchemaValidator).run(ARGS);

    verify(source).discover(CONFIG);
    verify(stdoutConsumer).accept(new AirbyteMessage().withType(Type.CATALOG).withCatalog(output));
    verify(jsonSchemaValidator).validate(any(), any());
  }

  @Test
  void testRead() throws Exception {
    final IntegrationConfig intConfig = IntegrationConfig.read(configPath, configuredCatalogPath,
        statePath);
    final AirbyteMessage message1 = new AirbyteMessage().withType(Type.RECORD)
        .withRecord(new AirbyteRecordMessage().withData(Jsons.jsonNode(ImmutableMap.of("names", "byron"))));
    final AirbyteMessage message2 = new AirbyteMessage().withType(Type.RECORD).withRecord(new AirbyteRecordMessage()
        .withData(Jsons.jsonNode(ImmutableMap.of("names", "reginald"))));

    when(cliParser.parse(ARGS)).thenReturn(intConfig);
    when(source.read(CONFIG, CONFIGURED_CATALOG, STATE))
        .thenReturn(AutoCloseableIterators.fromIterator(MoreIterators.of(message1, message2)));

    final ConnectorSpecification expectedConnSpec = mock(ConnectorSpecification.class);
    when(source.spec()).thenReturn(expectedConnSpec);
    when(expectedConnSpec.getConnectionSpecification()).thenReturn(CONFIG);

    JsonSchemaValidator jsonSchemaValidator = mock(JsonSchemaValidator.class);
    new IntegrationRunner(cliParser, stdoutConsumer, null, source, jsonSchemaValidator).run(ARGS);

    verify(source).read(CONFIG, CONFIGURED_CATALOG, STATE);
    verify(stdoutConsumer).accept(message1);
    verify(stdoutConsumer).accept(message2);
    verify(jsonSchemaValidator).validate(any(), any());
  }

  @Test
  void testWrite() throws Exception {
    final IntegrationConfig intConfig = IntegrationConfig.write(configPath, configuredCatalogPath);
    final AirbyteMessageConsumer airbyteMessageConsumerMock = mock(AirbyteMessageConsumer.class);
    when(cliParser.parse(ARGS)).thenReturn(intConfig);
    when(destination.getConsumer(CONFIG, CONFIGURED_CATALOG, stdoutConsumer)).thenReturn(airbyteMessageConsumerMock);

    final ConnectorSpecification expectedConnSpec = mock(ConnectorSpecification.class);
    when(destination.spec()).thenReturn(expectedConnSpec);
    when(expectedConnSpec.getConnectionSpecification()).thenReturn(CONFIG);

    JsonSchemaValidator jsonSchemaValidator = mock(JsonSchemaValidator.class);

    final IntegrationRunner runner = spy(new IntegrationRunner(cliParser, stdoutConsumer, destination, null, jsonSchemaValidator));
    runner.run(ARGS);

    verify(destination).getConsumer(CONFIG, CONFIGURED_CATALOG, stdoutConsumer);
    verify(jsonSchemaValidator).validate(any(), any());
  }

  @Test
  void testDestinationConsumerLifecycleSuccess() throws Exception {
    final AirbyteMessage singerMessage1 = new AirbyteMessage()
        .withType(AirbyteMessage.Type.RECORD)
        .withRecord(new AirbyteRecordMessage()
            .withData(Jsons.deserialize("{ \"color\": \"blue\" }"))
            .withStream(STREAM_NAME)
            .withEmittedAt(EMITTED_AT));
    final AirbyteMessage singerMessage2 = new AirbyteMessage()
        .withType(AirbyteMessage.Type.RECORD)
        .withRecord(new AirbyteRecordMessage()
            .withData(Jsons.deserialize("{ \"color\": \"yellow\" }"))
            .withStream(STREAM_NAME)
            .withEmittedAt(EMITTED_AT));
    final AirbyteMessage stateMessage = new AirbyteMessage()
        .withType(Type.STATE)
        .withState(new AirbyteStateMessage()
            .withData(Jsons.deserialize("{ \"checkpoint\": \"1\" }")));
    System.setIn(new ByteArrayInputStream((Jsons.serialize(singerMessage1) + "\n"
        + Jsons.serialize(singerMessage2) + "\n"
        + Jsons.serialize(stateMessage)).getBytes()));

    final AirbyteMessageConsumer airbyteMessageConsumerMock = mock(AirbyteMessageConsumer.class);
    IntegrationRunner.consumeWriteStream(airbyteMessageConsumerMock);

    InOrder inOrder = inOrder(airbyteMessageConsumerMock);
    inOrder.verify(airbyteMessageConsumerMock).accept(singerMessage1);
    inOrder.verify(airbyteMessageConsumerMock).accept(singerMessage2);
    inOrder.verify(airbyteMessageConsumerMock).accept(stateMessage);
    inOrder.verify(airbyteMessageConsumerMock).close();
  }

  @Test
  void testDestinationConsumerLifecycleFailure() throws Exception {
    final AirbyteMessage singerMessage1 = new AirbyteMessage()
        .withType(AirbyteMessage.Type.RECORD)
        .withRecord(new AirbyteRecordMessage()
            .withData(Jsons.deserialize("{ \"color\": \"blue\" }"))
            .withStream(STREAM_NAME)
            .withEmittedAt(EMITTED_AT));
    final AirbyteMessage singerMessage2 = new AirbyteMessage()
        .withType(AirbyteMessage.Type.RECORD)
        .withRecord(new AirbyteRecordMessage()
            .withData(Jsons.deserialize("{ \"color\": \"yellow\" }"))
            .withStream(STREAM_NAME)
            .withEmittedAt(EMITTED_AT));
    System.setIn(new ByteArrayInputStream((Jsons.serialize(singerMessage1) + "\n" + Jsons.serialize(singerMessage2)).getBytes()));

    final AirbyteMessageConsumer airbyteMessageConsumerMock = mock(AirbyteMessageConsumer.class);
    doThrow(new IOException("error")).when(airbyteMessageConsumerMock).accept(singerMessage1);

    assertThrows(IOException.class, () -> IntegrationRunner.consumeWriteStream(airbyteMessageConsumerMock));

    InOrder inOrder = inOrder(airbyteMessageConsumerMock);
    inOrder.verify(airbyteMessageConsumerMock).accept(singerMessage1);
    inOrder.verify(airbyteMessageConsumerMock).close();
    inOrder.verifyNoMoreInteractions();
  }

}
