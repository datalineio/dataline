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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.airbyte.commons.io.IOs;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteMessage.Type;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.validation.json.JsonSchemaValidator;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Scanner;
import java.util.Set;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Accepts EITHER a destination or a source. Routes commands from the commandline to the appropriate
 * methods on the integration. Keeps itself DRY for methods that are common between source and
 * destination.
 */
public class IntegrationRunner {

  private static final Logger LOGGER = LoggerFactory.getLogger(IntegrationRunner.class);

  private final IntegrationCliParser cliParser;
  private final Consumer<AirbyteMessage> outputRecordCollector;
  private final Integration integration;
  private final Destination destination;
  private final Source source;
  private static JsonSchemaValidator validator;

  public IntegrationRunner(Destination destination) {
    this(new IntegrationCliParser(), Destination::defaultOutputRecordCollector, destination, null);
  }

  public IntegrationRunner(Source source) {
    this(new IntegrationCliParser(), Destination::defaultOutputRecordCollector, null, source);
  }

  @VisibleForTesting
  IntegrationRunner(IntegrationCliParser cliParser,
                    Consumer<AirbyteMessage> outputRecordCollector,
                    Destination destination,
                    Source source) {
    Preconditions.checkState(destination != null ^ source != null, "can only pass in a destination or a source");
    this.cliParser = cliParser;
    this.outputRecordCollector = outputRecordCollector;
    // integration iface covers the commands that are the same for both source and destination.
    this.integration = source != null ? source : destination;
    this.source = source;
    this.destination = destination;
    validator = new JsonSchemaValidator();
  }

  @VisibleForTesting
  IntegrationRunner(IntegrationCliParser cliParser,
                    Consumer<AirbyteMessage> outputRecordCollector,
                    Destination destination,
                    Source source,
                    JsonSchemaValidator jsonSchemaValidator) {
    this(cliParser, outputRecordCollector, destination, source);
    this.validator = jsonSchemaValidator;
  }

  public void run(String[] args) throws Exception {
    LOGGER.info("Running integration: {}", integration.getClass().getName());

    final IntegrationConfig parsed = cliParser.parse(args);

    LOGGER.info("Command: {}", parsed.getCommand());
    LOGGER.info("Integration config: {}", parsed);

    switch (parsed.getCommand()) {
      // common
      case SPEC -> outputRecordCollector.accept(new AirbyteMessage().withType(Type.SPEC).withSpec(integration.spec()));
      case CHECK -> {
        final JsonNode config = parseConfig(parsed.getConfigPath());
        validateConfig(integration.spec().getConnectionSpecification(), config, "CHECK");
        outputRecordCollector.accept(new AirbyteMessage().withType(Type.CONNECTION_STATUS).withConnectionStatus(integration.check(config)));
      }
      // source only
      case DISCOVER -> {
        final JsonNode config = parseConfig(parsed.getConfigPath());
        validateConfig(integration.spec().getConnectionSpecification(), config, "DISCOVER");
        outputRecordCollector.accept(new AirbyteMessage().withType(Type.CATALOG).withCatalog(source.discover(config)));
      }
      // todo (cgardens) - it is incongruous that that read and write return airbyte message (the
      // envelope) while the other commands return what goes inside it.
      case READ -> {
        final JsonNode config = parseConfig(parsed.getConfigPath());
        validateConfig(integration.spec().getConnectionSpecification(), config, "READ");
        final ConfiguredAirbyteCatalog catalog = parseConfig(parsed.getCatalogPath(), ConfiguredAirbyteCatalog.class);
        final Optional<JsonNode> stateOptional = parsed.getStatePath().map(IntegrationRunner::parseConfig);
        final AutoCloseableIterator<AirbyteMessage> messageIterator = source.read(config, catalog, stateOptional.orElse(null));
        try (messageIterator) {
          messageIterator.forEachRemaining(outputRecordCollector::accept);
        }
      }
      // destination only
      case WRITE -> {
        final JsonNode config = parseConfig(parsed.getConfigPath());
        validateConfig(integration.spec().getConnectionSpecification(), config, "WRITE");
        final ConfiguredAirbyteCatalog catalog = parseConfig(parsed.getCatalogPath(), ConfiguredAirbyteCatalog.class);
        final AirbyteMessageConsumer consumer = destination.getConsumer(config, catalog, outputRecordCollector);
        consumeWriteStream(consumer);
      }
      default -> throw new IllegalStateException("Unexpected value: " + parsed.getCommand());
    }

    LOGGER.info("Completed integration: {}", integration.getClass().getName());
  }

  @VisibleForTesting
  static void consumeWriteStream(AirbyteMessageConsumer consumer) throws Exception {
    // use a Scanner that only processes new line characters to strictly abide with the
    // https://jsonlines.org/ standard
    final Scanner input = new Scanner(System.in).useDelimiter("[\r\n]+");
    try (consumer) {
      consumer.start();
      while (input.hasNext()) {
        final String inputString = input.next();
        final Optional<AirbyteMessage> singerMessageOptional = Jsons.tryDeserialize(inputString, AirbyteMessage.class);
        if (singerMessageOptional.isPresent()) {
          consumer.accept(singerMessageOptional.get());
        } else {
          // todo (cgardens) - decide if we want to throw here instead.
          LOGGER.error(inputString);
        }
      }
    }
  }

  private static void validateConfig(JsonNode schemaJson, JsonNode objectJson, String operationType) throws Exception {
    final Set<String> validationResult = validator.validate(schemaJson, objectJson);
    if (!validationResult.isEmpty()) {
      throw new Exception(String.format("Verification error(s) occurred for %s. Errors: %s ",
          operationType, validationResult.toString()));
    }
  }

  private static JsonNode parseConfig(Path path) {
    return Jsons.deserialize(IOs.readFile(path));
  }

  private static <T> T parseConfig(Path path, Class<T> klass) {
    final JsonNode jsonNode = parseConfig(path);
    return Jsons.object(jsonNode, klass);
  }

}
