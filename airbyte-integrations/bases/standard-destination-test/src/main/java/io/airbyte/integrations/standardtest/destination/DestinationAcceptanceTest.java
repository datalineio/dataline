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

package io.airbyte.integrations.standardtest.destination;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.airbyte.commons.jackson.MoreMappers;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.lang.Exceptions;
import io.airbyte.commons.resources.MoreResources;
import io.airbyte.commons.util.MoreLists;
import io.airbyte.config.JobGetSpecConfig;
import io.airbyte.config.OperatorDbt;
import io.airbyte.config.StandardCheckConnectionInput;
import io.airbyte.config.StandardCheckConnectionOutput;
import io.airbyte.config.StandardCheckConnectionOutput.Status;
import io.airbyte.config.WorkerDestinationConfig;
import io.airbyte.protocol.models.AirbyteCatalog;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteMessage.Type;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import io.airbyte.protocol.models.AirbyteStateMessage;
import io.airbyte.protocol.models.AirbyteStream;
import io.airbyte.protocol.models.CatalogHelpers;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.airbyte.protocol.models.DestinationSyncMode;
import io.airbyte.protocol.models.SyncMode;
import io.airbyte.workers.DbtTransformationRunner;
import io.airbyte.workers.DefaultCheckConnectionWorker;
import io.airbyte.workers.DefaultGetSpecWorker;
import io.airbyte.workers.WorkerException;
import io.airbyte.workers.WorkerUtils;
import io.airbyte.workers.normalization.NormalizationRunner;
import io.airbyte.workers.normalization.NormalizationRunnerFactory;
import io.airbyte.workers.process.AirbyteIntegrationLauncher;
import io.airbyte.workers.process.DockerProcessFactory;
import io.airbyte.workers.process.ProcessFactory;
import io.airbyte.workers.protocols.airbyte.AirbyteDestination;
import io.airbyte.workers.protocols.airbyte.DefaultAirbyteDestination;
import io.airbyte.workers.test_helpers.EntrypointEnvChecker;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DestinationAcceptanceTest {

  private static final String JOB_ID = "0";
  private static final int JOB_ATTEMPT = 0;

  private static final Logger LOGGER = LoggerFactory.getLogger(DestinationAcceptanceTest.class);

  private TestDestinationEnv testEnv;

  private Path jobRoot;
  protected Path localRoot;
  private ProcessFactory processFactory;

  /**
   * Name of the docker image that the tests will run against.
   *
   * @return docker image name
   */
  protected abstract String getImageName();

  /**
   * Configuration specific to the integration. Will be passed to integration where appropriate in
   * each test. Should be valid.
   *
   * @return integration-specific configuration
   */
  protected abstract JsonNode getConfig() throws Exception;

  /**
   * Configuration specific to the integration. Will be passed to integration where appropriate in
   * tests that test behavior when configuration is invalid. e.g incorrect password. Should be
   * invalid.
   *
   * @return integration-specific configuration
   */
  protected abstract JsonNode getFailCheckConfig() throws Exception;

  /**
   * Function that returns all of the records in destination as json at the time this method is
   * invoked. These will be used to check that the data actually written is what should actually be
   * there. Note: this returns a set and does not test any order guarantees.
   *
   * @param testEnv - information about the test environment.
   * @param streamName - name of the stream for which we are retrieving records.
   * @param namespace - the destination namespace records are located in. Null if not applicable.
   *        Usually a JDBC schema.
   * @param streamSchema - schema of the stream to be retrieved. This is only necessary for
   *        destinations in which data types cannot be accurately inferred (e.g. in CSV destination,
   *        every value is a string).
   * @return All of the records in the destination at the time this method is invoked.
   * @throws Exception - can throw any exception, test framework will handle.
   */
  protected abstract List<JsonNode> retrieveRecords(TestDestinationEnv testEnv,
                                                    String streamName,
                                                    String namespace,
                                                    JsonNode streamSchema)
      throws Exception;

  /**
   * Returns a destination's default schema. The default implementation assumes this corresponds to
   * the configuration's 'schema' field, as this is how most of our destinations implement this.
   * Destinations are free to appropriately override this. The return value is used to assert
   * correctness.
   *
   * If not applicable, Destinations are free to ignore this.
   *
   * @param config - integration-specific configuration returned by {@link #getConfig()}.
   * @return the default schema, if applicatble.
   */
  protected String getDefaultSchema(JsonNode config) throws Exception {
    if (config.get("schema") == null) {
      return null;
    }
    return config.get("schema").asText();
  }

  /**
   * Override to return true if a destination implements namespaces and should be tested as such.
   */
  protected boolean implementsNamespaces() {
    return false;
  }

  /**
   * Detects if a destination implements append mode from the spec.json that should include
   * 'supportsIncremental' = true
   *
   * @return - a boolean.
   */
  protected boolean implementsAppend() throws WorkerException {
    final ConnectorSpecification spec = runSpec();
    assertNotNull(spec);
    if (spec.getSupportsIncremental() != null) {
      return spec.getSupportsIncremental();
    } else {
      return false;
    }
  }

  protected boolean normalizationFromSpec() throws Exception {
    final ConnectorSpecification spec = runSpec();
    assertNotNull(spec);
    if (spec.getSupportsNormalization() != null) {
      return spec.getSupportsNormalization();
    } else {
      return false;
    }
  }

  protected boolean dbtFromSpec() throws WorkerException {
    final ConnectorSpecification spec = runSpec();
    assertNotNull(spec);
    if (spec.getSupportsDBT() != null) {
      return spec.getSupportsDBT();
    } else {
      return false;
    }
  }

  /**
   * Detects if a destination implements append dedup mode from the spec.json that should include
   * 'supportedDestinationSyncMode'
   *
   * @return - a boolean.
   */
  protected boolean implementsAppendDedup() throws WorkerException {
    final ConnectorSpecification spec = runSpec();
    assertNotNull(spec);
    if (spec.getSupportedDestinationSyncModes() != null) {
      return spec.getSupportedDestinationSyncModes().contains(DestinationSyncMode.APPEND_DEDUP);
    } else {
      return false;
    }
  }

  /**
   * Detects if a destination implements overwrite mode from the spec.json that should include
   * 'supportedDestinationSyncMode'
   *
   * @return - a boolean.
   */
  protected boolean implementsOverwrite() throws WorkerException {
    final ConnectorSpecification spec = runSpec();
    assertNotNull(spec);
    if (spec.getSupportedDestinationSyncModes() != null) {
      return spec.getSupportedDestinationSyncModes().contains(DestinationSyncMode.OVERWRITE);
    } else {
      return false;
    }
  }

  /**
   * Override to return true to if the destination implements basic normalization and it should be
   * tested here.
   *
   * @return - a boolean.
   */
  protected boolean supportsNormalization() {
    return false;
  }

  protected boolean supportsDBT() {
    return false;
  }

  /**
   * Override to return true if a destination implements size limits on record size (then destination
   * should redefine getMaxRecordValueLimit() too)
   */
  protected boolean implementsRecordSizeLimitChecks() {
    return false;
  }

  /**
   * Same idea as {@link #retrieveRecords(TestDestinationEnv, String, String, JsonNode)}. Except this
   * method should pull records from the table that contains the normalized records and convert them
   * back into the data as it would appear in an {@link AirbyteRecordMessage}. Only need to override
   * this method if {@link #normalizationFromSpec} returns true.
   *
   * @param testEnv - information about the test environment.
   * @param streamName - name of the stream for which we are retrieving records.
   * @param namespace - the destination namespace records are located in. Null if not applicable.
   *        Usually a JDBC schema.
   * @return All of the records in the destination at the time this method is invoked.
   * @throws Exception - can throw any exception, test framework will handle.
   */
  protected List<JsonNode> retrieveNormalizedRecords(TestDestinationEnv testEnv, String streamName, String namespace) throws Exception {
    throw new IllegalStateException("Not implemented");
  }

  /**
   * Function that performs any setup of external resources required for the test. e.g. instantiate a
   * postgres database. This function will be called before EACH test.
   *
   * @param testEnv - information about the test environment.
   * @throws Exception - can throw any exception, test framework will handle.
   */
  protected abstract void setup(TestDestinationEnv testEnv) throws Exception;

  /**
   * Function that performs any clean up of external resources required for the test. e.g. delete a
   * postgres database. This function will be called after EACH test. It MUST remove all data in the
   * destination so that there is no contamination across tests.
   *
   * @param testEnv - information about the test environment.
   * @throws Exception - can throw any exception, test framework will handle.
   */
  protected abstract void tearDown(TestDestinationEnv testEnv) throws Exception;

  protected List<String> resolveIdentifier(String identifier) {
    final List<String> result = new ArrayList<>();
    result.add(identifier);
    return result;
  }

  @BeforeEach
  void setUpInternal() throws Exception {
    Path testDir = Path.of("/tmp/airbyte_tests/");
    Files.createDirectories(testDir);
    final Path workspaceRoot = Files.createTempDirectory(testDir, "test");
    jobRoot = Files.createDirectories(Path.of(workspaceRoot.toString(), "job"));
    localRoot = Files.createTempDirectory(testDir, "output");
    LOGGER.info("jobRoot: {}", jobRoot);
    LOGGER.info("localRoot: {}", localRoot);
    testEnv = new TestDestinationEnv(localRoot);

    setup(testEnv);

    processFactory = new DockerProcessFactory(workspaceRoot, workspaceRoot.toString(), localRoot.toString(), "host");
  }

  @AfterEach
  void tearDownInternal() throws Exception {
    tearDown(testEnv);
  }

  /**
   * Verify that when the integrations returns a valid spec.
   */
  @Test
  public void testGetSpec() throws WorkerException {
    assertNotNull(runSpec());
  }

  /**
   * Verify that when given valid credentials, that check connection returns a success response.
   * Assume that the {@link DestinationAcceptanceTest#getConfig()} is valid.
   */
  @Test
  public void testCheckConnection() throws Exception {
    assertEquals(Status.SUCCEEDED, runCheck(getConfig()).getStatus());
  }

  /**
   * Verify that when given invalid credentials, that check connection returns a failed response.
   * Assume that the {@link DestinationAcceptanceTest#getFailCheckConfig()} is invalid.
   */
  @Test
  public void testCheckConnectionInvalidCredentials() throws Exception {
    assertEquals(Status.FAILED, runCheck(getFailCheckConfig()).getStatus());
  }

  /**
   * Verify that the integration successfully writes records. Tests a wide variety of messages and
   * schemas (aspirationally, anyway).
   */
  @ParameterizedTest
  @ArgumentsSource(DataArgumentsProvider.class)
  public void testSync(String messagesFilename, String catalogFilename) throws Exception {
    final AirbyteCatalog catalog = Jsons.deserialize(MoreResources.readResource(catalogFilename), AirbyteCatalog.class);
    final ConfiguredAirbyteCatalog configuredCatalog = CatalogHelpers.toDefaultConfiguredCatalog(catalog);
    final List<AirbyteMessage> messages = MoreResources.readResource(messagesFilename).lines()
        .map(record -> Jsons.deserialize(record, AirbyteMessage.class)).collect(Collectors.toList());

    final JsonNode config = getConfig();
    final String defaultSchema = getDefaultSchema(config);
    runSyncAndVerifyStateOutput(config, messages, configuredCatalog, false);
    retrieveRawRecordsAndAssertSameMessages(catalog, messages, defaultSchema);
  }

  /**
   * Verify that the integration overwrites the first sync with the second sync.
   */
  @Test
  public void testSecondSync() throws Exception {
    if (!implementsOverwrite()) {
      LOGGER.info("Destination's spec.json does not support overwrite sync mode.");
      return;
    }

    final AirbyteCatalog catalog =
        Jsons.deserialize(MoreResources.readResource(DataArgumentsProvider.EXCHANGE_RATE_CONFIG.catalogFile), AirbyteCatalog.class);
    final ConfiguredAirbyteCatalog configuredCatalog = CatalogHelpers.toDefaultConfiguredCatalog(catalog);
    final List<AirbyteMessage> firstSyncMessages = MoreResources.readResource(DataArgumentsProvider.EXCHANGE_RATE_CONFIG.messageFile).lines()
        .map(record -> Jsons.deserialize(record, AirbyteMessage.class)).collect(Collectors.toList());
    final JsonNode config = getConfig();
    runSyncAndVerifyStateOutput(config, firstSyncMessages, configuredCatalog, false);

    final List<AirbyteMessage> secondSyncMessages = Lists.newArrayList(
        new AirbyteMessage()
            .withType(Type.RECORD)
            .withRecord(new AirbyteRecordMessage()
                .withStream(catalog.getStreams().get(0).getName())
                .withEmittedAt(Instant.now().toEpochMilli())
                .withData(Jsons.jsonNode(ImmutableMap.builder()
                    .put("id", 1)
                    .put("currency", "USD")
                    .put("date", "2020-03-31T00:00:00Z")
                    // TODO(sherifnada) hack: write decimals with sigfigs because Snowflake stores 10.1 as "10" which
                    // fails destination tests
                    .put("HKD", 10.1)
                    .put("NZD", 700.1)
                    .build()))),
        new AirbyteMessage()
            .withType(Type.STATE)
            .withState(new AirbyteStateMessage().withData(Jsons.jsonNode(ImmutableMap.of("checkpoint", 2)))));

    runSyncAndVerifyStateOutput(config, secondSyncMessages, configuredCatalog, false);
    final String defaultSchema = getDefaultSchema(config);
    retrieveRawRecordsAndAssertSameMessages(catalog, secondSyncMessages, defaultSchema);
  }

  /**
   * Tests that we are able to read over special characters properly when processing line breaks in
   * destinations.
   */
  @Test
  public void testLineBreakCharacters() throws Exception {
    final AirbyteCatalog catalog =
        Jsons.deserialize(MoreResources.readResource(DataArgumentsProvider.EXCHANGE_RATE_CONFIG.catalogFile), AirbyteCatalog.class);
    final ConfiguredAirbyteCatalog configuredCatalog = CatalogHelpers.toDefaultConfiguredCatalog(catalog);
    final JsonNode config = getConfig();

    final List<AirbyteMessage> secondSyncMessages = Lists.newArrayList(
        new AirbyteMessage()
            .withType(Type.RECORD)
            .withRecord(new AirbyteRecordMessage()
                .withStream(catalog.getStreams().get(0).getName())
                .withEmittedAt(Instant.now().toEpochMilli())
                .withData(Jsons.jsonNode(ImmutableMap.builder()
                    .put("id", 1)
                    .put("currency", "USD\u2028")
                    .put("date", "2020-03-\n31T00:00:00Z\r")
                    // TODO(sherifnada) hack: write decimals with sigfigs because Snowflake stores 10.1 as "10" which
                    // fails destination tests
                    .put("HKD", 10.1)
                    .put("NZD", 700.1)
                    .build()))),
        new AirbyteMessage()
            .withType(Type.STATE)
            .withState(new AirbyteStateMessage().withData(Jsons.jsonNode(ImmutableMap.of("checkpoint", 2)))));

    runSyncAndVerifyStateOutput(config, secondSyncMessages, configuredCatalog, false);
    final String defaultSchema = getDefaultSchema(config);
    retrieveRawRecordsAndAssertSameMessages(catalog, secondSyncMessages, defaultSchema);
  }

  @Test
  public void specNormalizationValueShouldBeCorrect() throws Exception {
    final boolean normalizationFromSpec = normalizationFromSpec();
    assertEquals(normalizationFromSpec, supportsNormalization());
    boolean normalizationRunnerFactorySupportsDestinationImage;
    try {
      NormalizationRunnerFactory.create(getImageName(), processFactory);
      normalizationRunnerFactorySupportsDestinationImage = true;
    } catch (IllegalStateException e) {
      normalizationRunnerFactorySupportsDestinationImage = false;
    }
    assertEquals(normalizationFromSpec, normalizationRunnerFactorySupportsDestinationImage);
  }

  @Test
  public void specDBTValueShouldBeCorrect() throws WorkerException {
    assertEquals(dbtFromSpec(), supportsDBT());
  }

  /**
   * Verify that the integration successfully writes records incrementally. The second run should
   * append records to the datastore instead of overwriting the previous run.
   */
  @Test
  public void testIncrementalSync() throws Exception {
    if (!implementsAppend()) {
      LOGGER.info("Destination's spec.json does not include '\"supportsIncremental\" ; true'");
      return;
    }

    final AirbyteCatalog catalog =
        Jsons.deserialize(MoreResources.readResource(DataArgumentsProvider.EXCHANGE_RATE_CONFIG.catalogFile), AirbyteCatalog.class);
    final ConfiguredAirbyteCatalog configuredCatalog = CatalogHelpers.toDefaultConfiguredCatalog(catalog);
    configuredCatalog.getStreams().forEach(s -> {
      s.withSyncMode(SyncMode.INCREMENTAL);
      s.withDestinationSyncMode(DestinationSyncMode.APPEND);
    });

    final List<AirbyteMessage> firstSyncMessages = MoreResources.readResource(DataArgumentsProvider.EXCHANGE_RATE_CONFIG.messageFile).lines()
        .map(record -> Jsons.deserialize(record, AirbyteMessage.class)).collect(Collectors.toList());
    final JsonNode config = getConfig();
    runSyncAndVerifyStateOutput(config, firstSyncMessages, configuredCatalog, false);

    final List<AirbyteMessage> secondSyncMessages = Lists.newArrayList(
        new AirbyteMessage()
            .withType(Type.RECORD)
            .withRecord(new AirbyteRecordMessage()
                .withStream(catalog.getStreams().get(0).getName())
                .withEmittedAt(Instant.now().toEpochMilli())
                .withData(Jsons.jsonNode(ImmutableMap.builder()
                    .put("id", 1)
                    .put("currency", "USD")
                    .put("date", "2020-03-31T00:00:00Z")
                    // TODO(sherifnada) hack: write decimals with sigfigs because Snowflake stores 10.1 as "10" which
                    // fails destination tests
                    .put("HKD", 10.1)
                    .put("NZD", 700.1)
                    .build()))),
        new AirbyteMessage()
            .withType(Type.STATE)
            .withState(new AirbyteStateMessage().withData(Jsons.jsonNode(ImmutableMap.of("checkpoint", 2)))));
    runSyncAndVerifyStateOutput(config, secondSyncMessages, configuredCatalog, false);

    final List<AirbyteMessage> expectedMessagesAfterSecondSync = new ArrayList<>();
    expectedMessagesAfterSecondSync.addAll(firstSyncMessages);
    expectedMessagesAfterSecondSync.addAll(secondSyncMessages);

    final String defaultSchema = getDefaultSchema(config);
    retrieveRawRecordsAndAssertSameMessages(catalog, expectedMessagesAfterSecondSync, defaultSchema);
  }

  /**
   * Verify that the integration successfully writes records successfully both raw and normalized.
   * Tests a wide variety of messages an schemas (aspirationally, anyway).
   */
  @ParameterizedTest
  @ArgumentsSource(DataArgumentsProvider.class)
  public void testSyncWithNormalization(String messagesFilename, String catalogFilename) throws Exception {
    if (!normalizationFromSpec()) {
      return;
    }

    final AirbyteCatalog catalog = Jsons.deserialize(MoreResources.readResource(catalogFilename), AirbyteCatalog.class);
    final ConfiguredAirbyteCatalog configuredCatalog = CatalogHelpers.toDefaultConfiguredCatalog(catalog);
    final List<AirbyteMessage> messages = MoreResources.readResource(messagesFilename).lines()
        .map(record -> Jsons.deserialize(record, AirbyteMessage.class)).collect(Collectors.toList());

    final JsonNode config = getConfig();
    runSyncAndVerifyStateOutput(config, messages, configuredCatalog, true);

    String defaultSchema = getDefaultSchema(config);
    final List<AirbyteRecordMessage> actualMessages = retrieveNormalizedRecords(catalog, defaultSchema);
    assertSameMessages(messages, actualMessages, true);
  }

  /**
   * Verify that the integration successfully writes records successfully both raw and normalized and
   * run dedupe transformations.
   *
   * Although this test assumes append-dedup requires normalization, and almost all our Destinations
   * do so, this is not necessarily true. This explains {@link #implementsAppendDedup()}.
   */
  @Test
  public void testIncrementalDedupeSync() throws Exception {
    if (!implementsAppendDedup()) {
      LOGGER.info("Destination's spec.json does not include 'append_dedupe' in its '\"supportedDestinationSyncModes\"'");
      return;
    }

    final AirbyteCatalog catalog =
        Jsons.deserialize(MoreResources.readResource(DataArgumentsProvider.EXCHANGE_RATE_CONFIG.catalogFile), AirbyteCatalog.class);
    final ConfiguredAirbyteCatalog configuredCatalog = CatalogHelpers.toDefaultConfiguredCatalog(catalog);
    configuredCatalog.getStreams().forEach(s -> {
      s.withSyncMode(SyncMode.INCREMENTAL);
      s.withDestinationSyncMode(DestinationSyncMode.APPEND_DEDUP);
      s.withCursorField(Collections.emptyList());
      // use composite primary key of various types (string, float)
      s.withPrimaryKey(List.of(List.of("id"), List.of("currency"), List.of("date"), List.of("NZD")));
    });

    final List<AirbyteMessage> firstSyncMessages = MoreResources.readResource(DataArgumentsProvider.EXCHANGE_RATE_CONFIG.messageFile).lines()
        .map(record -> Jsons.deserialize(record, AirbyteMessage.class)).collect(Collectors.toList());
    final JsonNode config = getConfig();
    runSyncAndVerifyStateOutput(config, firstSyncMessages, configuredCatalog, true);

    final List<AirbyteMessage> secondSyncMessages = Lists.newArrayList(
        new AirbyteMessage()
            .withType(Type.RECORD)
            .withRecord(new AirbyteRecordMessage()
                .withStream(catalog.getStreams().get(0).getName())
                .withEmittedAt(Instant.now().toEpochMilli())
                .withData(Jsons.jsonNode(ImmutableMap.builder()
                    .put("id", 2)
                    .put("currency", "EUR")
                    .put("date", "2020-09-01T00:00:00Z")
                    .put("HKD", 10.5)
                    .put("NZD", 1.14)
                    .build()))),
        new AirbyteMessage()
            .withType(Type.RECORD)
            .withRecord(new AirbyteRecordMessage()
                .withStream(catalog.getStreams().get(0).getName())
                .withEmittedAt(Instant.now().toEpochMilli() + 100L)
                .withData(Jsons.jsonNode(ImmutableMap.builder()
                    .put("id", 1)
                    .put("currency", "USD")
                    .put("date", "2020-09-01T00:00:00Z")
                    .put("HKD", 5.4)
                    .put("NZD", 1.14)
                    .build()))),
        new AirbyteMessage()
            .withType(Type.STATE)
            .withState(new AirbyteStateMessage().withData(Jsons.jsonNode(ImmutableMap.of("checkpoint", 2)))));
    runSyncAndVerifyStateOutput(config, secondSyncMessages, configuredCatalog, true);

    final List<AirbyteMessage> expectedMessagesAfterSecondSync = new ArrayList<>();
    expectedMessagesAfterSecondSync.addAll(firstSyncMessages);
    expectedMessagesAfterSecondSync.addAll(secondSyncMessages);

    final Map<String, AirbyteMessage> latestMessagesOnly = expectedMessagesAfterSecondSync
        .stream()
        .filter(message -> message.getType() == Type.RECORD && message.getRecord() != null)
        .collect(Collectors.toMap(
            message -> message.getRecord().getData().get("id").asText() +
                message.getRecord().getData().get("currency").asText() +
                message.getRecord().getData().get("date").asText() +
                message.getRecord().getData().get("NZD").asText(),
            message -> message,
            // keep only latest emitted record message per primary key/cursor
            (a, b) -> a.getRecord().getEmittedAt() > b.getRecord().getEmittedAt() ? a : b));
    // Filter expectedMessagesAfterSecondSync and keep latest messages only (keep same message order)
    final List<AirbyteMessage> expectedMessages = expectedMessagesAfterSecondSync
        .stream()
        .filter(message -> message.getType() == Type.RECORD && message.getRecord() != null)
        .filter(message -> {
          final String key = message.getRecord().getData().get("id").asText() +
              message.getRecord().getData().get("currency").asText() +
              message.getRecord().getData().get("date").asText() +
              message.getRecord().getData().get("NZD").asText();
          return message.getRecord().getEmittedAt().equals(latestMessagesOnly.get(key).getRecord().getEmittedAt());
        }).collect(Collectors.toList());

    final String defaultSchema = getDefaultSchema(config);
    retrieveRawRecordsAndAssertSameMessages(catalog, expectedMessagesAfterSecondSync, defaultSchema);
    final List<AirbyteRecordMessage> actualMessages = retrieveNormalizedRecords(catalog, defaultSchema);
    assertSameMessages(expectedMessages, actualMessages, true);
  }

  /**
   * This test is running a sync using the exchange rate catalog and messages. However it also
   * generates and adds two extra messages with big records (near the destination limit as defined by
   * getMaxValueLengthLimit()
   *
   * The first big message should be small enough to fit into the destination while the second message
   * would be too big and fails to replicate.
   */
  @Test
  void testSyncVeryBigRecords() throws Exception {
    if (!implementsRecordSizeLimitChecks()) {
      return;
    }

    final AirbyteCatalog catalog =
        Jsons.deserialize(MoreResources.readResource(DataArgumentsProvider.EXCHANGE_RATE_CONFIG.catalogFile), AirbyteCatalog.class);
    final ConfiguredAirbyteCatalog configuredCatalog = CatalogHelpers.toDefaultConfiguredCatalog(catalog);
    final List<AirbyteMessage> messages = MoreResources.readResource(DataArgumentsProvider.EXCHANGE_RATE_CONFIG.messageFile).lines()
        .map(record -> Jsons.deserialize(record, AirbyteMessage.class)).collect(Collectors.toList());
    // Add a big message that barely fits into the limits of the destination
    messages.add(new AirbyteMessage()
        .withType(Type.RECORD)
        .withRecord(new AirbyteRecordMessage()
            .withStream(catalog.getStreams().get(0).getName())
            .withEmittedAt(Instant.now().toEpochMilli())
            .withData(Jsons.jsonNode(ImmutableMap.builder()
                .put("id", 3)
                // remove enough characters from max limit to fit the other columns and json characters
                .put("currency", generateBigString(-150))
                .put("date", "2020-10-10T00:00:00Z")
                .put("HKD", 10.5)
                .put("NZD", 1.14)
                .build()))));
    // Add a big message that does not fit into the limits of the destination
    final AirbyteMessage bigMessage = new AirbyteMessage()
        .withType(Type.RECORD)
        .withRecord(new AirbyteRecordMessage()
            .withStream(catalog.getStreams().get(0).getName())
            .withEmittedAt(Instant.now().toEpochMilli())
            .withData(Jsons.jsonNode(ImmutableMap.builder()
                .put("id", 3)
                .put("currency", generateBigString(0))
                .put("date", "2020-10-10T00:00:00Z")
                .put("HKD", 10.5)
                .put("NZD", 1.14)
                .build())));
    final JsonNode config = getConfig();
    final String defaultSchema = getDefaultSchema(config);
    final List<AirbyteMessage> allMessages = new ArrayList<>();
    allMessages.add(bigMessage);
    allMessages.addAll(messages);
    runSyncAndVerifyStateOutput(config, allMessages, configuredCatalog, false);
    retrieveRawRecordsAndAssertSameMessages(catalog, messages, defaultSchema);
  }

  private String generateBigString(int addExtraCharacters) {
    final int length = getMaxRecordValueLimit() + addExtraCharacters;
    return new Random()
        .ints('a', 'z' + 1)
        .limit(length)
        .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
        .toString();
  }

  /**
   * @return the max limit length allowed for values in the destination.
   */
  protected int getMaxRecordValueLimit() {
    return 1000000000;
  }

  @Test
  public void testCustomDbtTransformations() throws Exception {
    if (!normalizationFromSpec() || !dbtFromSpec()) {
      // we require normalization implementation for this destination, because we make sure to install
      // required dbt dependency in the normalization docker image in order to run this test successfully
      // (we don't actually rely on normalization running anything here though)
      return;
    }

    final JsonNode config = getConfig();

    final DbtTransformationRunner runner = new DbtTransformationRunner(processFactory, NormalizationRunnerFactory.create(
        getImageName(),
        processFactory));
    runner.start();
    final Path transformationRoot = Files.createDirectories(jobRoot.resolve("transform"));
    final OperatorDbt dbtConfig = new OperatorDbt()
        .withGitRepoUrl("https://github.com/fishtown-analytics/jaffle_shop.git")
        .withGitRepoBranch("main")
        .withDockerImage("airbyte/normalization:dev");
    //
    // jaffle_shop is a fictional ecommerce store maintained by fishtownanalytics/dbt.
    //
    // This dbt project transforms raw data from an app database into a customers and orders model ready
    // for analytics.
    // The repo is a self-contained playground dbt project, useful for testing out scripts, and
    // communicating some of the core dbt concepts:
    //
    // 1. First, it tests if connection to the destination works.
    dbtConfig.withDbtArguments("debug");
    if (!runner.run(JOB_ID, JOB_ATTEMPT, transformationRoot, config, WorkerUtils.DEFAULT_RESOURCE_REQUIREMENTS, dbtConfig)) {
      throw new WorkerException("dbt debug Failed.");
    }
    // 2. Install any dependencies packages, if any
    dbtConfig.withDbtArguments("deps");
    if (!runner.transform(JOB_ID, JOB_ATTEMPT, transformationRoot, config, WorkerUtils.DEFAULT_RESOURCE_REQUIREMENTS, dbtConfig)) {
      throw new WorkerException("dbt deps Failed.");
    }
    // 3. It contains seeds that includes some (fake) raw data from a fictional app as CSVs data sets.
    // This materializes the CSVs as tables in your target schema.
    // Note that a typical dbt project does not require this step since dbt assumes your raw data is
    // already in your warehouse.
    dbtConfig.withDbtArguments("seed");
    if (!runner.transform(JOB_ID, JOB_ATTEMPT, transformationRoot, config, WorkerUtils.DEFAULT_RESOURCE_REQUIREMENTS, dbtConfig)) {
      throw new WorkerException("dbt seed Failed.");
    }
    // 4. Run the models:
    // Note: If this steps fails, it might mean that you need to make small changes to the SQL in the
    // models folder to adjust for the flavor of SQL of your target database.
    dbtConfig.withDbtArguments("run");
    if (!runner.transform(JOB_ID, JOB_ATTEMPT, transformationRoot, config, WorkerUtils.DEFAULT_RESOURCE_REQUIREMENTS, dbtConfig)) {
      throw new WorkerException("dbt run Failed.");
    }
    // 5. Test the output of the models and tables have been properly populated:
    dbtConfig.withDbtArguments("test");
    if (!runner.transform(JOB_ID, JOB_ATTEMPT, transformationRoot, config, WorkerUtils.DEFAULT_RESOURCE_REQUIREMENTS, dbtConfig)) {
      throw new WorkerException("dbt test Failed.");
    }
    // 6. Generate dbt documentation for the project:
    dbtConfig.withDbtArguments("docs generate");
    if (!runner.transform(JOB_ID, JOB_ATTEMPT, transformationRoot, config, WorkerUtils.DEFAULT_RESOURCE_REQUIREMENTS, dbtConfig)) {
      throw new WorkerException("dbt docs generate Failed.");
    }
    runner.close();
  }

  @Test
  void testCustomDbtTransformationsFailure() throws Exception {
    if (!normalizationFromSpec() || !dbtFromSpec()) {
      // we require normalization implementation for this destination, because we make sure to install
      // required dbt dependency in the normalization docker image in order to run this test successfully
      // (we don't actually rely on normalization running anything here though)
      return;
    }

    final JsonNode config = getConfig();

    final DbtTransformationRunner runner = new DbtTransformationRunner(processFactory, NormalizationRunnerFactory.create(
        getImageName(),
        processFactory));
    runner.start();
    final Path transformationRoot = Files.createDirectories(jobRoot.resolve("transform"));
    final OperatorDbt dbtConfig = new OperatorDbt()
        .withGitRepoUrl("https://github.com/fishtown-analytics/dbt-learn-demo.git")
        .withGitRepoBranch("master")
        .withDockerImage("fishtownanalytics/dbt:0.19.1")
        .withDbtArguments("debug");
    if (!runner.run(JOB_ID, JOB_ATTEMPT, transformationRoot, config, WorkerUtils.DEFAULT_RESOURCE_REQUIREMENTS, dbtConfig)) {
      throw new WorkerException("dbt debug Failed.");
    }

    dbtConfig.withDbtArguments("test");
    assertFalse(runner.transform(JOB_ID, JOB_ATTEMPT, transformationRoot, config, WorkerUtils.DEFAULT_RESOURCE_REQUIREMENTS, dbtConfig),
        "dbt test should fail, as we haven't run dbt run on this project yet");
  }

  /**
   * Verify the destination uses the namespace field if it is set.
   */
  @Test
  void testSyncUsesAirbyteStreamNamespaceIfNotNull() throws Exception {
    if (!implementsNamespaces()) {
      return;
    }

    // TODO(davin): make these tests part of the catalog file.
    final AirbyteCatalog catalog =
        Jsons.deserialize(MoreResources.readResource(DataArgumentsProvider.EXCHANGE_RATE_CONFIG.catalogFile), AirbyteCatalog.class);
    final String namespace = "sourcenamespace";
    catalog.getStreams().forEach(stream -> stream.setNamespace(namespace));
    final ConfiguredAirbyteCatalog configuredCatalog = CatalogHelpers.toDefaultConfiguredCatalog(catalog);

    final List<AirbyteMessage> messages = MoreResources.readResource(DataArgumentsProvider.EXCHANGE_RATE_CONFIG.messageFile).lines()
        .map(record -> Jsons.deserialize(record, AirbyteMessage.class)).collect(Collectors.toList());
    messages.forEach(
        message -> {
          if (message.getRecord() != null) {
            message.getRecord().setNamespace(namespace);
          }
        });

    final JsonNode config = getConfig();
    final String defaultSchema = getDefaultSchema(config);
    runSyncAndVerifyStateOutput(config, messages, configuredCatalog, false);
    retrieveRawRecordsAndAssertSameMessages(catalog, messages, defaultSchema);
  }

  /**
   * Verify a destination is able to write tables with the same name to different namespaces.
   */
  @Test
  void testSyncWriteSameTableNameDifferentNamespace() throws Exception {
    if (!implementsNamespaces()) {
      return;
    }

    // TODO(davin): make these tests part of the catalog file.
    final var catalog =
        Jsons.deserialize(MoreResources.readResource(DataArgumentsProvider.EXCHANGE_RATE_CONFIG.catalogFile), AirbyteCatalog.class);
    final var namespace1 = "sourcenamespace";
    catalog.getStreams().forEach(stream -> stream.setNamespace(namespace1));

    final var diffNamespaceStreams = new ArrayList<AirbyteStream>();
    final var namespace2 = "diff_source_namespace";
    final var mapper = MoreMappers.initMapper();
    for (AirbyteStream stream : catalog.getStreams()) {
      var clonedStream = mapper.readValue(mapper.writeValueAsString(stream), AirbyteStream.class);
      clonedStream.setNamespace(namespace2);
      diffNamespaceStreams.add(clonedStream);
    }
    catalog.getStreams().addAll(diffNamespaceStreams);

    final var configuredCatalog = CatalogHelpers.toDefaultConfiguredCatalog(catalog);

    final var ns1Msgs = MoreResources.readResource(DataArgumentsProvider.EXCHANGE_RATE_CONFIG.messageFile).lines()
        .map(record -> Jsons.deserialize(record, AirbyteMessage.class)).collect(Collectors.toList());
    ns1Msgs.forEach(
        message -> {
          if (message.getRecord() != null) {
            message.getRecord().setNamespace(namespace1);
          }
        });
    final var ns2Msgs = MoreResources.readResource(DataArgumentsProvider.EXCHANGE_RATE_CONFIG.messageFile).lines()
        .map(record -> Jsons.deserialize(record, AirbyteMessage.class)).collect(Collectors.toList());
    ns2Msgs.forEach(
        message -> {
          if (message.getRecord() != null) {
            message.getRecord().setNamespace(namespace2);
          }
        });
    final var allMessages = new ArrayList<>(ns1Msgs);
    allMessages.addAll(ns2Msgs);

    final JsonNode config = getConfig();
    final String defaultSchema = getDefaultSchema(config);
    runSyncAndVerifyStateOutput(config, allMessages, configuredCatalog, false);
    retrieveRawRecordsAndAssertSameMessages(catalog, allMessages, defaultSchema);
  }

  /**
   * In order to launch a source on Kubernetes in a pod, we need to be able to wrap the entrypoint.
   * The source connector must specify its entrypoint in the AIRBYTE_ENTRYPOINT variable. This test
   * ensures that the entrypoint environment variable is set.
   */
  @Test
  public void testEntrypointEnvVar() throws Exception {
    final String entrypoint = EntrypointEnvChecker.getEntrypointEnvVariable(
        processFactory,
        JOB_ID,
        JOB_ATTEMPT,
        jobRoot,
        getImageName());

    assertNotNull(entrypoint);
    assertFalse(entrypoint.isBlank());
  }

  private ConnectorSpecification runSpec() throws WorkerException {
    return new DefaultGetSpecWorker(new AirbyteIntegrationLauncher(JOB_ID, JOB_ATTEMPT, getImageName(), processFactory))
        .run(new JobGetSpecConfig().withDockerImage(getImageName()), jobRoot);
  }

  private StandardCheckConnectionOutput runCheck(JsonNode config) throws WorkerException {
    return new DefaultCheckConnectionWorker(new AirbyteIntegrationLauncher(JOB_ID, JOB_ATTEMPT, getImageName(), processFactory))
        .run(new StandardCheckConnectionInput().withConnectionConfiguration(config), jobRoot);
  }

  protected AirbyteDestination getDestination() {
    return new DefaultAirbyteDestination(new AirbyteIntegrationLauncher(JOB_ID, JOB_ATTEMPT, getImageName(), processFactory));
  }

  protected void runSyncAndVerifyStateOutput(JsonNode config,
                                             List<AirbyteMessage> messages,
                                             ConfiguredAirbyteCatalog catalog,
                                             boolean runNormalization)
      throws Exception {
    final List<AirbyteMessage> destinationOutput = runSync(config, messages, catalog, runNormalization);
    final AirbyteMessage expectedStateMessage = MoreLists.reversed(messages)
        .stream()
        .filter(m -> m.getType() == Type.STATE)
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("All message sets used for testing should include a state record"));

    final AirbyteMessage actualStateMessage = MoreLists.reversed(destinationOutput)
        .stream()
        .filter(m -> m.getType() == Type.STATE)
        .findFirst()
        .orElseGet(() -> {
          fail("Destination failed to output state");
          return null;
        });

    assertEquals(expectedStateMessage, actualStateMessage);
  }

  private List<AirbyteMessage> runSync(JsonNode config, List<AirbyteMessage> messages, ConfiguredAirbyteCatalog catalog, boolean runNormalization)
      throws Exception {

    final WorkerDestinationConfig destinationConfig = new WorkerDestinationConfig()
        .withConnectionId(UUID.randomUUID())
        .withCatalog(catalog)
        .withDestinationConnectionConfiguration(config);

    final AirbyteDestination destination = getDestination();

    destination.start(destinationConfig, jobRoot);
    messages.forEach(message -> Exceptions.toRuntime(() -> destination.accept(message)));
    destination.notifyEndOfStream();

    List<AirbyteMessage> destinationOutput = new ArrayList<>();
    while (!destination.isFinished()) {
      destination.attemptRead().ifPresent(destinationOutput::add);
    }

    destination.close();

    if (!runNormalization) {
      return destinationOutput;
    }

    final NormalizationRunner runner = NormalizationRunnerFactory.create(
        getImageName(),
        processFactory);
    runner.start();
    final Path normalizationRoot = Files.createDirectories(jobRoot.resolve("normalize"));
    if (!runner.normalize(JOB_ID, JOB_ATTEMPT, normalizationRoot, destinationConfig.getDestinationConnectionConfiguration(),
        destinationConfig.getCatalog(), WorkerUtils.DEFAULT_RESOURCE_REQUIREMENTS)) {
      throw new WorkerException("Normalization Failed.");
    }
    runner.close();
    return destinationOutput;
  }

  protected void retrieveRawRecordsAndAssertSameMessages(AirbyteCatalog catalog, List<AirbyteMessage> messages, String defaultSchema)
      throws Exception {
    final List<AirbyteRecordMessage> actualMessages = new ArrayList<>();
    for (final AirbyteStream stream : catalog.getStreams()) {
      final String streamName = stream.getName();
      final String schema = stream.getNamespace() != null ? stream.getNamespace() : defaultSchema;
      List<AirbyteRecordMessage> msgList = retrieveRecords(testEnv, streamName, schema, stream.getJsonSchema())
          .stream()
          .map(data -> new AirbyteRecordMessage().withStream(streamName).withNamespace(schema).withData(data))
          .collect(Collectors.toList());
      actualMessages.addAll(msgList);
    }

    assertSameMessages(messages, actualMessages, false);
  }

  // ignores emitted at.
  protected void assertSameMessages(List<AirbyteMessage> expected, List<AirbyteRecordMessage> actual, boolean pruneAirbyteInternalFields) {
    final List<JsonNode> expectedProcessed = expected.stream()
        .filter(message -> message.getType() == AirbyteMessage.Type.RECORD)
        .map(AirbyteMessage::getRecord)
        .peek(recordMessage -> recordMessage.setEmittedAt(null))
        .map(recordMessage -> pruneAirbyteInternalFields ? safePrune(recordMessage) : recordMessage)
        .map(recordMessage -> recordMessage.getData())
        .collect(Collectors.toList());

    final List<JsonNode> actualProcessed = actual.stream()
        .map(recordMessage -> pruneAirbyteInternalFields ? safePrune(recordMessage) : recordMessage)
        .map(recordMessage -> recordMessage.getData())
        .collect(Collectors.toList());

    assertSameData(expectedProcessed, actualProcessed);
  }

  private void assertSameData(List<JsonNode> expected, List<JsonNode> actual) {
    LOGGER.info("Expected data {}", expected);
    LOGGER.info("Actual data   {}", actual);
    assertEquals(expected.size(), actual.size());
    final Iterator<JsonNode> expectedIterator = expected.iterator();
    final Iterator<JsonNode> actualIterator = actual.iterator();
    while (expectedIterator.hasNext() && actualIterator.hasNext()) {
      final JsonNode expectedData = expectedIterator.next();
      final JsonNode actualData = actualIterator.next();
      final Iterator<Entry<String, JsonNode>> expectedDataIterator = expectedData.fields();
      LOGGER.info("Expected row {}", expectedData);
      LOGGER.info("Actual row   {}", actualData);
      assertEquals(expectedData.size(), actualData.size(), "Unequal row size");
      while (expectedDataIterator.hasNext()) {
        final Entry<String, JsonNode> expectedEntry = expectedDataIterator.next();
        final JsonNode expectedValue = expectedEntry.getValue();
        JsonNode actualValue = null;
        String key = expectedEntry.getKey();
        for (String tmpKey : resolveIdentifier(expectedEntry.getKey())) {
          actualValue = actualData.get(tmpKey);
          if (actualValue != null) {
            key = tmpKey;
            break;
          }
        }
        LOGGER.info("For {} Expected {} vs Actual {}", key, expectedValue, actualValue);
        assertTrue(actualData.has(key));
        assertSameValue(expectedValue, actualValue);
      }
    }
  }

  // Allows subclasses to implement custom comparison asserts
  protected void assertSameValue(JsonNode expectedValue, JsonNode actualValue) {
    assertEquals(expectedValue, actualValue);
  }

  protected List<AirbyteRecordMessage> retrieveNormalizedRecords(AirbyteCatalog catalog, String defaultSchema) throws Exception {
    final List<AirbyteRecordMessage> actualMessages = new ArrayList<>();

    for (final AirbyteStream stream : catalog.getStreams()) {
      final String streamName = stream.getName();

      List<AirbyteRecordMessage> msgList = retrieveNormalizedRecords(testEnv, streamName, defaultSchema)
          .stream()
          .map(data -> new AirbyteRecordMessage().withStream(streamName).withData(data))
          .collect(Collectors.toList());
      actualMessages.addAll(msgList);
    }
    return actualMessages;
  }

  /**
   * Same as {@link #pruneMutate(JsonNode)}, except does a defensive copy and returns a new json node
   * object instead of mutating in place.
   *
   * @param record - record that will be pruned.
   * @return pruned json node.
   */
  private AirbyteRecordMessage safePrune(AirbyteRecordMessage record) {
    final AirbyteRecordMessage clone = Jsons.clone(record);
    pruneMutate(clone.getData());
    return clone;
  }

  /**
   * Prune fields that are added internally by airbyte and are not part of the original data. Used so
   * that we can compare data that is persisted by an Airbyte worker to the original data. This method
   * mutates the provided json in place.
   *
   * @param json - json that will be pruned. will be mutated in place!
   */
  private void pruneMutate(JsonNode json) {
    for (final String key : Jsons.keys(json)) {
      final JsonNode node = json.get(key);
      // recursively prune all airbyte internal fields.
      if (node.isObject() || node.isArray()) {
        pruneMutate(node);
      }

      // prune the following
      // - airbyte internal fields
      // - fields that match what airbyte generates as hash ids
      // - null values -- normalization will often return `<key>: null` but in the original data that key
      // likely did not exist in the original message. the most consistent thing to do is always remove
      // the null fields (this choice does decrease our ability to check that normalization creates
      // columns even if all the values in that column are null)
      final HashSet<String> airbyteInternalFields = Sets.newHashSet(
          "emitted_at",
          "ab_id",
          "normalized_at",
          "EMITTED_AT",
          "AB_ID",
          "NORMALIZED_AT",
          "HASHID");
      if (airbyteInternalFields.stream().anyMatch(internalField -> key.toLowerCase().contains(internalField.toLowerCase()))
          || json.get(key).isNull()) {
        ((ObjectNode) json).remove(key);
      }
    }
  }

  public static class TestDestinationEnv {

    private final Path localRoot;

    public TestDestinationEnv(Path localRoot) {
      this.localRoot = localRoot;
    }

    public Path getLocalRoot() {
      return localRoot;
    }

    @Override
    public String toString() {
      return "TestDestinationEnv{" +
          "localRoot=" + localRoot +
          '}';
    }

  }

}
