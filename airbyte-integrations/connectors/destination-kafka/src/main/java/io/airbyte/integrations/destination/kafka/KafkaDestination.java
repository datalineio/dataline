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

package io.airbyte.integrations.destination.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import io.airbyte.commons.json.Jsons;
import io.airbyte.integrations.BaseConnector;
import io.airbyte.integrations.base.AirbyteMessageConsumer;
import io.airbyte.integrations.base.Destination;
import io.airbyte.integrations.base.IntegrationRunner;
import io.airbyte.integrations.base.JavaBaseConstants;
import io.airbyte.integrations.destination.StandardNameTransformer;
import io.airbyte.protocol.models.AirbyteConnectionStatus;
import io.airbyte.protocol.models.AirbyteConnectionStatus.Status;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaDestination extends BaseConnector implements Destination {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaDestination.class);

  private final StandardNameTransformer namingResolver;

  public KafkaDestination() {
    this.namingResolver = new StandardNameTransformer();
  }

  @Override
  public AirbyteConnectionStatus check(JsonNode config) {
    try {
      final String testTopic = config.has("test_topic") ? config.get("test_topic").asText() : "";
      if (!testTopic.isBlank()) {
        final KafkaDestinationConfig kafkaDestinationConfig = KafkaDestinationConfig.getKafkaDestinationConfig(config);
        final KafkaProducer<String, JsonNode> producer = kafkaDestinationConfig.getProducer();
        final String key = UUID.randomUUID().toString();
        final JsonNode value = Jsons.jsonNode(ImmutableMap.of(
            JavaBaseConstants.COLUMN_NAME_AB_ID, key,
            JavaBaseConstants.COLUMN_NAME_EMITTED_AT, System.currentTimeMillis(),
            JavaBaseConstants.COLUMN_NAME_DATA, Jsons.jsonNode(ImmutableMap.of("test-key", "test-value"))));

        if (kafkaDestinationConfig.isTransactionalProducer()) {
          producer.initTransactions();
          producer.beginTransaction();
        }

        final RecordMetadata metadata = producer.send(new ProducerRecord<>(
            namingResolver.getIdentifier(testTopic), key, value)).get();
        producer.flush();

        if (kafkaDestinationConfig.isTransactionalProducer()) {
          producer.commitTransaction();
        }

        LOGGER.info("Successfully connected to Kafka brokers for topic '{}'.", metadata.topic());
      }
      return new AirbyteConnectionStatus().withStatus(Status.SUCCEEDED);
    } catch (Exception e) {
      LOGGER.error("Exception attempting to connect to the Kafka brokers: ", e);
      return new AirbyteConnectionStatus()
          .withStatus(Status.FAILED)
          .withMessage("Could not connect to the Kafka brokers with provided configuration. \n" + e.getMessage());
    }
  }

  @Override
  public AirbyteMessageConsumer getConsumer(JsonNode config,
                                            ConfiguredAirbyteCatalog catalog,
                                            Consumer<AirbyteMessage> outputRecordCollector) {
    return new KafkaRecordConsumer(KafkaDestinationConfig.getKafkaDestinationConfig(config),
        catalog,
        outputRecordCollector,
        namingResolver);
  }

  public static void main(String[] args) throws Exception {
    final Destination destination = new KafkaDestination();
    LOGGER.info("Starting destination: {}", KafkaDestination.class);
    new IntegrationRunner(destination).run(args);
    LOGGER.info("Completed destination: {}", KafkaDestination.class);
  }

}
