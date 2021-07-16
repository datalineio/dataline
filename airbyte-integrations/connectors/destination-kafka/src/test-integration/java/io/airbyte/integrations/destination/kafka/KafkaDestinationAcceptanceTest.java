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
import io.airbyte.integrations.base.JavaBaseConstants;
import io.airbyte.integrations.destination.NamingConventionTransformer;
import io.airbyte.integrations.destination.StandardNameTransformer;
import io.airbyte.integrations.standardtest.destination.DestinationAcceptanceTest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

public class KafkaDestinationAcceptanceTest extends DestinationAcceptanceTest {

  private static final String TOPIC_NAME = "test.topic";

  private KafkaContainer kafka;
  private final NamingConventionTransformer namingResolver = new StandardNameTransformer();

  @Override
  protected String getImageName() {
    return "airbyte/destination-kafka:dev";
  }

  @Override
  protected JsonNode getConfig() {
    return Jsons.jsonNode(ImmutableMap.builder()
        .put("bootstrap_servers", kafka.getBootstrapServers())
        .put("topic_pattern", "{namespace}.{stream}." + TOPIC_NAME)
        .put("sync", true)
        .put("security_protocol", "PLAINTEXT")
        .put("sasl_jaas_config", "")
        .put("sasl_mechanism", "PLAIN")
        .put("client_id", "test-client")
        .put("acks", "all")
        .put("transactional_id", "txn-id")
        .put("transaction_timeout_ms", 60000)
        .put("enable_idempotence", true)
        .put("compression_type", "none")
        .put("batch_size", 16384)
        .put("linger_ms", 0)
        .put("max_in_flight_requests_per_connection", 5)
        .put("client_dns_lookup", "use_all_dns_ips")
        .put("buffer_memory", 33554432)
        .put("max_request_size", 1048576)
        .put("retries", 2147483647)
        .put("socket_connection_setup_timeout_ms", 10000)
        .put("socket_connection_setup_timeout_max_ms", 30000)
        .put("delivery_timeout_ms", 120000)
        .put("send_buffer_bytes", -1)
        .put("receive_buffer_bytes", -1)
        .build());
  }

  @Override
  protected JsonNode getFailCheckConfig() {
    return Jsons.jsonNode(ImmutableMap.builder()
        .put("bootstrap_servers", kafka.getBootstrapServers())
        .put("topic_pattern", "{namespace}.{stream}." + TOPIC_NAME)
        .put("test_topic", "check-topic")
        .put("security_protocol", "SASL_PLAINTEXT")
        .put("sasl_jaas_config", "invalid")
        .put("sasl_mechanism", "PLAIN")
        .put("client_id", "test-client")
        .put("acks", "all")
        .put("transactional_id", "txn-id")
        .put("transaction_timeout_ms", 60000)
        .put("enable_idempotence", true)
        .put("compression_type", "none")
        .put("batch_size", 16384)
        .put("linger_ms", 0)
        .put("max_in_flight_requests_per_connection", 5)
        .put("client_dns_lookup", "use_all_dns_ips")
        .put("buffer_memory", 33554432)
        .put("max_request_size", 1048576)
        .put("retries", 2147483647)
        .put("socket_connection_setup_timeout_ms", 10000)
        .put("socket_connection_setup_timeout_max_ms", 30000)
        .put("delivery_timeout_ms", 120000)
        .put("send_buffer_bytes", -1)
        .put("receive_buffer_bytes", -1)
        .build());
  }

  @Override
  protected boolean implementsNamespaces() {
    return true;
  }

  @Override
  protected String getDefaultSchema(JsonNode config) {
    return "";
  }

  @Override
  protected List<JsonNode> retrieveNormalizedRecords(TestDestinationEnv testEnv, String streamName, String namespace) {
    return retrieveRecords(testEnv, streamName, namespace);
  }

  @Override
  protected List<JsonNode> retrieveRecords(TestDestinationEnv testEnv,
                                           String streamName,
                                           String namespace) {
    final Map<String, Object> props = ImmutableMap.<String, Object>builder()
        .put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers())
        .put(ConsumerConfig.GROUP_ID_CONFIG, namingResolver.getIdentifier(namespace + "-" + streamName))
        .put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        .put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
        .put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName())
        .build();
    final KafkaConsumer<String, JsonNode> consumer = new KafkaConsumer<>(props);
    final List<JsonNode> records = new ArrayList<>();
    final String topic = namingResolver.getIdentifier(namespace + "." + streamName + "." + TOPIC_NAME);

    consumer.subscribe(Collections.singletonList(topic));
    consumer.poll(Duration.ofMillis(20000L)).iterator()
        .forEachRemaining(record -> records.add(record.value().get(JavaBaseConstants.COLUMN_NAME_DATA)));
    consumer.close();

    return records;
  }

  @Override
  protected void setup(TestDestinationEnv testEnv) {
    kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.1.1"));
    kafka.start();
    try (var ignored = AdminClient.create(Map.of(
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {}
  }

  @Override
  protected void tearDown(TestDestinationEnv testEnv) {
    kafka.close();
  }

}
