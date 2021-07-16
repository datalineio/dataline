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
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonSerializer;

public class KafkaDestinationConfig {

  private final String topicPattern;
  private final boolean sync;
  private final KafkaProducer<String, JsonNode> producer;
  private final boolean transactionalProducer;

  private KafkaDestinationConfig(String topicPattern, boolean sync, JsonNode config) {
    this.topicPattern = topicPattern;
    this.sync = sync;
    this.producer = buildKafkaProducer(config);
    this.transactionalProducer = isTransactionalProducer(config);
  }

  public static KafkaDestinationConfig getKafkaDestinationConfig(JsonNode config) {
    return new KafkaDestinationConfig(
        config.get("topic_pattern").asText(),
        config.has("sync_producer") && config.get("sync_producer").booleanValue(),
        config);
  }

  private KafkaProducer<String, JsonNode> buildKafkaProducer(JsonNode config) {
    final Map<String, Object> props = ImmutableMap.<String, Object>builder()
        .put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.get("bootstrap_servers").asText())
        .put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, config.get("security_protocol").asText())
        .put(SaslConfigs.SASL_JAAS_CONFIG,
            config.has("sasl_jaas_config") ? config.get("sasl_jaas_config").asText() : null)
        .put(SaslConfigs.SASL_MECHANISM,
            config.has("sasl_mechanism") ? config.get("sasl_mechanism").asText() : null)
        .put(ProducerConfig.CLIENT_ID_CONFIG,
            config.has("client_id") ? config.get("client_id").asText() : null)
        .put(ProducerConfig.ACKS_CONFIG, config.get("acks").asText())
        .put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,
            config.has("transactional_id") ? config.get("transactional_id").asText() : null)
        .put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, config.get("transaction_timeout_ms").intValue())
        .put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, config.get("enable_idempotence").booleanValue())
        .put(ProducerConfig.COMPRESSION_TYPE_CONFIG, config.get("compression_type").asText())
        .put(ProducerConfig.BATCH_SIZE_CONFIG, config.get("batch_size").intValue())
        .put(ProducerConfig.LINGER_MS_CONFIG, config.get("linger_ms").intValue())
        .put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
            config.get("max_in_flight_requests_per_connection").intValue())
        .put(ProducerConfig.CLIENT_DNS_LOOKUP_CONFIG, config.get("client_dns_lookup").asText())
        .put(ProducerConfig.BUFFER_MEMORY_CONFIG, config.get("buffer_memory").intValue())
        .put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, config.get("max_request_size").intValue())
        .put(ProducerConfig.RETRIES_CONFIG, config.get("retries").intValue())
        .put(ProducerConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG,
            config.get("socket_connection_setup_timeout_ms").intValue())
        .put(ProducerConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG,
            config.get("socket_connection_setup_timeout_max_ms").intValue())
        .put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, config.get("delivery_timeout_ms").intValue())
        .put(ProducerConfig.SEND_BUFFER_CONFIG, config.get("send_buffer_bytes").intValue())
        .put(ProducerConfig.RECEIVE_BUFFER_CONFIG, config.get("receive_buffer_bytes").intValue())
        .put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
        .put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName())
        .build();

    final Map<String, Object> filteredProps = props.entrySet().stream()
        .filter(entry -> entry.getValue() != null && !entry.getValue().toString().isBlank())
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    return new KafkaProducer<>(filteredProps);
  }

  private boolean isTransactionalProducer(JsonNode config) {
    String transactionalId = config.has("transactional_id") ? config.get("transactional_id").asText() : null;
    return config.get("enable_idempotence").booleanValue() &&
        transactionalId != null &&
        !transactionalId.isBlank();
  }

  public String getTopicPattern() {
    return topicPattern;
  }

  public boolean isSync() {
    return sync;
  }

  public KafkaProducer<String, JsonNode> getProducer() {
    return producer;
  }

  public boolean isTransactionalProducer() {
    return transactionalProducer;
  }

}
