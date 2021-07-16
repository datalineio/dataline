# Kafka

## Overview

The Airbyte Kafka destination allows you to sync data to Kafka. Each stream is written to the corresponding Kafka topic.

### Sync overview

#### Output schema

Each stream will be output into a Kafka topic.

Currently, this connector only writes data with JSON format. More formats (e.g. Apache Avro) will be supported in the future.

Each record will contain in its key the uuid assigned by Airbyte, and in the value these 3 fields:

* `_airbyte_ab_id`: a uuid assigned by Airbyte to each event that is processed.
* `_airbyte_emitted_at`:  a timestamp representing when the event was pulled from the data source.
* `_airbyte_data`: a json blob representing with the event data.

#### Features

| Feature | Supported?\(Yes/No\) | Notes |
| :--- | :--- | :--- |
| Full Refresh Sync | No |  |
| Incremental - Append Sync | Yes |  |
| Namespaces | Yes |  |

## Getting started

### Requirements

To use the Kafka destination, you'll need:

* A Kafka cluster 1.0 or above.

### Setup guide

#### Network Access

Make sure your Kafka brokers can be accessed by Airbyte.

#### **Permissions**

Airbyte should be allowed to write messages into topics, and these topics should be created before writing into Kafka
or, at least, enable the configuration in the brokers ``auto.create.topics.enable`` (which is not recommended for
production environments).

#### Target topics

Each message will be written to its corresponding topic defined in the property ``topic_pattern``. In case you
define in this pattern values such as ``{namespace}`` and/or ``{stream}``, each message can be written to different
topics based on their values inferred when producing the records.

### Setup the Kafka destination in Airbyte

You should now have all the requirements needed to configure Kafka as a destination in the UI. You'll need the following information to configure the Kafka destination:

* **Bootstrap servers**
* **Topic pattern**
* **Test topic**
* **Sync producer**
* **Security protocol**
* **SASL JAAS config**
* **SASL mechanism**
* **Client ID**
* **ACKs**
* **Transactional ID**
* **Transaction timeout ms**
* **Enable idempotence**
* **Compression type**
* **Batch size**
* **Linger ms**
* **Max in flight requests per connection**
* **Client DNS lookup**
* **Buffer memory**
* **Max request size**
* **Retries**
* **Socket connection setup timeout**
* **Socket connection setup max timeout**
* **Delivery timeout**
* **Send buffer bytes**
* **Receive buffer bytes**

More info about this can be found in the [Kafka producer configs documentation site](https://kafka.apache.org/documentation/#producerconfigs).

*NOTE*: Some configurations for SSL are not available yet.
