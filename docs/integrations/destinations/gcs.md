# Google Cloud Storage

## Overview

This destination writes data to GCS bucket.

The Airbyte GCS destination allows you to sync data to cloud storage buckets. Each stream is written to its own directory under the bucket.

## Sync Mode

| Feature | Support | Notes |
| :--- | :---: | :--- |
| Full Refresh Sync | ✅ | Warning: this mode deletes all previously synced data in the configured bucket path. |
| Incremental - Append Sync | ✅ | |
| Namespaces | ❌ | Setting a specific bucket path is equivalent to having separate namespaces. |

## Configuration

| Parameter | Type | Notes |
| :--- | :---: | :--- |
| GCS Bucket Name | string | Name of the bucket to sync data into. |
| GCS Bucket Path | string | Subdirectory under the above bucket to sync the data into. |
| GCS Region | string | See [here](https://cloud.google.com/storage/docs/locations) for all region codes. |
| HMAC Key Access ID | string | HMAC key access ID . The access ID for the GCS bucket. When linked to a service account, this ID is 61 characters long; when linked to a user account, it is 24 characters long. See [HMAC key](https://cloud.google.com/storage/docs/authentication/hmackeys) for details. |
| HMAC Key Secret | string | The corresponding secret for the access ID. It is a 40-character base-64 encoded string. |
| Format | object | Format specific configuration. See below for details. |

Currently, only the [HMAC key](https://cloud.google.com/storage/docs/authentication/hmackeys) is supported. More credential types will be added in the future.

⚠️ Please note that under "Full Refresh Sync" mode, data in the configured bucket and path will be wiped out before each sync. We recommend you to provision a dedicated S3 resource for this sync to prevent unexpected data deletion from misconfiguration. ⚠️

The full path of the output data is:

```
<bucket-name>/<sorce-namespace-if-exists>/<stream-name>/<upload-date>-<upload-mills>-<partition-id>.<format-extension>
```

For example:

```
testing_bucket/data_output_path/public/users/2021_01_01_1609541171643_0.csv
↑              ↑                ↑      ↑     ↑          ↑             ↑ ↑
|              |                |      |     |          |             | format extension
|              |                |      |     |          |             partition id
|              |                |      |     |          upload time in millis
|              |                |      |     upload date in YYYY-MM-DD
|              |                |      stream name
|              |                source namespace (if it exists)
|              bucket path
bucket name
```

Please note that the stream name may contain a prefix, if it is configured on the connection.

The rationales behind this naming pattern are:
1. Each stream has its own directory.
2. The data output files can be sorted by upload time.
3. The upload time composes of a date part and millis part so that it is both readable and unique.

Currently, each data sync will only create one file per stream. In the future, the output file can be partitioned by size. Each partition is identifiable by the partition ID, which is always 0 for now.

## Output Schema

Each stream will be outputted to its dedicated directory according to the configuration. The complete datastore of each stream includes all the output files under that directory. You can think of the directory as equivalent of a Table in the database world.

- Under Full Refresh Sync mode, old output files will be purged before new files are created.
- Under Incremental - Append Sync mode, new output files will be added that only contain the new data.

### Avro

[Apache Avro](https://avro.apache.org/) serializes data in a compact binary format. Currently, the Airbyte S3 Avro connector always uses the [binary encoding](http://avro.apache.org/docs/current/spec.html#binary_encoding), and assumes that all data records follow the same schema.

#### Configuration

Here is the available compression codecs:

- No compression
- `deflate`
  - Compression level
    - Range `[0, 9]`. Default to 0.
    - Level 0: no compression & fastest.
    - Level 9: best compression & slowest.
- `bzip2`
- `xz`
  - Compression level
    - Range `[0, 9]`. Default to 6.
    - Level 0-3 are fast with medium compression.
    - Level 4-6 are fairly slow with high compression.
    - Level 7-9 are like level 6 but use bigger dictionaries and have higher memory requirements. Unless the uncompressed size of the file exceeds 8 MiB, 16 MiB, or 32 MiB, it is waste of memory to use the presets 7, 8, or 9, respectively.
- `zstandard`
  - Compression level
    - Range `[-5, 22]`. Default to 3.
    - Negative levels are 'fast' modes akin to `lz4` or `snappy`.
    - Levels above 9 are generally for archival purposes.
    - Levels above 18 use a lot of memory.
  - Include checksum
    - If set to `true`, a checksum will be included in each data block.
- `snappy`

#### Data schema

Under the hood, an Airbyte data stream in Json schema is converted to an Avro schema, and then the Json object is converted to an Avro record based on the Avro schema. Because the data stream can come from any data source, the Avro S3 destination connector has the following arbitrary rules.

1. Json schema types are mapped to Avro typea as follows:

| Json Data Type | Avro Data Type |
  | :---: | :---: |
| string | string |
| number | double |
| integer | int |
| boolean | boolean |
| null | null |
| object | record |
| array | array |

2. Built-in Json schema formats are not mapped to Avro logical types at this moment.
2. Combined restrictions ("allOf", "anyOf", and "oneOf") will be converted to type unions. The corresponding Avro schema can be less stringent. For example, the following Json schema

  ```json
  {
    "oneOf": [
      { "type": "string" },
      { "type": "integer" }
    ]
  }
  ```
will become this in Avro schema:

  ```json
  {
    "type": ["null", "string", "int"]
  }
  ```

2. Keyword `not` is not supported, as there is no equivalent validation mechanism in Avro schema.
3. Only alphanumeric characters and underscores (`/a-zA-Z0-9_/`) are allowed in a stream or field name. Any special character will be converted to an alphabet or underscore. For example, `spécial:character_names` will become `special_character_names`. The original names will be stored in the `doc` property in this format: `_airbyte_original_name:<original-name>`.
4. All field will be nullable. For example, a `string` Json field will be typed as `["null", "string"]` in Avro. This is necessary because the incoming data stream may have optional fields.
5. For array fields in Json schema, when the `items` property is an array, it means that each element in the array should follow its own schema sequentially. For example, the following specification means the first item in the array should be a string, and the second a number.

  ```json
  {
    "array_field": {
      "type": "array",
      "items": [
        { "type": "string" },
        { "type": "number" }
      ]
    }
  }
  ```

This is not supported in Avro schema. As a compromise, the converter creates a union, ["string", "number"], which is less stringent:

  ```json
  {
    "name": "array_field",
    "type": [
      "null",
      {
        "type": "array",
        "items": ["null", "string"]
      }
    ],
    "default": null
  }
  ```

6. Two Airbyte specific fields will be added to each Avro record:

| Field | Schema | Document |
  | :--- | :--- | :---: |
| `_airbyte_ab_id` | `uuid` | [link](http://avro.apache.org/docs/current/spec.html#UUID)
| `_airbyte_emitted_at` | `timestamp-millis` | [link](http://avro.apache.org/docs/current/spec.html#Timestamp+%28millisecond+precision%29) |

7. Currently `additionalProperties` is not supported. This means if the source is schemaless (e.g. Mongo), or has flexible fields, they will be ignored. We will have a solution soon. Feel free to submit a new issue if this is blocking for you.

For example, given the following Json schema:

```json
{
  "type": "object",
  "$schema": "http://json-schema.org/draft-07/schema#",
  "properties": {
    "id": {
      "type": "integer"
    },
    "user": {
      "type": ["null", "object"],
      "properties": {
        "id": {
          "type": "integer"
        },
        "field_with_spécial_character": {
          "type": "integer"
        }
      }
    },
    "created_at": {
      "type": ["null", "string"],
      "format": "date-time"
    }
  }
}
```

Its corresponding Avro schema will be:

```json
{
  "name" : "stream_name",
  "type" : "record",
  "fields" : [ {
    "name" : "_airbyte_ab_id",
    "type" : {
      "type" : "string",
      "logicalType" : "uuid"
    }
  }, {
    "name" : "_airbyte_emitted_at",
    "type" : {
      "type" : "long",
      "logicalType" : "timestamp-millis"
    }
  }, {
    "name" : "id",
    "type" : [ "null", "int" ],
    "default" : null
  }, {
    "name" : "user",
    "type" : [ "null", {
      "type" : "record",
      "name" : "user",
      "fields" : [ {
        "name" : "id",
        "type" : [ "null", "int" ],
        "default" : null
      }, {
        "name" : "field_with_special_character",
        "type" : [ "null", "int" ],
        "doc" : "_airbyte_original_name:field_with_spécial_character",
        "default" : null
      } ]
    } ],
    "default" : null
  }, {
    "name" : "created_at",
    "type" : [ "null", "string" ],
    "default" : null
  } ]
}
```

### CSV

Like most of the other Airbyte destination connectors, usually the output has three columns: a UUID, an emission timestamp, and the data blob. With the CSV output, it is possible to normalize (flatten) the data blob to multiple columns.

| Column | Condition | Description |
| :--- | :--- | :--- |
| `_airbyte_ab_id` | Always exists | A uuid assigned by Airbyte to each processed record. |
| `_airbyte_emitted_at` | Always exists. | A timestamp representing when the event was pulled from the data source. |
| `_airbyte_data` | When no normalization (flattening) is needed, all data reside under this column as a json blob. |
| root level fields | When root level normalization (flattening) is selected, the root level fields are expanded. |

For example, given the following json object from a source:

```json
{
  "user_id": 123,
  "name": {
    "first": "John",
    "last": "Doe"
  }
}
```

With no normalization, the output CSV is:

| `_airbyte_ab_id` | `_airbyte_emitted_at` | `_airbyte_data` |
| :--- | :--- | :--- |
| `26d73cde-7eb1-4e1e-b7db-a4c03b4cf206` | 1622135805000 | `{ "user_id": 123, name: { "first": "John", "last": "Doe" } }` |

With root level normalization, the output CSV is:

| `_airbyte_ab_id` | `_airbyte_emitted_at` | `user_id` | `name` |
| :--- | :--- | :--- | :--- |
| `26d73cde-7eb1-4e1e-b7db-a4c03b4cf206` | 1622135805000 | 123 | `{ "first": "John", "last": "Doe" }` |

### JSON Lines (JSONL)

[Json Lines](https://jsonlines.org/) is a text format with one JSON per line. Each line has a structure as follows:

```json
{
  "_airbyte_ab_id": "<uuid>",
  "_airbyte_emitted_at": "<timestamp-in-millis>",
  "_airbyte_data": "<json-data-from-source>"
}
```

For example, given the following two json objects from a source:

```json
[
  {
    "user_id": 123,
    "name": {
      "first": "John",
      "last": "Doe"
    }
  },
  {
    "user_id": 456,
    "name": {
      "first": "Jane",
      "last": "Roe"
    }
  }
]
```

They will be like this in the output file:

```jsonl
{ "_airbyte_ab_id": "26d73cde-7eb1-4e1e-b7db-a4c03b4cf206", "_airbyte_emitted_at": "1622135805000", "_airbyte_data": { "user_id": 123, "name": { "first": "John", "last": "Doe" } } }
{ "_airbyte_ab_id": "0a61de1b-9cdd-4455-a739-93572c9a5f20", "_airbyte_emitted_at": "1631948170000", "_airbyte_data": { "user_id": 456, "name": { "first": "Jane", "last": "Roe" } } }
```

### Parquet

#### Configuration

The following configuration is available to configure the Parquet output:

| Parameter | Type | Default | Description |
| :--- | :---: | :---: | :--- |
| `compression_codec` | enum | `UNCOMPRESSED` | **Compression algorithm**. Available candidates are: `UNCOMPRESSED`, `SNAPPY`, `GZIP`, `LZO`, `BROTLI`, `LZ4`, and `ZSTD`. |
| `block_size_mb` | integer | 128 (MB) | **Block size (row group size)** in MB. This is the size of a row group being buffered in memory. It limits the memory usage when writing. Larger values will improve the IO when reading, but consume more memory when writing. |
| `max_padding_size_mb` | integer | 8 (MB) | **Max padding size** in MB. This is the maximum size allowed as padding to align row groups. This is also the minimum size of a row group. |
| `page_size_kb` | integer | 1024 (KB) | **Page size** in KB. The page size is for compression. A block is composed of pages. A page is the smallest unit that must be read fully to access a single record. If this value is too small, the compression will deteriorate. |
| `dictionary_page_size_kb` | integer | 1024 (KB) | **Dictionary Page Size** in KB. There is one dictionary page per column per row group when dictionary encoding is used. The dictionary page size works like the page size but for dictionary. |
| `dictionary_encoding` | boolean | `true` | **Dictionary encoding**. This parameter controls whether dictionary encoding is turned on. |

These parameters are related to the `ParquetOutputFormat`. See the [Java doc](https://www.javadoc.io/doc/org.apache.parquet/parquet-hadoop/1.12.0/org/apache/parquet/hadoop/ParquetOutputFormat.html) for more details. Also see [Parquet documentation](https://parquet.apache.org/documentation/latest/#configurations) for their recommended configurations (512 - 1024 MB block size, 8 KB page size).

#### Data schema

Under the hood, an Airbyte data stream in Json schema is first converted to an Avro schema, then the Json object is converted to an Avro record, and finally the Avro record is outputted to the Parquet format. See the `Data schema` section from the [Avro output](#avro) for rules and limitations.

## Getting started

### Requirements

1. Allow connections from Airbyte server to your GCS cluster \(if they exist in separate VPCs\).
2. An GCP bucket with credentials \(for the COPY strategy\).

### Setup guide

* Fill up GCS info
  * **GCS Bucket Name**
    * See [this](https://cloud.google.com/storage/docs/creating-buckets) to create an S3 bucket.
  * **GCS Bucket Region**
  * **HMAC Key Access ID**
    * See [this](https://cloud.google.com/storage/docs/authentication/hmackeys) on how to generate an access key.
    * We recommend creating an Airbyte-specific user or service account. This user or account will require read and write permissions to objects in the bucket.
  * **Secret Access Key**
    * Corresponding key to the above access ID.
* Make sure your GCS bucket is accessible from the machine running Airbyte.
  * This depends on your networking setup.
  * The easiest way to verify if Airbyte is able to connect to your GCS bucket is via the check connection tool in the UI.

## CHANGELOG

| Version | Date | Pull Request | Subject |
| :--- | :---  | :--- | :--- |
| 0.1.0 | 2021-07-16 | [#4329](https://github.com/airbytehq/airbyte/pull/4784) | Initial release. |
