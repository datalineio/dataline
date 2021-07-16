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
| GCS Project ID | string | Project ID |
| GCS Bucket Name | string | Name of the bucket to sync data into. |
| GCS Bucket Path | string | Subdirectory under the above bucket to sync the data into. |
| GCS Region | string | See [here](https://cloud.google.com/storage/docs/locations) for all region codes. |
| Access Key ID | string | GCP HMAC credential. |
| Secret Access Key | string | GCP HMAC credential. |
| Format | object | Format specific configuration. See below for details. |

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
  * **GCP Project ID**
    * Fill in Project ID.
  * **GCS Bucket Name**
    * See [this](https://cloud.google.com/storage/docs/creating-buckets) to create an S3 bucket.
  * **GCS Bucket Region**
  * **Access Key Id**
    * See [this](https://cloud.google.com/storage/docs/authentication/hmackeys) on how to generate an access key.
    * We recommend creating an Airbyte-specific user. This user will require read and write permissions to objects in the staging bucket.
  * **Secret Access Key**
    * Corresponding key to the above key id.
* Make sure your GCS bucket is accessible from the machine running Airbyte.
  * This depends on your networking setup.
  * The easiest way to verify if Airbyte is able to connect to your GCS bucket is via the check connection tool in the UI.

## CHANGELOG

| Version | Date | Pull Request | Subject |
| :--- | :---  | :--- | :--- |
| 0.1.0 | 2021-06-24 | [#4329](https://github.com/airbytehq/airbyte/pull/4329) | Initial release with CSV Parquet outputs|
