# Source Acceptance Test Suite

Tests configurable via `acceptance-test-config.yml`. Each test has a number of inputs, 
you can provide multiple sets of inputs which will cause the same to run multiple times - one for each set of inputs.

Example of `acceptance-test-config.yml`:
```yaml
connector_image: string  # Docker image to test, for example 'airbyte/source-hubspot:0.1.0'
base_path: string  # Base path for all relative paths, optional, default - ./
tests:  # Tests configuration 
  spec: # list of the test inputs
  connection: # list of the test inputs
    - config_path: string  # set #1 of inputs
      status: string
    - config_path: string  # set #2 of inputs
      status: string
  # discovery:  # skip this test
  incremental: []  # skip this test as well
```
Ideally, the connector should pass all acceptance tests. If the test or its inputs omitted this test will be skipped. 
 
## Test Spec
Verify that a spec operation issued to the connector returns a valid spec.
| Input |  Type| Default | Note |
|--|--|--|--|
| `spec_path` | string | `secrets/spec.json` |Path to a JSON object representing the spec expected to be output by this connector |
| `timeout_seconds` | int | 10 |Test execution timeout in seconds|

## Test Connection
Verify that a check operation issued to the connector with the input config file returns a successful response.
| Input |  Type| Default | Note |
|--|--|--|--|
| `config_path` | string | `secrets/config.json` |Path to a JSON object representing a valid connector configuration|
| `status` | `succeed` `failed` `exception`| |Indicate if connection check should succeed with provided config|
| `timeout_seconds` | int | 30 |Test execution timeout in seconds|

## Test Discovery

Verifies when a discover operation is run on the connector using the given config file, a valid catalog is produced by the connector.
| Input |  Type| Default | Note |
|--|--|--|--|
| `config_path` | string | `secrets/config.json` |Path to a JSON object representing a valid connector configuration|
| `configured_catalog_path` | string| `integration_tests/configured_catalog.json` |Path to configured catalog|
| `timeout_seconds` | int | 30 |Test execution timeout in seconds|

## Test Basic Read

Configuring all streams in the input catalog to full refresh mode verifies that a read operation produces some RECORD messages.
| Input |  Type| Default | Note |
|--|--|--|--|
| `config_path` | string | `secrets/config.json` |Path to a JSON object representing a valid connector configuration|
| `configured_catalog_path` | string| `integration_tests/configured_catalog.json` |Path to configured catalog|
| `validate_output_from_all_streams` | boolean | False | Verify that **all** streams have records|
| `timeout_seconds` | int | 5*60 |Test execution timeout in seconds|
| `expect_records` | object |None| Compare produced records with expected records, see details below|
| `expect_records.path` | string | | File with expected records|
| `expect_records.extra_fields` | boolean | False | Allow output records to have other fields i.e: expected records are a subset |
| `expect_records.exact_order` | boolean | False | Ensure  that records produced in exact same order|
| `expect_records.extra_records` | boolean | True | Allow connector to produce extra records, but still enforce all records from the expected file to be produced|  

`expect_records` is a nested configuration, if omitted - the part of the test responsible for record matching will be skipped. Due to the fact that we can't identify records without primary keys, only the following flag combinations are supported:
| extra_fields | exact_order| extra_records |
|--|--|--|
|x|x||
||x|x|
||x||
|||x|
||||

### Example of `expected_records.txt`:
In general, the expected_records.json should contain the subset of output of the records of particular stream you need to test.
The required fields are: `stream, data, emitted_at`

```JSON
{"stream": "my_stream", "data": {"field_1": "value0", "field_2": "value0", "field_3": null, "field_4": {"is_true": true}, "field_5": 123}, "emitted_at": 1626172757000}
{"stream": "my_stream", "data": {"field_1": "value1", "field_2": "value1", "field_3": null, "field_4": {"is_true": false}, "field_5": 456}, "emitted_at": 1626172757000}
{"stream": "my_stream", "data": {"field_1": "value2", "field_2": "value2", "field_3": null, "field_4": {"is_true": true}, "field_5": 678}, "emitted_at": 1626172757000}
{"stream": "my_stream", "data": {"field_1": "value3", "field_2": "value3", "field_3": null, "field_4": {"is_true": false}, "field_5": 91011}, "emitted_at": 1626172757000}

```

## Test Full Refresh sync
### TestSequentialReads

This test performs two read operations on all streams which support full refresh syncs. It then verifies that the RECORD messages output from both were identical or the former is a strict subset of the latter.
| Input |  Type| Default | Note |
|--|--|--|--|
| `config_path` | string | `secrets/config.json` |Path to a JSON object representing a valid connector configuration|
| `configured_catalog_path` | string | `integration_tests/configured_catalog.json` |Path to configured catalog|
| `timeout_seconds` | int | 20*60 |Test execution timeout in seconds|

## Test Incremental sync
### TestTwoSequentialReads
This test verifies that all streams in the input catalog which support incremental sync can do so correctly. It does this by running two read operations: the first takes the configured catalog and config provided to this test as input. It then verifies that the sync produced a non-zero number of `RECORD` and `STATE` messages. The second read takes the same catalog and config used in the first test, plus the last `STATE` message output by the first read operation as the input state file. It verifies that either no records are produced \(since we read all records in the first sync\) or all records that produced have cursor value greater or equal to cursor value from `STATE` message. This test is performed only for streams that support incremental. Streams that do not support incremental sync are ignored. If no streams in the input catalog support incremental sync, this test is skipped.
| Input |  Type| Default | Note |
|--|--|--|--|
| `config_path` | string | `secrets/config.json` |Path to a JSON object representing a valid connector configuration|
| `configured_catalog_path` | string | `integration_tests/configured_catalog.json` |Path to configured catalog|
| `cursor_paths` | dict | {} | For each stream, the path of its cursor field in the output state messages. If omitted the path will be taken from the last piece of path from stream cursor_field.|
| `timeout_seconds` | int | 20*60 |Test execution timeout in seconds|

### TestStateWithAbnormallyLargeValues

This test verifies that sync produces no records when run with the STATE with abnormally large values
| Input |  Type| Default | Note |
|--|--|--|--|
| `config_path` | string | `secrets/config.json` |Path to a JSON object representing a valid connector configuration|
| `configured_catalog_path` | string | `integration_tests/configured_catalog.json` |Path to configured catalog|
| `future_state_path` | string | None |Path to the state file with abnormally large cursor values|
| `timeout_seconds` | int | 20*60 |Test execution timeout in seconds|
