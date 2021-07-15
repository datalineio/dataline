# Javascript Template Source 

This is the repository for the JavaScript Template source connector, written in JavaScript.
For information about how to use this connector within Airbyte, see [the documentation](https://docs.airbyte.io/integrations/sources/javascript-template).

## Local development

### Prerequisites
**To iterate on this connector, make sure to complete this prerequisites section.**

#### Build & Activate Virtual Environment
First, build the module by running the following from the `airbyte` project root directory: 
```
./gradlew :airbyte-integrations:connectors:source-javascript-template:build
```

This will generate a virtualenv for this module in `source-javascript-template/.venv`. Make sure this venv is active in your
development environment of choice. To activate the venv from the terminal, run:
```
cd airbyte-integrations/connectors/source-javascript-template # cd into the connector directory
source .venv/bin/activate
```
If you are in an IDE, follow your IDE's instructions to activate the virtualenv.

#### Create credentials
**If you are a community contributor**, follow the instructions in the [documentation](https://docs.airbyte.io/integrations/sources/javascript-template)
to generate the necessary credentials. Then create a file `secrets/config.json` conforming to the `source_javascript_template/spec.json` file.
See `sample_files/sample_config.json` for a sample config file.

**If you are an Airbyte core member**, copy the credentials in RPass under the secret name `source-javascript-template-integration-test-config`
and place them into `secrets/config.json`.


### Locally running the connector
```
npm install
node source.js spec
node source.js check --config secrets/config.json
node source.js discover --config secrets/config.json
node source.js read --config secrets/config.json --catalog sample_files/configured_catalog.json
```

### Unit Tests (wip)
To run unit tests locally, from the connector directory run:
```
npm test
```

### Locally running the connector docker image
```
# in airbyte root directory
./gradlew :airbyte-integrations:connectors:source-javascript-template:airbyteDocker
docker run --rm airbyte/source-javascript-template:dev spec
docker run --rm -v $(pwd)/airbyte-integrations/connectors/source-javascript-template/secrets:/secrets airbyte/source-javascript-template:dev check --config /secrets/config.json
docker run --rm -v $(pwd)/airbyte-integrations/connectors/source-javascript-template/secrets:/secrets airbyte/source-javascript-template:dev discover --config /secrets/config.json
docker run --rm -v $(pwd)/airbyte-integrations/connectors/source-javascript-template/secrets:/secrets -v $(pwd)/airbyte-integrations/connectors/source-javascript-template/sample_files:/sample_files airbyte/source-javascript-template:dev read --config /secrets/config.json --catalog /sample_files/configured_catalog.json
```

### Integration Tests
1. From the airbyte project root, run `./gradlew :airbyte-integrations:connectors:source-javascript-template:integrationTest` to run the standard integration test suite.
1. To run additional integration tests, place your integration tests in a new directory `integration_tests` and run them with `node test (wip)`.

## Dependency Management
All of your dependencies should go in `package.json`.
