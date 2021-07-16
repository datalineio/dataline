# Destination Google Cloud Storage (GCS)

In order to test the D3 destination, you need an Google Cloud Platform account.

## Community Contributor

As a community contributor, you can follow these steps to run integration tests.

- Create an GCS bucket for testing.
- Generate a [HMAC key](https://cloud.google.com/storage/docs/authentication/hmackeys) for the bucket with reading and writing permissions. Please note that currently only the HMAC key credential is supported. More credential types will be added in the future.
- Paste the bucket and key information into the config files under [`./sample_secrets`](./sample_secrets).
- Rename the directory from `sample_secrets` to `secrets`.
- Feel free to modify the config files with different settings in the acceptance test file (e.g. `GcsCsvDestinationAcceptanceTest.java`, method `getFormatConfig`), as long as they follow the schema defined in [spec.json](src/main/resources/spec.json).

## Airbyte Employee

- Access the `destination s3 creds` secrets on Last Pass, and put it in `sample_secrets/config.json`.
- Rename the directory from `sample_secrets` to `secrets`.

## Add New Output Format
- Add a new enum in `S3Format`.
- Modify `spec.json` to specify the configuration of this new format.
- Update `S3FormatConfigs` to be able to construct a config for this new format.
- Create a new package under `io.airbyte.integrations.destination.gcs`.
- Implement a new `GcsWriter`. The implementation can extend `BaseGcsWriter`.
- Write an acceptance test for the new output format. The test can extend `GcsDestinationAcceptanceTest`.
