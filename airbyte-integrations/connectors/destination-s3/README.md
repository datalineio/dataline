# S3 Test Configuration

In order to test the S3 destination, you need an AWS account (or alternative S3 account).

## Community Contributor

As a community contributor, you will need access to AWS to run the integration tests.

- Create an S3 bucket for testing.
- Get your `access_key_id` and `secret_access_key` that can read and write to the above bucket.
- Paste the bucket and key information into the config files under [`./sample_secrets`](./sample_secrets).
- Rename the directory from `sample_secrets` to `secrets`.
- Feel free to modify the config files with different settings in the acceptance test file (e.g. `S3CsvDestinationAcceptanceTest.java`, method `getFormatConfig`), as long as they follow the schema defined in [spec.json](src/main/resources/spec.json).

## Airbyte Employee

- Access the `destination s3 creds` secrets on Last Pass, and put it in `sample_secrets/config.json`.
- Rename the directory from `sample_secrets` to `secrets`.

## Add New Output Format
- Add a new enum in `S3Format`.
- Modify `spec.json` to specify the configuration of this new format.
- Update `S3FormatConfigs` to be able to construct a config for this new format.
- Create a new package under `io.airbyte.integrations.destination.s3`.
- Implement a new `S3Writer`. The implementation can extend `BaseS3Writer`.
- Write an acceptance test for the new output format. The test can extend `S3DestinationAcceptanceTest`.
