terraform {
  backend "gcs" {
    bucket      = "airbyte-connector-dev-infra-terraform"
    prefix      = "terraform/state"
    credentials = "secrets/svc_account_creds.json"
  }
}
