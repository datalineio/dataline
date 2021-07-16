locals {
  svc_account_creds = file("secrets/svc_account_creds.json")
}

terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "3.5.0"
    }
  }
}

provider "google" {
  credentials = local.svc_account_creds

  project = "dataline-integration-testing"
  region  = "us-central1"
}

module "source-mysql" {
  source = "mysql"
  region = var.region
}

