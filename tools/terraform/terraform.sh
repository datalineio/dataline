#!/usr/bin/env bash
# This file runs the Terraform CLI from a container to ensure repeatable results

set -e

ENV=
TF_DIR=

IMG=hashicorp/terraform:1.0.2

function _usage() {
    echo "
USAGE: $0 [terraform_workspace_path] [TERRAFORM COMMAND]

Example:
$0 gcp/connectors/ plan
    "
}

function _error() {
  echo "$@"
  _usage
  exit 1
}

function _terraform() {
  docker run --rm -it \
    -v $TF_DIR:/terraform/root \
    -w /terraform/root \
   "$IMG" "$@"
}

function main() {
    [ -f .root ] || _error "Must run from the root of the project"

    TF_DIR="terraform/$1"; [ -d "$TF_DIR" ] || _error "Missing TF dir"; shift
    TF_DIR=$(cd "$TF_DIR" && pwd)

    echo "Running on $TF_DIR"

    [ -z "$TF_DISABLE_INIT" ] && _terraform init

    _terraform fmt -recursive
    _terraform "$@"
}

main "$@"
