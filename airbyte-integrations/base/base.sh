#!/usr/bin/env bash

set -e

function echo2() {
  echo >&2 "$@"
}

function error() {
  echo2 "$@"
  exit 1
}

# todo: make it easy to select source or destination and validate based on selection
function main() {
  CMD="$1"
  shift 1

  ARGS=
  while [ $# -ne 0 ]; do
    case "$1" in
    --config)
      CONFIG_FILE="$2"
      shift 2
      ;;
    --catalog)
      CATALOG_FILE="$2"
      shift 2
      ;;
    --state)
      STATE_FILE="$2"
      shift 2
      ;;
    *)
      error "Unknown option: $1"
      ;;
    esac
  done

  case "$CMD" in
  spec)
    eval "$AIRBYTE_SPEC_CMD"
    ;;
  check)
    eval "$AIRBYTE_CHECK_CMD" --config "$CONFIG_FILE"
    ;;
  discover)
    eval "$AIRBYTE_DISCOVER_CMD" --config "$CONFIG_FILE"
    ;;
  read)
    # todo: state should be optional: --state "$STATE_FILE"
    eval "$AIRBYTE_READ_CMD" --config "$CONFIG_FILE" --catalog "$CATALOG_FILE"
    ;;
  *)
    error "Unknown command: $CMD"
    ;;
  esac
}

main "$@"
