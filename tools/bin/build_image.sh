#!/usr/bin/env bash

set -e

ROOT_DIR="$1"
PROJECT_DIR="$2"
DOCKERFILE="$3"
TAGGED_IMAGE="$4"
ID_FILE="$5"
FOLLOW_SYMLINKS="$6"

cd "$ROOT_DIR"
. tools/lib/lib.sh
assert_root

cd "$PROJECT_DIR"

function validate_dockerignore() {
  excludes_all=$(grep -w '^\*$' .dockerignore)
  excludes_except=$(grep -w '^!.*' .dockerignore)
  if [ -n "$excludes_all" ] || [ -n "$excludes_except" ]; then
    error "Cannot include exclusion exceptions when following symlinks. Please use an exclude pattern that doesn't use exclude-all (e.g: *) or exclude-except (e.g: !/some/pattern)"
  fi
}

args=(
    -f "$DOCKERFILE"
    -t "$TAGGED_IMAGE"
    --iidfile "$ID_FILE"
)

if [ "$FOLLOW_SYMLINKS" == "true" ]; then
  exclusions=()
  if [ -f ".dockerignore" ]; then
    validate_dockerignore
    exclusions+=(--exclude-from .dockerignore)
  fi
  # Docker does not follow symlinks in the build context. So we create a tar of the directory, following symlinks, and provide the archive to Docker
  # to use as the build context
  tar cL "${exclusions[@]}" . | docker build - "${args[@]}"
else
  docker build . "${args[@]}" --platform linux/amd64
fi
