#!/usr/bin/env bash

set -e

. tools/lib/lib.sh

GIT_REVISION=$(git rev-parse HEAD)
VER=0.26.1-jrhizor

./gradlew clean composeBuild --rerun-tasks
VERSION=$VER GIT_REVISION=$GIT_REVISION docker-compose -f docker-compose.build.yaml build
VERSION=$VER GIT_REVISION=$GIT_REVISION docker-compose -f docker-compose.build.yaml push
