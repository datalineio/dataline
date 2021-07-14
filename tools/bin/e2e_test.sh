#!/usr/bin/env bash

set -e

. tools/lib/lib.sh

assert_root

echo "Starting app..."

# todo (cgardens) - docker-compose 1.27.3 contained a bug that causes a failure if the volume path
#  does not exist when the volume is created. It was fixed in 1.27.4. Github actions virtual envs,
#  however, new ubuntu release upgraded to 1.27.3 on 09/24/20. Once github actions virtual envs
#  upgrades to 1.27.4, we can stop manually making the directory.
mkdir -p /tmp/airbyte_local

# Detach so we can run subsequent commands
VERSION=dev TRACKING_STRATEGY=logging docker-compose up -d
trap 'echo "docker-compose logs:" && docker-compose logs -t --tail 150 && docker-compose down && docker rm -f $(docker ps -q --filter name=airbyte_ci_pg)' EXIT

docker run -d -p 5433:5432 -e POSTGRES_PASSWORD=secret_password -e POSTGRES_DB=airbyte_ci --name airbyte_ci_pg postgres
echo "Waiting for services to begin"
sleep 30 # TODO need a better way to wait

echo "Running e2e tests via gradle"
CORE_ONLY=true ./gradlew --no-daemon :airbyte-e2e-testing:e2etest

