#!/usr/bin/env bash

set -e

. tools/lib/lib.sh

assert_root

echo "Starting app..."
#mkdir /tmp/airbyte_local
#echo "Made the dir..."
echo "docker --version"
echo $(docker --version)
echo "docker-compose --version"
echo $(docker-compose --version)
#echo "ls /tmp"
#ls /tmp
# Detach so we can run subsequent commands
VERSION=dev docker-compose up -d
trap "echo 'docker-compose logs:' && docker-compose logs -t --tail 150 && docker-compose down" EXIT

echo "Waiting for services to begin"
sleep 10 # TODO need a better way to wait

echo "Running e2e tests via gradle"
./gradlew --no-daemon :airbyte-tests:acceptanceTests
