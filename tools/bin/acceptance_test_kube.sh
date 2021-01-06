#!/usr/bin/env bash

set -e

. tools/lib/lib.sh

assert_root

trap 'trap - SIGTERM && kill 0' SIGINT SIGTERM EXIT

echo "Starting app..."

echo "Applying dev manifests to kubernetes..."
kubectl apply -k kube/overlays/dev

kubectl wait --for=condition=Available deployment/airbyte-server --timeout=300s || (kubectl describe pods && exit 1)
kubectl wait --for=condition=Available deployment/airbyte-scheduler --timeout=300s || (kubectl describe pods && exit 1)

kubectl port-forward svc/airbyte-server-svc 8001:8001 &

echo "Running e2e tests via gradle..."
./gradlew --no-daemon :airbyte-tests:acceptanceTests --scan --tests "*AcceptanceTestsKube"
