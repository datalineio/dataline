#!/usr/bin/env bash

set -e

. tools/lib/lib.sh

assert_root

# Since KIND does not have access to the local docker agent, manually load the minimum images required for the Kubernetes Acceptance Tests.
# See https://kind.sigs.k8s.io/docs/user/quick-start/#loading-an-image-into-your-cluster.
echo "Loading images into KIND..."
kind load docker-image airbyte/server:dev --name chart-testing
kind load docker-image airbyte/scheduler:dev --name chart-testing
kind load docker-image airbyte/webapp:dev --name chart-testing
kind load docker-image airbyte/seed:dev --name chart-testing
kind load docker-image airbyte/db:dev --name chart-testing

echo "Starting app..."

echo "Applying dev manifests to kubernetes..."
kubectl apply -k kube/overlays/dev

kubectl wait --for=condition=Available deployment/airbyte-server --timeout=300s || (kubectl describe pods && exit 1)
kubectl wait --for=condition=Available deployment/airbyte-scheduler --timeout=300s || (kubectl describe pods && exit 1)

# allocates a lot of time to start kube. takes a while for postgres+temporal to work things out
sleep 120s

server_logs () { echo "server logs:" && kubectl logs deployment.apps/airbyte-server; }
scheduler_logs () { echo "scheduler logs:" && kubectl logs deployment.apps/airbyte-scheduler; }
describe_pods () { echo "describe pods:" && kubectl describe pods; }
print_all_logs () { server_logs; scheduler_logs; describe_pods; }

trap "echo 'kube logs:' && print_all_logs" EXIT

kubectl port-forward svc/airbyte-server-svc 8001:8001 &

echo "Running worker integration tests..."
./gradlew --no-daemon :airbyte-workers:integrationTest --scan

echo "Running e2e tests via gradle..."
CORE_ONLY=true KUBE=true ./gradlew --no-daemon :airbyte-tests:acceptanceTests --scan
