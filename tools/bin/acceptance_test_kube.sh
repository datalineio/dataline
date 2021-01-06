#!/usr/bin/env bash

set -e

. tools/lib/lib.sh

assert_root

trap 'trap - SIGTERM && kill 0 && kubectl logs svc/airbyte-server-svc' SIGINT SIGTERM EXIT

echo "Starting app..."

echo "Applying dev manifests to kubernetes..."
kubectl apply -k kube/overlays/dev

sleep 2m


echo "Waiting for server to be ready..."
echo "============"
echo "============"
echo "============"
echo "============"
kubectl wait --for=condition=Available deployment/airbyte-server --timeout=200s
kubectl get pods
kubectl describe pods
echo "============"
echo "============"
echo "============"
echo "============"
kubectl wait --for=condition=Available deployment/airbyte-scheduler --timeout=200s

kubectl get pods
kubectl describe pods
echo "============"
echo "============"
echo "============"
echo "============"

kubectl port-forward svc/airbyte-server-svc 8001:8001 &

echo "Running e2e tests via gradle..."
./gradlew --no-daemon :airbyte-tests:acceptanceTests --scan --tests "*AcceptanceTestsKube"
