#!/usr/bin/env sh
docker run --rm -it \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -v /tmp:/tmp \
    -v $(pwd):/test_input \
    airbyte/source-acceptance-test:dev \
    --acceptance-test-config /test_input
