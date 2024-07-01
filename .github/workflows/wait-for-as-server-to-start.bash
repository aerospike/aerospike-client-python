#!/bin/bash

set -x
set -o pipefail

container_name=$1

while true; do
    docker exec $container_name asinfo --user=admin --password=admin -v status
    docker exec $container_name asinfo --user=admin --password=admin -v status | grep -qE "^ok"
    grep_return_code=$?
    if [[ $grep_return_code -eq 0 ]]; then
        # Server is ready when asinfo returns ok
        break
    fi
done
