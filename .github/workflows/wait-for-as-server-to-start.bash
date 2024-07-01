#!/bin/bash

set -x

container_name=$1

while true; do
    docker exec $container_name asinfo -v status | grep -E "^ok"
    docker_return_code=${PIPESTATUS[0]}
    if [[ $docker_return_code -ne 0 ]]; then
        exit 1
    fi
    grep_return_code=${PIPESTATUS[1]}
    if [[ $grep_return_code -eq 0 ]]; then
        # Server is ready when asinfo returns ok
        break
    fi
done
