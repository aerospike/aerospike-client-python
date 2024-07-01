#!/bin/bash

set -x

container_name=$1

while true; do
    docker exec $container_name asinfo -v status | grep -qE "^ok"
    return_code=$?
    if [[ $return_code -eq 0 ]]; then
        # Server is ready when asinfo returns ok
        break
    fi
done
