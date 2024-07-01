#!/bin/bash

set -x
# We only care about the last command's result (i.e grep)
set -o pipefail

container_name=$1

while true; do
    # Print first command in case it fails and we need to figure out why
    docker exec $container_name asinfo -v status | tee >(echo) | grep -qE "^ok"
    grep_return_code=$?
    if [[ $grep_return_code -eq 0 ]]; then
        # Server is ready when asinfo returns ok
        break
    fi
done
