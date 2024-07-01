#!/bin/bash

set -x

container_name=$1

while true; do
    # Print first command in case it fails and we need to figure out why
    # Stderr doesn't get printed for some reason, so redirect it to stdout
    docker exec $container_name asinfo --user=superuser --password=superuser -v status
    docker exec $container_name asinfo --user=superuser --password=superuser -v status 2>&1 | tee >(echo) | grep -qE "^ok"
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
