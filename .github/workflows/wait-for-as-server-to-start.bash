#!/bin/bash

set -x
set -o pipefail

container_name=$1
is_security_enabled=$2

while true; do
    if [[ $is_security_enabled == true ]]; then
        # TODO: passing in hardcoded credentials since I can't figure out how to use --instance with global astools.conf
        user_credentials="--user=admin --password=admin"
    fi

    # We use asinfo since the Aerospike server Docker image doesn't support healthcheck
    docker exec $container_name asinfo $user_credentials -v status 2>&1 | tee --append /dev/tty | grep -qE "^ok"
    return_code=$?
    if [[ $return_code -eq 0 ]]; then
        # Server is ready when asinfo returns ok
        break
    fi
done
