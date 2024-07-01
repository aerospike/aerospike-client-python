#!/bin/bash

set -x
set -o pipefail

container_name=$1
is_security_enabled=$2

while true; do
    if [[ $is_security_enabled == true ]]; then
        $instance_argument = "--instance security"
    fi

    docker exec $container_name asinfo $instance_argument -v status | grep -qE "^ok"
    grep_return_code=$?
    if [[ $grep_return_code -eq 0 ]]; then
        # Server is ready when asinfo returns ok
        break
    fi
done
