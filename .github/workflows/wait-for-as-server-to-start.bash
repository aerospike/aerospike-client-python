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

    # An unset variable will have a default empty value
    docker exec $container_name asinfo $user_credentials -v status | grep -qE "^ok"
    if [[ $? -eq 0 ]]; then
        # Server is ready when asinfo returns ok
        break
    fi
done
