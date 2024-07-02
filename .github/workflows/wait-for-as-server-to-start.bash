#!/bin/bash

set -x
# Makes sure that docker exec command does not silently fail
set -o pipefail

container_name=$1
is_security_enabled=$2

while true; do
    if [[ $is_security_enabled == true ]]; then
        # We need to pass credentials to asinfo if server requires it
        # TODO: passing in hardcoded credentials since I can't figure out how to use --instance with global astools.conf
        user_credentials="--user=admin --password=admin"
    fi

    # An unset variable will have a default empty value
    # Intermediate step is to print docker exec command's output in case it fails
    # Sometimes, errors only appear in stdout and not stderr (like an asinfo -v "invalid_command")
    # But piping and passing stdin to grep will hide stdout. grep doesn't have a way to print all lines passed as input
    # ack does have an option but it doesn't come installed by default
    docker exec $container_name asinfo $user_credentials -v status | tee >(cat) | grep -qE "^ok"
    if [[ $? -eq 0 ]]; then
        # Server is ready when asinfo returns ok
        break
    fi
done
