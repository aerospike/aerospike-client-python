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
    # Sometimes, errors only appear in stdout and not stderr, like if asinfo throws an error because of no credentials
    # (This is a bug in asinfo since all error messages should be sent to stderr)
    # But piping and passing stdin to grep will hide the first command's stdout.
    # grep doesn't have a way to print all lines passed as input.
    # ack does have an option but it doesn't come installed by default
    # shellcheck disable=SC2086 # The flags in user credentials should be separate anyways. Not one string
    if docker exec "$container_name" asinfo $user_credentials -v status | tee >(cat) | grep -qE "^ok"; then
        # Server is ready when asinfo returns ok
        break
    fi
done
