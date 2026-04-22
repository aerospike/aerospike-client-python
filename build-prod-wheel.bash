#!/bin/bash

set -e
set -x

KEEP_CONTAINER=$1

this_file_abs_path=$(realpath "${BASH_SOURCE[0]}")
dir_containing_this_file=$(dirname "$this_file_abs_path")

helper_script=_build-prod-wheel-natively.bash

os=$(uname -s)

if [[ ! "$os" =~ Linux* ]]; then
    # Build wheel natively
    "$dir_containing_this_file/${helper_script}"
    exit 0
fi

# Use UUID to minimize chance of colliding with another container
CONTAINER_NAME=manylinux-$(uuidgen)

MOUNTED_REPO_DIR=/repo
docker run -d -v "${dir_containing_this_file}:${MOUNTED_REPO_DIR}/" --rm --name "$CONTAINER_NAME" "$MANYLINUX_IMAGE_NAME" tail -f /dev/null

cleanup() {
    if [[ -z $KEEP_CONTAINER ]]; then
        docker stop "$CONTAINER_NAME"
    fi
}
trap 'cleanup' EXIT SIGINT SIGTERM ERR

docker exec "$CONTAINER_NAME" git config --global --add safe.directory "$MOUNTED_REPO_DIR"
docker exec -w "$MOUNTED_REPO_DIR" "$CONTAINER_NAME" ./${helper_script} "$PYTHON_VERSION"
