#!/bin/bash

set -e
set -x

abs_path_for_this_file=$(realpath "${BASH_SOURCE[0]}")
dir_containing_this_file=$(dirname "$abs_path_for_this_file")

helper_script=_build-wheel-natively.bash

if [[ "$BUILD_MANYLINUX_WHEEL" == "false" ]]; then
    # Build wheel natively
    "$dir_containing_this_file/${helper_script}"
    exit 0
fi

if [[ -z "$MANYLINUX_REGISTRY_NAME" || -z "$MANYLINUX_IMAGE_DIGEST" ]]; then
    echo "MANYLINUX_REGISTRY_NAME and MANYLINUX_IMAGE_DIGEST env var must be defined."
    exit 1
fi


arch=$(uname -m)
MANYLINUX_IMAGE_NAME="$MANYLINUX_REGISTRY_NAME/manylinux_2_28_${arch}@sha256:$MANYLINUX_IMAGE_DIGEST"

# Use UUID to minimize chance of colliding with another container
CONTAINER_NAME=manylinux-$(uuidgen)

MOUNTED_REPO_DIR=/repo
jf docker login artifact.aerospike.io/database-docker-dev-local
docker run -d -v "${dir_containing_this_file}:${MOUNTED_REPO_DIR}/" --rm --name "$CONTAINER_NAME" "$MANYLINUX_IMAGE_NAME" tail -f /dev/null

cleanup() {
    if [[ -z $KEEP_CONTAINER ]]; then
        docker stop "$CONTAINER_NAME"
    fi
}
trap 'cleanup' EXIT SIGINT SIGTERM ERR

# Addresses a git error where the user owning the repo folder is different from the container user
docker exec "$CONTAINER_NAME" git config --global --add safe.directory "$MOUNTED_REPO_DIR"

active_python_minor_version=$(python3 --version | cut -f 2- -d' ' | cut -f -2 -d'.')

docker exec -w "$MOUNTED_REPO_DIR" "$CONTAINER_NAME" ./${helper_script} "$active_python_minor_version"
