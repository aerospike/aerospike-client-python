#!/bin/bash

set -e
set -x

KEEP_CONTAINER=$1
MANYLINUX_REGISTRY_NAME=$2

this_file_abs_path=$(realpath "${BASH_SOURCE[0]}")
dir_containing_this_file=$(dirname "$this_file_abs_path")

helper_script=_build-prod-wheel-natively.bash

os=$(uname -s)

if [[ ! "$os" =~ Linux* ]]; then
    # Build wheel natively
    "$dir_containing_this_file/${helper_script}"
    exit 0
fi

arch=$(uname -m)
if [[ "$arch" == "aarch64" ]]; then
    MANYLINUX_IMAGE_DIGEST="6d32fb959e76ed2b2117b28d141b3a92aed81805fe23e357a9b247ea30b88ae5"
else
    MANYLINUX_IMAGE_DIGEST="22071eca37ed46821078ac1a6d4e9eccbbd96baa8d07e199ca4df4a299f9c120"
fi

MANYLINUX_IMAGE_NAME="$MANYLINUX_REGISTRY_NAME/manylinux_2_28_${arch}:$MANYLINUX_IMAGE_DIGEST"

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
