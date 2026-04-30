#!/bin/bash

set -e
set -x

abs_path_for_this_file=$(realpath "${BASH_SOURCE[0]}")
dir_containing_this_file=$(dirname "$abs_path_for_this_file")

helper_script=_build-prod-wheel-natively.bash

os=$(uname -s)

if [[ ! "$os" =~ Linux* ]]; then
    # Build wheel natively
    "$dir_containing_this_file/${helper_script}"
    exit 0
fi

MANYLINUX_REGISTRY_NAME=$1

if [[ -z "$MANYLINUX_REGISTRY_NAME" ]]; then
    echo "MANYLINUX_REGISTRY_NAME env var must be defined."
    exit 1
fi


arch=$(uname -m)
if [[ "$arch" == "aarch64" ]]; then
    MANYLINUX_IMAGE_DIGEST="3e814781f3025a4659eefdc2aa1dca593eb1d9e0d6c6e1d1f543d17429eb5bdb"
else
    MANYLINUX_IMAGE_DIGEST="6d32fb959e76ed2b2117b28d141b3a92aed81805fe23e357a9b247ea30b88ae5"
fi

MANYLINUX_IMAGE_NAME="$MANYLINUX_REGISTRY_NAME/manylinux_2_28_${arch}@sha256:$MANYLINUX_IMAGE_DIGEST"

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

# Addresses a git error where the user owning the repo folder is different from the container user
docker exec "$CONTAINER_NAME" git config --global --add safe.directory "$MOUNTED_REPO_DIR"

active_python_minor_version=$(python3 --version | cut -f 2- -d' ' | cut -f -2 -d'.')

docker exec -w "$MOUNTED_REPO_DIR" "$CONTAINER_NAME" ./${helper_script} "$active_python_minor_version"
