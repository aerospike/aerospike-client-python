#!/bin/bash

set -e
set -x

image_name=$1
python_version=$2

# Use UUID to minimize chance of colliding with another container
CONTAINER_NAME=manylinux-$(uuidgen)

this_file_abs_path=$(realpath "${BASH_SOURCE[0]}")
dir_containing_this_file=$(dirname "$this_file_abs_path")
MOUNTED_REPO_DIR=/repo
docker run -d -v "${dir_containing_this_file}:${MOUNTED_REPO_DIR}/" --rm --name "$CONTAINER_NAME" "$image_name" tail -f /dev/null
docker exec -w "$MOUNTED_REPO_DIR" "$CONTAINER_NAME" ./build-prod-wheel.bash "$python_version"
mkdir wheelhouse
docker cp "${CONTAINER_NAME}:${MOUNTED_REPO_DIR}/wheelhouse/*.whl" wheelhouse/

# TODO: Also stop container when sigkill/sigint is received
docker stop "$CONTAINER_NAME"
