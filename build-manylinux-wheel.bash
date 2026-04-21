#!/bin/bash

set -e
set -x

image_name=$1

# Use UUID to minimize chance of colliding with another container
CONTAINER_NAME=manylinux-$(uuidgen)

this_files_dir=$(realpath "${BASH_SOURCE[0]}" | dirname)
MOUNTED_REPO_DIR=/repo
docker run -d -v "${this_files_dir}:${MOUNTED_REPO_DIR}/" --rm --name "$CONTAINER_NAME" "$image_name" tail -f /dev/null
docker exec "$CONTAINER_NAME" $MOUNTED_REPO_DIR/build-prod-wheel.bash
mkdir wheelhouse
docker cp "${CONTAINER_NAME}:${MOUNTED_REPO_DIR}/wheelhouse/*.whl" wheelhouse/

# TODO: Also stop container when sigkill/sigint is received
docker stop "$CONTAINER_NAME"
