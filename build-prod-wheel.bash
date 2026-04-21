#!/bin/bash

set -e
set -x

manylinux_image_name=$1
python_version=$2
keep_container=$3

this_file_abs_path=$(realpath "${BASH_SOURCE[0]}")
dir_containing_this_file=$(dirname "$this_file_abs_path")

if [[ -z "$manylinux_image_name" ]]; then
    # Build wheel natively
    "$dir_containing_this_file"/_build-prod-wheel.bash
    exit 0
fi

# Use UUID to minimize chance of colliding with another container
CONTAINER_NAME=manylinux-$(uuidgen)

MOUNTED_REPO_DIR=/repo
docker run -d -v "${dir_containing_this_file}:${MOUNTED_REPO_DIR}/" --rm --name "$CONTAINER_NAME" "$manylinux_image_name" tail -f /dev/null

cleanup() {
    if [[ -z $keep_container ]]; then
        docker stop "$CONTAINER_NAME"
    fi
}
trap 'cleanup' EXIT SIGINT SIGTERM ERR

docker exec "$CONTAINER_NAME" git config --global --add safe.directory "$MOUNTED_REPO_DIR"
docker exec -w "$MOUNTED_REPO_DIR" "$CONTAINER_NAME" ./_build-prod-wheel.bash "$python_version"
