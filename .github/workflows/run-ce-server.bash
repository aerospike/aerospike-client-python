#!/bin/bash

if [[ $# -lt 2 ]]; then
    echo "Usage: <script-name> container_name server_tag" >&2
    exit 1
fi

CONTAINER_NAME=$1
SERVER_TAG=$2

docker run -d --name "$CONTAINER_NAME" -p 3000:3000 -e DEFAULT_TTL=2592000 "aerospike/aerospike-server:$SERVER_TAG"
cd .github/workflows/docker-setup
timeout 30s bash wait-for-as-server-to-start.bash
