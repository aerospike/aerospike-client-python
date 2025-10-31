#!/bin/bash

# Makes sure that if the "docker exec" command fails, it is not ignored
set -o pipefail
set -e
set -x

# We use bash because we need the not (!) operator

AEROSPIKE_YAML_PATH=/workdir/aerospike-dev.yaml

CALL_FROM_YQ_CONTAINER() {
    docker run --rm -v ./:/workdir mikefarah/yq "$1" -i $AEROSPIKE_YAML_PATH
}

if [[ "$MUTUAL_TLS" == "1" ]]; then
    CALL_FROM_YQ_CONTAINER ".network.service.tls-authenticate-client = any"
    CALL_FROM_YQ_CONTAINER ".network.service.tls-name = docker"
    CALL_FROM_YQ_CONTAINER ".network.service.tls-port = 4333"
    # TODO: needs volume for these certs
    CALL_FROM_YQ_CONTAINER ".network.service.tls.ca-file = /etc/ssl/certs/ca.cer"
    CALL_FROM_YQ_CONTAINER ".network.service.tls.cert-file = /etc/ssl/certs/server.cer"
    CALL_FROM_YQ_CONTAINER ".network.service.tls.key-file = /etc/ssl/private/server.pem"
    CALL_FROM_YQ_CONTAINER ".network.service.tls.name = docker"
else
    # TODO: Make sure these are idempotent
    CALL_FROM_YQ_CONTAINER "del(.network.service.tls-authenticate-client)"
    CALL_FROM_YQ_CONTAINER "del(.network.service.tls-name)"
    CALL_FROM_YQ_CONTAINER "del(.network.service.tls-port)"
    CALL_FROM_YQ_CONTAINER "del(.network.tls)"
# fi

if [[ "$SECURITY" == "1" ]]; then
    CALL_FROM_YQ_CONTAINER ".security.enable-quotas = false"
    CALL_FROM_YQ_CONTAINER ".security.log.report-violation = true"
else
    CALL_FROM_YQ_CONTAINER "del(.security)"
fi

if [[ "$STRONG_CONSISTENCY" == "1" ]]; then
    CALL_FROM_YQ_CONTAINER ".namespaces[0].strong-consistency = true"
    CALL_FROM_YQ_CONTAINER ".namespaces[0].strong-consistency-allow-expunge = true"
else
    CALL_FROM_YQ_CONTAINER ".namespaces[0].strong-consistency = false"
    CALL_FROM_YQ_CONTAINER ".namespaces[0].strong-consistency-allow-expunge = false"
fi

CALL_FROM_TOOLS_CONTAINER="docker run --rm -v ./:/workdir --network host aerospike/aerospike-tools"

$CALL_FROM_TOOLS_CONTAINER asconfig convert -f $AEROSPIKE_YAML_PATH -o /workdir/aerospike.conf
cat aerospike.conf

docker run -d --rm --name aerospike -p 4333:4333 -p 3000:3000 -v ./:/custom-dir/ aerospike/aerospike-server-enterprise --config-file /custom-dir/aerospike.conf

if [[ "$SECURITY" == "1" ]]; then
    export SECURITY_FLAGS="-U admin -P admin"
fi

# docker logs -f aerospike

# TODO: make sure server container is alive or waiting is pointless
while true; do
    # Intermediate step is to send docker exec command's output to stdout in case it fails
    # Sometimes, errors only appear in stdout and not stderr, like if asinfo throws an error because of no credentials
    # (This is a bug in asinfo since all error messages should be sent to stderr)
    # But piping and passing stdin to grep will hide the first command's stdout.
    # grep doesn't have a way to print all lines passed as input.
    # ack does have an option but it doesn't come installed by default
    echo "Checking if we can reach the server via the service port..."
    if $CALL_FROM_TOOLS_CONTAINER asinfo $SECURITY_FLAGS -v status | tee >(cat) | grep -qE "^ok"; then
        # Server is ready when asinfo returns ok
        echo "Can reach server now."
        break
    fi
    echo "Server didn't return ok via the service port. Polling again..."
done

# Although the server may be reachable via the service port, the cluster may not be fully initialized yet.
# If we try to connect too soon (e.g right after "status" returns ok), the client may throw error code -1
while true; do
    echo "Waiting for server to stabilize (i.e return a cluster key)..."
    # We assume that when an ERROR is returned, the cluster is not stable yet (i.e not fully initialized)
    # The Dockerfile uses a roster from a previously running Aerospike server in a Docker container
    # When we reuse this roster, the server assumes all of its partitions are dead because it's running on a new
    # storage device. That is why we ignore-migrations here
    if $CALL_FROM_TOOLS_CONTAINER asinfo $SECURITY_FLAGS -v "cluster-stable:ignore-migrations=true" 2>&1 | (! grep -qE "^ERROR"); then
        echo "Server is in a stable state."
        break
    fi

    echo "Server did not return a cluster key. Polling again..."
done

# Set up security
if [[ "$SECURITY" == "1" ]]; then
    $CALL_FROM_TOOLS_CONTAINER asadm $SECURITY_FLAGS --enable --execute "manage acl create user superuser password superuser roles read-write-udf, sys-admin, user-admin, data-admin"
fi

# Strong consistency
# Set up roster
if [[ "$STRONG_CONSISTENCY" == "1" ]]; then
    if [[ "$SECURITY" == "1" ]]; then
        export SECURITY_FLAGS="-U superuser -P superuser"
    fi
    $CALL_FROM_TOOLS_CONTAINER asadm $SECURITY_FLAGS --enable --execute "manage roster stage observed ns test"
    $CALL_FROM_TOOLS_CONTAINER asadm $SECURITY_FLAGS --enable --execute "manage recluster"
fi
