#!/bin/bash

# Makes sure that if the "docker exec" command fails, it is not ignored
set -o pipefail

# We use bash because we need the not (!) operator

CALL_FROM_TOOLS_CONTAINER="docker exec aerospike/aerospike-tools"

while true; do
    # Intermediate step is to send docker exec command's output to stdout in case it fails
    # Sometimes, errors only appear in stdout and not stderr, like if asinfo throws an error because of no credentials
    # (This is a bug in asinfo since all error messages should be sent to stderr)
    # But piping and passing stdin to grep will hide the first command's stdout.
    # grep doesn't have a way to print all lines passed as input.
    # ack does have an option but it doesn't come installed by default
    echo "Checking if we can reach the server via the service port..."
    if $CALL_FROM_TOOLS_CONTAINER asinfo -v status | tee >(cat) | grep -qE "^ok"; then
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
    if $CALL_FROM_TOOLS_CONTAINER asinfo -v "cluster-stable:ignore-migrations=true" 2>&1 | (! grep -qE "^ERROR"); then
        echo "Server is in a stable state."
        break
    fi

    echo "Server did not return a cluster key. Polling again..."
done

if [[ -z "$NO_SECURITY" ]]; then
    export SECURITY_FLAGS="-U admin -P admin"
fi

$CALL_FROM_TOOLS_CONTAINER asadm $SECURITY_FLAGS --enable --execute "manage acl create user superuser password superuser roles read-write-udf, sys-admin, user-admin, data-admin"

# Strong consistency
# Set up roster
$CALL_FROM_TOOLS_CONTAINER asadm $SECURITY_FLAGS --enable --execute "manage roster stage observed ns test"
$CALL_FROM_TOOLS_CONTAINER asadm $SECURITY_FLAGS --enable --execute "manage recluster"
