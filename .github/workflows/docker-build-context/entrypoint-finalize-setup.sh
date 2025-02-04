#!/bin/bash

set -x
# Makes sure that if the "docker exec" command fails, it is not ignored
set -o pipefail
set -m

asd --fgdaemon --config-file /etc/aerospike/aerospike-dev.conf &

while true; do
    # An unset variable will have a default empty value
    # Intermediate step is to print docker exec command's output in case it fails
    # Sometimes, errors only appear in stdout and not stderr, like if asinfo throws an error because of no credentials
    # (This is a bug in asinfo since all error messages should be sent to stderr)
    # But piping and passing stdin to grep will hide the first command's stdout.
    # grep doesn't have a way to print all lines passed as input.
    # ack does have an option but it doesn't come installed by default
    # shellcheck disable=SC2086 # The flags in user credentials should be separate anyways. Not one string
    echo "Checking if we can reach the server via the service port..."
    if asinfo -v status | tee >(cat) | grep -qE "^ok"; then
        # Server is ready when asinfo returns ok
        echo "Can reach server now."
        # docker container inspect "$container_name"
        break
    fi
    asinfo -v status
    echo "Server didn't return ok via the service port. Polling again..."
done

# Although the server may be reachable via the service port, the cluster may not be fully initialized yet.
# If we try to connect too soon (e.g right after "status" returns ok), the client may throw error code -1
while true; do
    echo "Waiting for server to stabilize (i.e return a cluster key)..."
    # We assume that when an ERROR is returned, the cluster is not stable yet (i.e not fully initialized)
    cluster_stable_info_cmd="cluster-stable"
    # The Dockerfile uses a roster from a previously running Aerospike server in a Docker container
    # When we reuse this roster, the server assumes all of its partitions are dead because it's running on a new
    # storage device.
    cluster_stable_info_cmd="$cluster_stable_info_cmd:ignore-migrations=true"
    if asinfo -v $cluster_stable_info_cmd 2>&1 | (! grep -qE "^ERROR"); then
        echo "Server is in a stable state."
        break
    fi

    echo "Server did not return a cluster key. Polling again..."
done

asadm --enable --execute "manage revive ns test"
asadm --enable --execute "manage recluster"

# Allows HEALTHCHECK to report this container as healthy, now
touch $HEALTHCHECK_FILE_PATH

fg
