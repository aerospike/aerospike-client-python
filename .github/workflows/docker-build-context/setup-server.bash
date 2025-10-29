#!/bin/bash

if [[ -z "$NO_SECURITY" ]]; then
    export SECURITY_FLAGS="-U superuser -P superuser -h aerospike"
fi

# We don't need to timeout here.
# If the wait script runs forever, users running the container manually will know that
# the container is "unhealthy" by checking the status
# And our Github Actions code will wait for the container to be healthy or timeout after 30 seconds.
/workdir/wait-for-as-server-to-start.bash

if [[ -z "$NO_SC" ]]; then
    # Finish setting up strong consistency
    asadm $SECURITY_FLAGS --enable --execute "manage revive ns test"
    asadm $SECURITY_FLAGS --enable --execute "manage recluster"
fi

touch /healthy
