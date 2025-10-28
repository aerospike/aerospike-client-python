if [[ -z "$NO_SECURITY" ]]; then
    export SECURITY_FLAGS="-U admin -P admin"
fi

# We don't need to timeout here.
# If the wait script runs forever, users running the container manually will know that
# the container is "unhealthy" by checking the status
# And our Github Actions code will wait for the container to be healthy or timeout after 30 seconds.
/wait-for-as-server-to-start.bash

if [[ -z "$NO_SC" ]]; then
    # Finish setting up strong consistency
    asadm $SECURITY_FLAGS --enable --execute "manage revive ns test"
    asadm $SECURITY_FLAGS --enable --execute "manage recluster"
fi

# Allows HEALTHCHECK to report this container as healthy, now
touch $HEALTHCHECK_FILE_PATH

fg
