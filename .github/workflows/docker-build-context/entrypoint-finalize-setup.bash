#!/bin/bash
# Entrypoint script used by our custom EE server image

# Bash required for using job control

set -x
set -m
set -e

python3 toggle-features-in-conf-files.py

# Disable features if needed
cd /opt/aerospike/smd
if [[ -n "$NO_SECURITY" ]]; then
    rm security.smd
fi
if [[ -n "$NO_SC" ]]; then
    rm roster.smd
fi

asd --fgdaemon --config-file /etc/aerospike/aerospike.conf &

# We don't need to timeout here.
# If the wait script runs forever, users running the container manually will know that
# the container is "unhealthy" by checking the status
# And our Github Actions code will wait for the container to be healthy or timeout after 30 seconds.
bash /wait-for-as-server-to-start.bash

# Finish setting up strong consistency
if [[ -z "$NO_SC" ]]; then
    asadm --enable --execute "manage revive ns test"
    asadm --enable --execute "manage recluster"
fi

# Allows HEALTHCHECK to report this container as healthy, now
touch $HEALTHCHECK_FILE_PATH

fg
