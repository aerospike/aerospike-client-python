#!/bin/bash
# Entrypoint script used by our custom EE server image

# Bash required for using job control

set -x
set -m
set -e

asd --fgdaemon --config-file $AEROSPIKE_CONF_PATH &

# We don't need to timeout here.
# If the wait script runs forever, users running the container manually will know that
# the container is "unhealthy" by checking the status
# And our Github Actions code will wait for the container to be healthy or timeout after 30 seconds.
cat -e /wait-for-as-server-to-start.bash
bash /wait-for-as-server-to-start.bash

# Finish setting up strong consistency
asadm --enable --execute "manage revive ns test"
asadm --enable --execute "manage recluster"

# Allows HEALTHCHECK to report this container as healthy, now
touch $HEALTHCHECK_FILE_PATH

fg
