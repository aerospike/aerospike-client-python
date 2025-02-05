#!/bin/bash

set -x
set -m

asd --fgdaemon --config-file $AEROSPIKE_CONF_PATH &

# timeout uses sh shell by default, so we need to be specific
timeout 30s bash $WAIT_SCRIPT_FILE_PATH

# Finish setting up strong consistency
asadm --enable --execute "manage revive ns test"
asadm --enable --execute "manage recluster"

# Allows HEALTHCHECK to report this container as healthy, now
touch $HEALTHCHECK_FILE_PATH

fg
