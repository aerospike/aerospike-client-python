#!/bin/bash

set -x
# Makes sure that if the "docker exec" command fails, it is not ignored
set -o pipefail
set -m

asd --fgdaemon --config-file /etc/aerospike/aerospike-dev.conf &

# timeout uses sh shell by default, so we need to be specific
timeout 30s bash /wait-for-as-server-to-start.bash

asadm --enable --execute "manage revive ns test"
asadm --enable --execute "manage recluster"

# Allows HEALTHCHECK to report this container as healthy, now
touch $HEALTHCHECK_FILE_PATH

fg
