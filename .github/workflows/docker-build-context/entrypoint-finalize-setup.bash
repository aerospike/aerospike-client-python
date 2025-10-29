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

# We have to remove the .jinja part
asd --fgdaemon --config-file ${AEROSPIKE_CONF_PATH%.*} &

# Allows HEALTHCHECK to report this container as healthy, now
touch $HEALTHCHECK_FILE_PATH

fg
