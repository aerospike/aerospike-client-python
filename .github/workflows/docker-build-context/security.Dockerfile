FROM base

# Shared between build stages to get node ID from roster file and to build final image
ARG ROSTER_FILE_NAME=roster.smd
# Temp file for passing node id from the two build stages mentioned above
# Docker doesn't support command substitution for setting values for ARG variables, so we have to do this
ARG NODE_ID_FILE_NAME=node_id

WORKDIR /opt/aerospike/smd

# Not using asconfig to edit config because we are working with a template file, which may not have valid values yet
RUN echo -e "security {\n\tenable-quotas true\n}\n" >> $AEROSPIKE_CONF_TEMPLATE_PATH
# security.smd was generated manually by
# 1. Starting a new Aerospike EE server using Docker
# 2. Creating the superuser user
# 3. Copying /opt/aerospike/smd/security.smd from the container and committing it to this repo
# This file should always work
# TODO: generate this automatically, somehow.
COPY security.smd .
