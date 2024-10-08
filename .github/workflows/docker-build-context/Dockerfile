ARG image
FROM $image as configure-server

WORKDIR /opt/aerospike/smd

ENV AEROSPIKE_CONF_TEMPLATE_PATH=/etc/aerospike/aerospike.template.conf
# Enable authentication
RUN echo -e "security {\n\tenable-quotas true\n}\n" >> $AEROSPIKE_CONF_TEMPLATE_PATH
# security.smd was generated manually by
# 1. Starting a new Aerospike EE server using Docker
# 2. Creating the superuser user
# 3. Copying /opt/aerospike/smd/security.smd from the container and committing it to this repo
# This file should always work
# TODO: generate this automatically, somehow
COPY security.smd .

# Enable strong consistency
RUN sed -i "s/\(namespace.*{\)/\1\n\tstrong-consistency true/" $AEROSPIKE_CONF_TEMPLATE_PATH
RUN sed -i "s/\(namespace.*{\)/\1\n\tstrong-consistency-allow-expunge true/" $AEROSPIKE_CONF_TEMPLATE_PATH
ENV ROSTER_FILE_NAME=roster.smd
COPY $ROSTER_FILE_NAME .

# There's no tag for the latest major version to prevent breaking changes in jq
# This is the next best thing
FROM ghcr.io/jqlang/jq:1.7
COPY --from=configure-server $ROSTER_FILE_NAME $ROSTER_FILE_NAME
ENV NODE_ID=$(jq --raw-output '.[1].value' $ROSTER_FILE_NAME)

FROM configure-server
RUN sed -i "s/\(^service {\)/\1\n\tnode-id $NODE_ID/" $AEROSPIKE_CONF_TEMPLATE_PATH
