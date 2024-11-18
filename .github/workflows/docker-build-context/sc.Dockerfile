ARG ROSTER_FILE_NAME=roster.smd
ARG NODE_ID_FILE_NAME=node_id

# jq docker image doesn't have a shell
# We need a shell to fetch and pass the node id to the next build stage
FROM busybox AS get-node-id
# There's no tag for the latest major version to prevent breaking changes in jq
# This is the next best thing
COPY --from=ghcr.io/jqlang/jq:1.7 /jq /bin/
ARG ROSTER_FILE_NAME
COPY $ROSTER_FILE_NAME .
ARG NODE_ID_FILE_NAME
RUN jq --raw-output '.[1].value' $ROSTER_FILE_NAME > $NODE_ID_FILE_NAME

FROM base

WORKDIR /opt/aerospike/smd
ARG ROSTER_FILE_NAME
COPY $ROSTER_FILE_NAME .

ARG NODE_ID_FILE_NAME
COPY --from=get-node-id-for-sc $NODE_ID_FILE_NAME .
RUN sed -i "s/\(^service {\)/\1\n\tnode-id $(cat $NODE_ID_FILE_NAME)/" $AEROSPIKE_CONF_TEMPLATE_PATH
RUN rm $NODE_ID_FILE_NAME

RUN sed -i "s/\(namespace.*{\)/\1\n\tstrong-consistency true/" $AEROSPIKE_CONF_TEMPLATE_PATH
RUN sed -i "s/\(namespace.*{\)/\1\n\tstrong-consistency-allow-expunge true/" $AEROSPIKE_CONF_TEMPLATE_PATH
