ARG SERVER_IMAGE=aerospike/aerospike-server-enterprise

# Shared between build stages to get node ID from roster file and to build final image
ARG ROSTER_FILE_NAME=roster.smd
# Temp file for passing node id from the two build stages mentioned above
# Docker doesn't support command substitution for setting values for ARG variables, so we have to do this
ARG NODE_ID_FILE_NAME=node_id

# Shared between build stages to generate the server certificate and to build final image
# aerospike.conf needed to get cluster name for certificate
ARG AEROSPIKE_CONF_TEMPLATE_PATH=/etc/aerospike/aerospike.template.conf
ARG SERVER_KEY_FILE_NAME=server.pem
ARG SERVER_CERT_FILE_NAME=server.cer
# Cluster name fetched from stage to generate cert also needs to be set in aerospike.conf in our final image
ARG CLUSTER_NAME_FILE_NAME=cluster_name

FROM $SERVER_IMAGE AS enable-security

WORKDIR /opt/aerospike/smd

# Not using asconfig to edit config because we are working with a template file, which may not have valid values yet
ARG AEROSPIKE_CONF_TEMPLATE_PATH
RUN echo -e "security {\n\tenable-quotas true\n}\n" >> $AEROSPIKE_CONF_TEMPLATE_PATH
# security.smd was generated manually by
# 1. Starting a new Aerospike EE server using Docker
# 2. Creating the superuser user
# 3. Copying /opt/aerospike/smd/security.smd from the container and committing it to this repo
# This file should always work
# TODO: generate this automatically, somehow.
COPY security.smd .

FROM enable-security AS enable-sc-part1

RUN sed -i "s/\(namespace.*{\)/\1\n\tstrong-consistency true/" $AEROSPIKE_CONF_TEMPLATE_PATH
RUN sed -i "s/\(namespace.*{\)/\1\n\tstrong-consistency-allow-expunge true/" $AEROSPIKE_CONF_TEMPLATE_PATH
ARG ROSTER_FILE_NAME
COPY $ROSTER_FILE_NAME .

# Strong consistency only: fetch node id from roster.smd

# jq docker image doesn't have a shell
# We need a shell to fetch and pass the node id to the next build stage
FROM busybox AS get-node-id-for-sc
# There's no tag for the latest major version to prevent breaking changes in jq
# This is the next best thing
COPY --from=ghcr.io/jqlang/jq:1.7 /jq /bin/
ARG ROSTER_FILE_NAME
COPY $ROSTER_FILE_NAME .
ARG NODE_ID_FILE_NAME
RUN jq --raw-output '.[1].value' $ROSTER_FILE_NAME > $NODE_ID_FILE_NAME

FROM enable-sc-part1 AS enable-sc-part2
ARG NODE_ID_FILE_NAME
COPY --from=get-node-id-for-sc $NODE_ID_FILE_NAME .
RUN sed -i "s/\(^service {\)/\1\n\tnode-id $(cat $NODE_ID_FILE_NAME)/" $AEROSPIKE_CONF_TEMPLATE_PATH
RUN rm $NODE_ID_FILE_NAME

# Use separate build stage since we don't want openssl bundled in the final image

FROM $SERVER_IMAGE AS generate-server-cert-for-tls

RUN apt update
RUN apt install -y openssl

ARG AEROSPIKE_CONF_TEMPLATE_PATH
ARG CLUSTER_NAME_FILE_NAME
RUN grep -Eo "cluster-name [a-z]+" $AEROSPIKE_CONF_TEMPLATE_PATH | awk '{print $2}' > $CLUSTER_NAME_FILE_NAME

# Generate server private key and CSR

ARG SERVER_CSR_FILE_NAME=server.csr
ARG SERVER_KEY_FILE_NAME
RUN openssl req -newkey rsa:4096 -keyout $SERVER_KEY_FILE_NAME -nodes -new -out $SERVER_CSR_FILE_NAME -subj "/C=XX/ST=StateName/L=CityName/O=CompanyName/OU=CompanySectionName/CN=$(cat $CLUSTER_NAME_FILE_NAME)"

# Send CSR to CA and get server certificate
# We use an external CA because we want the client to use that same CA to connect to the server
ARG CA_KEY_FILE_NAME
ARG CA_CERT_FILE_NAME
COPY $CA_KEY_FILE_NAME .
COPY $CA_CERT_FILE_NAME .
ARG SERVER_CERT_FILE_NAME
RUN openssl x509 -req -in $SERVER_CSR_FILE_NAME -CA $CA_CERT_FILE_NAME -CAkey $CA_KEY_FILE_NAME -out $SERVER_CERT_FILE_NAME

FROM enable-sc-part2 AS enable-tls

ARG SSL_WORKING_DIR=/etc/ssl
WORKDIR $SSL_WORKING_DIR
ARG SERVER_KEY_FILE_NAME
ARG SERVER_CERT_FILE_NAME
ARG SERVER_KEY_ABS_PATH=$SSL_WORKING_DIR/private/$SERVER_KEY_FILE_NAME
ARG SERVER_CERT_ABS_PATH=$SSL_WORKING_DIR/certs/$SERVER_CERT_FILE_NAME

COPY --from=generate-server-cert-for-tls $SERVER_KEY_FILE_NAME $SERVER_KEY_ABS_PATH
COPY --from=generate-server-cert-for-tls $SERVER_CERT_FILE_NAME $SERVER_CERT_ABS_PATH

# Service sub-stanza under network stanza
ARG CLUSTER_NAME_FILE_NAME
COPY --from=generate-server-cert-for-tls $CLUSTER_NAME_FILE_NAME .
RUN sed -i "s|\(^\tservice {\)|\1\n\t\ttls-name $(cat $CLUSTER_NAME_FILE_NAME)|" $AEROSPIKE_CONF_TEMPLATE_PATH
RUN sed -i "s|\(^\tservice {\)|\1\n\t\ttls-authenticate-client false|" $AEROSPIKE_CONF_TEMPLATE_PATH
ARG TLS_PORT=4333
RUN sed -i "s|\(^\tservice {\)|\1\n\t\ttls-port $TLS_PORT|" $AEROSPIKE_CONF_TEMPLATE_PATH

RUN sed -i "s|\(^network {\)|\1\n\ttls $(cat $CLUSTER_NAME_FILE_NAME) {\n\t\tcert-file $SERVER_CERT_ABS_PATH\n\t}|" $AEROSPIKE_CONF_TEMPLATE_PATH
RUN sed -i "s|\(^\ttls $(cat $CLUSTER_NAME_FILE_NAME) {\)|\1\n\t\tkey-file $SERVER_KEY_ABS_PATH|" $AEROSPIKE_CONF_TEMPLATE_PATH

EXPOSE $TLS_PORT

# Cleanup
RUN rm $CLUSTER_NAME_FILE_NAME
