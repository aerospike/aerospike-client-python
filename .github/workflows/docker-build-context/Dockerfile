ARG server_image=aerospike/aerospike-server-enterprise
ARG ROSTER_FILE_NAME=roster.smd
# Temp file for passing node id from one build stage to another
# Docker doesn't support command substitution for setting values for ARG variables, so we have to do this
ARG NODE_ID_FILE_NAME=node_id
ARG AEROSPIKE_CONF_TEMPLATE_PATH=/etc/aerospike/aerospike.template.conf
ARG SSL_WORKING_DIR=/etc/ssl
ARG CLUSTER_NAME_FILE_NAME=cluster_name

ARG SERVER_KEY_REL_PATH=private/server.pem
ARG SERVER_CERT_REL_PATH=certs/server.pem

FROM $server_image as configure-server

ARG AEROSPIKE_CONF_TEMPLATE_PATH

WORKDIR /opt/aerospike/smd

# Enable authentication

# Not using asconfig to edit config because we are working with a template file, which may not have valid values yet
RUN echo -e "security {\n\tenable-quotas true\n}\n" >> $AEROSPIKE_CONF_TEMPLATE_PATH
# security.smd was generated manually by
# 1. Starting a new Aerospike EE server using Docker
# 2. Creating the superuser user
# 3. Copying /opt/aerospike/smd/security.smd from the container and committing it to this repo
# This file should always work
# TODO: generate this automatically, somehow.
COPY security.smd .

# Enable strong consistency
RUN sed -i "s/\(namespace.*{\)/\1\n\tstrong-consistency true/" $AEROSPIKE_CONF_TEMPLATE_PATH
RUN sed -i "s/\(namespace.*{\)/\1\n\tstrong-consistency-allow-expunge true/" $AEROSPIKE_CONF_TEMPLATE_PATH
ARG ROSTER_FILE_NAME
COPY $ROSTER_FILE_NAME .

# Fetch node id from roster.smd

# There's no tag for the latest major version to prevent breaking changes in jq
# This is the next best thing
FROM ghcr.io/jqlang/jq:1.7 as get-jq
# jq docker image doesn't have a shell
# We need a shell to fetch and pass the node id to the next build stage
FROM busybox as get-node-id
COPY --from=get-jq /jq /bin/
ARG ROSTER_FILE_NAME
COPY $ROSTER_FILE_NAME .
ARG NODE_ID_FILE_NAME
RUN jq --raw-output '.[1].value' $ROSTER_FILE_NAME > $NODE_ID_FILE_NAME

FROM configure-server as set-node-id
ARG NODE_ID_FILE_NAME
COPY --from=get-node-id $NODE_ID_FILE_NAME .
RUN sed -i "s/\(^service {\)/\1\n\tnode-id $(cat $NODE_ID_FILE_NAME)/" $AEROSPIKE_CONF_TEMPLATE_PATH
RUN rm $NODE_ID_FILE_NAME

# Configure TLS

FROM $server_image as generate-server-cert

RUN apt update
RUN apt install -y openssl

ARG SSL_WORKING_DIR
WORKDIR $SSL_WORKING_DIR
ARG SERVER_CSR_REL_PATH=server.csr
ARG SERVER_KEY_REL_PATH
ARG SERVER_CERT_REL_PATH
ARG AEROSPIKE_CONF_TEMPLATE_PATH
ARG CLUSTER_NAME_FILE_NAME
RUN grep -Eo "cluster-name [a-z]+" $AEROSPIKE_CONF_TEMPLATE_PATH | awk '{print $2}' > $CLUSTER_NAME_FILE_NAME

# Generate server private key and CSR
RUN openssl req -newkey rsa:4096 -keyout $SERVER_KEY_REL_PATH -nodes -new -out $SERVER_CSR_REL_PATH -subj "/C=XX/ST=StateName/L=CityName/O=CompanyName/OU=CompanySectionName/CN=$(cat $CLUSTER_NAME_FILE_NAME)"
# Send CSR to CA and get server certificate
ARG CA_KEY_FILE_NAME
ARG CA_CERT_FILE_NAME
COPY $CA_KEY_FILE_NAME private/
COPY $CA_CERT_FILE_NAME certs/
RUN openssl x509 -req -in $SERVER_CSR_REL_PATH -CA certs/$CA_CERT_FILE_NAME -CAkey private/$CA_KEY_FILE_NAME -out $SERVER_CERT_REL_PATH

FROM set-node-id as configure-tls

ARG SSL_WORKING_DIR
COPY --from=generate-server-cert $SSL_WORKING_DIR $SSL_WORKING_DIR
ARG TLS_PORT=4333

# Service sub-stanza under network stanza
ARG CLUSTER_NAME_FILE_NAME
RUN sed -i "s|\(^\tservice {\)|\1\n\t\ttls-name $(cat $CLUSTER_NAME_FILE_NAME)|" $AEROSPIKE_CONF_TEMPLATE_PATH
RUN sed -i "s|\(^\tservice {\)|\1\n\t\ttls-authenticate-client false|" $AEROSPIKE_CONF_TEMPLATE_PATH
RUN sed -i "s|\(^\tservice {\)|\1\n\t\ttls-port $TLS_PORT|" $AEROSPIKE_CONF_TEMPLATE_PATH

ARG SERVER_KEY_REL_PATH
ARG SERVER_CERT_REL_PATH
RUN sed -i "s|\(^network {\)|\1\n\ttls $(cat $CLUSTER_NAME_FILE_NAME) {\n\t\tcert-file $SSL_WORKING_DIR/$SERVER_CERT_REL_PATH\n\t}|" $AEROSPIKE_CONF_TEMPLATE_PATH
RUN sed -i "s|\(^\ttls $(cat $CLUSTER_NAME_FILE_NAME) {\)|\1\n\t\tkey-file $SSL_WORKING_DIR/$SERVER_KEY_REL_PATH|" $AEROSPIKE_CONF_TEMPLATE_PATH

RUN rm $CLUSTER_NAME_FILE_NAME
