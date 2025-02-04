ARG SERVER_IMAGE=aerospike/aerospike-server-enterprise

# Shared between build stages to get node ID from roster file and to build final image
ARG ROSTER_FILE_NAME=roster.smd
# Temp file for passing node id from the two build stages mentioned above
# Docker doesn't support command substitution for setting values for ARG variables, so we have to do this
ARG NODE_ID_FILE_NAME=node_id

# Shared between build stages to generate the server certificate and to build final image
# aerospike.conf needed to get cluster name for certificate
ARG AEROSPIKE_CONF_TEMPLATE_PATH=/etc/aerospike/aerospike-dev.conf
ARG SERVER_KEY_FILE_NAME=server.pem
ARG SERVER_CERT_FILE_NAME=server.cer
# Temp file
# Cluster name fetched from stage to generate cert also needs to be set in aerospike.conf in our final image
ARG CLUSTER_NAME_FILE_NAME=cluster_name

ARG CA_CERT_FILE_NAME=ca.cer

FROM $SERVER_IMAGE AS enable-security

WORKDIR /opt/aerospike/smd

# security.smd was generated manually by
# 1. Starting a new Aerospike EE server using Docker
# 2. Creating the superuser user
# 3. Copying /opt/aerospike/smd/security.smd from the container and committing it to this repo
# This file should always work
# TODO: generate this automatically, somehow.
COPY security.smd .

FROM enable-security AS enable-strong-consistency

ARG ROSTER_FILE_NAME
COPY $ROSTER_FILE_NAME .

# Use a separate build stage to generate certs since we don't want openssl bundled in the final image

FROM $SERVER_IMAGE AS generate-server-cert-for-tls

RUN apt update
RUN apt install -y openssl

ARG AEROSPIKE_CONF_TEMPLATE_PATH
ARG CLUSTER_NAME_FILE_NAME
COPY aerospike-dev.conf /etc/aerospike/
RUN grep -Eo "cluster-name [a-z]+" $AEROSPIKE_CONF_TEMPLATE_PATH | awk '{print $2}' > $CLUSTER_NAME_FILE_NAME

# Generate server private key and CSR

ARG SERVER_CSR_FILE_NAME=server.csr
ARG SERVER_KEY_FILE_NAME
RUN openssl req -newkey rsa:4096 -keyout $SERVER_KEY_FILE_NAME -nodes -new -out $SERVER_CSR_FILE_NAME -subj "/C=XX/ST=StateName/L=CityName/O=CompanyName/OU=CompanySectionName/CN=$(cat $CLUSTER_NAME_FILE_NAME)"

ARG CA_KEY_FILE_NAME=ca.pem
ARG CA_CERT_FILE_NAME=ca.cer

# Send CSR to CA and get server certificate
# We use an external CA because we want the client to use that same CA to verify the server certificate upon connecting

# TODO: also make this an option.
COPY $CA_KEY_FILE_NAME .
COPY $CA_CERT_FILE_NAME .
ARG SERVER_CERT_FILE_NAME
RUN openssl x509 -req -in $SERVER_CSR_FILE_NAME -CA $CA_CERT_FILE_NAME -CAkey $CA_KEY_FILE_NAME -out $SERVER_CERT_FILE_NAME

FROM enable-strong-consistency AS enable-tls

ARG SSL_WORKING_DIR=/etc/ssl
WORKDIR $SSL_WORKING_DIR
ARG SERVER_KEY_FILE_NAME
ARG SERVER_CERT_FILE_NAME
ARG SERVER_KEY_INSTALL_PATH=$SSL_WORKING_DIR/private/$SERVER_KEY_FILE_NAME
ARG SERVER_CERT_INSTALL_PATH=$SSL_WORKING_DIR/certs/$SERVER_CERT_FILE_NAME

COPY --from=generate-server-cert-for-tls $SERVER_KEY_FILE_NAME $SERVER_KEY_INSTALL_PATH
COPY --from=generate-server-cert-for-tls $SERVER_CERT_FILE_NAME $SERVER_CERT_INSTALL_PATH

COPY aerospike-dev.conf /etc/aerospike/

ARG TLS_PORT=4333
EXPOSE $TLS_PORT

HEALTHCHECK --interval=1s --timeout=1s CMD test -f /finalized

# Run our own set up steps after server finishes starting up
COPY entrypoint-finalize-setup.sh /
ENTRYPOINT ["/usr/bin/as-tini-static", "-r", "SIGUSR1", "-t", "SIGTERM", "--", "/entrypoint-finalize-setup.sh"]
