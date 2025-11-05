ARG SERVER_IMAGE=aerospike/aerospike-server-enterprise

FROM $SERVER_IMAGE AS shared

# Define our variables once, here.
# Shared between build stages to generate the server certificate and to build final server image

# Use our own conf file for the final server image.
# The entrypoint script will use these templates to generate conf files
ARG AEROSPIKE_CONF_FILE_NAME=aerospike-dev.conf.jinja
# Our entrypoint script needs to pass this custom aerospike conf to the server binary
ENV AEROSPIKE_CONF_PATH=/etc/aerospike/$AEROSPIKE_CONF_FILE_NAME
COPY $AEROSPIKE_CONF_FILE_NAME $AEROSPIKE_CONF_PATH

ARG SERVER_KEY_FILE_NAME=server.pem
ARG SERVER_CERT_FILE_NAME=server.cer

FROM shared AS enable-security

WORKDIR /opt/aerospike/smd

# security.smd was generated manually by
# 1. Starting a new Aerospike EE server using Docker
# 2. Creating the superuser user
# 3. Copying /opt/aerospike/smd/security.smd from the container and committing it to this repo
# This file should always work
# TODO: generate this automatically, somehow.
COPY security.smd .

# Do this so we don't have to pass in user credentials every time we use Aerospike tools in the entrypoint script
COPY astools.conf.jinja /etc/aerospike/

FROM enable-security AS enable-strong-consistency

COPY roster.smd .

# Use a separate build stage to generate certs since we don't want openssl bundled in the final image

FROM shared AS generate-server-cert-for-tls

RUN apt update
RUN apt install -y openssl

# Need to pass cluster name to openssl command
ARG CLUSTER_NAME_FILE_NAME=cluster_name
RUN grep -Eo "cluster-name [a-z]+" $AEROSPIKE_CONF_PATH | awk '{print $2}' > $CLUSTER_NAME_FILE_NAME

# Generate server private key and CSR

ARG SERVER_CSR_FILE_NAME=server.csr
RUN openssl req -newkey rsa:4096 -keyout $SERVER_KEY_FILE_NAME -nodes -new -out $SERVER_CSR_FILE_NAME -subj "/C=XX/ST=StateName/L=CityName/O=CompanyName/OU=CompanySectionName/CN=$(cat $CLUSTER_NAME_FILE_NAME)"

# Send CSR to CA. CA will output a server certificate
# We use an external CA because we want the client to use that same CA to verify the server certificate when connecting
# via TLS.
# By default we use the "dummy" CA cert and key that are committed in the repo
ARG CA_KEY_FILE_NAME=ca.pem
ARG CA_CERT_FILE_NAME=ca.cer
COPY $CA_KEY_FILE_NAME .
COPY $CA_CERT_FILE_NAME .
RUN openssl x509 -req -in $SERVER_CSR_FILE_NAME -CA $CA_CERT_FILE_NAME -CAkey $CA_KEY_FILE_NAME -out $SERVER_CERT_FILE_NAME

FROM enable-strong-consistency AS enable-tls

ARG SSL_WORKING_DIR=/etc/ssl
WORKDIR $SSL_WORKING_DIR
ARG SERVER_KEY_INSTALL_PATH=$SSL_WORKING_DIR/private/$SERVER_KEY_FILE_NAME
ARG SERVER_CERT_INSTALL_PATH=$SSL_WORKING_DIR/certs/$SERVER_CERT_FILE_NAME
ARG CA_CERT_INSTALL_PATH=$SSL_WORKING_DIR/certs/$CA_CERT_FILE_NAME

COPY --from=generate-server-cert-for-tls $SERVER_KEY_FILE_NAME $SERVER_KEY_INSTALL_PATH
COPY --from=generate-server-cert-for-tls $SERVER_CERT_FILE_NAME $SERVER_CERT_INSTALL_PATH
COPY --from=generate-server-cert-for-tls $CA_CERT_FILE_NAME $CA_CERT_INSTALL_PATH

# User can set their own TLS port if they want
ARG TLS_PORT=4333
EXPOSE $TLS_PORT

# Could not figure out how to stop Windows self-hosted runners
# from changing line endings for wait-for-as-server-to-start.bash.
# .gitattributes does not work, neither does changing git-config settings.
# This should work for now.
FROM ubuntu:24.04 AS dos2unix

# Run our own set up steps after server finishes starting up
WORKDIR /my-scripts
COPY entrypoint-finalize-setup.bash wait-for-as-server-to-start.bash .
# Both Github Actions and the entrypoint script can reuse this wait script
RUN apt update
RUN apt install -y dos2unix
RUN dos2unix *.bash

FROM enable-tls AS finish-setup

# We need to install jinja2 in our server image
RUN apt update
# Verified that installing these should not interfere with the server
# There are a few packages being upgraded with this command, but it should be ok
RUN apt install -y python3 python3-pip python3-venv
WORKDIR /
# We use a venv to handle newer Python versions (3.12+)
# Those versions require a flag to "break system packages" when installing pip packages directly to the system
RUN python3 -m venv .venv
ENV PATH="/.venv/bin:$PATH"
RUN python3 -m pip install jinja2
COPY toggle-features-in-conf-files.py .

ENV HEALTHCHECK_FILE_PATH=/finalized
HEALTHCHECK --interval=1s --timeout=1s CMD test -f $HEALTHCHECK_FILE_PATH

COPY --from=dos2unix /my-scripts/* .

# Entrypoint is in root dir
ENTRYPOINT ["/usr/bin/as-tini-static", "-r", "SIGUSR1", "-t", "SIGTERM", "--", "/entrypoint-finalize-setup.bash"]
