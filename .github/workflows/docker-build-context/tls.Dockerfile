ARG SERVER_KEY_FILE_NAME=server.pem
ARG SERVER_CERT_FILE_NAME=server.cer
# Cluster name fetched from stage to generate cert also needs to be set in aerospike.conf in our final image
ARG CLUSTER_NAME_FILE_NAME=cluster_name

FROM base AS generate-server-cert

RUN apt update
RUN apt install -y openssl

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

FROM base as configure-tls

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
# Cleanup
RUN rm $CLUSTER_NAME_FILE_NAME

RUN sed -i "s|\(^\tservice {\)|\1\n\t\ttls-authenticate-client false|" $AEROSPIKE_CONF_TEMPLATE_PATH

ARG TLS_PORT=4333
RUN sed -i "s|\(^\tservice {\)|\1\n\t\ttls-port $TLS_PORT|" $AEROSPIKE_CONF_TEMPLATE_PATH
EXPOSE $TLS_PORT

RUN sed -i "s|\(^network {\)|\1\n\ttls $(cat $CLUSTER_NAME_FILE_NAME) {\n\t\tcert-file $SERVER_CERT_ABS_PATH\n\t}|" $AEROSPIKE_CONF_TEMPLATE_PATH
RUN sed -i "s|\(^\ttls $(cat $CLUSTER_NAME_FILE_NAME) {\)|\1\n\t\tkey-file $SERVER_KEY_ABS_PATH|" $AEROSPIKE_CONF_TEMPLATE_PATH
