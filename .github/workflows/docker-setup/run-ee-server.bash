#!/bin/bash

# Makes sure that if the "docker exec" command fails, it is not ignored
set -o pipefail
set -e
set -x

# We use bash because we need the not (!) operator

AEROSPIKE_YAML_FILE_NAME=aerospike-dev.yaml
BIND_MOUNT_DEST_FOLDER=/workdir
AEROSPIKE_YAML_CONTAINER_PATH=${BIND_MOUNT_DEST_FOLDER}/${AEROSPIKE_YAML_FILE_NAME}

CALL_FROM_YQ_CONTAINER() {
    docker run --rm -v ./$AEROSPIKE_YAML_FILE_NAME:$AEROSPIKE_YAML_CONTAINER_PATH mikefarah/yq "$1" -i $AEROSPIKE_YAML_CONTAINER_PATH
}


if [[ "$MUTUAL_TLS" == "1" ]]; then
    CALL_FROM_YQ_CONTAINER ".network.service.tls-authenticate-client = \"any\""
    CALL_FROM_YQ_CONTAINER ".network.service.tls-name = \"docker\""
    CALL_FROM_YQ_CONTAINER ".network.service.tls-port = \"4333\""
    CALL_FROM_YQ_CONTAINER ".network.tls[0].ca-file = \"/etc/ssl/certs/ca.cer\""
    CALL_FROM_YQ_CONTAINER ".network.tls[0].cert-file = \"/etc/ssl/certs/server.cer\""
    CALL_FROM_YQ_CONTAINER ".network.tls[0].key-file = \"/etc/ssl/private/server.pem\""
    CALL_FROM_YQ_CONTAINER ".network.tls[0].name = \"docker\""
else
    # TODO: Make sure these are idempotent
    CALL_FROM_YQ_CONTAINER "del(.network.service.tls-authenticate-client)"
    CALL_FROM_YQ_CONTAINER "del(.network.service.tls-name)"
    CALL_FROM_YQ_CONTAINER "del(.network.service.tls-port)"
    CALL_FROM_YQ_CONTAINER "del(.network.tls)"
fi

if [[ "$SECURITY" == "1" ]]; then
    CALL_FROM_YQ_CONTAINER ".security.enable-quotas = \"true\""
    CALL_FROM_YQ_CONTAINER ".security.log.report-violation = \"true\""
else
    CALL_FROM_YQ_CONTAINER "del(.security)"
fi

if [[ "$STRONG_CONSISTENCY" == "1" ]]; then
    CALL_FROM_YQ_CONTAINER ".namespaces[0].strong-consistency = \"true\""
    CALL_FROM_YQ_CONTAINER ".namespaces[0].strong-consistency-allow-expunge = \"true\""
else
    CALL_FROM_YQ_CONTAINER ".namespaces[0].strong-consistency = \"false\""
    CALL_FROM_YQ_CONTAINER ".namespaces[0].strong-consistency-allow-expunge = \"false\""
fi

# We want to save our aerospike.conf in this directory.
CALL_FROM_TOOLS_CONTAINER="docker run --rm -v ./:$BIND_MOUNT_DEST_FOLDER --network host aerospike/aerospike-tools"

AEROSPIKE_CONF_NAME=aerospike.conf
$CALL_FROM_TOOLS_CONTAINER asconfig convert -f $AEROSPIKE_YAML_CONTAINER_PATH -o ${BIND_MOUNT_DEST_FOLDER}/$AEROSPIKE_CONF_NAME
cat $AEROSPIKE_CONF_NAME

# Generate server private key and CSR
# openssl req -newkey rsa:4096 -keyout server.pem -nodes -new -out server.csr -subj "/C=XX/ST=StateName/L=CityName/O=CompanyName/OU=CompanySectionName/CN=docker"
# Generate server cert
# openssl x509 -req -in server.csr -CA ca.cer -CAkey ca.pem -out server.cer

BASE_IMAGE=${BASE_IMAGE:-"aerospike/aerospike-server-enterprise"}

docker run -d --rm --name aerospike -p 4333:4333 -p 3000:3000 \
    -v ./ca.cer:/etc/ssl/certs/ca.cer \
    -v ./server.cer:/etc/ssl/certs/server.cer \
    -v ./server.pem:/etc/ssl/private/server.pem \
    -v ./$AEROSPIKE_CONF_NAME:$BIND_MOUNT_DEST_FOLDER/$AEROSPIKE_CONF_NAME \
    $BASE_IMAGE --config-file $BIND_MOUNT_DEST_FOLDER/$AEROSPIKE_CONF_NAME

if [[ "$SECURITY" == "1" ]]; then
    export SECURITY_FLAGS="-U admin -P admin"
fi

# docker logs -f aerospike

# TODO: make sure server container is alive or waiting is pointless
./wait-for-as-server-to-start.bash

# Set up security
SUPERUSER_NAME_AND_PASSWORD=superuser
if [[ "$SECURITY" == "1" ]]; then
    $CALL_FROM_TOOLS_CONTAINER asadm $SECURITY_FLAGS --enable --execute "manage acl create user $SUPERUSER_NAME_AND_PASSWORD password $SUPERUSER_NAME_AND_PASSWORD roles read-write-udf, sys-admin, user-admin, data-admin"
fi

# Strong consistency
# Set up roster
if [[ "$STRONG_CONSISTENCY" == "1" ]]; then
    if [[ "$SECURITY" == "1" ]]; then
        export SECURITY_FLAGS="-U $SUPERUSER_NAME_AND_PASSWORD -P $SUPERUSER_NAME_AND_PASSWORD"
    fi
    $CALL_FROM_TOOLS_CONTAINER asadm $SECURITY_FLAGS --enable --execute "manage roster stage observed ns test"
    $CALL_FROM_TOOLS_CONTAINER asadm $SECURITY_FLAGS --enable --execute "manage recluster"
fi
