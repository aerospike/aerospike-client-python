#!/bin/bash

# Makes sure that if the "docker exec" command fails, it is not ignored
set -o pipefail
set -e
set -x

# We use bash because we need the not (!) operator

AEROSPIKE_YAML_PATH=/workdir/aerospike-dev.yaml

CALL_FROM_YQ_CONTAINER() {
    docker run --rm -v ./:/workdir mikefarah/yq "$1" -i $AEROSPIKE_YAML_PATH
    # cat aerospike-dev.yaml
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

CALL_FROM_TOOLS_CONTAINER="docker run --rm -v ./:/workdir --network host aerospike/aerospike-tools"

$CALL_FROM_TOOLS_CONTAINER asconfig convert -f $AEROSPIKE_YAML_PATH -o /workdir/aerospike.conf
cat aerospike.conf

# Generate server private key and CSR
# openssl req -newkey rsa:4096 -keyout server.pem -nodes -new -out server.csr -subj "/C=XX/ST=StateName/L=CityName/O=CompanyName/OU=CompanySectionName/CN=docker"
# Generate server cert
# openssl x509 -req -in server.csr -CA ca.cer -CAkey ca.pem -out server.cer

docker run -d --rm --name aerospike -p 4333:4333 -p 3000:3000 \
    -v ./ca.cer:/etc/ssl/certs/ca.cer \
    -v ./server.cer:/etc/ssl/certs/server.cer \
    -v ./server.pem:/etc/ssl/private/server.pem \
    -v ./aerospike.conf:/custom-dir/aerospike.conf \
    aerospike/aerospike-server-enterprise --config-file /custom-dir/aerospike.conf

if [[ "$SECURITY" == "1" ]]; then
    export SECURITY_FLAGS="-U admin -P admin"
fi

# docker logs -f aerospike

# TODO: make sure server container is alive or waiting is pointless
./wait-for-as-server-to-start.bash

# Set up security
if [[ "$SECURITY" == "1" ]]; then
    $CALL_FROM_TOOLS_CONTAINER asadm $SECURITY_FLAGS --enable --execute "manage acl create user superuser password superuser roles read-write-udf, sys-admin, user-admin, data-admin"
fi

# Strong consistency
# Set up roster
if [[ "$STRONG_CONSISTENCY" == "1" ]]; then
    if [[ "$SECURITY" == "1" ]]; then
        export SECURITY_FLAGS="-U superuser -P superuser"
    fi
    $CALL_FROM_TOOLS_CONTAINER asadm $SECURITY_FLAGS --enable --execute "manage roster stage observed ns test"
    $CALL_FROM_TOOLS_CONTAINER asadm $SECURITY_FLAGS --enable --execute "manage recluster"
fi
