#!/bin/bash

# Makes sure that if the "docker exec" command fails, it is not ignored
set -o pipefail
set -e
set -x

# We use bash because we need the not (!) operator

# Input defaults

BASE_IMAGE=${BASE_IMAGE:-"aerospike/aerospike-server-enterprise"}
# Server features
MUTUAL_TLS=${MUTUAL_TLS:-"0"}
SECURITY=${SECURITY:-"0"}
STRONG_CONSISTENCY=${STRONG_CONSISTENCY:-"0"}

# End inputs

aerospike_yaml_file_name=aerospike-dev.yaml
bind_mount_dest_folder=/workdir
aerospike_yaml_container_path=${bind_mount_dest_folder}/${aerospike_yaml_file_name}

call_from_yq_container() {
    # We set the container's user to our own because aerospike.yaml only has write permissions for the owning user
    # The container's default user is different from the host
    docker run --rm --user $(id -u):$(id -g) -v ./$aerospike_yaml_file_name:$aerospike_yaml_container_path mikefarah/yq "$1" -i $aerospike_yaml_container_path
}


if [[ "$MUTUAL_TLS" == "1" ]]; then
    call_from_yq_container ".network.service.tls-authenticate-client = \"any\""
    call_from_yq_container ".network.service.tls-name = \"docker\""
    call_from_yq_container ".network.service.tls-port = \"4333\""
    call_from_yq_container ".network.tls[0].ca-file = \"/etc/ssl/certs/ca.cer\""
    call_from_yq_container ".network.tls[0].cert-file = \"/etc/ssl/certs/server.cer\""
    call_from_yq_container ".network.tls[0].key-file = \"/etc/ssl/private/server.pem\""
    call_from_yq_container ".network.tls[0].name = \"docker\""
else
    # TODO: Make sure these are idempotent
    call_from_yq_container "del(.network.service.tls-authenticate-client)"
    call_from_yq_container "del(.network.service.tls-name)"
    call_from_yq_container "del(.network.service.tls-port)"
    call_from_yq_container "del(.network.tls)"
fi

if [[ "$SECURITY" == "1" ]]; then
    call_from_yq_container ".security.enable-quotas = \"true\""
    call_from_yq_container ".security.log.report-violation = \"true\""
else
    call_from_yq_container "del(.security)"
fi

if [[ "$STRONG_CONSISTENCY" == "1" ]]; then
    call_from_yq_container ".namespaces[0].strong-consistency = \"true\""
    call_from_yq_container ".namespaces[0].strong-consistency-allow-expunge = \"true\""
else
    call_from_yq_container ".namespaces[0].strong-consistency = \"false\""
    call_from_yq_container ".namespaces[0].strong-consistency-allow-expunge = \"false\""
fi

# We want to save our aerospike.conf in this directory.
call_from_tools_container="docker run --rm -v ./:$bind_mount_dest_folder --network host aerospike/aerospike-tools"

aerospike_conf_name=aerospike.conf
$call_from_tools_container asconfig convert -f $aerospike_yaml_container_path -o ${bind_mount_dest_folder}/$aerospike_conf_name
cat $aerospike_conf_name

# Generate server private key and CSR
# openssl req -newkey rsa:4096 -keyout server.pem -nodes -new -out server.csr -subj "/C=XX/ST=StateName/L=CityName/O=CompanyName/OU=CompanySectionName/CN=docker"
# Generate server cert
# openssl x509 -req -in server.csr -CA ca.cer -CAkey ca.pem -out server.cer

docker run -d --rm --name aerospike -p 4333:4333 -p 3000:3000 \
    -v ./ca.cer:/etc/ssl/certs/ca.cer \
    -v ./server.cer:/etc/ssl/certs/server.cer \
    -v ./server.pem:/etc/ssl/private/server.pem \
    -v ./$aerospike_conf_name:$bind_mount_dest_folder/$aerospike_conf_name \
    $BASE_IMAGE --config-file $bind_mount_dest_folder/$aerospike_conf_name

if [[ "$SECURITY" == "1" ]]; then
    export SECURITY_FLAGS="-U admin -P admin"
fi

# docker logs -f aerospike

# TODO: make sure server container is alive or waiting is pointless
timeout 30s bash ./wait-for-as-server-to-start.bash

# Set up security
superuser_name_and_password=superuser
if [[ "$SECURITY" == "1" ]]; then
    $call_from_tools_container asadm $SECURITY_FLAGS --enable --execute "manage acl create user $superuser_name_and_password password $superuser_name_and_password roles read-write-udf, sys-admin, user-admin, data-admin"
fi

# Strong consistency
# Set up roster
if [[ "$STRONG_CONSISTENCY" == "1" ]]; then
    if [[ "$SECURITY" == "1" ]]; then
        SECURITY_FLAGS="-U $superuser_name_and_password -P $superuser_name_and_password"
    fi
    $call_from_tools_container asadm $SECURITY_FLAGS --enable --execute "manage roster stage observed ns test"
    $call_from_tools_container asadm $SECURITY_FLAGS --enable --execute "manage recluster"
fi
