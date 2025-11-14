#!/bin/bash

# Makes sure that if the "docker exec" command fails, it is not ignored
set -o pipefail
set -e
set -x

# Prevent Windows shell from mangling paths
export MSYS_NO_PATHCONV=1

# Input defaults

BASE_IMAGE=${BASE_IMAGE:-"aerospike/aerospike-server-enterprise"}
# Server features
MUTUAL_TLS=${MUTUAL_TLS:-"0"}
SECURITY=${SECURITY:-"0"}
STRONG_CONSISTENCY=${STRONG_CONSISTENCY:-"0"}

# End inputs

VOLUME_NAME=aerospike-conf-vol
docker volume create $VOLUME_NAME

volume_dest_folder=/workdir
aerospike_yaml_file_name=aerospike-dev.yaml
ca_cert_file_name="ca.cer"
server_cert_file_name="server.cer"

container_name_for_populating_volume=container_for_populating_volume
docker run --name $container_name_for_populating_volume --rm -v $VOLUME_NAME:$volume_dest_folder -d alpine tail -f /dev/null
docker cp ./ $container_name_for_populating_volume:$volume_dest_folder
docker stop $container_name_for_populating_volume

aerospike_yaml_container_path=${volume_dest_folder}/${aerospike_yaml_file_name}

call_from_yq_container() {
    docker run --rm --user $(id -u):$(id -g) -v $VOLUME_NAME:$volume_dest_folder mikefarah/yq "$1" -i $aerospike_yaml_container_path
}

# del() operations are idempotent
if [[ "$MUTUAL_TLS" == "1" ]]; then
    call_from_yq_container ".network.service.tls-authenticate-client = \"any\""
    call_from_yq_container ".network.service.tls-name = \"docker\""
    call_from_yq_container ".network.service.tls-port = \"4333\""
    call_from_yq_container ".network.tls[0].ca-file = \"$volume_dest_folder/$ca_cert_file_name\""
    call_from_yq_container ".network.tls[0].cert-file = \"$volume_dest_folder/$server_cert_file_name\""
    call_from_yq_container ".network.tls[0].key-file = \"$volume_dest_folder/server.pem\""
    call_from_yq_container ".network.tls[0].name = \"docker\""
else
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
call_from_tools_container="docker run --rm -v $VOLUME_NAME:$volume_dest_folder --network host aerospike/aerospike-tools"

aerospike_conf_name=aerospike.conf
$call_from_tools_container asconfig convert -f $aerospike_yaml_container_path -o ${volume_dest_folder}/$aerospike_conf_name

# Generate server private key and CSR
# openssl req -newkey rsa:4096 -keyout server.pem -nodes -new -out server.csr -subj "/C=XX/ST=StateName/L=CityName/O=CompanyName/OU=CompanySectionName/CN=docker"
# Generate server cert
# openssl x509 -req -in server.csr -CA ca.cer -CAkey ca.pem -out server.cer

# Some Docker container may have a low max fd limit
# Just set max fd limit to make sure the server doesn't fail.
# Somehow the container can have a lower soft/hard fd limit than the Docker daemon / host
docker run --ulimit nofile=15000 -d --rm --name aerospike -p 4333:4333 -p 3000:3000 \
    -v $VOLUME_NAME:$volume_dest_folder \
    $BASE_IMAGE --config-file $volume_dest_folder/$aerospike_conf_name

if [[ "$SECURITY" == "1" ]]; then
    export SECURITY_FLAGS="-U admin -P admin"
fi

# docker logs -f aerospike

# TODO: make sure server container is alive or waiting is pointless
./wait-for-as-server-to-start.bash

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
