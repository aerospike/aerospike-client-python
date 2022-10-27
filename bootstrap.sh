#!/usr/bin/env bash

# Download and install aerospike server
wget -O aerospike.tgz https://download.aerospike.com/artifacts/aerospike-server-community/6.1.0.1/aerospike-server-community-6.1.0.1-ubuntu20.04.tgz
tar -xvf aerospike.tgz
cd aerospike-server-community-6.1.0.1-ubuntu20.04/
sudo ./asinstall
sudo systemctl start aerospike
# Configure aerospike server to run tests
asinfo -v "set-config:context=namespace;id=test;allow-ttl-without-nsup=true"
