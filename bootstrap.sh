#!/usr/bin/env bash

# Download and install aerospike server
wget -O aerospike.tgz https://download.aerospike.com/artifacts/aerospike-server-community/6.1.0.1/aerospike-server-community-6.1.0.1-ubuntu20.04.tgz
tar -xvf aerospike.tgz
cd aerospike-server-community-6.1.0.1-ubuntu20.04/
sudo ./asinstall
# Configure aerospike server to run tests
sed -Ei "s/^namespace test.*$/namespace test {\n\tdefault-ttl 3d\n\tallow-ttl-without-nsup true/" /etc/aerospike/aerospike.conf
sudo systemctl start aerospike
