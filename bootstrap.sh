#!/usr/bin/env bash

test -e /vagrant_data/*.deb
is_using_server_rc=$?
if [[ $is_using_server_rc == 1 ]]; then
    wget -r --no-parent -l1 --accept 'aerospike-server-community_*_ubuntu22.04_x86_64.tgz' --no-directories https://download.aerospike.com/artifacts/aerospike-server-community/latest/
    tar -xvf *.tgz

    fileName=$(ls *.tgz)
    folderName=${fileName%.*}
    cd $folderName

    ./asinstall
else
    mv /vagrant_data/*.deb .
    dpkg -i *.deb
fi

# Configure aerospike server to run tests
# asinfo doesn't come with QE builds
sed -i "s/namespace test {/namespace test {\n\tallow-ttl-without-nsup true/" /etc/aerospike/aerospike.conf

systemctl start aerospike
