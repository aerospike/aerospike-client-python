#!/bin/sh

set -e
set -x

# Update packages to get latest git version
yum update -y
yum install -y git openssl-devel glibc-devel autoconf automake libtool zlib-devel openssl-devel python-devel

git clone --branch $2 --recurse-submodules https://github.com/$1/aerospike-client-python
cd aerospike-client-python/

python3 -m pip install build
python3 -m build
python3 -m pip install dist/*.whl

cd test
python3 -m pip install -r requirements.txt
python3 -m pytest new_tests/

# Wheel install test
pip uninstall -y aerospike
python3 -m pip install aerospike
