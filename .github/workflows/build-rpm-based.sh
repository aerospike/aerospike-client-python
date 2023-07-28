#!/bin/sh

set -e
set -x

# Executed inside the aerospike-client-python directory
yum install -y openssl-devel glibc-devel autoconf automake libtool zlib-devel openssl-devel python-devel
# Amazon Linux 2023's default python doesn't come with pip
python3 -m ensurepip
python3 -m pip install -r requirements.txt
python3 -m build
python3 -m pip install dist/*.whl

cd test
python3 -m pip install -r requirements.txt
python3 -m pytest new_tests/
