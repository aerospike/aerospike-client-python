#!/bin/sh

set -e
set -x

cd aerospike-client-python/

yum install -y openssl-devel glibc-devel autoconf automake libtool zlib-devel openssl-devel python-devel
python3 -m pip install build
python3 -m build
python3 -m pip install dist/*.whl

cd test
python3 -m pip install -r requirements.txt
python3 -m pytest new_tests/

# Wheel install test
pip uninstall -y aerospike
python3 -m pip install aerospike
