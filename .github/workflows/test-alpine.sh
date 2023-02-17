#!/bin/sh

apk add git py3-pip python3-dev zlib-dev automake make musl-dev gcc openssl-dev lua-dev libuv-dev doxygen graphviz
git clone --branch $1 --recurse-submodules https://github.com/aerospike/aerospike-client-python

cd aerospike-client-python/
python3 -m pip install build
python3 -m build
python3 -m pip install dist/*.whl

cd test/
python3 -m pip install pytest
python3 -m pytest new_tests/
