#!/bin/sh

set -e
set -x

apk add py3-pip python3-dev zlib-dev automake make musl-dev gcc openssl-dev lua-dev libuv-dev doxygen graphviz

# Executed inside the aerospike-client-python directory
python3 -m pip install -r requirements.txt
python3 -m build
python3 -m pip install dist/*.whl

cd test/
python3 -m pip install -r requirements.txt
python3 -m pytest new_tests/
