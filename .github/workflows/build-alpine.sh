#!/bin/sh

set -e
set -x

apk add py3-pip python3-dev zlib-dev automake make musl-dev gcc openssl-dev lua-dev libuv-dev doxygen graphviz
cd aerospike-client-python/

python3 -m pip install build
python3 -m build
python3 -m pip install dist/*.whl

cd test/
python3 -m pip install pytest
python3 -m pytest new_tests/
