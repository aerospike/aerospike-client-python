#!/bin/bash

# Compile wheels
for PYBIN in /opt/python/*/bin; do
    "${PYBIN}/pip" wheel ./ -w /work/tempwheels
done

# Bundle external shared libraries into the wheels
for whl in /work/tempwheels/*.whl; do
    auditwheel repair "$whl" --plat manylinux2010_x86_64 -w /work/wheels/
done

for PYBIN in /opt/python/*/bin/; do
    ${PYBIN}/pip install aerospike -f /work/wheels/
    ${PYBIN}/python -c "import aerospike; print('Installed aerospike version{}'.format(aerospike.__version__))"
done
