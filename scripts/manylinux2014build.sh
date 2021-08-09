# Compile wheels

echo $PYTHONS
IFS=',' read -ra ADDR <<< "$PYTHONS"

for i in "${ADDR[@]}"; do
    echo "compiling  "${i}/pip wheel ./ -w /work/tempwheels" ..."
    "${i}/pip" wheel ./ -w /work/tempwheels
done

# Bundle external shared libraries into the wheels
for whl in /work/tempwheels/*.whl; do
    auditwheel repair "$whl" --plat manylinux2014_x86_64 -w /work/wheels/
done

for i in "${ADDR[@]}"; do
    ${i}/pip install aerospike -f /work/wheels/
    ${i}/python -c "import aerospike; print('Installed aerospike version{}'.format(aerospike.__version__))"
done

echo "Building wheel $PYTHONS are done"
