# Compile wheels

echo $PYTHONS
IFS=',' read -ra ADDR <<< "$PYTHONS"

echo $SANITIZED_PYTHONS
IFS=',' read -ra ADDR2 <<< "$SANITIZED_PYTHONS"

ARCH=`uname -m`

# build sanitized wheels
for i in "${ADDR2[@]}"; do
    echo "compiling  "${i}/pip wheel ./ -w /work/tempsanwheels" ..."
    env SANITIZER=1 "${i}/pip" wheel ./ -w /work/tempsanwheels
done

# build non-sanitized wheels
for i in "${ADDR[@]}"; do
    echo "compiling  "${i}/pip wheel ./ -w /work/tempwheels" ..."
    "${i}/pip" wheel ./ -w /work/tempwheels
done

# Bundle external shared libraries into the non-sanitized wheels
for whl in /work/tempwheels/*.whl; do
    auditwheel repair "$whl" --plat manylinux2014_$ARCH -w /work/wheels/
done

# Bundle external shared libraries into the sanitized wheels
for whl in /work/tempsanwheels/*.whl; do
    auditwheel repair "$whl" --plat manylinux2014_$ARCH -w /work/sanwheels/
done

# tar sanitized wheels to prevent conflict with non-sanitized
tar -cvf /work/wheels/sanwheels.tgz -C /work/sanwheels/ .

for i in "${ADDR[@]}"; do
    ${i}/pip uninstall -y aerospike
    ${i}/pip install aerospike -f /work/wheels/
    ${i}/python -c "import aerospike; print('Installed aerospike version{}'.format(aerospike.__version__))"
done

echo "Building wheel $PYTHONS are done"

