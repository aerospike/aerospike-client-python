ARG CPU_ARCH=x86_64
FROM quay.io/pypa/manylinux2014_$CPU_ARCH
ARG OPENSSL_VERSION
LABEL com.aerospike.clients.openssl-version=$OPENSSL_VERSION

RUN yum install -y perl-core wget

# devtoolset-11 contains a newer version of binutils 2.36, which contains a bug fix for missing symbols
# We don't use it though because we want to make sure the compiled openssl 3 library is compatible with manylinux2014's
# default env

ARG OPENSSL_TAR_NAME=openssl-$OPENSSL_VERSION
RUN wget https://www.openssl.org/source/$OPENSSL_TAR_NAME.tar.gz
RUN tar xzvf $OPENSSL_TAR_NAME.tar.gz
WORKDIR $OPENSSL_TAR_NAME

# The default folder pointed to by --prefix contains a default openssl installation
ARG OPENSSL_INSTALL_DIR=/opt/openssl3
LABEL com.aerospike.clients.openssl-install-dir=$OPENSSL_INSTALL_DIR

RUN ./Configure --prefix=$OPENSSL_INSTALL_DIR --openssldir=/etc/opt/openssl3
RUN make
# These tests are expected to fail because we are using a buggy version of nm
# https://github.com/openssl/openssl/issues/18953
RUN make V=1 TESTS='-test_symbol_presence*' test
RUN make install
