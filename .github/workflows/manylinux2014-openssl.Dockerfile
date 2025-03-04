ARG CPU_ARCH=x86_64
FROM quay.io/pypa/manylinux2014_$CPU_ARCH
ARG OPENSSL_VERSION
LABEL com.aerospike.clients.openssl-version=$OPENSSL_VERSION

RUN yum install -y perl-core wget

WORKDIR /
ARG OPENSSL_TAR_NAME=openssl-$OPENSSL_VERSION
RUN wget https://www.openssl.org/source/$OPENSSL_TAR_NAME.tar.gz
RUN tar xzvf $OPENSSL_TAR_NAME.tar.gz
WORKDIR $OPENSSL_TAR_NAME

# The default folder pointed to by --prefix contains a default openssl installation
# But we're assuming it's fine to replace the default openssl that comes with the image
# We aren't going to use this image in production, anyways
RUN ./Configure
RUN make
# These tests are expected to fail because we are using a buggy version of nm
# https://github.com/openssl/openssl/issues/18953
# devtoolset-11 contains a newer version of binutils 2.36, which contains a bug fix for nm
# We don't use it though because we want to make sure the compiled openssl 3 library is compatible with manylinux2014's
# default env
RUN make V=1 TESTS='-test_symbol_presence*' test
RUN make install
