FROM quay.io/pypa/manylinux_2_28
ARG OPENSSL_VERSION
LABEL com.aerospike.clients.openssl-version=$OPENSSL_VERSION

# https://computingforgeeks.com/how-to-install-openssl-3-x-on-centos-rhel-7/
RUN yum install -y perl-Text-Template.noarch perl-IPC-Cmd perl-Test-Simple wget

# devtoolset-11 contains a newer version of binutils 2.36, which contains a bug fix for missing symbols
# We don't use it though because we want to make sure the compiled openssl 3 library is compatible with manylinux2014's
# default env

ARG OPENSSL_TAR_NAME=openssl-$OPENSSL_VERSION
RUN wget https://www.openssl.org/source/$OPENSSL_TAR_NAME.tar.gz
RUN tar xzvf $OPENSSL_TAR_NAME.tar.gz
WORKDIR $OPENSSL_TAR_NAME

# The default folder pointed to by --prefix contains a default openssl installation
ARG OPENSSL_INSTALL_DIR=/opt/openssl3
RUN ./Configure --prefix=$OPENSSL_INSTALL_DIR --openssldir=/etc/opt/openssl3
RUN make
# There are 2 expected tests to fail
RUN make test
RUN make install
RUN ln -s $OPENSSL_INSTALL_DIR/lib64/libssl.so.3 /usr/local/lib64/libssl.so.3
RUN ln -s $OPENSSL_INSTALL_DIR/lib64/libcrypto.so.3 /usr/local/lib64/libcrypto.so.3
