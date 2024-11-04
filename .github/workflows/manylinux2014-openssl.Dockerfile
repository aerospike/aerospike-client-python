FROM quay.io/pypa/manylinux2014_x86_64
ARG MANYLINUX_OPENSSL_VERSION

RUN yum install -y perl-IPC-Cmd perl-Test-Simple wget

ARG OPENSSL_TAR_NAME=openssl-$MANYLINUX_OPENSSL_VERSION
RUN wget https://www.openssl.org/source/$OPENSSL_TAR_NAME.tar.gz
RUN tar xzvf $OPENSSL_TAR_NAME.tar.gz
WORKDIR $OPENSSL_TAR_NAME

RUN ./Configure --prefix=/opt/openssl --openssldir=/usr/local/ssl
RUN make
RUN make test
RUN make install
