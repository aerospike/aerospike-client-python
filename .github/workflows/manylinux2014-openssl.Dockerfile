FROM quay.io/pypa/manylinux2014_x86_64
ARG OPENSSL_VERSION

# https://computingforgeeks.com/how-to-install-openssl-3-x-on-centos-rhel-7/
RUN yum install -y centos-release-scl devtoolset-11 perl-Text-Template.noarch perl-IPC-Cmd perl-Test-Simple wget
# devtoolset-11 contains a newer version of binutils 2.36, which contains a bug fix for missing symbols
SHELL ["/usr/bin/scl", "enable", "devtoolset-11"]

ARG OPENSSL_TAR_NAME=openssl-$OPENSSL_VERSION
RUN wget https://www.openssl.org/source/$OPENSSL_TAR_NAME.tar.gz
RUN tar xzvf $OPENSSL_TAR_NAME.tar.gz
WORKDIR $OPENSSL_TAR_NAME

RUN ./Configure --prefix=/opt/openssl3 --openssldir=/usr/local/ssl3
RUN make
RUN make test
RUN make install
