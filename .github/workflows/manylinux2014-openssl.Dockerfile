FROM quay.io/pypa/manylinux2014_x86_64
ARG OPENSSL_VERSION

# https://computingforgeeks.com/how-to-install-openssl-3-x-on-centos-rhel-7/
RUN yum -y groupinstall "Development Tools"
RUN yum install -y perl-IPC-Cmd perl-Test-Simple wget

ARG OPENSSL_TAR_NAME=openssl-$OPENSSL_VERSION
RUN wget https://www.openssl.org/source/$OPENSSL_TAR_NAME.tar.gz
RUN tar xzvf $OPENSSL_TAR_NAME.tar.gz
WORKDIR $OPENSSL_TAR_NAME

RUN ./Configure
RUN make
RUN make test
RUN make install
