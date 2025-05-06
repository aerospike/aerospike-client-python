ARG CPU_ARCH=x86_64
FROM quay.io/pypa/manylinux_2_28_$CPU_ARCH
ARG OPENSSL_VERSION
LABEL com.aerospike.clients.openssl-version=$OPENSSL_VERSION

RUN yum install -y perl-core wget

WORKDIR /
ARG OPENSSL_TAR_NAME=openssl-$OPENSSL_VERSION
RUN wget https://www.openssl.org/source/$OPENSSL_TAR_NAME.tar.gz
RUN tar xzvf $OPENSSL_TAR_NAME.tar.gz
WORKDIR $OPENSSL_TAR_NAME

RUN ./Configure
RUN make
RUN make V=1 test
RUN make install
