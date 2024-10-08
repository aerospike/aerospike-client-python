ARG image
FROM $image

WORKDIR /opt/aerospike/smd

# Enable authentication
RUN echo -e "security {\n\tenable-quotas true\n}\n" >> /etc/aerospike/aerospike.template.conf
# security.smd was generated manually by
# 1. Starting a new Aerospike EE server using Docker
# 2. Creating the superuser user
# 3. Copying /opt/aerospike/smd/security.smd from the container and committing it to this repo
# This file should always work
# TODO: generate this automatically, somehow
COPY security.smd .

# Enable strong consistency
RUN sed -i "s/\(namespace.*{\)/\1\n\tstrong-consistency true/" /etc/aerospike/aerospike.template.conf
RUN sed -i "s/\(namespace.*{\)/\1\n\tstrong-consistency-allow-expunge true/" /etc/aerospike/aerospike.template.conf
ENV ROSTER_FILE_NAME=roster.smd
COPY $ROSTER_FILE_NAME .
RUN apt update
RUN apt-get install -y jq
RUN sed -i "s/\(^service {\)/\1\n\tnode-id $(jq --raw-output '.[1].value' $ROSTER_FILE_NAME)/" /etc/aerospike/aerospike.template.conf
# TODO: uninstall jq
