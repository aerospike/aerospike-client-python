ARG rc=""
ARG tag=latest
FROM aerospike/aerospike-server-enterprise$rc:$tag
RUN echo -e "security {\n\tenable-quotas true\n}\n" >> /etc/aerospike/aerospike.template.conf
