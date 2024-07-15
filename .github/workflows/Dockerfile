ARG image
FROM $image
RUN echo -e "security {\n\tenable-quotas true\n}\n" >> /etc/aerospike/aerospike.template.conf
COPY security.smd /opt/aerospike/smd/
