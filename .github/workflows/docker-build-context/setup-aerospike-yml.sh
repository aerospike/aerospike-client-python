set -x

CONFIG_YAML_PATH=/workdir/aerospike-dev.yaml

yq eval ".namespaces[0].strong-consistency = $STRONG_CONSISTENCY" -i $CONFIG_YAML_PATH
yq eval ".namespaces[0].strong-consistency-allow-expunge = $STRONG_CONSISTENCY" -i $CONFIG_YAML_PATH

yq eval ".security.enable-quotas = $SECURITY" -i $CONFIG_YAML_PATH
yq eval ".security.log.report-violation = $SECURITY" -i $CONFIG_YAML_PATH

# if [ "$MUTUAL_TLS" == "1" ]; then
  # yq eval '.network.tls[0].name = docker' -i $CONFIG_YAML_PATH
  # TODO
  # yq eval 'network.tls[0].ca-file = ""' -i $CONFIG_YAML_PATH
  # yq eval 'network.tls[0].cert-file = ""' -i $CONFIG_YAML_PATH
  # yq eval 'network.tls[0].key-file = ""' -i $CONFIG_YAML_PATH
# fi
