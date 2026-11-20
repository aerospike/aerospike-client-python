"""L3 discovery test of TLS against an Aerospike cluster on the Aerospike Kubernetes Operator, where
every pod mounts one shared certificate secret, set up by the setup-kind-ako action with enable-tls.
The certificate names the cluster tls-name, not the endpoint hostname the client dials, so the
client must validate each node against the tls-name the cluster advertises.
"""

import os

import pytest
from cryptography import x509

import aerospike
import discovery_info as di
import l3_cluster as l3

SET_NAME = "l3-tls-name"
MIN_PODS = 2


def assert_setup(cluster: l3.Cluster, tls_name: str, endpoint: str) -> None:
    """Assert the cluster serves TLS under tls_name with TLS alternate addresses on at least
    MIN_PODS pods, and that the certificate in the secret mounted where the tls-name's cert-file
    lives has exactly tls_name as its dNSName SANs and no SAN of any type for the endpoint."""
    assert cluster.get(".spec.aerospikeConfig.network.service['tls-name']") == tls_name, "AerospikeCluster tls-name"
    alternate_access = cluster.get(".spec.aerospikeNetworkPolicy.alternateAccess")
    assert cluster.get(".spec.aerospikeNetworkPolicy.tlsAlternateAccess") == alternate_access
    assert cluster.size() >= MIN_PODS, f"K-6 needs at least {MIN_PODS} pods"

    cert_file = cluster.get(f".spec.aerospikeConfig.network.tls[?(@.name==\"{tls_name}\")]['cert-file']")
    directory, _, file_name = cert_file.rpartition("/")
    assert directory, f"cert-file {cert_file}"
    source = cluster.get(f".spec.storage.volumes[?(@.aerospike.path==\"{directory}\")].source.secret.secretName")
    assert source, f"No secret volume at {directory}"

    pem = cluster.mounted_certificate(source, file_name)
    certificate = x509.load_pem_x509_certificate(pem)
    sans = certificate.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
    dns_names = sans.get_values_for_type(x509.DNSName)
    print(f"L3-TLS-CERT secret={cluster.namespace}/{source} key={file_name} sans={list(sans)} "
          f"dnsNames={dns_names} endpoint={endpoint}")
    assert dns_names == [tls_name], "dNSName SANs"
    for san in sans:
        assert str(san.value).lower() != endpoint.lower(), f"SAN {san} names the endpoint {endpoint}"


def tls_alternates(cluster: l3.Cluster, pods: list[l3.Pod], endpoint: str) -> dict[str, str]:
    """Return the TLS alternate address each pod advertises, the endpoint hostname with the pod's
    own Service NodePort for the TLS port, by pod name."""
    tls_port = int(cluster.get(".spec.aerospikeConfig.network.service['tls-port']"))
    alternates = {}
    for pod in pods:
        node_port = cluster.node_port(pod.name, tls_port)
        assert node_port > 0, f"{pod.name} has no Service NodePort for {tls_port}"
        alternates[pod.name] = f"{endpoint}:{node_port}"
    return alternates


# K-6: every pod advertises the cluster tls-name for each of its peers and the endpoint hostname
# with its own Service NodePort as its TLS alternate address. The shared certificate in the secret
# carries the tls-name as a dNSName SAN and no SAN for the endpoint hostname. A client seeded at the
# endpoint hostname with the tls-name holds one node per pod at that address, and every node serves
# the keys it is master of. The same seed with the endpoint hostname as tls-name fails the TLS
# handshake.
@pytest.mark.l3(tls=True)
def test_k6_shared_certificate_carries_tls_name_and_client_validates_against_it(warnings: list[str]) -> None:
    cluster = l3.Cluster()
    endpoint = l3.endpoint()
    tls_name = os.environ["DISCOVERY_TLS_NAME"]
    assert_setup(cluster, tls_name, endpoint)
    pods = cluster.pods()
    alternates = tls_alternates(cluster, pods, endpoint)

    tls = {"enable": True, "cafile": os.environ["DISCOVERY_TLS_CA_FILE"]}
    _, seed_port = di.split_endpoint(alternates[pods[0].name])
    client = l3.connect(seed=(endpoint, seed_port, tls_name), tls=tls)
    try:
        held = l3.wait_for_size(client, len(pods))
        held_pods = []
        for name, entry in held.items():
            info = di.node_info(client, name)
            access = info("service-tls-std")
            pod = l3.pod_of(pods, access)
            assert pod is not None, f"{name} TLS access {access} is no pod IP in {pods}"
            held_pods.append(pod.name)
            peers_reply = info("peers-tls-alt")
            print(f"L3-TLS-NAME pod={pod.name} node={name} client={entry} peersTlsAlt={peers_reply}")
            assert info("service-tls-alt") == alternates[pod.name], f"{pod.name} TLS alternate address"
            di.assert_at(entry, alternates[pod.name])
            peers = di.peers(peers_reply)
            assert {peer for peer, _, _ in peers} == set(held) - {name}, f"{pod.name} peers"
            assert {peer_tls_name for _, peer_tls_name, _ in peers} == {tls_name}, f"{pod.name} peer tls-names"
        assert sorted(held_pods) == sorted(alternates), "One node per pod"
        l3.assert_served(client, SET_NAME, size=len(pods), row="L3-K-6")
    finally:
        client.close()
    assert di.logged_legacy_matches(warnings) == []

    with pytest.raises(aerospike.exception.AerospikeError):
        l3.connect(seed=(endpoint, seed_port, endpoint), tls=tls)
    assert f"TLS name '{endpoint}' mismatch" in warnings
