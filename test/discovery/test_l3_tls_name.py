"""L3 discovery test of TLS against an Aerospike cluster on the Aerospike Kubernetes Operator, where
every pod mounts one shared certificate secret, set up by the setup-kind-ako action with enable-tls.
The certificate names the cluster tls-name, not the endpoint hostname the client dials, so the
client must validate each node against the tls-name the cluster advertises.
"""

import os
import subprocess

import pytest

import aerospike
import discovery_info as di
import l3_cluster as l3

SET_NAME = "l3-tls-name"
MIN_PODS = 2


def subject_alternative_names(pem: bytes) -> list[str]:
    """Return a certificate's SANs as openssl prints them, such as DNS:name or IP Address:ip."""
    result = subprocess.run(
        ["openssl", "x509", "-noout", "-ext", "subjectAltName"], input=pem, capture_output=True, check=True
    )
    # The first line is the extension's name
    entries = result.stdout.decode().splitlines()[1:]
    return [san.strip() for line in entries for san in line.split(",") if san.strip()]


def assert_certificate(cluster: l3.Cluster, tls_name: str, endpoint: str) -> None:
    """Read the server certificate from the secret mounted where the tls-name's cert-file lives and
    assert its dNSName SANs are exactly the tls-name, with no SAN of any type for the endpoint."""
    cert_file = cluster.get(f".spec.aerospikeConfig.network.tls[?(@.name==\"{tls_name}\")]['cert-file']")
    directory, _, file_name = cert_file.rpartition("/")
    assert directory, f"cert-file {cert_file}"
    secret = cluster.get(f".spec.storage.volumes[?(@.aerospike.path==\"{directory}\")].source.secret.secretName")
    assert secret, f"No secret volume at {directory}"

    sans = subject_alternative_names(cluster.secret_data(secret, file_name))
    dns_names = [san.partition(":")[2] for san in sans if san.startswith("DNS:")]
    print(f"L3-TLS-CERT secret={cluster.namespace}/{secret} key={file_name} sans={sans} dnsNames={dns_names} "
          f"endpoint={endpoint}")
    assert dns_names == [tls_name], "dNSName SANs"
    for san in sans:
        assert san.partition(":")[2].lower() != endpoint.lower(), f"SAN {san} names the endpoint {endpoint}"


# K-6: every pod advertises the cluster tls-name for each of its peers and the endpoint hostname
# with its own Service NodePort as its TLS alternate address. The shared certificate in the secret
# carries the tls-name as a dNSName SAN and no SAN for the endpoint hostname. A client seeded at the
# endpoint hostname with the tls-name holds one node per pod at that address, and every node serves
# the keys it is master of. The same seed with the endpoint hostname as tls-name fails the TLS
# handshake.
@pytest.mark.l3(tls=True)
def test_k6_shared_certificate_carries_tls_name_and_client_validates_against_it(warnings):
    cluster = l3.Cluster()
    endpoint = l3.endpoint()
    tls_name = os.environ["DISCOVERY_TLS_NAME"]
    assert cluster.get(".spec.aerospikeConfig.network.service['tls-name']") == tls_name, "AerospikeCluster tls-name"
    alternate_access = cluster.get(".spec.aerospikeNetworkPolicy.alternateAccess")
    assert cluster.get(".spec.aerospikeNetworkPolicy.tlsAlternateAccess") == alternate_access
    assert cluster.size() >= MIN_PODS, f"K-6 needs at least {MIN_PODS} pods"
    assert_certificate(cluster, tls_name, endpoint)

    tls_port = int(cluster.get(".spec.aerospikeConfig.network.service['tls-port']"))
    pods = cluster.pods()
    expected = {}
    for pod in pods:
        node_port = cluster.node_port(pod.name, tls_port)
        print(f"L3-TLS-POD {pod} tlsServiceNodePort={node_port}")
        assert node_port > 0, f"{pod.name} has no Service NodePort for {tls_port}"
        expected[pod.name] = f"{endpoint}:{node_port}"

    tls = {"enable": True, "cafile": os.environ["DISCOVERY_TLS_CA_FILE"]}
    seed_port = cluster.node_port(pods[0].name, tls_port)
    client = l3.connect((endpoint, seed_port, tls_name), tls=tls)
    try:
        held = l3.wait_for_size(client, len(pods))
        matched = set()
        advertised_peers = set()
        for name, entry in held.items():
            info = di.node_info(client, name)
            access = info("service-tls-std")
            pod = l3.pod_of(pods, access)
            assert pod is not None, f"{name} TLS access {access} is no pod IP in {pods}"
            assert pod.name not in matched, f"{pod.name} matched twice"
            matched.add(pod.name)
            tls_alternate = info("service-tls-alt")
            peers_reply = info("peers-tls-alt")
            peers = di.peers(peers_reply)
            print(f"L3-TLS-NAME pod={pod.name} node={name} tlsAlternate={tls_alternate} expected={expected[pod.name]} "
                  f"client={entry} peersTlsAlt={peers_reply}")
            assert tls_alternate == expected[pod.name], f"{pod.name} TLS alternate address"
            di.assert_at(entry, expected[pod.name])
            assert len(peers) == len(pods) - 1, f"{pod.name} peers {peers_reply}"
            for peer, peer_tls_name, _ in peers:
                assert peer_tls_name == tls_name, f"{pod.name} tls-name for peer {peer}"
                assert peer in held, f"{pod.name} advertises unknown peer {peer}, nodes {sorted(held)}"
                advertised_peers.add(peer)
        assert matched == set(expected)
        assert advertised_peers == set(held), "Nodes advertised as peers"
        l3.assert_served(client, SET_NAME, len(pods), "L3-K-6")
    finally:
        client.close()
    assert di.legacy_matches(warnings) == []

    with pytest.raises(aerospike.exception.AerospikeError) as raised:
        client = l3.connect((endpoint, seed_port, endpoint), tls=tls)
        print(f"L3-TLS-FQDN-ACCEPTED nodes={sorted(di.client_nodes(client))}")
        client.close()
    print(f"L3-TLS-FQDN-REJECTED seed={endpoint}:{seed_port} tls-name={endpoint} error={raised.value!r}")
    assert f"TLS name '{endpoint}' mismatch" in warnings
