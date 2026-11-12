"""L2 discovery tests of TLS through the proxy against the ee subset of the discovery-l2-topology
action, which runs Enterprise servers with TLS.

Every node has a clear and a TLS Toxiproxy listener, the TLS one named after the clear one with a
-tls suffix, and advertises its private address in the standard TLS list, which the client has no
route to. The port-map translator maps each advertised TLS endpoint to that node's TLS listener.
The proxy passes TLS through, so each node terminates it with a certificate for its tls-name. In
the default profile the certificate carries the tls-name only as a SAN, and its CN does not
match. In the cn-only profile it carries the tls-name only as its CN.
"""

import os
from dataclasses import dataclass

import pytest

import aerospike
import discovery_info as di

NAMESPACE = "test"
SET_NAME = "l2-tls"
TEND_INTERVAL_MS = 250
JOIN_TIMEOUT_SEC = 30
TLS_PROXY_SUFFIX = "-tls"


@dataclass
class ToxiproxyTlsNode:
    name: str
    port: int
    tls_port: int
    tls_advertised: list
    tls_name: str

    def info(self, command: str) -> str:
        reply = di.request(os.environ["DISCOVERY_ENDPOINT_HOSTNAME"], self.port, command)
        return reply[command]


@pytest.fixture(scope="module")
def nodes() -> list[ToxiproxyTlsNode]:
    """Query every clear listener for its node's name, the TLS endpoints it advertises and, from its
    peers, every node's tls-name."""
    host = os.environ["DISCOVERY_ENDPOINT_HOSTNAME"]
    ports = {}
    for entry in os.environ["DISCOVERY_PROXY_MAP"].split(","):
        proxy, _, port = entry.rpartition(":")
        ports[proxy] = int(port)

    replies = {}
    for proxy, port in ports.items():
        if not proxy.endswith(TLS_PROXY_SUFFIX):
            replies[proxy] = di.request(host, port, "node", "service-tls-std", "peers-tls-std")
    tls_names = {}
    for reply in replies.values():
        for name, tls_name, _ in di.peers(reply["peers-tls-std"]):
            tls_names[name] = tls_name

    found = []
    for proxy, reply in replies.items():
        name = reply["node"]
        tls_advertised = di.endpoints(reply["service-tls-std"])
        assert tls_advertised, f"{name} advertises no TLS address"
        assert tls_names.get(name), f"No peer lists a tls-name for {name}: {tls_names}"
        node = ToxiproxyTlsNode(name, port=ports[proxy], tls_port=ports[proxy + TLS_PROXY_SUFFIX],
                                tls_advertised=tls_advertised, tls_name=tls_names[name])
        print(f"L2-NODE {node}")
        found.append(node)
    return found


def connect(nodes: list[ToxiproxyTlsNode], seed: ToxiproxyTlsNode, tls_name: str) -> aerospike.Client:
    host = os.environ["DISCOVERY_ENDPOINT_HOSTNAME"]
    port_map = {advertised: f"{host}:{node.tls_port}" for node in nodes for advertised in node.tls_advertised}
    config = {
        "hosts": [(host, seed.tls_port, tls_name)],
        "tls": {"enable": True, "cafile": os.environ["AEROSPIKE_CA_CERT"]},
        "translation": {"provider": "port-map", "port_map": port_map},
        "tend_interval": TEND_INTERVAL_MS,
        "connect_timeout": 1000,
    }
    return aerospike.client(config)


# N-6: TLS through the proxy. Seeded through the first node's TLS listener with its tls-name, every
# node joins at its TLS listener and serves the keys it is master of. The handshake validates the
# certificate against the node's tls-name, not the endpoint hostname, through its SAN, with no
# legacy CN match, and a seed whose tls-name is the endpoint hostname fails the handshake.
@pytest.mark.l2("default", subset="ee")
def test_n6_tls_validates_against_node_tls_name(nodes: list[ToxiproxyTlsNode], warnings: list[str]) -> None:
    host = os.environ["DISCOVERY_ENDPOINT_HOSTNAME"]
    first = nodes[0]
    assert first.tls_name != host, f"tls-name {first.tls_name} is the endpoint hostname"

    client = connect(nodes, seed=first, tls_name=first.tls_name)
    try:
        di.wait_for_nodes(client, [node.name for node in nodes], JOIN_TIMEOUT_SEC, TEND_INTERVAL_MS)
        held = di.client_nodes(client)
        for node in nodes:
            print(f"L2-N6-NODE {node.name} client={held[node.name]} tlsName={node.tls_name}")
            di.assert_at(held[node.name], f"{host}:{node.tls_port}")
        infos = {node.name: node.info for node in nodes}
        di.assert_served_by_owners(client, NAMESPACE, SET_NAME, row="L2-N6", infos=infos)
    finally:
        client.close()
    assert di.logged_legacy_matches(warnings) == []

    with pytest.raises(aerospike.exception.AerospikeError) as raised:
        connect(nodes, seed=first, tls_name=host)
    print(f"L2-N6-ERROR {raised.value}")
    assert f"TLS name '{host}' mismatch" in warnings


# N-17: a certificate that carries the tls-name only as its CN is rejected. The C client still
# accepts it through its deprecated legacy name check, which PRD FR-45 keeps until the next major
# release. Until then, the expected failure is the accepted handshake with the deprecation warning
# logged; any other failure, or an unexpected pass, fails the test.
@pytest.mark.l2("cn-only", subset="ee")
@pytest.mark.xfail(
    strict=True,
    raises=pytest.fail.Exception,
    reason=(
        "The C client accepts a CN-only certificate through its deprecated legacy name check until the next "
        "major release"
    ),
)
def test_n17_cn_only_certificate_is_rejected(nodes: list[ToxiproxyTlsNode], warnings: list[str]) -> None:
    first = nodes[0]
    with pytest.raises(aerospike.exception.AerospikeError) as raised:
        client = connect(nodes, seed=first, tls_name=first.tls_name)
        print(f"L2-N17-ACCEPTED nodes={sorted(di.client_nodes(client))} legacy={di.logged_legacy_matches(warnings)}")
        client.close()
        legacy = di.logged_legacy_matches(warnings)
        assert legacy, "Accepted a CN-only certificate without the legacy name check warning"
    print(f"L2-N17-ERROR {raised.value}")
    assert f"TLS name '{first.tls_name}' mismatch" in warnings
