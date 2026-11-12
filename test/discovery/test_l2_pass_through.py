"""L2 discovery tests against the pass-through profiles of the discovery-l2-topology action.

In pass-through-names each node advertises its own alternate-access-address hostname with its own
Toxiproxy listener port. In pass-through-ports every node advertises one hostname with its own
listener port. Every hostname resolves to the Toxiproxy container, and the client has no route to
the node addresses. The client reads the alternate list with use_services_alternate and the strict
pass-through translator, so it uses every address as advertised.
"""

import os
from dataclasses import dataclass
from typing import Iterable

import pytest

import aerospike
import discovery_info as di

NAMESPACE = "test"
SET_NAME = "l2-pass-through"
TEND_INTERVAL_MS = 250
JOIN_TIMEOUT_SEC = 30
COLLISION_RETRIES = 3


@dataclass
class Node:
    name: str
    proxy: str
    port: int
    alternate: str

    def info(self, command: str) -> str:
        reply = di.request(os.environ["DISCOVERY_ENDPOINT_HOSTNAME"], self.port, command)
        return reply[command]


def parse_port(endpoint: str) -> int:
    port = endpoint.rpartition(":")[2]
    return int(port)


@pytest.fixture(scope="module")
def nodes() -> list[Node]:
    """Query every listener for its node's name and its one alternate address."""
    host = os.environ["DISCOVERY_ENDPOINT_HOSTNAME"]
    proxies = {}
    for entry in os.environ["DISCOVERY_PROXY_MAP"].split(","):
        proxy = entry.rpartition(":")[0]
        proxies[parse_port(entry)] = proxy

    found = []
    for port in os.environ["DISCOVERY_ENDPOINT_PORTS"].split(","):
        port = int(port)
        info = di.request(host, port, "node", "service-clear-alt")
        alternates = di.endpoints(info["service-clear-alt"])
        assert len(alternates) == 1, f"{info['node']} on listener {port} advertises {alternates}"
        node = Node(info["node"], proxies[port], port, alternates[0])
        print(f"L2-NODE {node}")
        found.append(node)
    return found


def seed(node: Node) -> tuple[str, int]:
    return di.split_endpoint(node.alternate)


def connect(node: Node) -> aerospike.Client:
    config = {
        "hosts": [seed(node)],
        "use_services_alternate": True,
        "translation": {"provider": "pass-through", "on_miss": "strict"},
        "tend_interval": TEND_INTERVAL_MS,
        "connect_timeout": 1000,
    }
    return aerospike.client(config)


def wait_for_nodes(client: aerospike.Client, names: Iterable[str]) -> None:
    di.wait_for_nodes(client, names, JOIN_TIMEOUT_SEC, TEND_INTERVAL_MS)


def assert_routes_to_owners(row: str, nodes: list[Node]) -> None:
    for node in nodes:
        assert parse_port(node.alternate) == node.port, f"{node} advertises another port"

    client = connect(nodes[0])
    try:
        wait_for_nodes(client, [node.name for node in nodes])
        held = di.client_nodes(client)
        for node in nodes:
            print(f"L2-{row}-NODE {node.name} client={held[node.name]} alternate={node.alternate}")
            di.assert_at(held[node.name], node.alternate)
        infos = {node.name: node.info for node in nodes}
        di.assert_served_by_owners(client, NAMESPACE, SET_NAME, f"L2-{row}", infos)
    finally:
        client.close()


@pytest.mark.l2("pass-through-names")
def test_n15_distinct_names_route_to_every_owner(nodes: list[Node]) -> None:
    hostnames = {seed(node)[0] for node in nodes}
    assert len(hostnames) == len(nodes), f"Hostnames {hostnames}"
    assert_routes_to_owners("N-15", nodes)


@pytest.mark.l2("pass-through-ports")
def test_n16_shared_name_routes_to_every_owner(nodes: list[Node]) -> None:
    hostnames = {seed(node)[0] for node in nodes}
    assert len(hostnames) == 1, f"Hostnames {hostnames}"
    assert_routes_to_owners("N-16", nodes)


def next_listener_port() -> int:
    ports = di.listener_ports()
    return max(ports) + 1


def add_listener(container: str, port: int, like: str) -> None:
    """Add a Toxiproxy listener on port for container, forwarding to the port the like proxy uses."""
    upstream = di.toxiproxy("GET", f"/proxies/{like}")["upstream"]
    upstream_port = parse_port(upstream)
    body = {"name": container, "listen": f"0.0.0.0:{port}", "upstream": f"{container}:{upstream_port}", "enabled": True}
    di.toxiproxy("POST", "/proxies", body)


# N-16 scale-out and NFR-1: a node started mid-test that advertises the same hostname with its own
# port is active within 2 x tend_interval of the peers-generation bump that announces it
@pytest.mark.l2("pass-through-ports")
def test_n16_new_node_on_shared_name_active_within_two_tend_intervals(
    nodes: list[Node], added_nodes: list[str]
) -> None:
    observer = nodes[0]
    names = {node.name for node in nodes}
    hostname = seed(observer)[0]
    client = connect(observer)
    try:
        wait_for_nodes(client, names)
        generation = observer.info("peers-generation")
        port = next_listener_port()
        container = di.start_node(f"alternate-port={port}")["container"]
        added_nodes.append(container)
        add_listener(container, port, like=observer.proxy)
        current = observer.info("peers-generation")
        assert current == generation, f"peers-generation moved to {current} before {container} had a listener"

        joined, bump_to_active_ms = di.wait_joined(client, observer.info, generation, names, di.CONTROL_TIMEOUT_SEC)
        budget_ms = 2 * TEND_INTERVAL_MS
        print(f"L2-N16-TIMING node={joined} container={container} bumpToActiveMs={bump_to_active_ms:.1f}")
        di.assert_at(joined, f"{hostname}:{port}")
        assert bump_to_active_ms <= budget_ms, f"Active {bump_to_active_ms:.1f} ms after the bump, budget {budget_ms}"
    finally:
        client.close()


# N-18 on pass-through: two nodes started mid-test advertise the same hostname and port. Seeded through
# a node that does not collide, neither becomes a node, the collision is logged on every tend naming
# both nodes, the address and the translator, and the other nodes keep serving the keys they are
# master of. Only one of the two gets the shared port's listener, once both are in the cluster.
@pytest.mark.l2("pass-through-ports")
def test_n18_pass_through_colliding_nodes_create_no_node(
    nodes: list[Node], added_nodes: list[str], warnings: list[str]
) -> None:
    observer = nodes[0]
    names = [node.name for node in nodes]
    hostname = seed(observer)[0]
    port = next_listener_port()
    endpoint = f"{hostname}:{port}"
    for _ in range(2):
        container = di.start_node(f"alternate-port={port}")["container"]
        added_nodes.append(container)
    di.wait_cluster_stable([node.info for node in nodes], len(nodes) + 2)
    add_listener(added_nodes[0], port, like=observer.proxy)
    colliding = di.peer_names(observer.info("peers-clear-alt"), endpoint)
    print(f"L2-N18-ADVERTISED endpoint={endpoint} nodes={colliding}")
    assert len(colliding) == 2, f"Nodes advertising {endpoint}: {colliding}"

    client = connect(observer)
    try:
        wait_for_nodes(client, names)

        def logged() -> bool:
            return all(len(di.collisions(warnings, name)) >= COLLISION_RETRIES for name in colliding)

        retried = di.wait_for(logged, JOIN_TIMEOUT_SEC)
        assert sorted(di.client_nodes(client)) == sorted(names)
        assert retried, f"Collision of {colliding} not logged every tend: {warnings}"
        message = di.collisions(warnings, colliding[0])[0]
        print(f"L2-N18-LOG {message}")
        assert message in di.collision_messages(translator="pass-through", endpoint=endpoint, names=colliding)
        infos = {node.name: node.info for node in nodes}
        di.assert_keys_on_masters_work(client, NAMESPACE, SET_NAME, row="L2-N18-pass-through", infos=infos)
    finally:
        client.close()
