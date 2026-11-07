"""L2 discovery tests against the pass-through profiles of the discovery-l2-topology action.

In pass-through-names each node advertises its own alternate-access-address hostname with its own
Toxiproxy listener port. In pass-through-ports every node advertises one hostname with its own
listener port. Every hostname resolves to the Toxiproxy container, and the client has no route to
the node addresses. The client reads the alternate list with use_services_alternate and the strict
pass-through translator, so it uses every address as advertised.
"""

import json
import os
import socket
import time
import urllib.request
import uuid
from dataclasses import dataclass
from pathlib import Path

import pytest

import aerospike
import discovery_info as di

NAMESPACE = "test"
SET_NAME = "l2-pass-through"
TEND_INTERVAL_MS = 250
JOIN_TIMEOUT_SEC = 30
CONTROL_TIMEOUT_SEC = 60
POLL_INTERVAL_SEC = 0.005


@dataclass
class Node:
    name: str
    proxy: str
    port: int
    alternate: str

    def info(self, command):
        reply = di.request(os.environ["DISCOVERY_ENDPOINT_HOSTNAME"], self.port, command)
        return reply[command]


def parse_port(endpoint):
    port = endpoint.rpartition(":")[2]
    return int(port)


@pytest.fixture(scope="module")
def nodes():
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


def seed(node):
    return di.split_endpoint(node.alternate)


def connect(node):
    config = {
        "hosts": [seed(node)],
        "use_services_alternate": True,
        "translation": {"provider": "pass-through", "on_miss": "strict"},
        "tend_interval": TEND_INTERVAL_MS,
        "connect_timeout": 1000,
    }
    return aerospike.client(config)


def client_nodes(client):
    return {entry["node_name"]: entry for entry in client.get_node_names()}


def wait_for_nodes(client, names):
    expected = sorted(names)

    def joined():
        held = client_nodes(client)
        return sorted(held) == expected

    complete = di.wait_for(joined, JOIN_TIMEOUT_SEC)
    held = client_nodes(client)
    assert complete, f"Client nodes {sorted(held)}, expected {expected}"
    # The client exposes no partition map, so give it the tends that fetch every node's partitions
    time.sleep(4 * TEND_INTERVAL_MS / 1000)


def assert_at_advertised_address(entry, advertised):
    host, port = di.split_endpoint(advertised)
    resolved = socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
    addresses = {info[4][0] for info in resolved}
    assert entry["address"] in addresses | {host}, f"{entry} is not at {host} {sorted(addresses)}"
    assert entry["port"] == port, f"{entry} is not at port {port}"


def assert_routes_to_owners(row, nodes):
    for node in nodes:
        assert parse_port(node.alternate) == node.port, f"{node} advertises another port"

    client = connect(nodes[0])
    try:
        wait_for_nodes(client, [node.name for node in nodes])
        held = client_nodes(client)
        for node in nodes:
            print(f"L2-{row}-NODE {node.name} client={held[node.name]} alternate={node.alternate}")
            assert_at_advertised_address(held[node.name], node.alternate)
        infos = {node.name: node.info for node in nodes}
        di.assert_served_by_owners(client, NAMESPACE, SET_NAME, f"L2-{row}", infos)
    finally:
        client.close()


@pytest.mark.l2("pass-through-names")
def test_n15_distinct_names_route_to_every_owner(nodes):
    hostnames = {seed(node)[0] for node in nodes}
    assert len(hostnames) == len(nodes), f"Hostnames {hostnames}"
    assert_routes_to_owners("N-15", nodes)


@pytest.mark.l2("pass-through-ports")
def test_n16_shared_name_routes_to_every_owner(nodes):
    hostnames = {seed(node)[0] for node in nodes}
    assert len(hostnames) == 1, f"Hostnames {hostnames}"
    assert_routes_to_owners("N-16", nodes)


def toxiproxy(method, path, body=None):
    data = None
    if body is not None:
        data = json.dumps(body).encode()
    url = os.environ["TOXIPROXY_URL"] + path
    request = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=10) as response:
        text = response.read().decode()
    if text:
        return json.loads(text)
    return None


def next_listener_port():
    proxies = toxiproxy("GET", "/proxies")
    ports = {parse_port(proxy["listen"]) for proxy in proxies.values()}
    return max(ports) + 1


def control(command):
    """Send a request to the discovery node server and return its acknowledgement."""
    directory = Path(os.environ["DISCOVERY_CONTROL_DIR"])
    request_id = uuid.uuid4().hex
    tmp = directory / f"{request_id}.tmp"
    ack = directory / f"{request_id}.ack"
    tmp.write_text(command + "\n")
    tmp.rename(directory / f"{request_id}.request")
    answered = di.wait_for(ack.exists, CONTROL_TIMEOUT_SEC)
    assert answered, f"No acknowledgement for '{command}' in {directory}"

    reply = {}
    for line in ack.read_text().splitlines():
        key, separator, value = line.partition("=")
        if separator:
            reply[key] = value
    ack.unlink()
    print(f"L2-CONTROL {command} {reply}")
    assert "error" not in reply, f"'{command}' failed: {reply['error']}"
    return reply


def wait_cluster_stable(nodes):
    command = f"cluster-stable:size={len(nodes)};ignore-migrations=false"

    def stable():
        keys = {node.info(command) for node in nodes}
        return len(keys) == 1 and not keys.pop().startswith("ERROR")

    settled = di.wait_for(stable, CONTROL_TIMEOUT_SEC)
    assert settled, f"Cluster not stable at size {len(nodes)}"


@pytest.fixture
def added_nodes(nodes):
    """Collect the containers a test starts, and remove them and their listeners afterwards."""
    containers = []
    yield containers
    for container in containers:
        toxiproxy("DELETE", f"/proxies/{container}")
        control(f"stop {container}")
    if containers:
        wait_cluster_stable(nodes)


def add_listener(container, port, like):
    """Add a Toxiproxy listener on port for container, forwarding to the port the like proxy uses."""
    upstream = toxiproxy("GET", f"/proxies/{like}")["upstream"]
    upstream_port = parse_port(upstream)
    body = {"name": container, "listen": f"0.0.0.0:{port}", "upstream": f"{container}:{upstream_port}", "enabled": True}
    toxiproxy("POST", "/proxies", body)


# N-16 scale-out and NFR-1: a node started mid-test that advertises the same hostname with its own
# port is active within 2 x tend_interval of the peers-generation bump that announces it
@pytest.mark.l2("pass-through-ports")
def test_n16_new_node_on_shared_name_active_within_two_tend_intervals(nodes, added_nodes):
    observer = nodes[0]
    names = {node.name for node in nodes}
    hostname = seed(observer)[0]
    client = connect(observer)
    try:
        wait_for_nodes(client, names)
        generation = observer.info("peers-generation")
        port = next_listener_port()
        container = control(f"start alternate-port={port}")["container"]
        added_nodes.append(container)
        add_listener(container, port, observer.proxy)
        current = observer.info("peers-generation")
        assert current == generation, f"peers-generation moved to {current} before {container} had a listener"

        bump_ns = None
        active_ns = None
        joined = None
        deadline = time.monotonic() + CONTROL_TIMEOUT_SEC
        while (bump_ns is None or active_ns is None) and time.monotonic() < deadline:
            now = time.monotonic_ns()
            if bump_ns is None and observer.info("peers-generation") != generation:
                bump_ns = now
            if active_ns is None:
                held = client_nodes(client)
                new = [entry for name, entry in held.items() if name not in names]
                if new:
                    active_ns = now
                    joined = new[0]
            time.sleep(POLL_INTERVAL_SEC)

        assert bump_ns is not None, f"peers-generation stayed at {generation}"
        assert joined is not None, f"{container} never joined the client"
        bump_to_active_ms = (active_ns - bump_ns) / 1e6
        budget_ms = 2 * TEND_INTERVAL_MS
        print(f"L2-N16-TIMING node={joined} container={container} bumpToActiveMs={bump_to_active_ms:.1f}")
        assert_at_advertised_address(joined, f"{hostname}:{port}")
        assert bump_to_active_ms <= budget_ms, f"Active {bump_to_active_ms:.1f} ms after the bump, budget {budget_ms}"
    finally:
        client.close()
