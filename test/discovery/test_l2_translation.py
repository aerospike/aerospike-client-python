"""L2 discovery tests of the port-map translator and the seed provider against the default and
reachable profiles of the discovery-l2-topology action.

Every node advertises its private address in the standard list, which the client has no route to,
and is reached through its own Toxiproxy listener. The port-map translator maps each advertised
endpoint, or each node name, to that listener. In the reachable profile the node named by
DISCOVERY_REACHABLE_NODE advertises a hostname that resolves to the proxy, with its own listener
port.
"""

import os
import socket
import threading
import time
from dataclasses import dataclass

import pytest

import aerospike
import discovery_info as di
from aerospike_helpers.discovery import SeedCandidateProvider

NAMESPACE = "test"
SET_NAME = "l2-translation"
TEND_INTERVAL_MS = 250
JOIN_TIMEOUT_SEC = 30
MISS_RETRIES = 3
KEY_COUNT = 300
BLOCK_SEC = 3
REFRESH_TIMEOUT_MS = 1000


@dataclass
class Node:
    name: str
    proxy: str
    port: int
    advertised: list

    @property
    def listener(self):
        return f"{os.environ['DISCOVERY_ENDPOINT_HOSTNAME']}:{self.port}"

    def info(self, command):
        reply = di.request(os.environ["DISCOVERY_ENDPOINT_HOSTNAME"], self.port, command)
        return reply[command]


@pytest.fixture(scope="module")
def nodes():
    """Query every listener for its node's name and the endpoints it advertises in the standard list."""
    host = os.environ["DISCOVERY_ENDPOINT_HOSTNAME"]
    proxies = {}
    for entry in os.environ["DISCOVERY_PROXY_MAP"].split(","):
        proxy, _, port = entry.rpartition(":")
        proxies[int(port)] = proxy

    found = []
    for port in os.environ["DISCOVERY_ENDPOINT_PORTS"].split(","):
        port = int(port)
        info = di.request(host, port, "node", "service-clear-std")
        advertised = di.endpoints(info["service-clear-std"])
        assert advertised, f"{info['node']} on listener {port} advertises no clear address"
        node = Node(info["node"], proxies[port], port, advertised)
        print(f"L2-NODE {node}")
        found.append(node)
    return found


@pytest.fixture
def warnings():
    """Collect the client's warning and error log messages."""
    messages = []

    def log(level, function, path, line, message):
        messages.append(message)
        print(f"CLIENT-LOG {level} {message}")

    aerospike.set_log_handler(log)
    aerospike.set_log_level(aerospike.LOG_LEVEL_WARN)
    yield messages
    aerospike.set_log_level(aerospike.LOG_LEVEL_ERROR)
    aerospike.set_log_handler()


def port_map_by_endpoint(nodes):
    return {advertised: node.listener for node in nodes for advertised in node.advertised}


def port_map_by_node_name(nodes):
    return {node.name: node.listener for node in nodes}


def connect(seed, translation, discovery=None):
    config = {
        "hosts": [],
        "translation": translation,
        "tend_interval": TEND_INTERVAL_MS,
        "connect_timeout": 1000,
    }
    if seed is not None:
        config["hosts"] = [(os.environ["DISCOVERY_ENDPOINT_HOSTNAME"], seed.port)]
    if discovery is not None:
        config["discovery"] = discovery
    return aerospike.client(config)


def client_nodes(client):
    return {entry["node_name"]: entry for entry in client.get_node_names()}


def wait_for_nodes(client, names):
    expected = sorted(names)

    def joined():
        return sorted(client_nodes(client)) == expected

    complete = di.wait_for(joined, JOIN_TIMEOUT_SEC)
    held = client_nodes(client)
    assert complete, f"Client nodes {sorted(held)}, expected {expected}"
    # The client exposes no partition map, so give it the tends that fetch every node's partitions
    time.sleep(4 * TEND_INTERVAL_MS / 1000)


def assert_at(entry, endpoint):
    host, port = di.split_endpoint(endpoint)
    resolved = socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
    addresses = {info[4][0] for info in resolved}
    assert entry["address"] in addresses | {host}, f"{entry} is not at {host} {sorted(addresses)}"
    assert entry["port"] == port, f"{entry} is not at port {port}"


def assert_port_map_joins(row, nodes, port_map):
    print(f"L2-PORT-MAP {row} {port_map}")
    client = connect(nodes[0], {"provider": "port-map", "port_map": port_map})
    try:
        wait_for_nodes(client, [node.name for node in nodes])
        held = client_nodes(client)
        for node in nodes:
            print(f"L2-{row}-NODE {node.name} client={held[node.name]} listener={node.listener}")
            assert_at(held[node.name], node.listener)
        infos = {node.name: node.info for node in nodes}
        di.assert_served_by_owners(client, NAMESPACE, SET_NAME, f"L2-{row}", infos)
    finally:
        client.close()


# N-2: port-map keyed by advertised host:port. All nodes join, each at its listener, and each serves
# the keys it is master of
@pytest.mark.l2("default")
def test_n2_port_map_by_endpoint_joins_all_nodes(nodes):
    assert_port_map_joins("N-2-endpoint", nodes, port_map_by_endpoint(nodes))


# N-2: port-map keyed by node name
@pytest.mark.l2("default")
def test_n2_port_map_by_node_name_joins_all_nodes(nodes):
    assert_port_map_joins("N-2-node-name", nodes, port_map_by_node_name(nodes))


def misses(messages, node):
    return [message for message in messages if f"has no mapping for node {node.name}" in message]


def master_owners(nodes):
    owners = {}
    for node in nodes:
        for pid in di.master_partitions(node.info, NAMESPACE):
            owners[pid] = node.name
    assert len(owners) == di.PARTITIONS, f"{di.PARTITIONS - len(owners)} partitions have no master"
    return owners


# N-8: on_miss strict. The last node has no mapping, so the client holds no node for it, logs the
# miss naming the node, its endpoint, the translator and the setting that avoids it on every tend,
# and keeps serving the keys the mapped nodes are master of
@pytest.mark.l2("default")
def test_n8_strict_miss_creates_no_node(nodes, warnings):
    assert len(nodes) >= 2, f"N-8 needs a mapped node and a victim: {nodes}"
    victim = nodes[-1]
    mapped = nodes[:-1]
    mapped_names = sorted(node.name for node in mapped)

    client = connect(nodes[0], {"provider": "port-map", "port_map": port_map_by_endpoint(mapped), "on_miss": "strict"})
    try:
        wait_for_nodes(client, mapped_names)
        retried = di.wait_for(lambda: len(misses(warnings, victim)) >= MISS_RETRIES, JOIN_TIMEOUT_SEC)
        found = misses(warnings, victim)
        print(f"L2-N8-RESULT victim={victim.name} nodes={sorted(client_nodes(client))} missWarnings={len(found)}")
        assert retried, f"Missed peer {victim.name} not retried every tend: {warnings}"
        assert sorted(client_nodes(client)) == mapped_names

        message = found[0]
        print(f"L2-N8-LOG {message}")
        expected = [
            f"Address translator port-map has no mapping for node {victim.name} advertised address {advertised} "
            "in the standard list. Add a mapping for that endpoint or node, or set translation.on_miss to pass_through"
            for advertised in victim.advertised
        ]
        assert message in expected

        owners = master_owners(nodes)
        owned = 0
        missed = 0
        read_policy = {"replica": aerospike.POLICY_REPLICA_MASTER}
        for i in range(KEY_COUNT):
            user_key = f"n8-{i}"
            pid = client.get_key_partition_id(NAMESPACE, SET_NAME, user_key)
            if owners[pid] == victim.name:
                missed += 1
                continue
            key = (NAMESPACE, SET_NAME, user_key)
            client.put(key, {"v": i})
            _, _, bins = client.get(key, policy=read_policy)
            assert bins == {"v": i}
            owned += 1
        print(f"L2-N8-KEYS victim={victim.name} keysOnMappedMasters={owned} keysOnMissedPartitions={missed}")
        assert owned > 0
        assert missed > 0
    finally:
        client.close()


# N-10: port-map covers every node but the reachable one and on_miss is left at pass_through. The
# reachable node joins at its advertised address, every node serves the keys it is master of, and
# no miss is logged.
@pytest.mark.l2("reachable")
def test_n10_unmapped_peer_passes_through_unchanged(nodes, warnings):
    container = os.environ["DISCOVERY_REACHABLE_NODE"]
    reachable = next(node for node in nodes if node.proxy == container)
    assert reachable is not nodes[0], f"Reachable node {reachable} must not be the first listener"
    mapped = [node for node in nodes if node is not reachable]
    advertised = reachable.advertised[0]
    port_map = port_map_by_endpoint(mapped)
    assert advertised not in port_map
    print(f"L2-N10-ADVERTISED node={reachable.name} container={container} advertised={reachable.advertised}")

    client = connect(mapped[0], {"provider": "port-map", "port_map": port_map})
    try:
        wait_for_nodes(client, [node.name for node in nodes])
        held = client_nodes(client)
        print(f"L2-N10-NODE {reachable.name} client={held[reachable.name]} advertised={advertised}")
        assert_at(held[reachable.name], advertised)
        infos = {node.name: node.info for node in nodes}
        di.assert_served_by_owners(client, NAMESPACE, SET_NAME, "L2-N10", infos)
        assert [message for message in warnings if "has no mapping" in message] == []
    finally:
        client.close()


class BlockingSeeds(SeedCandidateProvider):
    """Returns the seed listener on every refresh, and blocks the second refresh until released."""

    def __init__(self, seed):
        self.seed = seed
        self.contexts = []
        self.blocked = threading.Event()
        self.release = threading.Event()
        self.closed = threading.Event()

    def refresh_seed_candidates(self, context):
        self.contexts.append(context)
        if len(self.contexts) == 2:
            self.blocked.set()
            self.release.wait(5 * BLOCK_SEC)
        return [self.seed]

    def needs_periodic_refresh(self):
        return True

    def close(self):
        self.closed.set()


def blocking_provider_client(nodes):
    seeds = BlockingSeeds((os.environ["DISCOVERY_ENDPOINT_HOSTNAME"], nodes[0].port))
    discovery = {
        "provider": seeds,
        "refresh_interval_ms": TEND_INTERVAL_MS,
        "refresh_timeout_ms": REFRESH_TIMEOUT_MS,
    }
    client = connect(None, {"provider": "port-map", "port_map": port_map_by_node_name(nodes)}, discovery)
    return seeds, client


def assert_commands_work(client, row, seconds):
    deadline = time.monotonic() + seconds
    i = 0
    while time.monotonic() < deadline:
        key = (NAMESPACE, SET_NAME, f"{row}-{i}")
        client.put(key, {"v": i})
        _, _, bins = client.get(key)
        assert bins == {"v": i}
        i += 1
        time.sleep(0.05)
    return i


# M-5: in thread mode a seed provider refresh that blocks does not hold up tend. Commands keep
# working while it blocks, and the next refresh sees every tend that ran meanwhile.
@pytest.mark.l2("default")
def test_m5_blocked_refresh_does_not_block_tend(nodes, warnings):
    seeds, client = blocking_provider_client(nodes)
    try:
        wait_for_nodes(client, [node.name for node in nodes])
        assert di.wait_for(seeds.blocked.is_set, JOIN_TIMEOUT_SEC), "The provider was not refreshed again"
        commands = assert_commands_work(client, "M-5", BLOCK_SEC)
        assert sorted(client_nodes(client)) == sorted(node.name for node in nodes)
        seeds.release.set()
        assert di.wait_for(lambda: len(seeds.contexts) >= 3, JOIN_TIMEOUT_SEC), "No refresh after the release"
        blocked = seeds.contexts[1].tend_iteration
        after = seeds.contexts[2].tend_iteration
        print(f"L2-M5 commands={commands} blockedIteration={blocked} nextIteration={after}")
        assert after - blocked >= BLOCK_SEC * 1000 // TEND_INTERVAL_MS // 2
        assert any("exceeding refresh_timeout_ms" in message for message in warnings)
    finally:
        seeds.release.set()
        client.close()


# M-5: close waits at most refresh_timeout_ms for a blocked refresh, and the provider is closed once
# the refresh returns
@pytest.mark.l2("default")
def test_m5_close_during_blocked_refresh_returns(nodes, warnings):
    seeds, client = blocking_provider_client(nodes)
    try:
        assert di.wait_for(seeds.blocked.is_set, JOIN_TIMEOUT_SEC), "The provider was not refreshed again"
        begin = time.monotonic()
        client.close()
        elapsed_ms = (time.monotonic() - begin) * 1000
        print(f"L2-M5-CLOSE elapsedMs={elapsed_ms:.0f}")
        assert elapsed_ms < REFRESH_TIMEOUT_MS + 2000
        assert any("did not return in time" in message for message in warnings)
    finally:
        seeds.release.set()
    del client
    # close() runs on the main thread, through a pending call, once the discovery thread releases it
    assert di.wait_for(seeds.closed.is_set, JOIN_TIMEOUT_SEC, interval=0.05), "The provider was never closed"
