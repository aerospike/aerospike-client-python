"""L2 discovery tests of the port-map translator and the seed providers against the default,
reachable and port-map-range profiles of the discovery-l2-topology action.

Every node advertises its private address in the standard list, which the client has no route to,
and is reached through its own Toxiproxy listener. The port-map translator maps each advertised
endpoint, or each node name, to that listener. In the reachable profile the node named by
DISCOVERY_REACHABLE_NODE advertises a hostname that resolves to the proxy, with its own listener
port. In the port-map-range profile the nodes sit in the address range DISCOVERY_PORT_MAP_RANGE,
and every address of the range has a listener on DISCOVERY_PORT_MAP_PORT_BASE plus its last octet,
including addresses no node has yet. In the default profile DISCOVERY_DNS_SERVER serves an SRV
record for the endpoint per listener.
"""

import os
import socket
import threading
import time
from typing import Any, Iterator, Optional

import pytest

import aerospike
import discovery_info as di
from aerospike_helpers.discovery import SeedCandidateContext, SeedCandidateProvider, SeedHost

NAMESPACE = "test"
SET_NAME = "l2-translation"
TEND_INTERVAL_MS = 250
CONNECT_TIMEOUT_MS = 1000
JOIN_TIMEOUT_SEC = 30
MISS_RETRIES = 3
KEY_COUNT = 300
BLOCK_SEC = 3
REFRESH_TIMEOUT_MS = 1000
# Long enough that no periodic refresh runs during a test
RESEED_REFRESH_INTERVAL_MS = 600000
SINGLE_TRY = {"max_retries": 0, "total_timeout": 1000}


@pytest.fixture(scope="module")
def nodes() -> list[di.Node]:
    return di.standard_nodes()


@pytest.fixture
def disabled_listeners() -> Iterator[list[str]]:
    """Collect the Toxiproxy listeners a test disables, and enable them again afterwards."""
    proxies = []
    yield proxies
    for proxy in proxies:
        di.set_listener(proxy, enabled=True)


def port_map_by_node_name(nodes: list[di.Node]) -> dict[str, str]:
    return {node.name: node.listener for node in nodes}


def connect(
    seed: Optional[di.Node], translation: Optional[dict] = None, discovery: Optional[dict] = None, **options: Any
) -> aerospike.Client:
    config = {
        "hosts": [],
        "tend_interval": TEND_INTERVAL_MS,
        "connect_timeout": CONNECT_TIMEOUT_MS,
        **options,
    }
    if seed is not None:
        config["hosts"] = [(os.environ["DISCOVERY_ENDPOINT_HOSTNAME"], seed.port)]
    if translation is not None:
        config["translation"] = translation
    if discovery is not None:
        config["discovery"] = discovery
    return aerospike.client(config)


def wait_for_nodes(client: aerospike.Client, names: list[str]) -> None:
    di.wait_for_nodes(client, names, JOIN_TIMEOUT_SEC, TEND_INTERVAL_MS)


def wait_for_no_nodes(client: aerospike.Client) -> None:
    def empty() -> bool:
        return not client.get_node_names()

    dropped = di.wait_for(empty, JOIN_TIMEOUT_SEC)
    assert dropped, f"Client kept nodes {sorted(di.client_nodes(client))} with every listener disabled"


def disable_listeners(nodes: list[di.Node], disabled_listeners: list[str]) -> None:
    for node in nodes:
        disabled_listeners.append(node.proxy)
        di.set_listener(node.proxy, enabled=False)


def enable_listeners(disabled_listeners: list[str]) -> None:
    for proxy in disabled_listeners:
        di.set_listener(proxy, enabled=True)
    disabled_listeners.clear()


def assert_port_map_joins(row: str, nodes: list[di.Node], port_map: dict[str, str]) -> None:
    print(f"L2-PORT-MAP {row} {port_map}")
    client = connect(seed=nodes[0], translation={"provider": "port-map", "port_map": port_map})
    try:
        wait_for_nodes(client, [node.name for node in nodes])
        held = di.client_nodes(client)
        for node in nodes:
            print(f"L2-{row}-NODE {node.name} client={held[node.name]} listener={node.listener}")
            di.assert_at(held[node.name], node.listener)
        infos = {node.name: node.info for node in nodes}
        di.assert_served_by_owners(client, NAMESPACE, SET_NAME, f"L2-{row}", infos)
    finally:
        client.close()


# N-1: baseline with no translator. The seed answers through its listener, but every peer advertises
# an address the client has no route to, so init fails naming each of them
@pytest.mark.l2("default")
def test_n1_without_translator_fails_on_peers(nodes: list[di.Node]) -> None:
    seed, peers = nodes[0], nodes[1:]
    assert peers, f"N-1 needs peers: {nodes}"
    for peer in peers:
        for endpoint in peer.advertised:
            address = di.split_endpoint(endpoint)
            with pytest.raises(OSError):
                socket.create_connection(address, timeout=1)

    with pytest.raises(aerospike.exception.ClientError) as raised:
        connect(seed)
    message = raised.value.msg
    print(f"L2-N1-ERROR {message}")
    assert message.startswith("Peers not reachable: "), message
    for peer in peers:
        for endpoint in peer.advertised:
            assert endpoint in message, f"{peer.name} {endpoint} not named in: {message}"


# N-2 and N-5: port-map keyed by advertised host:port. All nodes join, each as its own node at its own
# listener, and each serves the keys it is master of with no proxy hop, so every partition owner is a
# distinct node rather than one seed-only node
@pytest.mark.l2("default")
def test_n2_n5_port_map_by_endpoint_joins_all_nodes(nodes: list[di.Node]) -> None:
    assert_port_map_joins(row="N-2-endpoint", nodes=nodes, port_map=di.port_map_by_endpoint(nodes))


# N-2: port-map keyed by node name
@pytest.mark.l2("default")
def test_n2_port_map_by_node_name_joins_all_nodes(nodes: list[di.Node]) -> None:
    assert_port_map_joins(row="N-2-node-name", nodes=nodes, port_map=port_map_by_node_name(nodes))


# N-4: the dns provider seeds the client from the endpoint's SRV records, one per listener, served by
# the DNS server on the client network, and port-map translates the peers
@pytest.mark.l2("default")
def test_n4_dns_provider_seeds_through_endpoint(nodes: list[di.Node]) -> None:
    host = os.environ["DISCOVERY_ENDPOINT_HOSTNAME"]
    dns = {"names": [f"_aerospike._tcp.{host}"], "record_type": "srv", "servers": [os.environ["DISCOVERY_DNS_SERVER"]]}
    translation = {"provider": "port-map", "port_map": di.port_map_by_endpoint(nodes)}
    client = connect(seed=None, translation=translation, discovery={"provider": "dns", "dns": dns})
    try:
        wait_for_nodes(client, [node.name for node in nodes])
        infos = {node.name: node.info for node in nodes}
        di.assert_served_by_owners(client, NAMESPACE, SET_NAME, row="L2-N4", infos=infos)
    finally:
        client.close()


# N-7: with one node's listener disabled, the keys it is master of fail and the other nodes keep
# serving theirs. Once the listener is enabled again the node serves its keys again.
@pytest.mark.l2("default")
def test_n7_node_removed_from_endpoint_degrades_and_returns(
    nodes: list[di.Node], disabled_listeners: list[str]
) -> None:
    victim = nodes[-1]
    others = nodes[:-1]
    names = [node.name for node in nodes]
    victim_partitions = di.master_partitions(victim.info, NAMESPACE)
    client = connect(seed=nodes[0], translation={"provider": "port-map", "port_map": di.port_map_by_endpoint(nodes)})
    try:
        wait_for_nodes(client, names)
        victim_keys = []
        for i in range(KEY_COUNT):
            key = (NAMESPACE, SET_NAME, f"N-7-{i}")
            if client.get_key_partition_id(*key) in victim_partitions:
                victim_keys.append(key)
        assert victim_keys, f"No key has its master on {victim.name}"

        disable_listeners([victim], disabled_listeners)
        failing = di.wait_for(lambda: not di.writes(client, victim_keys[0], SINGLE_TRY), JOIN_TIMEOUT_SEC)
        assert failing, f"Writes to {victim.name} still succeed with its listener disabled"
        failed = [key for key in victim_keys if not di.writes(client, key, SINGLE_TRY)]
        print(f"L2-N7-DEGRADED victim={victim.name} nodes={sorted(di.client_nodes(client))} failed={len(failed)}")
        assert len(failed) == len(victim_keys)
        infos = {node.name: node.info for node in others}
        di.assert_keys_on_masters_work(client, NAMESPACE, SET_NAME, row="L2-N7-degraded", infos=infos)

        enable_listeners(disabled_listeners)
        recovered = di.wait_for(lambda: all(di.writes(client, key, SINGLE_TRY) for key in victim_keys), JOIN_TIMEOUT_SEC)
        assert recovered, f"Writes to {victim.name} did not recover"
        wait_for_nodes(client, names)
        infos = {node.name: node.info for node in nodes}
        di.assert_served_by_owners(client, NAMESPACE, SET_NAME, row="L2-N7-recovered", infos=infos)
    finally:
        client.close()


def misses(messages: list[str], node_name: str) -> list[str]:
    return [message for message in messages if f"has no mapping for node {node_name}" in message]


def miss_message(node_name: str, advertised: str) -> str:
    return (
        f"Address translator port-map has no mapping for node {node_name} advertised address {advertised} "
        "in the standard list. Add a mapping for that endpoint or node, or set translation.on_miss to pass_through"
    )


# N-8: on_miss strict. The last node has no mapping, so the client holds no node for it, logs the
# miss naming the node, its endpoint, the translator and the setting that avoids it on every tend,
# and keeps serving the keys the mapped nodes are master of
@pytest.mark.l2("default")
def test_n8_strict_miss_creates_no_node(nodes: list[di.Node], warnings: list[str]) -> None:
    assert len(nodes) >= 2, f"N-8 needs a mapped node and a victim: {nodes}"
    victim = nodes[-1]
    mapped = nodes[:-1]
    mapped_names = sorted(node.name for node in mapped)

    client = connect(
        seed=nodes[0],
        translation={"provider": "port-map", "port_map": di.port_map_by_endpoint(mapped), "on_miss": "strict"},
    )
    try:
        wait_for_nodes(client, mapped_names)
        retried = di.wait_for(lambda: len(misses(warnings, victim.name)) >= MISS_RETRIES, JOIN_TIMEOUT_SEC)
        found = misses(warnings, victim.name)
        print(f"L2-N8-RESULT victim={victim.name} nodes={sorted(di.client_nodes(client))} missWarnings={len(found)}")
        assert retried, f"Missed peer {victim.name} not retried every tend: {warnings}"
        assert sorted(di.client_nodes(client)) == mapped_names

        message = found[0]
        print(f"L2-N8-LOG {message}")
        assert message in [miss_message(victim.name, advertised) for advertised in victim.advertised]
        infos = {node.name: node.info for node in mapped}
        di.assert_keys_on_masters_work(client, NAMESPACE, SET_NAME, row="L2-N8", infos=infos)
    finally:
        client.close()


# N-10: port-map covers every node but the reachable one and on_miss is left at pass_through. The
# reachable node joins at its advertised address, every node serves the keys it is master of, and
# no miss is logged.
@pytest.mark.l2("reachable")
def test_n10_unmapped_peer_passes_through_unchanged(nodes: list[di.Node], warnings: list[str]) -> None:
    container = os.environ["DISCOVERY_REACHABLE_NODE"]
    reachable = next(node for node in nodes if node.proxy == container)
    assert reachable is not nodes[0], f"Reachable node {reachable} must not be the first listener"
    mapped = [node for node in nodes if node is not reachable]
    advertised = reachable.advertised[0]
    port_map = di.port_map_by_endpoint(mapped)
    assert advertised not in port_map
    print(f"L2-N10-ADVERTISED node={reachable.name} container={container} advertised={reachable.advertised}")

    client = connect(seed=mapped[0], translation={"provider": "port-map", "port_map": port_map})
    try:
        wait_for_nodes(client, [node.name for node in nodes])
        held = di.client_nodes(client)
        print(f"L2-N10-NODE {reachable.name} client={held[reachable.name]} advertised={advertised}")
        di.assert_at(held[reachable.name], advertised)
        infos = {node.name: node.info for node in nodes}
        di.assert_served_by_owners(client, NAMESPACE, SET_NAME, row="L2-N10", infos=infos)
        assert [message for message in warnings if "has no mapping" in message] == []
    finally:
        client.close()


def address_range() -> tuple[str, int, int]:
    """Return the prefix and the first and last octets of DISCOVERY_PORT_MAP_RANGE."""
    first, _, last = os.environ["DISCOVERY_PORT_MAP_RANGE"].partition("-")
    prefix, _, first_octet = first.rpartition(".")
    assert last.rpartition(".")[0] == prefix, f"Range {first}-{last}"
    last_octet = last.rpartition(".")[2]
    return prefix, int(first_octet), int(last_octet)


def range_listener(octet: int) -> str:
    host = os.environ["DISCOVERY_ENDPOINT_HOSTNAME"]
    port_base = int(os.environ["DISCOVERY_PORT_MAP_PORT_BASE"])
    return f"{host}:{port_base + octet}"


def service_port(nodes: list[di.Node]) -> int:
    return di.split_endpoint(nodes[0].advertised[0])[1]


def range_port_map(nodes: list[di.Node]) -> dict[str, str]:
    """Map every address of the range, on the nodes' service port, to its range listener, and check
    that the listeners exist and the nodes are in the range."""
    prefix, first, last = address_range()
    port = service_port(nodes)
    listeners = di.listener_ports()
    port_map = {}
    for octet in range(first, last + 1):
        listener = range_listener(octet)
        assert di.split_endpoint(listener)[1] in listeners, f"No listener {listener} for {prefix}.{octet}"
        port_map[f"{prefix}.{octet}:{port}"] = listener
    for node in nodes:
        assert node.advertised[0] in port_map, f"{node} is outside {os.environ['DISCOVERY_PORT_MAP_RANGE']}"
    print(f"L2-RANGE-PORT-MAP {port_map}")
    return port_map


def free_address(nodes: list[di.Node]) -> tuple[str, int]:
    prefix, first, last = address_range()
    used = {di.split_endpoint(advertised)[0] for node in nodes for advertised in node.advertised}
    for octet in range(first, last + 1):
        address = f"{prefix}.{octet}"
        if address not in used:
            return address, octet
    raise AssertionError(f"No free address in {os.environ['DISCOVERY_PORT_MAP_RANGE']}: {sorted(used)}")


# N-11 and N-19: a port-map pre-filled for every address of the range, including addresses no node
# has yet, joins the cluster with no warning. A node started on a free address of the range is
# active within 2 x tend_interval of the peers-generation bump that announces it (NFR-1), at the
# range listener for its address and with no client configuration change, and then serves the keys
# it is master of. N-11 runs here because the client's port-map is fixed once it is created, so a
# node added mid-test must already be in the map.
@pytest.mark.l2("port-map-range")
def test_n11_n19_prefilled_port_map_joins_new_node_within_two_tend_intervals(
    nodes: list[di.Node], added_nodes: list[str], warnings: list[str]
) -> None:
    observer = nodes[0]
    names = [node.name for node in nodes]
    port_map = range_port_map(nodes)
    client = connect(seed=observer, translation={"provider": "port-map", "port_map": port_map})
    try:
        wait_for_nodes(client, names)
        assert warnings == []

        address, octet = free_address(nodes)
        generation = observer.info("peers-generation")
        reply = di.control(f"start address={address}")
        added_nodes.append(reply["container"])
        assert reply["address"] == address

        joined, bump_to_active_ms = di.wait_joined(client, observer.info, generation, names, di.CONTROL_TIMEOUT_SEC)
        listener = range_listener(octet)
        budget_ms = 2 * TEND_INTERVAL_MS
        print(f"L2-N19-TIMING node={joined} address={address} bumpToActiveMs={bump_to_active_ms:.1f}")
        di.assert_at(joined, listener)
        assert bump_to_active_ms <= budget_ms, f"Active {bump_to_active_ms:.1f} ms after the bump, budget {budget_ms}"

        port = di.split_endpoint(listener)[1]
        added = di.Node(joined["node_name"], proxy=reply["container"], port=port,
                        advertised=[f"{address}:{service_port(nodes)}"])
        grown = nodes + [added]
        di.wait_cluster_stable([node.info for node in grown], len(grown))
        wait_for_nodes(client, [node.name for node in grown])
        infos = {node.name: node.info for node in grown}
        di.assert_served_by_owners(client, NAMESPACE, SET_NAME, row="L2-N19", infos=infos)
    finally:
        client.close()


class Seeds(SeedCandidateProvider):
    """Returns the seed listener on every refresh, and records each refresh's context and time."""

    def __init__(self, seed: SeedHost) -> None:
        self.seed = seed
        self.contexts = []
        self.times = []

    def refresh_seed_candidates(self, context: SeedCandidateContext) -> list[SeedHost]:
        self.contexts.append(context)
        self.times.append(time.monotonic())
        return [self.seed]

    def needs_periodic_refresh(self) -> bool:
        return True


# N-12: with every listener disabled the client drops to zero nodes and makes one urgent refresh of
# its seed provider. Once the listeners are enabled again it re-seeds from the seeds it holds within
# 2 x tend_interval plus the connect timeout, without waiting for refresh_interval_ms.
@pytest.mark.l2("default")
def test_n12_reseed_on_zero_active_nodes(nodes: list[di.Node], disabled_listeners: list[str]) -> None:
    names = [node.name for node in nodes]
    seeds = Seeds((os.environ["DISCOVERY_ENDPOINT_HOSTNAME"], nodes[0].port))
    discovery = {"provider": seeds, "refresh_interval_ms": RESEED_REFRESH_INTERVAL_MS}
    client = connect(
        seed=None,
        translation={"provider": "port-map", "port_map": port_map_by_node_name(nodes)},
        discovery=discovery,
    )
    try:
        wait_for_nodes(client, names)
        assert len(seeds.times) == 1, f"Refreshed at {seeds.times}"

        disabled = time.monotonic()
        disable_listeners(nodes, disabled_listeners)
        wait_for_no_nodes(client)
        urgent = di.wait_for(lambda: len(seeds.times) >= 2, JOIN_TIMEOUT_SEC)
        assert urgent, "No urgent refresh while the client had no nodes"

        enabled = time.monotonic()
        enable_listeners(disabled_listeners)
        reseeded = di.wait_for(client.get_node_names, JOIN_TIMEOUT_SEC, interval=0.01)
        reseed_ms = (time.monotonic() - enabled) * 1000
        assert reseeded, "The client did not re-seed"
        budget_ms = 2 * TEND_INTERVAL_MS + CONNECT_TIMEOUT_MS
        refreshes = [t for t in seeds.times if t >= disabled]
        print(f"L2-N12-RESEED enableToFirstNodeMs={reseed_ms:.0f} budgetMs={budget_ms} refreshesInOutage={len(refreshes)}")
        assert len(refreshes) == 1
        assert reseed_ms <= budget_ms, f"Re-seeded {reseed_ms:.0f} ms after the listeners were enabled"

        wait_for_nodes(client, names)
        infos = {node.name: node.info for node in nodes}
        di.assert_served_by_owners(client, NAMESPACE, SET_NAME, row="L2-N12", infos=infos)
    finally:
        client.close()


# N-14: use_services_alternate against nodes with no alternate address. The seed's alternate list is
# empty, so init fails with an error naming the node and the setting instead of an empty cluster
@pytest.mark.l2("default")
def test_n14_use_services_alternate_without_alternate_fails_init(nodes: list[di.Node]) -> None:
    seed = nodes[0]
    assert seed.info("service-clear-alt") == "", f"{seed.name} advertises an alternate address"

    with pytest.raises(aerospike.exception.InvalidNodeError) as raised:
        connect(seed, use_services_alternate=True)
    message = raised.value.msg
    print(f"L2-N14-ERROR {message}")
    assert message.startswith(f"Node {seed.name} "), message
    assert "has no alternate-access-address and use_services_alternate is true" in message


# N-18 on port-map: two peers are mapped to the same listener. Seeded through the third node, neither
# becomes a node, the collision is logged on every tend naming both nodes, the address and the
# translator, and the seed keeps serving the keys it is master of
@pytest.mark.l2("default")
def test_n18_port_map_colliding_nodes_create_no_node(nodes: list[di.Node], warnings: list[str]) -> None:
    assert len(nodes) >= 3, f"N-18 needs a seed and two colliding peers: {nodes}"
    seed, a, b = nodes[:3]
    port_map = di.port_map_by_endpoint(nodes)
    for advertised in b.advertised:
        port_map[advertised] = a.listener

    client = connect(seed=seed, translation={"provider": "port-map", "port_map": port_map})
    try:

        def logged() -> bool:
            return all(len(di.collisions(warnings, node.name)) >= MISS_RETRIES for node in (a, b))

        retried = di.wait_for(logged, JOIN_TIMEOUT_SEC)
        assert sorted(di.client_nodes(client)) == [seed.name]
        assert retried, f"Collision of {a.name} and {b.name} not logged every tend: {warnings}"
        message = di.collisions(warnings, a.name)[0]
        print(f"L2-N18-LOG {message}")
        assert message in di.collision_messages(translator="port-map", endpoint=a.listener, names=[a.name, b.name])
        di.assert_keys_on_masters_work(client, NAMESPACE, SET_NAME, row="L2-N18-port-map", infos={seed.name: seed.info})
    finally:
        client.close()


# N-19 outside the range: a node started on an address the pre-filled port-map does not cover is
# decided by on_miss. With strict the miss is logged on every tend and no node is created. With the
# pass_through default the unchanged address, which the client has no route to, is tried on every
# tend and no node is created.
@pytest.mark.l2("port-map-range")
def test_n19_node_outside_range_decided_by_on_miss(
    nodes: list[di.Node], added_nodes: list[str], warnings: list[str]
) -> None:
    observer = nodes[0]
    names = sorted(node.name for node in nodes)
    port_map = range_port_map(nodes)
    prefix, _, last = address_range()
    address = f"{prefix}.{last + 1}"
    advertised = f"{address}:{service_port(nodes)}"
    reply = di.control(f"start address={address}")
    added_nodes.append(reply["container"])
    di.wait_cluster_stable([node.info for node in nodes], len(nodes) + 1)
    outside = di.peer_names(observer.info("peers-clear-std"), advertised)
    print(f"L2-N19-OUTSIDE nodes={outside} advertised={advertised}")
    assert len(outside) == 1, f"Nodes advertising {advertised}: {outside}"
    assert advertised not in port_map

    client = connect(seed=observer, translation={"provider": "port-map", "port_map": port_map, "on_miss": "strict"})
    try:
        wait_for_nodes(client, names)
        retried = di.wait_for(lambda: len(misses(warnings, outside[0])) >= MISS_RETRIES, JOIN_TIMEOUT_SEC)
        assert sorted(di.client_nodes(client)) == names
        assert retried, f"Missed node {outside[0]} not retried every tend: {warnings}"
        message = misses(warnings, outside[0])[0]
        print(f"L2-N19-STRICT-LOG {message}")
        assert message == miss_message(outside[0], advertised)
    finally:
        client.close()

    warnings.clear()
    tried = f"Failed to connect to peer {address} {service_port(nodes)}."
    client = connect(seed=observer, translation={"provider": "port-map", "port_map": port_map})
    try:
        wait_for_nodes(client, names)

        def attempts() -> list[str]:
            return [message for message in warnings if message.startswith(tried)]

        retried = di.wait_for(lambda: len(attempts()) >= MISS_RETRIES, JOIN_TIMEOUT_SEC)
        assert sorted(di.client_nodes(client)) == names
        assert retried, f"Unchanged address {advertised} not tried every tend: {warnings}"
        print(f"L2-N19-PASS-THROUGH-LOG {attempts()[0]}")
    finally:
        client.close()


class BlockingSeeds(Seeds):
    """Blocks the second refresh until released."""

    def __init__(self, seed: SeedHost) -> None:
        super().__init__(seed)
        self.blocked = threading.Event()
        self.release = threading.Event()
        self.closed = threading.Event()

    def refresh_seed_candidates(self, context: SeedCandidateContext) -> list[SeedHost]:
        seeds = super().refresh_seed_candidates(context)
        if len(self.contexts) == 2:
            self.blocked.set()
            self.release.wait(5 * BLOCK_SEC)
        return seeds

    def close(self) -> None:
        self.closed.set()


def blocking_provider_client(nodes: list[di.Node]) -> tuple[BlockingSeeds, aerospike.Client]:
    seeds = BlockingSeeds((os.environ["DISCOVERY_ENDPOINT_HOSTNAME"], nodes[0].port))
    discovery = {
        "provider": seeds,
        "refresh_interval_ms": TEND_INTERVAL_MS,
        "refresh_timeout_ms": REFRESH_TIMEOUT_MS,
    }
    client = connect(
        seed=None,
        translation={"provider": "port-map", "port_map": port_map_by_node_name(nodes)},
        discovery=discovery,
    )
    return seeds, client


def assert_commands_work(client: aerospike.Client, row: str, seconds: int) -> int:
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
def test_m5_blocked_refresh_does_not_block_tend(nodes: list[di.Node], warnings: list[str]) -> None:
    seeds, client = blocking_provider_client(nodes)
    try:
        wait_for_nodes(client, [node.name for node in nodes])
        assert di.wait_for(seeds.blocked.is_set, JOIN_TIMEOUT_SEC), "The provider was not refreshed again"
        commands = assert_commands_work(client, row="M-5", seconds=BLOCK_SEC)
        assert sorted(di.client_nodes(client)) == sorted(node.name for node in nodes)
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
def test_m5_close_during_blocked_refresh_returns(nodes: list[di.Node], warnings: list[str]) -> None:
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
    # The discovery thread calls close() once the blocked refresh returns
    assert di.wait_for(seeds.closed.is_set, JOIN_TIMEOUT_SEC, interval=0.05), "The provider was never closed"
