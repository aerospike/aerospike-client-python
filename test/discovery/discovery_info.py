"""Info and topology helpers shared by the L2 and L3 discovery tests.

The L2 tests talk to each node through its Toxiproxy listener, including nodes the client does not
hold, so they send info requests over a raw socket. The L3 cluster has security enabled, so the L3
tests go through the client's info_single_node() instead. Both pass a function that takes one
info command and returns its value.

The L2 tests also change the topology mid-test: they enable and disable Toxiproxy listeners, and
ask the discovery node server to start and stop nodes.
"""

import base64
import json
import os
import re
import socket
import struct
import time
import urllib.request
import uuid
from pathlib import Path

import aerospike

PARTITIONS = 4096
INFO_VERSION = 2
INFO_TYPE = 1
CONTROL_TIMEOUT_SEC = 60
# One peer in a peers-* reply: [node,tls-name,[address[:port],...]]
PEER = re.compile(r"\[([0-9A-Fa-f]+),([^,\[\]]*),\[((?:[^\[\]]|\[[^\]]*\])*)\]\]")


def request(host, port, *commands, timeout=2.0):
    """Send info commands to host:port and return a dict of command to value."""
    body = "".join(command + "\n" for command in commands).encode()
    header = struct.pack(">Q", (INFO_VERSION << 56) | (INFO_TYPE << 48) | len(body))
    with socket.create_connection((host, port), timeout=timeout) as sock:
        sock.sendall(header + body)
        reply_header = read_exactly(sock, 8)
        size = struct.unpack(">Q", reply_header)[0] & 0xFFFFFFFFFFFF
        reply = read_exactly(sock, size).decode()

    values = {}
    for line in reply.split("\n"):
        name, separator, value = line.partition("\t")
        if separator:
            values[name] = value
    return values


def read_exactly(sock, size):
    data = b""
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise ConnectionError(f"Connection closed after {len(data)} of {size} bytes")
        data += chunk
    return data


def node_info(client, node_name):
    """Return an info function that sends each command to one node through the client."""

    def info(command):
        reply = client.info_single_node(command, node_name)
        return reply.partition("\t")[2].rstrip("\n")

    return info


def endpoints(reply):
    """Parse a service-clear-std or service-clear-alt reply into host:port strings."""
    return [endpoint for endpoint in reply.split(";") if endpoint]


def split_endpoint(endpoint):
    host, _, port = endpoint.rpartition(":")
    return host.strip("[]"), int(port)


def peers(reply):
    """Parse a peers-* reply, gen,port,[[node,tls-name,[address[:port],...]],...], into
    (node, tls-name, host:port strings) tuples. An address without a port has the reply's port."""
    _, port, entries = reply.split(",", 2)
    found = []
    for name, tls_name, addresses in PEER.findall(entries):
        endpoints = []
        for address in addresses.split(","):
            if not address:
                continue
            if address.startswith("["):
                has_port = "]:" in address
            else:
                has_port = ":" in address
            if not has_port:
                address = f"{address}:{port}"
            endpoints.append(address)
        found.append((name, tls_name, endpoints))
    return found


def peer_names(reply, endpoint):
    """Return the names of the peers in a peers-* reply that advertise endpoint."""
    return sorted(name for name, _, endpoints in peers(reply) if endpoint in endpoints)


def client_nodes(client):
    """Return the client's nodes, each a get_node_names() entry, by node name."""
    return {entry["node_name"]: entry for entry in client.get_node_names()}


def wait_for_nodes(client, names, timeout, tend_interval_ms):
    """Wait until the client holds exactly the named nodes."""
    expected = sorted(names)

    def joined():
        return sorted(client_nodes(client)) == expected

    complete = wait_for(joined, timeout)
    held = client_nodes(client)
    assert complete, f"Client nodes {sorted(held)}, expected {expected}"
    # The client exposes no partition map, so give it the tends that fetch every node's partitions
    time.sleep(4 * tend_interval_ms / 1000)


def assert_at(entry, endpoint):
    """Assert that a get_node_names() entry is at endpoint, a host:port string."""
    host, port = split_endpoint(endpoint)
    resolved = socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
    addresses = {info[4][0] for info in resolved}
    assert entry["address"] in addresses | {host}, f"{entry} is not at {host} {sorted(addresses)}"
    assert entry["port"] == port, f"{entry} is not at port {port}"


def wait_joined(client, observer, generation, names, timeout, interval=0.005):
    """Wait until the observer node's peers-generation moves on from generation and the client holds
    a node not in names. Return that node's get_node_names() entry and the milliseconds from the
    bump to the join."""
    bump_ns = None
    active_ns = None
    joined = None
    deadline = time.monotonic() + timeout
    while (bump_ns is None or active_ns is None) and time.monotonic() < deadline:
        now = time.monotonic_ns()
        if bump_ns is None and observer("peers-generation") != generation:
            bump_ns = now
        if active_ns is None:
            held = client_nodes(client)
            new = [entry for name, entry in held.items() if name not in names]
            if new:
                active_ns = now
                joined = new[0]
        time.sleep(interval)

    assert bump_ns is not None, f"peers-generation stayed at {generation}"
    assert joined is not None, "No new node joined the client"
    return joined, (active_ns - bump_ns) / 1e6


def toxiproxy(method, path, body=None):
    """Call the Toxiproxy control API and return its decoded reply, or None for an empty one."""
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


def listener_ports():
    """Return the port of every Toxiproxy listener."""
    proxies = toxiproxy("GET", "/proxies")
    return {split_endpoint(proxy["listen"])[1] for proxy in proxies.values()}


def set_listener(proxy, enabled):
    """Enable or disable a Toxiproxy listener. Disabling it also closes its connections."""
    toxiproxy("POST", f"/proxies/{proxy}", {"enabled": enabled})


def control(command):
    """Send a request to the discovery node server and return its acknowledgement."""
    directory = Path(os.environ["DISCOVERY_CONTROL_DIR"])
    request_id = uuid.uuid4().hex
    tmp = directory / f"{request_id}.tmp"
    ack = directory / f"{request_id}.ack"
    tmp.write_text(command + "\n")
    tmp.rename(directory / f"{request_id}.request")
    answered = wait_for(ack.exists, CONTROL_TIMEOUT_SEC)
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


def wait_cluster_stable(infos, size):
    """Wait until every node, given by its info function, reports the same cluster key for a cluster
    of size nodes with no migrations left."""
    command = f"cluster-stable:size={size};ignore-migrations=false"

    def stable():
        keys = {info(command) for info in infos}
        return len(keys) == 1 and not keys.pop().startswith("ERROR")

    settled = wait_for(stable, CONTROL_TIMEOUT_SEC)
    assert settled, f"Cluster not stable at size {size}"


def namespace_stats(info, namespace, names):
    """Return the named integer statistics of a namespace."""
    stats = {}
    for token in info(f"namespace/{namespace}").split(";"):
        name, _, value = token.partition("=")
        if name in names:
            stats[name] = int(value)
    missing = set(names) - set(stats)
    assert not missing, f"Namespace {namespace} has no statistics {sorted(missing)}"
    return stats


def master_partitions(info, namespace):
    """Return the partition ids a node is master of, read from its replicas bitmap."""
    for entry in info("replicas").split(";"):
        name, _, fields = entry.partition(":")
        if name != namespace:
            continue
        # regime,n_replicas,master bitmap,replica bitmaps...
        master_bitmap = fields.split(",")[2]
        bitmap = base64.b64decode(master_bitmap)
        return {pid for pid in range(PARTITIONS) if bitmap[pid >> 3] & (0x80 >> (pid & 7))}
    raise AssertionError(f"No replicas entry for namespace {namespace}")


def assert_served_by_owners(client, namespace, set_name, row, infos, key_count=300):
    """Write and read keys with replica master and assert, from each node's namespace statistics,
    that each node served exactly the keys it is master of with no server proxy hop.

    infos maps each node name to its info function.
    """
    owners = {}
    for name, info in infos.items():
        for pid in master_partitions(info, namespace):
            assert pid not in owners, f"Partition {pid} has masters {owners[pid]} and {name}"
            owners[pid] = name
    assert len(owners) == PARTITIONS, f"{PARTITIONS - len(owners)} partitions have no master"

    stat_names = (
        "client_write_success",
        "client_read_success",
        "client_proxy_complete",
        "client_proxy_error",
        "client_proxy_timeout",
    )
    before = {name: namespace_stats(info, namespace, stat_names) for name, info in infos.items()}
    read_policy = {"replica": aerospike.POLICY_REPLICA_MASTER}
    served = dict.fromkeys(infos, 0)

    for i in range(key_count):
        user_key = f"{row}-{i}"
        pid = client.get_key_partition_id(namespace, set_name, user_key)
        key = (namespace, set_name, user_key)
        client.put(key, {"v": i})
        _, _, bins = client.get(key, policy=read_policy)
        assert bins == {"v": i}
        served[owners[pid]] += 1

    for name, info in infos.items():
        after = namespace_stats(info, namespace, stat_names)
        delta = {stat: after[stat] - before[name][stat] for stat in stat_names}
        proxied = delta["client_proxy_complete"] + delta["client_proxy_error"] + delta["client_proxy_timeout"]
        print(f"{row}-SERVED node={name} keys={served[name]} delta={delta}")
        assert served[name] > 0, f"{name} served no keys"
        assert delta["client_write_success"] == served[name], f"{name} writes"
        assert delta["client_read_success"] == served[name], f"{name} reads"
        assert proxied == 0, f"{name} proxied {proxied} commands"


def collisions(messages, name):
    """Return the translation collision messages that name a node."""
    return [message for message in messages if "translated nodes" in message and name in message]


def collision_messages(translator, endpoint, names):
    """Return the C client's collision message for two nodes translated to endpoint, in either order."""
    first, second = names
    return [
        f"Address translator {translator} translated nodes {a} and {b} to the same address {endpoint}. "
        "Give each node its own reachable address or mapping"
        for a, b in ((first, second), (second, first))
    ]


def assert_keys_on_masters_work(client, namespace, set_name, row, infos, key_count=300):
    """Write and read every key whose master is one of the nodes in infos, which maps each node name
    to its info function, and assert that some keys have their master on another node."""
    owners = {}
    for name, info in infos.items():
        for pid in master_partitions(info, namespace):
            owners[pid] = name

    read_policy = {"replica": aerospike.POLICY_REPLICA_MASTER}
    served = 0
    elsewhere = 0
    for i in range(key_count):
        user_key = f"{row}-{i}"
        pid = client.get_key_partition_id(namespace, set_name, user_key)
        if pid not in owners:
            elsewhere += 1
            continue
        key = (namespace, set_name, user_key)
        client.put(key, {"v": i})
        _, _, bins = client.get(key, policy=read_policy)
        assert bins == {"v": i}
        served += 1
    print(f"{row}-KEYS nodes={sorted(infos)} keysOnTheirMasters={served} keysElsewhere={elsewhere}")
    assert served > 0, f"No key has its master on {sorted(infos)}"
    assert elsewhere > 0, f"Every key has its master on {sorted(infos)}"


def wait_for(condition, timeout, interval=0.25):
    """Poll condition until it returns a truthy value, and return that value or None."""
    deadline = time.monotonic() + timeout
    while True:
        result = condition()
        if result:
            return result
        if time.monotonic() >= deadline:
            return None
        time.sleep(interval)
