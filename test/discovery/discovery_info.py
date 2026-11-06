"""Info helpers shared by the L2 and L3 discovery tests.

The L2 tests talk to each node through its Toxiproxy listener, including nodes the client does not
hold, so they send info requests over a raw socket. The L3 cluster has security enabled, so the L3
tests go through the client's info_single_node() instead. Both pass a function that takes one
info command and returns its value.
"""

import base64
import socket
import struct
import time

import aerospike

PARTITIONS = 4096
INFO_VERSION = 2
INFO_TYPE = 1


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
