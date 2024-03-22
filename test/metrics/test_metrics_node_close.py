import subprocess
import time

import aerospike
from aerospike_helpers.metrics import MetricsListeners, MetricsPolicy, Cluster, Node, ConnectionStats, NodeMetrics


# Tests that the Node object returned from the node_close callback has these fields
# and makes sure these fields have the correct types
def test_node_is_populated(node: Node):
    assert type(node) == Node
    assert type(node.name) == str
    assert type(node.address) == str
    assert type(node.port) == int
    assert type(node.conns) == ConnectionStats
    assert type(node.conns.in_use) == int
    assert type(node.conns.in_pool) == int
    assert type(node.conns.opened) == int
    assert type(node.conns.closed) == int
    assert type(node.error_count) == int
    assert type(node.timeout_count) == int
    # Check NodeMetrics
    assert type(node.metrics) == NodeMetrics
    metrics = node.metrics
    latency_buckets = [
        metrics.conn_latency,
        metrics.write_latency,
        metrics.read_latency,
        metrics.batch_latency,
        metrics.query_latency
    ]
    for buckets in latency_buckets:
        assert type(buckets) == list
        # Just hardcode the default bucket_count used in our test's MetricsPolicy
        assert len(buckets) == 7
        for bucket in buckets:
            assert type(bucket) == int


# Flag for callback to set
node_close_called: bool = False

# Callbacks


def enable():
    pass


def disable(_: Cluster):
    pass


def node_close(node: Node):
    print("Node close listener is running")
    global node_close_called
    node_close_called = True

    test_node_is_populated(node)


def snapshot(_: Cluster):
    pass


NODE_COUNT = 3
print(f"Creating {NODE_COUNT} node cluster...")
subprocess.run(["aerolab", "cluster", "create", f"--count={NODE_COUNT}"], check=True)

try:
    # Wait for server to fully start up
    time.sleep(5)

    config = {
        "hosts": [
            ("127.0.0.1", 3000)
        ]
    }
    try:
        print("Connecting to server using Python client...")
        c = aerospike.client(config)
        print("Waiting for client to collect all information about cluster nodes...")
        time.sleep(5)
        listeners = MetricsListeners(
            enable_listener=enable,
            disable_listener=disable,
            node_close_listener=node_close,
            snapshot_listener=snapshot
        )
        policy = MetricsPolicy(metrics_listeners=listeners)
        print("Enabling metrics...")
        c.enable_metrics(policy=policy)

        # Close one node
        print("Closing one node...")
        subprocess.run(["aerolab", "cluster", "stop", "--nodes=1"], check=True)
        # Run with --force or else it will ask to confirm
        subprocess.run(["aerolab", "cluster", "destroy", "--nodes=1", "--force"], check=True)

        print("Giving client time to run the node_close listener...")
        time.sleep(33)

        assert node_close_called is True
    finally:
        c.close()
finally:
    # Cleanup
    subprocess.run(["aerolab", "cluster", "stop"], check=True)
    subprocess.run(["aerolab", "cluster", "destroy", "--force"], check=True)
