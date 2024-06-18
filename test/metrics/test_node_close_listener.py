import subprocess
import time
import json

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


NODE_COUNT = 1
print(f"Creating {NODE_COUNT} node cluster...")
subprocess.run(["aerolab", "cluster", "create", f"--count={NODE_COUNT}"], check=True)

try:
    print("Wait for server to fully start up...")
    time.sleep(5)

    # Connect to the first node
    completed_process = subprocess.run(["aerolab", "cluster", "list", "--json"], check=True, capture_output=True)
    list_of_nodes = json.loads(completed_process.stdout)

    def get_first_node(node_info: dict):
        return node_info["NodeNo"] == "1"

    filtered_list_of_nodes = filter(get_first_node, list_of_nodes)
    first_node = list(filtered_list_of_nodes)[0]
    first_node_port = int(first_node["DockerExposePorts"])
    HOST_NAME = "127.0.0.1"

    config = {
        "hosts": [
            (HOST_NAME, first_node_port)
        ],
        # The nodes use internal Docker IP addresses as their access addresses
        # But we cannot connect to those from the host
        # But the nodes use localhost as the alternate address
        # So we can connect to that instead
        "use_services_alternate": True
    }
    print(f"Connecting to {HOST_NAME}:{first_node_port} using Python client...")
    c = aerospike.client(config)
    try:
        # Show logs to confirm that node is removed from the client's perspective
        aerospike.set_log_level(aerospike.LOG_LEVEL_DEBUG)
        aerospike.set_log_handler()
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

        NODE_TO_CLOSE = 2
        print(f"Closing node {NODE_TO_CLOSE}...")
        subprocess.run(["aerolab", "cluster", "stop", f"--nodes={NODE_TO_CLOSE}"], check=True)
        # Run with --force or else it will ask to confirm
        subprocess.run(["aerolab", "cluster", "destroy", f"--nodes={NODE_TO_CLOSE}", "--force"], check=True)

        print("Giving client time to run the node_close listener...")
        time.sleep(10)

        assert node_close_called is True
    finally:
        c.close()
finally:
    # Cleanup
    subprocess.run(["aerolab", "cluster", "stop"], check=True)
    subprocess.run(["aerolab", "cluster", "destroy", "--force"], check=True)
