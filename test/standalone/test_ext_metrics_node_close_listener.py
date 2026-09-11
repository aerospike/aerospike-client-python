import time
import docker

import aerospike
from aerospike_helpers.metrics import MetricsListeners, MetricsPolicy, Cluster, Node, ConnectionStats, NamespaceMetrics


# Copied from new_tests/test_metrics.py
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
    assert type(node.conns.recovered) == int
    assert type(node.conns.aborted) == int
    assert type(node.error_count) == int
    assert type(node.timeout_count) == int
    # Check NodeMetrics
    assert type(node.metrics) == NamespaceMetrics
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


docker_client = docker.from_env()
print("Running server container...")
SERVER_PORT_NUMBER = 3000
container = docker_client.containers.run("aerospike/aerospike-server", detach=True,
                                         ports={"3000/tcp": SERVER_PORT_NUMBER})
print("Waiting for server to initialize...")
time.sleep(5)

try:
    print("Connecting to server...")
    config = {
        "hosts": [
            ("127.0.0.1", SERVER_PORT_NUMBER)
        ],
    }
    as_client = aerospike.client(config)
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
        as_client.enable_metrics(policy=policy)

        # Close the last node
        print("Closing node...")
        container.stop()
        container.remove()

        print("Giving client time to run the node_close listener...")
        elapsed_secs = 0
        while elapsed_secs < 10:
            if node_close_called:
                break
            time.sleep(1)
            elapsed_secs += 1

        assert node_close_called is True
        # Need to print assert result somehow
        print("node_close_called is true. Passed")
    finally:
        # Calling close() after we lose connection to the whole cluster is safe. It will be a no-op
        # It is not explicitly documented for the Python client or C client, but this behavior was verified with C
        # client developer
        as_client.close()
finally:
    docker_client.close()
