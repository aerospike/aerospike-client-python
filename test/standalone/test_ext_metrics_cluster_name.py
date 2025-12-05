import time

import docker

import aerospike
from aerospike_helpers.metrics import MetricsPolicy, MetricsListeners, Cluster, Node

docker_client = docker.from_env()

# Cluster name should be "docker"
print("Running server container...")
container = docker_client.containers.run("aerospike/aerospike-server", detach=True, ports={"3000/tcp": 3000})
print("Waiting for server to initialize...")
time.sleep(5)

try:
    config = {
        "hosts": [
            ("127.0.0.1", 3000)
        ],
        "cluster_name": "docker"
    }
    print("Connecting to server...")
    as_client = aerospike.client(config)

    try:
        def enable():
            pass

        snapshot_triggered = False
        disable_triggered = False

        def snapshot(cluster: Cluster):
            print(f"Snapshot listener run. Cluster name: {cluster.cluster_name}")
            global snapshot_triggered
            snapshot_triggered = True
            assert cluster.cluster_name == "docker"

        def disable(cluster: Cluster):
            print(f"Disable listener run. Cluster name: {cluster.cluster_name}")
            global disable_triggered
            disable_triggered = True
            assert cluster.cluster_name == "docker"

        def node_close(_: Node):
            pass

        listeners = MetricsListeners(
            enable_listener=enable,
            disable_listener=disable,
            snapshot_listener=snapshot,
            node_close_listener=node_close
        )
        policy = MetricsPolicy(metrics_listeners=listeners, interval=1)
        as_client.enable_metrics(policy=policy)
        # Leave enough time for snapshot callback to run
        time.sleep(3)
        # Run disable callback
        as_client.disable_metrics()

        # Ensure both snapshot and disable callbacks were called
        # They both check that the cluster name is parsed properly into the Python client's Cluster object
        assert snapshot_triggered is True
        assert disable_triggered is True

    finally:
        # Cleanup
        as_client.close()
finally:
    container.stop()
    container.remove()
    docker_client.close()
