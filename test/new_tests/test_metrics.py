from aerospike import exception as e
from aerospike_helpers.metrics import MetricsPolicy, MetricsListeners, Cluster, Node, ConnectionStats, NodeMetrics
import pytest
import shutil
import glob
import os
import time
from typing import Optional


enable_triggered = False
disable_triggered = False
node_close_triggered = False
snapshot_triggered = False

cluster_from_disable_listener: Optional[Cluster] = None
cluster_from_snapshot_listener: Optional[Cluster] = None


class MyMetricsListeners:
    def enable():
        global enable_triggered
        enable_triggered = True

    def disable(cluster: Cluster):
        global disable_triggered
        disable_triggered = True
        global cluster_from_disable_listener
        cluster_from_disable_listener = cluster

    def node_close(node: Node):
        global node_close_triggered
        node_close_triggered = True

    def snapshot(cluster: Cluster):
        global snapshot_triggered
        snapshot_triggered = True
        global cluster_from_snapshot_listener
        cluster_from_snapshot_listener = cluster

    def enable_throw_exc():
        raise Exception()


class TestMetrics:
    @pytest.fixture(autouse=True)
    def setup(self, as_connection, request):
        # Set defaults (in case they were overwritten by a test)
        self.metrics_log_folder = "."

        def teardown():
            # Remove all metrics log files
            self.metrics_log_files = f"{self.metrics_log_folder}/metrics-*.log"
            for item in glob.glob(self.metrics_log_files):
                print(f"Removing {item}")
                os.remove(item)
            # Remove folder containing log files if we used one
            if self.metrics_log_folder != '.' and os.path.exists(self.metrics_log_folder):
                print(f"Removing {self.metrics_log_folder}")
                shutil.rmtree(self.metrics_log_folder)

            self.as_connection.disable_metrics()

        request.addfinalizer(teardown)

    def test_enable_metrics(self):
        retval = self.as_connection.enable_metrics()
        assert retval is None

    def test_enable_metrics_invalid_args(self):
        with pytest.raises(TypeError):
            self.as_connection.enable_metrics(None, 1)

    def test_enable_metrics_with_default_metrics_policy(self):
        self.policy = MetricsPolicy()
        self.as_connection.enable_metrics(policy=self.policy)

    def test_enable_metrics_with_metrics_policy_custom_settings(self):
        self.metrics_log_folder = "./metrics-logs"

        listeners = MetricsListeners(
            enable_listener=MyMetricsListeners.enable,
            disable_listener=MyMetricsListeners.disable,
            node_close_listener=MyMetricsListeners.node_close,
            snapshot_listener=MyMetricsListeners.snapshot
        )
        # Save bucket count for testing later
        bucket_count = 5
        self.policy = MetricsPolicy(
            metrics_listeners=listeners,
            report_dir=self.metrics_log_folder,
            report_size_limit=1000,
            interval=2,
            latency_columns=bucket_count,
            latency_shift=2
        )

        # Ensure that callbacks haven't been called yet
        global enable_triggered
        global disable_triggered
        global snapshot_triggered
        assert enable_triggered is False
        assert disable_triggered is False
        assert snapshot_triggered is False

        self.as_connection.enable_metrics(policy=self.policy)
        time.sleep(3)
        self.as_connection.disable_metrics()

        # These callbacks should've been called
        assert enable_triggered is True
        assert disable_triggered is True
        assert snapshot_triggered is True

        # The Cluster objects returned from the disable and snapshot callbacks should be populated
        global cluster_from_disable_listener
        global cluster_from_snapshot_listener
        for cluster in [cluster_from_disable_listener, cluster_from_snapshot_listener]:
            assert type(cluster) == Cluster
            assert cluster.cluster_name is None or type(cluster.cluster_name) == str
            assert type(cluster.invalid_node_count) == int
            assert type(cluster.tran_count) == int
            assert type(cluster.retry_count) == int
            assert type(cluster.nodes) == list
            # Also check the Node and ConnectionStats objects in the Cluster object were populated
            for node in cluster.nodes:
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
                    assert len(buckets) == bucket_count
                    for bucket in buckets:
                        assert type(bucket) == int

    def test_enable_metrics_with_enable_listener_throwing_exception(self):
        listeners = MetricsListeners(
            enable_listener=MyMetricsListeners.enable_throw_exc,
            disable_listener=MyMetricsListeners.disable,
            node_close_listener=MyMetricsListeners.node_close,
            snapshot_listener=MyMetricsListeners.snapshot
        )
        policy = MetricsPolicy(
            listeners,
        )
        with pytest.raises(e.AerospikeError):
            self.as_connection.enable_metrics(policy=policy)

    @pytest.mark.parametrize(
        "policy", [
            MetricsPolicy(metrics_listeners=1),
            MetricsPolicy(metrics_listeners=MetricsListeners(
                enable_listener=1,
                disable_listener=MyMetricsListeners.disable,
                node_close_listener=MyMetricsListeners.node_close,
                snapshot_listener=MyMetricsListeners.snapshot
            )),
            MetricsPolicy(report_dir=1),
            MetricsPolicy(report_size_limit=True),
            MetricsPolicy(interval=True),
            MetricsPolicy(latency_columns=True),
            MetricsPolicy(latency_shift=True)
        ]
    )
    def test_enable_metrics_invalid_policy_args(self, policy):
        with pytest.raises(e.ParamError):
            self.as_connection.enable_metrics(policy=policy)
