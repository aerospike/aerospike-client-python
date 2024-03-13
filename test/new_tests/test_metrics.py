from aerospike import Client
from aerospike_helpers.metrics import MetricsPolicy, MetricsListeners, Cluster, Node
import pytest


class TestMetrics:
    def test_enable_metrics(self, as_connection: Client):
        as_connection.enable_metrics()

    def test_enable_metrics_invalid_args(self, as_connection: Client):
        with pytest.raises(TypeError):
            as_connection.enable_metrics(1)

    def test_enable_metrics_with_default_metrics_policy(self, as_connection: Client):
        policy = MetricsPolicy()
        as_connection.enable_metrics(policy=policy)

    def test_enable_metrics_with_metrics_policy_custom_settings(self, as_connection: Client):
        def enable():
            pass

        def disable(cluster: Cluster):
            pass

        def node_close(node: Node):
            pass

        def snapshot(cluster: Cluster):
            pass

        listeners = MetricsListeners(
            enable_listener=enable,
            disable_listener=disable,
            node_close_listener=node_close,
            snapshot_listener=snapshot
        )
        policy = MetricsPolicy(
            metrics_listeners=listeners,
            report_dir="./metrics-logs",
            report_size_limit=1000,
            interval=2,
            latency_columns=5,
            latency_shift=2
        )
        as_connection.enable_metrics(policy=policy)
