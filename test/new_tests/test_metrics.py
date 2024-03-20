from aerospike import exception as e
from aerospike_helpers.metrics import MetricsPolicy, MetricsListeners, Cluster, Node
import pytest
import os


class TestMetrics:
    @pytest.fixture(scope="class")
    def metrics_cleanup(self):
        # Set defaults (in case overwritten by a test)
        self.metrics_log_path = "./metrics-*.log"
        yield
        os.remove(self.metrics_log_path)

    def test_enable_metrics(self):
        retval = self.as_connection.enable_metrics()
        assert retval is None

    def test_enable_metrics_invalid_args(self):
        with pytest.raises(TypeError):
            self.as_connection.enable_metrics(1)

    def test_enable_metrics_with_default_metrics_policy(self):
        self.policy = MetricsPolicy()
        self.as_connection.enable_metrics(policy=self.policy)

    def test_enable_metrics_with_metrics_policy_custom_settings(self):
        def enable():
            pass

        def disable(cluster: Cluster):
            pass

        def node_close(node: Node):
            pass

        def snapshot(cluster: Cluster):
            pass

        self.metrics_log_path = "./metrics-logs"

        listeners = MetricsListeners(
            enable_listener=enable,
            disable_listener=disable,
            node_close_listener=node_close,
            snapshot_listener=snapshot
        )
        self.policy = MetricsPolicy(
            metrics_listeners=listeners,
            report_dir=self.metrics_log_path,
            report_size_limit=1000,
            interval=2,
            latency_columns=5,
            latency_shift=2
        )
        self.as_connection.enable_metrics(policy=self.policy)

    @pytest.mark.parametrize(
        "policy", [
            MetricsPolicy(metrics_listeners=1),
            MetricsPolicy(metrics_listeners=MetricsListeners(
                enable_listener=1
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
