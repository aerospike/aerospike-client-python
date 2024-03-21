from aerospike import exception as e
from aerospike_helpers.metrics import MetricsPolicy, MetricsListeners, Cluster, Node
import pytest
import shutil
import glob
import os
import time


enable_triggered = False
disable_triggered = False
node_close_triggered = False
snapshot_triggered = False


class MyMetricsListeners:
    def enable():
        global enable_triggered
        enable_triggered = True

    def disable(cluster: Cluster):
        global disable_triggered
        disable_triggered = True

    def node_close(node: Node):
        global node_close_triggered
        node_close_triggered = True

    def snapshot(cluster: Cluster):
        global snapshot_triggered
        snapshot_triggered = True


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
        self.policy = MetricsPolicy(
            metrics_listeners=listeners,
            report_dir=self.metrics_log_folder,
            report_size_limit=1000,
            interval=2,
            latency_columns=5,
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
