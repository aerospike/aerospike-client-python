from aerospike import exception as e
from aerospike_helpers.metrics import MetricsPolicy, MetricsListeners, Cluster, Node, ConnectionStats, NodeMetrics
import pytest
import shutil
import glob
import os
import time
from typing import Optional


# Flags for testing callbacks
enable_triggered = False
disable_triggered = False
snapshot_triggered = False

# Cluster objects returned from callbacks
cluster_from_disable_listener: Optional[Cluster] = None
cluster_from_snapshot_listener: Optional[Cluster] = None


class Unstringable:
    def __str__(self):
        raise Exception()


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
        pass

    def snapshot(cluster: Cluster):
        global snapshot_triggered
        snapshot_triggered = True
        global cluster_from_snapshot_listener
        cluster_from_snapshot_listener = cluster

    def enable_throw_exc():
        raise ValueError("enable threw an error")

    def enable_throw_exc_with_bad_value():
        obj = Unstringable()
        raise ValueError(obj)

    def disable_throw_exc(_: Cluster):
        raise ValueError("disable threw an error")


class TestMetrics:
    @pytest.fixture(autouse=True)
    def setup(self, as_connection, request):
        # Clear results from previous tests
        global enable_triggered
        global disable_triggered
        global snapshot_triggered
        enable_triggered = False
        disable_triggered = False
        snapshot_triggered = False

        global cluster_from_disable_listener
        global cluster_from_snapshot_listener
        cluster_from_disable_listener = None
        cluster_from_snapshot_listener = None

        # Set defaults (in case they were overwritten by a test)
        self.metrics_log_folder = "."

        def teardown():
            # Close any file descriptors for metrics logs before we remove the files
            self.as_connection.disable_metrics()

            # Remove all metrics log files
            metrics_log_files = f"{self.metrics_log_folder}/metrics-*.log"
            for item in glob.glob(metrics_log_files):
                os.remove(item)
            # Remove folder containing log files if we used one
            if self.metrics_log_folder != '.' and os.path.exists(self.metrics_log_folder):
                shutil.rmtree(self.metrics_log_folder)

        request.addfinalizer(teardown)

    def test_enable_metrics(self):
        retval = self.as_connection.enable_metrics()
        assert retval is None

    def test_enable_metrics_extra_args(self):
        with pytest.raises(TypeError):
            self.as_connection.enable_metrics(None, 1)

    @pytest.mark.parametrize(
        "policy",
        [
            MetricsPolicy(),
            None
        ]
    )
    def test_enable_metrics_with_valid_arg_types(self, policy):
        self.as_connection.enable_metrics(policy=policy)

    def test_enable_metrics_with_invalid_arg(self):
        with pytest.raises(e.ParamError) as excinfo:
            self.as_connection.enable_metrics(1)
        assert excinfo.value.msg == "policy parameter must be an aerospike_helpers.MetricsPolicy type"

    def test_metrics_writer(self):
        policy = MetricsPolicy(
            interval=1
        )
        self.as_connection.enable_metrics(policy)
        time.sleep(3)
        self.as_connection.disable_metrics()

        # A metrics log file should've been created
        metrics_log_filenames = glob.glob("./metrics-*.log")
        assert len(metrics_log_filenames) > 0

    def test_setting_metrics_policy_custom_settings(self):
        self.metrics_log_folder = "./metrics-logs"

        listeners = MetricsListeners(
            enable_listener=MyMetricsListeners.enable,
            disable_listener=MyMetricsListeners.disable,
            node_close_listener=MyMetricsListeners.node_close,
            snapshot_listener=MyMetricsListeners.snapshot
        )
        # Save bucket count for testing later
        bucket_count = 5
        policy = MetricsPolicy(
            metrics_listeners=listeners,
            report_dir=self.metrics_log_folder,
            report_size_limit=1000,
            interval=2,
            latency_columns=bucket_count,
            latency_shift=2
        )

        self.as_connection.enable_metrics(policy=policy)
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
                    assert len(buckets) == bucket_count
                    for bucket in buckets:
                        assert type(bucket) == int

    # Unable to test the case where an exception value could not be retrieved
    # Having the callback raise an Exception without a value does not trigger this
    @pytest.mark.parametrize(
        "enable_callback, err_msg_details",
        [
            (MyMetricsListeners.enable_throw_exc, "Exception value: enable threw an error"),
            (MyMetricsListeners.enable_throw_exc_with_bad_value, "str() on exception value threw an error")
        ]
    )
    def test_enable_listener_throwing_exception(self, enable_callback: callable, err_msg_details: str):
        listeners = MetricsListeners(
            enable_listener=enable_callback,
            disable_listener=MyMetricsListeners.disable,
            node_close_listener=MyMetricsListeners.node_close,
            snapshot_listener=MyMetricsListeners.snapshot
        )
        policy = MetricsPolicy(
            listeners,
        )
        with pytest.raises(e.AerospikeError) as excinfo:
            self.as_connection.enable_metrics(policy=policy)
        assert excinfo.value.msg == f"Python callback enable_listener threw a ValueError exception. {err_msg_details}"

    @pytest.mark.parametrize(
        # Policy containing field with invalid type
        # The last 2 parameters are for the expected error message
        "policy, field_name, expected_field_type", [
            (
                MetricsPolicy(metrics_listeners=1),
                "metrics_listeners",
                "aerospike_helpers.metrics.MetricsListeners"
            ),
            (
                MetricsPolicy(
                    metrics_listeners=MetricsListeners(
                        enable_listener=1,
                        disable_listener=MyMetricsListeners.disable,
                        node_close_listener=MyMetricsListeners.node_close,
                        snapshot_listener=MyMetricsListeners.snapshot
                    )
                ),
                "metrics_listeners.enable_listener",
                "callable"
            ),
            (
                MetricsPolicy(report_dir=1),
                "report_dir",
                "str"
            ),
            (
                MetricsPolicy(report_size_limit=True),
                "report_size_limit",
                "unsigned 64-bit integer"
            ),
            (
                MetricsPolicy(interval=True),
                "interval",
                "unsigned 32-bit integer"
            ),
            (
                MetricsPolicy(latency_columns=True),
                "latency_columns",
                "unsigned 32-bit integer"
            ),
            (
                MetricsPolicy(latency_shift=True),
                "latency_shift",
                "unsigned 32-bit integer"
            ),
            # Pass in an integer larger than allowed for an unsigned 32-bit integer
            (
                MetricsPolicy(latency_shift=2**32),
                "latency_shift",
                "unsigned 32-bit integer"
            )
        ]
    )
    def test_metrics_policy_invalid_args(self, policy, field_name, expected_field_type):
        with pytest.raises(e.ParamError) as excinfo:
            self.as_connection.enable_metrics(policy=policy)
        assert excinfo.value.msg == f"MetricsPolicy.{field_name} must be a {expected_field_type} type"

    def test_metrics_policy_report_dir_too_long(self):
        policy = MetricsPolicy(report_dir=str('.' * 257))
        with pytest.raises(e.ParamError) as excinfo:
            self.as_connection.enable_metrics(policy=policy)
        assert excinfo.value.msg == "MetricsPolicy.report_dir must be less than 256 chars"

    def test_disable_metrics(self):
        retval = self.as_connection.disable_metrics()
        assert retval is None

    def test_disable_metrics_invalid_args(self):
        with pytest.raises(TypeError):
            self.as_connection.disable_metrics(1)

    def test_disable_metrics_throwing_exc(self):
        listeners = MetricsListeners(
            enable_listener=MyMetricsListeners.enable,
            disable_listener=MyMetricsListeners.disable_throw_exc,
            node_close_listener=MyMetricsListeners.node_close,
            snapshot_listener=MyMetricsListeners.snapshot
        )
        policy = MetricsPolicy(
            listeners,
        )
        self.as_connection.enable_metrics(policy=policy)

        with pytest.raises(e.AerospikeError) as excinfo:
            self.as_connection.disable_metrics()
        assert excinfo.value.msg == "Python callback disable_listener threw a ValueError exception. Exception value: "\
            "disable threw an error"
