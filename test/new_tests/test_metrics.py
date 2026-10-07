from aerospike import exception as e
from aerospike_helpers.metrics import (
    MetricsPolicy,
    MetricsListeners,
    Cluster,
    Node,
    ConnectionStats,
    NamespaceMetrics,
    MetricsSnapshot,
    NodeSnapshot,
    NamespaceSnapshot,
    LATENCY_MILLISECONDS,
    LATENCY_MICROSECONDS,
)
import pytest
import shutil
import glob
import os
import time
from typing import Optional
import re
import aerospike
from .test_base_class import TestBaseClass
from importlib.metadata import version

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
    # Shared between some test cases
    listeners = MetricsListeners(
        enable_listener=MyMetricsListeners.enable,
        disable_listener=MyMetricsListeners.disable,
        node_close_listener=MyMetricsListeners.node_close,
        snapshot_listener=MyMetricsListeners.snapshot
    )

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

    @pytest.mark.parametrize(
        "policy",
        [
            1,
            # We're testing a negative code path in a helper function
            # where the object's actual type belongs to aerospike_helpers but does not match the expected
            # type from aerospike_helpers
            # The actual type needs to be in the same submodule as the expected type (metrics in aerospike_helpers)
            listeners
        ]
    )
    def test_enable_metrics_with_invalid_arg(self, policy):
        with pytest.raises(e.ParamError) as excinfo:
            self.as_connection.enable_metrics(policy)
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

        # The client language and version should be correct
        try:
            with open(metrics_log_filenames[0]) as f:
                # Skip header
                f.readline()
                # Each line will show the client language and version
                data = f.readline()
            # Each line includes cluster[cluster_name,client_language,client_version,...
            # cluster_name can be empty
            regex = re.search(pattern=r"cluster\[[a-zA-Z0-9_\-$,]*,([A-Za-z]+),([0-9.a-zA-Z+]+),", string=data)
            client_language, client_version = regex.groups()

            assert client_language == "python"
            assert client_version == version("aerospike")
        finally:
            for item in metrics_log_filenames:
                os.remove(item)

    @pytest.fixture(scope="function", params=[None, "my_app"])
    def get_client_and_app_id(self, request):
        config = TestBaseClass.get_connection_config()
        config["app_id"] = request.param
        client = aerospike.client(config)

        yield request.param, client

        client.close()

    def test_setting_metrics_policy_custom_settings(self, get_client_and_app_id):
        app_id, client = get_client_and_app_id

        self.metrics_log_folder = "./metrics-logs"

        # Save bucket count for testing later
        bucket_count = 5
        policy = MetricsPolicy(
            metrics_listeners=self.listeners,
            report_dir=self.metrics_log_folder,
            report_size_limit=1000,
            interval=2,
            latency_columns=bucket_count,
            latency_shift=2,
            labels={"a": "b"},
        )

        client.enable_metrics(policy=policy)
        time.sleep(3)
        client.disable_metrics()

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
            assert type(cluster.command_count) == int
            assert type(cluster.retry_count) == int
            assert type(cluster.nodes) == list
            if type(app_id) == str:
                assert cluster.app_id == app_id
            elif TestBaseClass.auth_in_use():
                # Or username if the app_id is not set
                assert cluster.app_id == TestBaseClass.user
            else:
                assert cluster.app_id == "not-set"

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
                assert type(node.conns.recovered) == int
                assert type(node.conns.aborted) == int
                # Check NodeMetrics
                assert type(node.metrics) == list
                ns_metrics = node.metrics
                for ns_metric in ns_metrics:
                    assert type(ns_metric) == NamespaceMetrics
                    assert type(ns_metric.ns) == str
                    assert type(ns_metric.bytes_in) == int
                    assert type(ns_metric.bytes_out) == int
                    assert type(ns_metric.error_count) == int
                    assert type(ns_metric.timeout_count) == int
                    assert type(ns_metric.key_busy_count) == int
                    latency_buckets = [
                        ns_metric.conn_latency,
                        ns_metric.write_latency,
                        ns_metric.read_latency,
                        ns_metric.batch_latency,
                        ns_metric.query_latency
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
                MetricsPolicy(report_size_limit="1"),
                "report_size_limit",
                "unsigned 64-bit integer"
            ),
            (
                MetricsPolicy(interval="1"),
                "interval",
                "unsigned 32-bit integer"
            ),
            (
                MetricsPolicy(latency_columns="1"),
                "latency_columns",
                "unsigned 8-bit integer"
            ),
            (
                MetricsPolicy(latency_shift="1"),
                "latency_shift",
                "unsigned 8-bit integer"
            ),
            # Pass in an integer larger than allowed for an unsigned 8-bit integer
            (
                MetricsPolicy(latency_shift=2**8),
                "latency_shift",
                "unsigned 8-bit integer"
            ),
            (
                MetricsPolicy(latency_unit="ms"),
                "latency_unit",
                "unsigned 8-bit integer"
            ),
            (
                MetricsPolicy(operational_enabled=1),
                "operational_enabled",
                "bool"
            ),
            (
                MetricsPolicy(usage_enabled="yes"),
                "usage_enabled",
                "bool"
            ),
            # Invalid labels
            (
                MetricsPolicy(labels={1: "a"}),
                "labels",
                "dict[str, str]"
            ),
            (
                MetricsPolicy(labels={"a": 1}),
                "labels",
                "dict[str, str]"
            ),
            (
                MetricsPolicy(labels=[]),
                "labels",
                "dict[str, str]"
            ),
        ]
    )
    def test_metrics_policy_invalid_args(self, policy, field_name, expected_field_type):
        with pytest.raises(e.ParamError) as excinfo:
            self.as_connection.enable_metrics(policy=policy)
        assert excinfo.value.msg == f"MetricsPolicy.{field_name} must be a {expected_field_type} type"

    def test_metrics_policy_report_dir_too_long(self):
        policy = MetricsPolicy(
            # We are testing that the Python client's udata for aerospike_enable_metrics() is freed properly on error
            # because we never end up calling aerospike_enable_metrics()
            # This is for code coverage purposes
            metrics_listeners=self.listeners,
            report_dir=str('.' * 257),
        )
        with pytest.raises(e.ParamError) as excinfo:
            self.as_connection.enable_metrics(policy=policy)
        assert excinfo.value.msg == "MetricsPolicy.report_dir must be less than 256 chars"

    # Use default metrics writer implementation
    # We are checking that enable_metrics() does not seg fault
    def test_enable_metrics_with_invalid_report_size_limit(self):
        policy = MetricsPolicy(report_size_limit=1)
        with pytest.raises(e.ClientError):
            self.as_connection.enable_metrics(policy=policy)

    def test_disable_metrics(self):
        retval = self.as_connection.disable_metrics()
        assert retval is None

    def test_disable_metrics_invalid_args(self):
        with pytest.raises(TypeError):
            self.as_connection.disable_metrics(1)

    def test_metrics_listeners_deprecated(self):
        with pytest.warns(DeprecationWarning, match="MetricsListeners is deprecated"):
            MetricsListeners(
                enable_listener=MyMetricsListeners.enable,
                disable_listener=MyMetricsListeners.disable,
                node_close_listener=MyMetricsListeners.node_close,
                snapshot_listener=MyMetricsListeners.snapshot,
            )

    def test_add_exporter_requires_export_method(self):
        policy = MetricsPolicy(report_dir="")
        with pytest.raises(TypeError):
            policy.add_exporter(object())

    def test_exporter_receives_snapshot(self):
        snapshots = []

        class RecordingExporter:
            def export(self, snapshot):
                snapshots.append(snapshot)

        policy = MetricsPolicy(
            report_dir="",
            interval=1,
            labels={"region": "test"},
            operational_enabled=True,
        )
        policy.add_exporter(RecordingExporter())
        self.as_connection.enable_metrics(policy)
        time.sleep(3)
        self.as_connection.disable_metrics()

        assert snapshots
        live = [item for item in snapshots if item.metrics_enabled]
        assert live
        snapshot = live[-1]
        assert isinstance(snapshot, MetricsSnapshot)
        assert snapshot.client_type == "python"
        assert snapshot.operational_metrics_enabled is True
        assert snapshot.usage_metrics_enabled is False
        assert snapshot.labels == {"region": "test"}
        assert snapshot.latency_columns == 7
        assert snapshot.latency_shift == 1
        assert snapshot.latency_unit == LATENCY_MILLISECONDS
        assert isinstance(snapshot.mem, int)
        assert isinstance(snapshot.nodes, list)
        assert snapshot.nodes_departed == [] or isinstance(snapshot.nodes_departed, list)
        assert isinstance(snapshot.app_id, str) and snapshot.app_id
        for node in snapshot.nodes:
            assert isinstance(node, NodeSnapshot)
            assert isinstance(node.sync, ConnectionStats)
            assert isinstance(node.async_conns, ConnectionStats)
            assert isinstance(node.conn_open_failures, int)
            assert isinstance(node.conn_tls_handshake_failures, int)
            assert isinstance(node.conn_auth_failures, int)
            for namespace in node.namespaces:
                assert isinstance(namespace, NamespaceSnapshot)
                assert set(namespace.latency) == {"conn", "write", "read", "batch", "query"}
                for buckets in namespace.latency.values():
                    assert len(buckets) == 7

        assert glob.glob("./metrics-*.log") == []

    def test_exporter_failure_does_not_stop_the_next_exporter(self):
        calls = []

        class FailingExporter:
            def export(self, snapshot):
                calls.append("fail")
                raise RuntimeError("exporter failed")

        class RecordingExporter:
            def export(self, snapshot):
                calls.append("ok")

        policy = MetricsPolicy(report_dir="", interval=1)
        policy.add_exporter(FailingExporter())
        policy.add_exporter(RecordingExporter())
        self.as_connection.enable_metrics(policy)
        time.sleep(3)
        self.as_connection.disable_metrics()

        assert "fail" in calls
        assert "ok" in calls

    def test_latency_unit_must_be_a_known_value(self):
        policy = MetricsPolicy(latency_unit=2)
        with pytest.raises(e.ParamError) as excinfo:
            self.as_connection.enable_metrics(policy)
        assert excinfo.value.msg == (
            "MetricsPolicy.latency_unit must be "
            "LATENCY_MILLISECONDS or LATENCY_MICROSECONDS"
        )

    def test_get_metrics_snapshot(self):
        before = self.as_connection.get_metrics_snapshot()
        assert isinstance(before, MetricsSnapshot)
        assert before.metrics_enabled is False
        assert before.operational_metrics_enabled is False
        assert before.usage_metrics_enabled is False
        assert before.nodes_departed == []
        assert before.latency_unit == LATENCY_MILLISECONDS

        policy = MetricsPolicy(
            report_dir="",
            operational_enabled=True,
            usage_enabled=True,
            latency_unit=LATENCY_MICROSECONDS,
        )
        self.as_connection.enable_metrics(policy)
        snapshot = self.as_connection.get_metrics_snapshot()
        self.as_connection.disable_metrics()

        assert snapshot.metrics_enabled is True
        assert snapshot.operational_metrics_enabled is True
        assert snapshot.usage_metrics_enabled is True
        assert snapshot.latency_unit == LATENCY_MICROSECONDS
        assert snapshot.nodes_departed == []
        for node in snapshot.nodes:
            assert isinstance(node.conn_open_failures, int)
            assert isinstance(node.conn_tls_handshake_failures, int)
            assert isinstance(node.conn_auth_failures, int)

    def test_empty_report_dir_skips_file_exporter(self):
        policy = MetricsPolicy(report_dir="", interval=1)
        self.as_connection.enable_metrics(policy)
        time.sleep(2)
        self.as_connection.disable_metrics()
        assert glob.glob("./metrics-*.log") == []

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
