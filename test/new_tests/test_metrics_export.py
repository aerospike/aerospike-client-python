"""Exporter contract for the legacy client.

These tests stay on the Python session and snapshot types. They do not need a
cluster: enable/disable in the C client calls this session's prepare, start,
and stop.
"""

import threading
import time

import pytest

from aerospike_helpers.metrics import (
    Cluster,
    ConnectionStats,
    MetricsListeners,
    MetricsPolicy,
    MetricsSnapshot,
    NamespaceMetrics,
    Node,
)
from aerospike_helpers.metrics.export import ExportSession, MetricsExporterType
from aerospike_helpers.metrics.snapshot import CLIENT_TYPE


class Recorder:
    def __init__(self):
        self.snapshots = []
        self.closed = False

    def export(self, snapshot):
        self.snapshots.append(snapshot)

    def close(self):
        self.closed = True


class Boom:
    def __init__(self):
        self.calls = 0

    def export(self, snapshot):
        self.calls += 1
        raise RuntimeError("exporter failed")

    def close(self):
        raise AssertionError("disable must not close an exporter")


class AsyncExporter:
    async def export(self, snapshot):
        return None


def _namespace(name, conn, read, error_count=4, timeout_count=9, key_busy=2):
    metrics = NamespaceMetrics()
    metrics.ns = name
    metrics.bytes_in = 10
    metrics.bytes_out = 20
    metrics.error_count = error_count
    metrics.timeout_count = timeout_count
    metrics.key_busy_count = key_busy
    metrics.conn_latency = list(conn)
    metrics.read_latency = list(read)
    metrics.write_latency = [1]
    metrics.batch_latency = []
    metrics.query_latency = [4, 0]
    return metrics


def _node(name, conn_buckets, read_buckets, in_use=2, in_pool=3):
    stats = ConnectionStats()
    stats.in_use = in_use
    stats.in_pool = in_pool
    stats.opened = 8
    stats.closed = 1
    stats.recovered = 0
    stats.aborted = 0
    node = Node()
    node.name = name
    node.address = "127.0.0.1"
    node.port = 3000
    node.conns = stats
    node.metrics = [
        _namespace("test", conn_buckets, read_buckets),
        _namespace("other", [3, 4], [0, 1], error_count=1, timeout_count=5, key_busy=0),
    ]
    async_stats = ConnectionStats()
    async_stats.in_use = 7
    async_stats.in_pool = 1
    async_stats.opened = 7
    async_stats.closed = 0
    node.async_conns = async_stats
    return node


def _cluster():
    cluster = Cluster()
    cluster.cluster_name = "seen"
    cluster.app_id = None
    cluster.invalid_node_count = 1
    cluster.command_count = 12
    cluster.retry_count = 3
    cluster.nodes = [
        _node("BB9", [1, 2], [5, 6], in_use=2, in_pool=3),
    ]
    return cluster


def _custom_policy(*exporters, export_interval=30):
    policy = MetricsPolicy(
        exporter="custom",
        export_interval=export_interval,
        interval=11,
        latency_columns=5,
        latency_shift=3,
        labels={"job": "app"},
    )
    for exporter in exporters:
        policy.add_exporter(exporter)
    return policy


def test_canonical_dict_matches_exporter_document():
    snapshot = MetricsSnapshot.from_cluster(
        _cluster(),
        labels={"job": "app"},
        latency_columns=5,
        latency_shift=3,
    )
    document = snapshot.to_canonical_dict()

    assert document["client_type"] == CLIENT_TYPE == "python"
    assert document["client_version"] == "" or isinstance(document["client_version"], str)
    assert document["cluster_name"] == "seen"
    assert document["app_id"] == "not-set"
    assert document["labels"] == {"job": "app"}
    assert document["metrics_enabled"] is True
    assert document["operational_metrics_enabled"] is True
    assert document["usage_metrics_enabled"] is False
    assert document["latency_unit"] == "milliseconds"
    assert document["latency_columns"] == 5
    assert document["latency_shift"] == 3
    assert document["total_nodes"] == 1
    assert document["invalid_node_count"] == 1
    assert document["command_count"] == 12
    assert document["command_retries"] == 3
    assert document["open_connections"] == 5
    assert document["recover_queue_size"] == 0
    assert document["delay_queue_timeout_count"] == 0
    assert document["batch_partial_failure"] == 0
    assert document["validation_open_failure"] == 0
    assert document["validation_tls_handshake_failure"] == 0
    assert document["validation_auth_failure"] == 0
    assert document["cluster_latencies"] == []
    assert document["usage"]["blocking"] == 0
    assert "shape_point" in document["usage"]

    node = document["nodes"][0]
    assert node["node_name"] == "BB9"
    assert node["node_address"] == "127.0.0.1"
    assert node["node_port"] == 3000
    assert node["connections"] == {"in_use": 2, "in_pool": 3, "opened": 8, "closed": 1}
    # [1, 2] + [3, 4]; a mismatched third series would be skipped.
    assert node["conn_latency"]["type"] == "conn"
    assert node["conn_latency"]["bucket_counts"] == [4, 6]
    assert node["conn_latency"]["count"] == 10
    assert "async" not in node
    assert snapshot.nodes[0].async_connections.in_use == 7

    namespaces = {item["namespace"]: item for item in node["namespaces"]}
    test_ns = namespaces["test"]
    assert test_ns["errors"] == 4
    assert test_ns["key_busy"] == 2
    assert test_ns["bytes_in"] == 10
    assert test_ns["bytes_out"] == 20
    assert test_ns["client_timeouts"] == 0
    assert test_ns["server_timeouts"] == 0
    assert test_ns["record_too_big"] == 0
    assert test_ns["device_overload"] == 0
    assert [item["type"] for item in test_ns["latencies"]] == [
        "read", "write", "batch", "query"
    ]
    assert test_ns["latencies"][0]["bucket_counts"] == [5, 6]
    assert "conn" not in [item["type"] for item in test_ns["latencies"]]


def test_departed_node_is_in_the_document_until_a_successful_export():
    recorder = Recorder()
    session = ExportSession()
    session.prepare_enable(_custom_policy(recorder))
    session.start()
    departed = _node("LEFT", [1], [1])
    session.store_departed(departed)
    session.store_cluster(_cluster())

    session.export_once()
    document = recorder.snapshots[-1].to_canonical_dict()
    assert [node["node_name"] for node in document["nodes_departed"]] == ["LEFT"]
    assert session.peek().to_canonical_dict()["nodes_departed"] == []

    session.export_once()
    assert recorder.snapshots[-1].to_canonical_dict()["nodes_departed"] == []


def test_failed_export_keeps_departed_nodes_and_does_not_skip_the_next():
    bad = Boom()
    good = Recorder()
    session = ExportSession()
    session.suspend_retry_interval = 3600
    session.prepare_enable(_custom_policy(bad, good))
    session.start()
    session.store_cluster(_cluster())
    session.store_departed(_node("LEFT", [1], [1]))

    session.export_once()
    assert bad.calls == 1
    assert len(good.snapshots) == 1
    assert session.peek().nodes_departed[0].node_name == "LEFT"

    session.export_once()
    session.export_once()
    assert bad.calls == 3
    assert len(good.snapshots) == 3
    # The failing cycle did not drain. The next cycle skips the suspended
    # exporter; the one that still runs can drain.
    assert session.peek().nodes_departed[0].node_name == "LEFT"

    session.export_once()
    assert bad.calls == 3
    assert len(good.snapshots) == 4
    assert session.peek().nodes_departed == ()

    bad.export = lambda snapshot: setattr(bad, "calls", bad.calls + 1)
    session.suspend_retry_interval = 0
    session.export_once()
    assert bad.calls == 4


def test_rejoined_node_is_not_departed():
    recorder = Recorder()
    session = ExportSession()
    session.prepare_enable(_custom_policy(recorder))
    session.start()
    session.store_departed(_node("BB9", [1], [1]))
    session.store_cluster(_cluster())
    session.export_once()
    assert recorder.snapshots[-1].to_canonical_dict()["nodes_departed"] == []


def test_stop_keeps_exporters_and_does_not_close_them():
    seen = threading.Event()

    class Waiting(Recorder):
        def export(self, snapshot):
            super().export(snapshot)
            seen.set()

    exporter = Waiting()
    session = ExportSession()
    session.prepare_enable(_custom_policy(exporter, export_interval=0.05))
    session.store_cluster(_cluster())
    session.start()
    assert session._thread.name == "aerospike-metrics-export"
    assert session._thread.daemon is True
    assert seen.wait(2)

    session.stop()
    assert exporter.closed is False
    assert session.exporters == (exporter,)
    held = len(exporter.snapshots)
    time.sleep(0.2)
    assert len(exporter.snapshots) == held

    seen.clear()
    session.start()
    assert seen.wait(2)
    assert len(exporter.snapshots) > held
    session.stop()
    assert exporter.closed is False


def test_async_export_is_rejected():
    policy = MetricsPolicy()
    with pytest.raises(TypeError, match="async"):
        policy.add_exporter(AsyncExporter())
    with pytest.raises(TypeError, match="missing export"):
        policy.add_exporter(object())
    with pytest.raises(ValueError, match="none, file, custom"):
        MetricsPolicy(exporter="otel")
    with pytest.raises(ValueError, match="positive"):
        MetricsPolicy(export_interval=0)


def test_modes_install_listeners_only_when_the_file_writer_must_not_run():
    user_called = []
    user = MetricsListeners(
        enable_listener=lambda: user_called.append("enable"),
        snapshot_listener=lambda cluster: user_called.append("snapshot"),
        node_close_listener=lambda node: user_called.append("close"),
        disable_listener=lambda cluster: user_called.append("disable"),
    )
    omitted = MetricsPolicy(metrics_listeners=user, interval=4)
    session = ExportSession()
    assert session.prepare_enable(omitted) is omitted
    assert omitted.interval == 4

    custom = _custom_policy(Recorder())
    custom.metrics_listeners = user
    prepared = session.prepare_enable(custom)
    assert prepared is not custom
    assert custom.metrics_listeners is user
    prepared.metrics_listeners.enable_listener()
    prepared.metrics_listeners.snapshot_listener(_cluster())
    assert user_called == ["enable", "snapshot"]
    assert session.peek().total_nodes == 1
    assert session.exporters[0].snapshots == []

    session.start()
    session.export_once()
    assert len(session.exporters[0].snapshots) == 1
    session.stop()

    none = MetricsPolicy(exporter="none", metrics_listeners=user)
    none_prepared = session.prepare_enable(none)
    assert none_prepared is not none
    assert session.mode is MetricsExporterType.NONE

    empty_file = MetricsPolicy(exporter="file", report_dir="")
    assert session.prepare_enable(empty_file) is not empty_file

    file_policy = MetricsPolicy(exporter="FILE", report_dir="logs")
    assert session.prepare_enable(file_policy) is file_policy
    assert session.mode is MetricsExporterType.FILE
    assert MetricsExporterType("FILE") is MetricsExporterType.FILE


def test_add_exporter_without_a_mode_selects_custom_and_resume_reuses_it():
    recorder = Recorder()
    policy = MetricsPolicy(export_interval=30)
    policy.add_exporter(recorder)
    session = ExportSession()
    prepared = session.prepare_enable(policy)
    assert prepared is not policy
    assert session.mode is MetricsExporterType.CUSTOM
    session.start()
    session.store_cluster(_cluster())
    session.export_once()
    session.stop()
    assert len(recorder.snapshots) == 1
    assert recorder.closed is False

    resumed = session.prepare_enable(None)
    assert resumed is not None
    session.start()
    session.export_once()
    assert len(recorder.snapshots) == 2
    assert session.exporters == (recorder,)
    session.stop()
