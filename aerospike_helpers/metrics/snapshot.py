# Copyright 2026 Aerospike, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Point-in-time metrics snapshots passed to exporters.

The listener types (:class:`~aerospike_helpers.metrics.Cluster`,
:class:`~aerospike_helpers.metrics.Node`) stay the deprecated callback
objects. :class:`MetricsSnapshot` is the structured object ``export``
receives. :meth:`MetricsSnapshot.to_canonical_dict` is the document the
shared telemetry exporter walks, so this client and the Python SDK can
publish through one exporter package.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from functools import lru_cache
from importlib.metadata import version
import os
import resource
import sys
import time
from types import MappingProxyType
from typing import Any, Mapping, Optional, Sequence, Tuple

# Distinct from the SDK's ``python-sdk`` so one MeterProvider can keep both.
CLIENT_TYPE = "python"

_APP_ID_UNSET = "not-set"
_LATENCY_UNIT = "milliseconds"
_NAMESPACE_LATENCY_TYPES = ("read", "write", "batch", "query")


@dataclass(frozen=True)
class ConnectionMetricsSnapshot:
    """Sync connection pool counts for one node.

    Async pool counts stay on :attr:`NodeMetricsSnapshot.async_connections`
    and are omitted from :meth:`MetricsSnapshot.to_canonical_dict`.
    """

    in_use: int = 0
    in_pool: int = 0
    opened: int = 0
    closed: int = 0

    def as_dict(self) -> dict:
        return {
            "in_use": self.in_use,
            "in_pool": self.in_pool,
            "opened": self.opened,
            "closed": self.closed,
        }


@dataclass(frozen=True)
class LatencySnapshot:
    """One latency histogram.

    The listener path only carries bucket counts. ``count`` is the sum of
    those buckets. ``min``, ``max``, and ``sum`` stay 0 because the C client
    does not report them on the listener objects.
    """

    type: str
    bucket_counts: Tuple[int, ...] = ()
    min: int = 0
    max: int = 0
    sum: float = 0.0
    count: int = 0

    def as_dict(self) -> dict:
        return {
            "type": self.type,
            "bucket_counts": list(self.bucket_counts),
            "min": self.min,
            "max": self.max,
            "sum": self.sum,
            "count": self.count,
        }


@dataclass(frozen=True)
class NamespaceMetricsSnapshot:
    """Per-namespace command metrics. Connection latency is not here."""

    namespace: str
    errors: int = 0
    client_timeouts: int = 0
    server_timeouts: int = 0
    key_busy: int = 0
    record_too_big: int = 0
    device_overload: int = 0
    bytes_in: int = 0
    bytes_out: int = 0
    latencies: Tuple[LatencySnapshot, ...] = ()

    def as_dict(self) -> dict:
        return {
            "namespace": self.namespace,
            "errors": self.errors,
            "client_timeouts": self.client_timeouts,
            "server_timeouts": self.server_timeouts,
            "key_busy": self.key_busy,
            "record_too_big": self.record_too_big,
            "device_overload": self.device_overload,
            "bytes_in": self.bytes_in,
            "bytes_out": self.bytes_out,
            "latencies": [latency.as_dict() for latency in self.latencies],
        }


@dataclass(frozen=True)
class NodeMetricsSnapshot:
    """One node's pool, connection latency, and namespace metrics."""

    node_name: str
    node_address: str
    node_port: int
    connections: ConnectionMetricsSnapshot = field(default_factory=ConnectionMetricsSnapshot)
    conn_latency: LatencySnapshot = field(
        default_factory=lambda: LatencySnapshot(type="conn")
    )
    connection_open_failure: int = 0
    connection_tls_handshake_failure: int = 0
    connection_auth_failure: int = 0
    namespaces: Tuple[NamespaceMetricsSnapshot, ...] = ()
    async_connections: Optional[ConnectionMetricsSnapshot] = None

    def as_dict(self) -> dict:
        return {
            "node_name": self.node_name,
            "node_address": self.node_address,
            "node_port": self.node_port,
            "connections": self.connections.as_dict(),
            "conn_latency": self.conn_latency.as_dict(),
            "connection_open_failure": self.connection_open_failure,
            "connection_tls_handshake_failure": self.connection_tls_handshake_failure,
            "connection_auth_failure": self.connection_auth_failure,
            "namespaces": [namespace.as_dict() for namespace in self.namespaces],
        }


@dataclass(frozen=True)
class UsageMetricsSnapshot:
    """Client-wide feature counters. This client leaves them at zero."""

    blocking: int = 0
    deferred: int = 0
    background: int = 0
    shape_point: int = 0
    shape_batch: int = 0
    shape_query: int = 0
    object_mapping_read: int = 0
    object_mapping_write: int = 0
    filter_ael: int = 0
    filter_exp: int = 0

    def as_dict(self) -> dict:
        return {
            "blocking": self.blocking,
            "deferred": self.deferred,
            "background": self.background,
            "shape_point": self.shape_point,
            "shape_batch": self.shape_batch,
            "shape_query": self.shape_query,
            "object_mapping_read": self.object_mapping_read,
            "object_mapping_write": self.object_mapping_write,
            "filter_ael": self.filter_ael,
            "filter_exp": self.filter_exp,
        }


@dataclass(frozen=True)
class MetricsSnapshot:
    """Immutable snapshot passed to ``export``.

    Valid for the duration of that call. Exporters that retain data copy
    :meth:`to_canonical_dict`.
    """

    timestamp: datetime
    cluster_name: str = ""
    app_id: str = _APP_ID_UNSET
    labels: Mapping[str, str] = field(default_factory=dict)
    metrics_enabled: bool = False
    operational_metrics_enabled: bool = False
    usage_metrics_enabled: bool = False
    latency_unit: str = _LATENCY_UNIT
    latency_columns: int = 7
    latency_shift: int = 1
    total_nodes: int = 0
    recover_queue_size: int = 0
    invalid_node_count: int = 0
    delay_queue_timeout_count: int = 0
    open_connections: int = 0
    command_count: int = 0
    command_retries: int = 0
    batch_partial_failure: int = 0
    cpu_percent: float = 0.0
    memory_bytes: int = 0
    validation_open_failure: int = 0
    validation_tls_handshake_failure: int = 0
    validation_auth_failure: int = 0
    cluster_latencies: Tuple[LatencySnapshot, ...] = ()
    nodes: Tuple[NodeMetricsSnapshot, ...] = ()
    nodes_departed: Tuple[NodeMetricsSnapshot, ...] = ()
    usage: UsageMetricsSnapshot = field(default_factory=UsageMetricsSnapshot)
    client_type: str = CLIENT_TYPE
    client_version: str = ""

    def with_departed(
        self, nodes: Sequence[NodeMetricsSnapshot]
    ) -> "MetricsSnapshot":
        """Return a copy whose departed-node list is ``nodes``."""
        return replace(self, nodes_departed=tuple(nodes))

    def with_process(self, cpu_percent: float, memory_bytes: int) -> "MetricsSnapshot":
        """Return a copy with process CPU and resident memory filled in."""
        return replace(
            self, cpu_percent=float(cpu_percent), memory_bytes=int(memory_bytes)
        )

    def to_canonical_dict(self) -> dict:
        """The snake_case document the shared exporter serializes.

        Fields the C client does not record are present and zero so an
        exporter does not special-case a missing key. The combined listener
        ``timeout_count`` is not copied into ``client_timeouts`` or
        ``server_timeouts``. Async pool counts are not included.
        """
        return {
            "timestamp": self.timestamp.isoformat(),
            "client_type": self.client_type,
            "client_version": self.client_version,
            "cluster_name": self.cluster_name,
            "app_id": self.app_id,
            "labels": dict(self.labels),
            "metrics_enabled": self.metrics_enabled,
            "operational_metrics_enabled": self.operational_metrics_enabled,
            "usage_metrics_enabled": self.usage_metrics_enabled,
            "latency_unit": self.latency_unit,
            "latency_columns": self.latency_columns,
            "latency_shift": self.latency_shift,
            "total_nodes": self.total_nodes,
            "recover_queue_size": self.recover_queue_size,
            "invalid_node_count": self.invalid_node_count,
            "delay_queue_timeout_count": self.delay_queue_timeout_count,
            "validation_open_failure": self.validation_open_failure,
            "validation_tls_handshake_failure": self.validation_tls_handshake_failure,
            "validation_auth_failure": self.validation_auth_failure,
            "open_connections": self.open_connections,
            "command_count": self.command_count,
            "command_retries": self.command_retries,
            "batch_partial_failure": self.batch_partial_failure,
            "cpu_percent": self.cpu_percent,
            "memory_bytes": self.memory_bytes,
            "cluster_latencies": [latency.as_dict() for latency in self.cluster_latencies],
            "nodes": [node.as_dict() for node in self.nodes],
            "nodes_departed": [node.as_dict() for node in self.nodes_departed],
            "usage": self.usage.as_dict(),
        }

    @classmethod
    def from_cluster(
        cls,
        cluster: Any,
        *,
        labels: Optional[Mapping[str, str]] = None,
        latency_columns: int = 7,
        latency_shift: int = 1,
        metrics_enabled: bool = True,
        nodes_departed: Sequence[NodeMetricsSnapshot] = (),
    ) -> "MetricsSnapshot":
        """Build a snapshot from a listener :class:`~aerospike_helpers.metrics.Cluster`."""
        nodes = tuple(
            _node_from_listener(node) for node in (getattr(cluster, "nodes", None) or ())
        )
        app_id = getattr(cluster, "app_id", None)
        cluster_name = getattr(cluster, "cluster_name", None)
        open_connections = sum(
            node.connections.in_use + node.connections.in_pool for node in nodes
        )
        return cls(
            timestamp=datetime.now(timezone.utc),
            cluster_name="" if cluster_name is None else str(cluster_name),
            app_id=_APP_ID_UNSET if not app_id else str(app_id),
            labels=MappingProxyType(dict(labels or {})),
            metrics_enabled=metrics_enabled,
            operational_metrics_enabled=metrics_enabled,
            usage_metrics_enabled=False,
            latency_columns=int(latency_columns),
            latency_shift=int(latency_shift),
            total_nodes=len(nodes),
            invalid_node_count=int(getattr(cluster, "invalid_node_count", 0) or 0),
            open_connections=open_connections,
            command_count=int(getattr(cluster, "command_count", 0) or 0),
            command_retries=int(getattr(cluster, "retry_count", 0) or 0),
            nodes=nodes,
            nodes_departed=tuple(nodes_departed),
            client_version=client_version(),
        )

    @classmethod
    def empty(
        cls,
        *,
        labels: Optional[Mapping[str, str]] = None,
        latency_columns: int = 7,
        latency_shift: int = 1,
        metrics_enabled: bool = False,
    ) -> "MetricsSnapshot":
        """A snapshot with no nodes, used before the first tend copy arrives."""
        return cls(
            timestamp=datetime.now(timezone.utc),
            labels=MappingProxyType(dict(labels or {})),
            metrics_enabled=metrics_enabled,
            operational_metrics_enabled=metrics_enabled,
            latency_columns=int(latency_columns),
            latency_shift=int(latency_shift),
            client_version=client_version(),
        )


def node_from_listener(node: Any) -> NodeMetricsSnapshot:
    """Convert a listener :class:`~aerospike_helpers.metrics.Node`."""
    return _node_from_listener(node)


def sample_process() -> Tuple[float, int]:
    """Return ``(cpu_percent, rss_bytes)`` for this process.

    CPU is process time since the previous call as a percentage of wall
    time. The first call measures from when this function was first used.
    Failures become ``(0.0, 0)`` so export still runs.
    """
    try:
        return _PROCESS_SAMPLER.sample()
    except Exception:
        return 0.0, 0


class _ProcessSampler:
    def __init__(self) -> None:
        self._cpu_time = time.process_time()
        self._wall = time.monotonic()

    def sample(self) -> Tuple[float, int]:
        cpu_time = time.process_time()
        wall = time.monotonic()
        elapsed = wall - self._wall
        cpu = 100.0 * (cpu_time - self._cpu_time) / elapsed if elapsed > 0 else 0.0
        self._cpu_time, self._wall = cpu_time, wall
        return max(cpu, 0.0), _resident_bytes()


def _resident_bytes() -> int:
    try:
        with open("/proc/self/statm", encoding="ascii") as statm:
            pages = int(statm.read().split()[1])
        return pages * os.sysconf("SC_PAGE_SIZE")
    except (OSError, ValueError, IndexError):
        pass
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin" or "bsd" in sys.platform:
        return int(peak)
    return int(peak) * 1024


_PROCESS_SAMPLER = _ProcessSampler()


@lru_cache(maxsize=1)
def client_version() -> str:
    """Installed ``aerospike`` version, or ``""`` when it cannot be read."""
    try:
        return version("aerospike")
    except Exception:
        return ""


def _node_from_listener(node: Any) -> NodeMetricsSnapshot:
    namespaces = tuple(
        _namespace_from_listener(metrics)
        for metrics in (getattr(node, "metrics", None) or ())
    )
    conn_latency = _latency("conn", _sum_buckets(
        getattr(metrics, "conn_latency", ()) or ()
        for metrics in (getattr(node, "metrics", None) or ())
    ))
    async_stats = getattr(node, "async_conns", None)
    if async_stats is None:
        async_stats = getattr(node, "async_connections", None)
    port = getattr(node, "port", 0)
    return NodeMetricsSnapshot(
        node_name=str(getattr(node, "name", "") or ""),
        node_address=str(getattr(node, "address", "") or ""),
        node_port=int(port or 0),
        connections=_connection_from_stats(getattr(node, "conns", None)),
        conn_latency=conn_latency,
        namespaces=namespaces,
        async_connections=(
            None if async_stats is None else _connection_from_stats(async_stats)
        ),
    )


def _namespace_from_listener(metrics: Any) -> NamespaceMetricsSnapshot:
    latencies = tuple(
        _latency(name, getattr(metrics, name + "_latency", ()) or ())
        for name in _NAMESPACE_LATENCY_TYPES
    )
    return NamespaceMetricsSnapshot(
        namespace=str(getattr(metrics, "ns", "") or ""),
        errors=int(getattr(metrics, "error_count", 0) or 0),
        key_busy=int(getattr(metrics, "key_busy_count", 0) or 0),
        bytes_in=int(getattr(metrics, "bytes_in", 0) or 0),
        bytes_out=int(getattr(metrics, "bytes_out", 0) or 0),
        latencies=latencies,
    )


def _connection_from_stats(stats: Any) -> ConnectionMetricsSnapshot:
    if stats is None:
        return ConnectionMetricsSnapshot()
    return ConnectionMetricsSnapshot(
        in_use=int(getattr(stats, "in_use", 0) or 0),
        in_pool=int(getattr(stats, "in_pool", 0) or 0),
        opened=int(getattr(stats, "opened", 0) or 0),
        closed=int(getattr(stats, "closed", 0) or 0),
    )


def _latency(latency_type: str, buckets: Any) -> LatencySnapshot:
    counts = _as_counts(buckets)
    return LatencySnapshot(
        type=latency_type,
        bucket_counts=counts,
        count=sum(counts),
    )


def _as_counts(buckets: Any) -> Tuple[int, ...]:
    if buckets is None:
        return ()
    return tuple(int(value or 0) for value in buckets)


def _sum_buckets(groups: Any) -> Tuple[int, ...]:
    total: list = []
    for buckets in groups:
        values = list(_as_counts(buckets))
        if not values:
            continue
        if not total:
            total = values
        elif len(values) != len(total):
            continue
        else:
            for index, value in enumerate(values):
                total[index] += value
    return tuple(total)
