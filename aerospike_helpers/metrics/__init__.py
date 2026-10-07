##########################################################################
# Copyright 2013-2024 Aerospike, Inc.
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
##########################################################################

"""Classes used for metrics.

:class:`ConnectionStats`, :class:`NamespaceMetrics`, :class:`Node`, and :class:`Cluster` do not have a constructor
because they are not meant to be created by the user. They are only meant to be returned from :class:`MetricsListeners`
callbacks for reading data about the server and client. :class:`MetricsListeners` is deprecated.

:class:`MetricsSnapshot`, :class:`NodeSnapshot`, :class:`NamespaceSnapshot`, and :class:`EventLoopSnapshot` are
returned to metrics exporters. They are copies of one export and stay valid after ``export`` returns.

:class:`NodeStats` and :class:`ClusterStats` also do not have a constructor because they are meant to be returned using
a Python client API method.
"""

import warnings
from typing import Optional, Callable, Protocol

# Match as_metrics_latency_unit in the C client.
LATENCY_MILLISECONDS = 0
LATENCY_MICROSECONDS = 1


class ConnectionStats:
    """Connection statistics.

    Attributes:
        in_use (int): Connections actively being used in database commands on this node.
            There can be multiple pools per node. This value is a summary of those pools on this node.
        in_pool (int): Connections residing in pool(s) on this node.
            There can be multiple pools per node. This value is a summary of those pools on this node.
        opened (int): Total number of node connections opened since node creation.
        closed (int): Total number of node connections closed since node creation.
        recovered (int): Total number of recovered connections since node creation. A recovered connection is a
            connection that timed out on a socket read and then independently drained (read all incoming
            data) so the connection can be put back into the connection pool. The recovery process is
            attempted when the ``timeout_delay`` policy is greater than zero.
        aborted (int): Total number of aborted connections since node creation. An aborted connection is a connection
            that timed out on a socket read and the drain (read all incoming data) failed. The drain failure is
            mostly likely due a downed node and results in the connection being closed. The recovery process
            is attempted when the ``timeout_delay`` policy is greater than zero.
        """
    pass


_ERROR_COUNT_DOCSTRING = "Command error count since node was initialized. If the error is retryable, multiple errors \
    per command may occur."
_TIMEOUT_COUNT_DOCSTRING = "Command timeout count since node was initialized. If the timeout is retryable \
    (i.e socket_timeout), multiple timeouts per command may occur."
_KEY_BUSY_COUNT_DOCSTRING = "Command key busy error count since node was initialized."


class NamespaceMetrics:
    """
    Namespace metrics.

    Each command group has its own histogram (i.e list of latency buckets).
    Latency histogram counts are cumulative and not reset on each metrics snapshot interval.

    Attributes:
        ns (str): namespace
        bytes_in (int): Bytes received from the server.
        bytes_out (int): Bytes sent to the server.
        error_count (int): {}
        timeout_count (int): {}
        key_busy_count (int): {}
        conn_latency (list[int])
        write_latency (list[int])
        read_latency (list[int])
        batch_latency (list[int])
        query_latency (list[int])
    """
    pass


if isinstance(NamespaceMetrics.__doc__, str):
    NamespaceMetrics.__doc__ = NamespaceMetrics.__doc__.format(
        _ERROR_COUNT_DOCSTRING,
        _TIMEOUT_COUNT_DOCSTRING,
        _KEY_BUSY_COUNT_DOCSTRING
    )


class Node:
    """Server node representation.

    Attributes:
        name (str): The name of the node.
        address (str): The IP address / host name of the node (not including the port number).
        port (int): Port number of the node's address.
        conns (:py:class:`ConnectionStats`): Synchronous connection stats on this node.
        metrics (list[:py:class:`NamespaceMetrics`]): Node/namespace metrics
    """
    pass


class Cluster:
    """Cluster of server nodes.

    Attributes:
        cluster_name (Optional[str]): Expected cluster name for all nodes. May be :py:obj:`None`.
        app_id (str): Application identifier. Will be set to the client's username if not set to a string in the client
            config's ``app_id`` option. If the client does not have a username, this will be set to ``not-set``.
        invalid_node_count (int): Count of add node failures in the most recent cluster tend iteration.
        command_count (int): Command count. The value is cumulative and not reset per metrics interval.
        retry_count (int): Command retry count. There can be multiple retries for a single command.
            The value is cumulative and not reset per metrics interval.
        nodes (list[:py:class:`Node`]): Active nodes in cluster.
    """
    pass


# as_node_stats has a reference to the corresponding as_node object
# Here, we are using specific as_node fields to identify that as_node instead of storing the full as_node.
# Since as_node has a ton of fields, we don't want to return the whole as_node.
#
# We also don't want to have a reference to a Node class instance
# because our Node class has fields we don't want to expose when returning ClusterStats to the user
# i.e Node's namespace metrics when extended metrics is disabled.
class NodeStats:
    """Node statistics.

    Attributes:
        name: The name of the node.
        address: The IP address / host name of the node (not including the port number).
        port: Port number of the node's address.
        conns: Synchronous connection stats on this node.
        error_count: {}
        timeout_count: {}
        key_busy_count: {}
    """
    name: str
    address: str
    port: int
    conns: ConnectionStats
    error_count: int
    timeout_count: int
    key_busy_count: int


if isinstance(NodeStats.__doc__, str):
    NodeStats.__doc__ = NodeStats.__doc__.format(
        _ERROR_COUNT_DOCSTRING,
        _TIMEOUT_COUNT_DOCSTRING,
        _KEY_BUSY_COUNT_DOCSTRING
    )


# - We don't need to expose as_cluster_stats.nodes_size since len(nodes) represents the number of nodes.
class ClusterStats:
    """
    Cluster statistics.

    Attributes:
        nodes: Statistics for all nodes.
        retry_count: Count of command retries since cluster was started.
        thread_pool_queued_tasks: Count of sync batch/scan/query tasks awaiting execution.
            If the count is greater than zero, then all threads in the thread pool are active.
        recover_queue_size: Count of sync sockets currently in timeout recovery.
    """
    nodes: list[NodeStats]
    retry_count: int
    thread_pool_queued_tasks: int
    recover_queue_size: int


class NamespaceSnapshot:
    """Per-namespace counters and latency histograms copied into a metrics snapshot.

    Histogram bucket counts are cumulative since metrics were enabled. Bucket order for the
    default shape is ``<= 1 ms``, ``> 1 ms``, ``> 2 ms``, ``> 4 ms``, ``> 8 ms``, ``> 16 ms``,
    ``> 32 ms``.

    Attributes:
        name (str): Namespace name. Empty when the command had no namespace.
        errors (int): Command errors that are not counted in a more specific counter.
        timeouts (int): Command timeouts.
        key_busy (int): Key busy errors.
        bytes_in (int): Bytes received from the server.
        bytes_out (int): Bytes sent to the server.
        latency (dict[str, list[int]]): Histogram bucket counts keyed by ``conn``, ``write``,
            ``read``, ``batch``, and ``query``.
    """
    pass


class NodeSnapshot:
    """Per-node metrics copied into a metrics snapshot.

    This client keeps separate synchronous and asynchronous connection pools. Both are reported.
    ``sync`` is the shared connection series.

    Attributes:
        name (str): Node name.
        address (str): Node address, without the port.
        port (int): Node service port.
        sync (:class:`ConnectionStats`): Synchronous connection pool.
        async_conns (:class:`ConnectionStats`): Asynchronous connection pool.
            Named ``async_conns`` because ``async`` is a Python keyword.
        conn_open_failures (int): Failed attempts to open a connection. Zero unless
            operational metrics are enabled.
        conn_tls_handshake_failures (int): Failed TLS handshakes. Zero unless
            operational metrics are enabled.
        conn_auth_failures (int): Failed authentications. Zero unless operational
            metrics are enabled.
        namespaces (list[:class:`NamespaceSnapshot`]): Namespace metrics on this node.
            Empty unless operational metrics are enabled.
    """
    pass


class EventLoopSnapshot:
    """Asynchronous event-loop gauges. Empty when async event loops are not in use.

    Attributes:
        process_size (int): Commands in process on the event loop.
        queue_size (int): Commands queued on the event loop.
    """
    pass


class MetricsSnapshot:
    """Point-in-time metrics snapshot passed to each exporter.

    The object is a copy. It remains valid after ``export`` returns. Counters and histogram
    buckets are cumulative since metrics were enabled. Gauges are the values at snapshot time.

    Enabling metrics does not turn on operational or usage metrics. Set
    :attr:`MetricsPolicy.operational_enabled` or :attr:`MetricsPolicy.usage_enabled`.
    This client does not record a usage catalog, so usage counters stay at zero
    even when ``usage_metrics_enabled`` is true.

    Attributes:
        timestamp (str): Local time, ``YYYY-MM-DD HH:MM:SS``. Same clock as the learn-metrics log.
        metrics_enabled (bool): True when this snapshot was collected with metrics on.
        operational_metrics_enabled (bool): True when latency, error, byte, CPU, and memory
            figures were collected.
        usage_metrics_enabled (bool): True when the policy requested usage metrics.
        cluster_name (str): Cluster name. Empty when the cluster has no name.
        client_type (str): Client language. ``python`` for this client.
        client_version (str): Client version.
        app_id (str): Application identifier.
        labels (dict[str, str]): Static labels from the metrics policy.
        recover_queue_size (int): Sync sockets currently in timeout recovery.
        invalid_node_count (int): Add-node failures in the most recent cluster tend iteration.
        delay_queue_timeout_count (int): Commands that timed out in the delay queue.
        command_count (int): Commands issued. Cumulative.
        retry_count (int): Command retries. Cumulative.
        cpu (int): Process CPU percent. Zero unless operational metrics are enabled.
        mem (int): Process resident set size in bytes. Zero unless operational metrics
            are enabled.
        event_loops (list[:class:`EventLoopSnapshot`]): Async event-loop gauges.
        nodes (list[:class:`NodeSnapshot`]): Nodes still in the cluster.
        nodes_departed (list[:class:`NodeSnapshot`]): Final samples for nodes removed since the
            previous export. Often empty. Replaces the node-close callback for exporters.
            Empty on :meth:`~aerospike.Client.get_metrics_snapshot`.
        latency_columns (int): Histogram width.
        latency_shift (int): Histogram boundary spacing.
        latency_unit (int): Histogram bucket unit. :data:`LATENCY_MILLISECONDS` or
            :data:`LATENCY_MICROSECONDS`.
    """
    pass


class MetricsExporter(Protocol):
    """Receives one metrics snapshot per export interval.

    Any object with an ``export`` method can be registered. Subclassing this protocol is optional.
    One exporter raising an exception does not skip the others. After repeated failures the client
    suspends that exporter and retries it later.
    """

    def export(self, snapshot: MetricsSnapshot) -> None:
        """Handle one snapshot. The snapshot may be retained after this method returns."""


def _require_exporter(exporter) -> None:
    export = getattr(exporter, "export", None)
    if not callable(export):
        raise TypeError(
            "exporter must be an object with a callable export(snapshot) method"
        )


class MetricsListeners:
    """Metrics listener callbacks.

    .. deprecated::
        Prefer :meth:`MetricsPolicy.add_exporter`. ``MetricsListeners`` remains until the next
        major release.

    All callbacks must be set.

    Attributes:
        enable_listener (Callable[[], None]): Periodic extended metrics has been enabled for the given cluster.
        snapshot_listener (Callable[[Cluster], None]): A metrics snapshot has been requested for the given cluster.
        node_close_listener (Callable[[Node], None]): A node is being dropped from the cluster.
        disable_listener (Callable[[Cluster], None]): Periodic extended metrics has been disabled for the given cluster.
    """
    def __init__(
            self,
            enable_listener: Callable[[], None],
            snapshot_listener: Callable[[Cluster], None],
            node_close_listener: Callable[[Node], None],
            disable_listener: Callable[[Cluster], None]
    ):
        warnings.warn(
            "MetricsListeners is deprecated and will be removed in the next major release. "
            "Register a metrics exporter with MetricsPolicy.add_exporter().",
            DeprecationWarning,
            stacklevel=2,
        )
        self.enable_listener = enable_listener
        self.snapshot_listener = snapshot_listener
        self.node_close_listener = node_close_listener
        self.disable_listener = disable_listener


class MetricsPolicy:
    """Client periodic metrics configuration.

    Attributes:
        metrics_listeners (Optional[:py:class:`MetricsListeners`]): Deprecated four-callback listener.
            If set, those callbacks are used and ``report_dir`` does not also install the file exporter.
            Prefer :meth:`add_exporter`.
        exporters (list): Exporters that receive each metrics snapshot. Append with :meth:`add_exporter`.
            The application owns the exporters. When this list is empty, listeners are not set, and
            ``report_dir`` is non-empty, enabling metrics installs the built-in learn-metrics file exporter.
        report_dir (str): Directory for the built-in learn-metrics file exporter.
            A non-empty path installs that exporter when no exporter has been added and
            ``metrics_listeners`` is not set. An empty string installs nothing. Collection can still run
            with no exporter. The default ``"."`` writes log files in the current directory.
        report_size_limit (int): Metrics file size soft limit in bytes for listeners that write logs.
            When report_size_limit is reached or exceeded, the current metrics file is closed and a new
            metrics file is created with a new timestamp. If report_size_limit is zero, the metrics file
            size is unbounded and the file will only be closed when :py:meth:`~aerospike.Client.disable_metrics` or
            :py:meth:`~aerospike.Client.close()` is called.
        interval (int): How often the metrics thread exports, measured in cluster tend intervals.
            The thread sleeps ``interval * tend_interval`` milliseconds (default 30 * 1000).
            Export does not run on the tend thread.
        latency_columns (int): Number of elapsed time range buckets in latency histograms.
        latency_shift (int): Power of 2 multiple between each range bucket in latency histograms starting at column 3.
            The bucket units are in milliseconds by default. The first 2 buckets are "<=1ms" and ">1ms".
        latency_unit (int): Histogram bucket unit. :data:`LATENCY_MILLISECONDS` (default) or
            :data:`LATENCY_MICROSECONDS`.
        operational_enabled (bool): Record command-path operational metrics: latency, namespace
            errors and bytes, CPU, and memory. Enabling metrics does not turn this on.
            Connection pool gauges are collected whenever metrics are enabled.
        usage_enabled (bool): Record client-wide feature usage counters. This client does not
            yet increment a usage catalog, so the counters stay at zero.
        labels (dict[str, str]): List of name/value labels that is applied when exporting metrics.

            Example:

            .. testcode::

                # latencyColumns=7 latencyShift=1
                # <=1ms >1ms >2ms >4ms >8ms >16ms >32ms

                # latencyColumns=5 latencyShift=3
                # <=1ms >1ms >8ms >64ms >512ms
    """
    def __init__(
            self,
            metrics_listeners: Optional[MetricsListeners] = None,
            report_dir: str = ".",
            report_size_limit: int = 0,
            interval: int = 30,
            latency_columns: int = 7,
            latency_shift: int = 1,
            labels: dict[str, str] = {},
            exporters: Optional[list] = None,
            latency_unit: int = LATENCY_MILLISECONDS,
            operational_enabled: bool = False,
            usage_enabled: bool = False,
    ):
        self.metrics_listeners = metrics_listeners
        self.report_dir = report_dir
        self.report_size_limit = report_size_limit
        self.interval = interval
        self.latency_columns = latency_columns
        self.latency_shift = latency_shift
        self.latency_unit = latency_unit
        self.operational_enabled = operational_enabled
        self.usage_enabled = usage_enabled
        self.labels = labels
        self.exporters = []
        if exporters:
            for exporter in exporters:
                self.add_exporter(exporter)

    def add_exporter(self, exporter) -> None:
        """Append an exporter. Exporters are called in registration order with the same snapshot.

        Enabling metrics does not take ownership of the exporter object. Keep it alive for as long
        as metrics stay enabled. The client releases its reference after metrics are disabled.
        """
        _require_exporter(exporter)
        self.exporters.append(exporter)
