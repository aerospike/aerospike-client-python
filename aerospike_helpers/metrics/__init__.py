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

:class:`ConnectionStats`, :class:`NodeMetrics`, :class:`Node`, and :class:`Cluster` do not have a constructor
because they are not meant to be created by the user. They are only meant to be returned from :class:`MetricsListeners`
callbacks for reading data about the server and client.
"""

from typing import Optional, Callable


class ConnectionStats:
    """Connection statistics.

    Attributes:
        in_use (int): Connections actively being used in database commands on this node.
            There can be multiple pools per node. This value is a summary of those pools on this node.
        in_pool (int): Connections residing in pool(s) on this node.
            There can be multiple pools per node. This value is a summary of those pools on this node.
        opened (int): Total number of node connections opened since node creation.
        closed (int): Total number of node connections closed since node creation.
    """
    pass


class NodeMetrics:
    """
    Each type of latency has a list of latency buckets.

    Latency bucket counts are cumulative and not reset on each metrics snapshot interval.

    Attributes:
        conn_latency (list[int])
        write_latency (list[int])
        read_latency (list[int])
        batch_latency (list[int])
        query_latency (list[int])
    """
    pass


class Node:
    """Server node representation.

    Attributes:
        name (str): The name of the node.
        address (str): The IP address / host name of the node (not including the port number).
        port (int): Port number of the node's address.
        conns (:py:class:`ConnectionStats`): Synchronous connection stats on this node.
        error_count (int): Command error count since node was initialized. If the error is retryable,
            multiple errors per command may occur.
        timeout_count (int): Command timeout count since node was initialized.
            If the timeout is retryable (i.e socketTimeout), multiple timeouts per command may occur.
        metrics (:py:class:`NodeMetrics`): Node metrics
    """
    pass


class Cluster:
    """Cluster of server nodes.

    Attributes:
        cluster_name (Optional[str]): Expected cluster name for all nodes. May be :py:obj:`None`.
        invalid_node_count (int): Count of add node failures in the most recent cluster tend iteration.
        command_count (int): Command count. The value is cumulative and not reset per metrics interval.
        retry_count (int): Command retry count. There can be multiple retries for a single command.
            The value is cumulative and not reset per metrics interval.
        nodes (list[:py:class:`Node`]): Active nodes in cluster.
    """
    pass


class MetricsListeners:
    """Metrics listener callbacks.

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
        self.enable_listener = enable_listener
        self.snapshot_listener = snapshot_listener
        self.node_close_listener = node_close_listener
        self.disable_listener = disable_listener


class MetricsPolicy:
    """Client periodic metrics configuration.

    Attributes:
        metrics_listeners (Optional[:py:class:`MetricsListeners`]): Listeners that handles metrics notification events.
            If set to :py:obj:`None`, the default listener implementation will be used, which writes the metrics
            snapshot to a file which can later be read and forwarded to OpenTelemetry by a separate offline
            application. Otherwise, use all listeners set in the class instance.

            The listener could be overridden to send the metrics snapshot directly to OpenTelemetry.
        report_dir (str): Directory path to write metrics log files for listeners that write logs.
        report_size_limit (int): Metrics file size soft limit in bytes for listeners that write logs.
            When report_size_limit is reached or exceeded, the current metrics file is closed and a new
            metrics file is created with a new timestamp. If report_size_limit is zero, the metrics file
            size is unbounded and the file will only be closed when :py:meth:`~aerospike.Client.disable_metrics` or
            :py:meth:`~aerospike.Client.close()` is called.
        interval (int): Number of cluster tend iterations between metrics notification events. One tend iteration
            is defined as ``"tend_interval"`` in the client config plus the time to tend all nodes.
        latency_columns (int): Number of elapsed time range buckets in latency histograms.
        latency_shift (int): Power of 2 multiple between each range bucket in latency histograms starting at column 3.
            The bucket units are in milliseconds. The first 2 buckets are "<=1ms" and ">1ms".

            Example::

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
            latency_shift: int = 1
    ):
        self.metrics_listeners = metrics_listeners
        self.report_dir = report_dir
        self.report_size_limit = report_size_limit
        self.interval = interval
        self.latency_columns = latency_columns
        self.latency_shift = latency_shift
