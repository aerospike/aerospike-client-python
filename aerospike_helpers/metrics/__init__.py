from typing import Callable


class ConnectionStats:
    """
    in_use (int): Connections actively being used in database transactions on this node.
        There can be multiple pools per node. This value is a summary of those pools on this node.
    in_pool (int): Connections residing in pool(s) on this node.
        There can be multiple pools per node. This value is a summary of those pools on this node.
    opened (int):
    closed (int):
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
        error_count (int): Transaction error count since node was initialized. If the error is retryable,
            multiple errors per transaction may occur.
        timeout_count (int): Transaction timeout count since node was initialized.
            If the timeout is retryable (ie socketTimeout), multiple timeouts per transaction may occur.
        metrics (:py:class:`NodeMetrics`): Node metrics
    """
    pass


class Cluster:
    cluster_name: str
    invalid_node_count: int
    transaction_count: int
    retry_count: int
    nodes: list[Node]


class MetricsListeners:
    enable_listener: Callable[[], None]
    snapshot_listener: Callable[[Cluster], None]
    node_close_listener: Callable[[Node], None]
    disable_listener: Callable[[Cluster], None]


class MetricsPolicy:
    metrics_listeners: MetricsListeners
    report_dir: str
    report_size_limit: int
    interval: int
    latency_columns: int
    latency_shift: int
