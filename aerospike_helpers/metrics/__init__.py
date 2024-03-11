class ConnectionStats:
    in_use: int
    in_pool: int
    opened: int
    closed: int


class NodeMetrics:
    conn_latency: list[int]
    write_latency: list[int]
    read_latency: list[int]
    batch_latency: list[int]
    query_latency: list[int]


class Node:
    name: str
    address: str
    port: int
    conns: ConnectionStats
    error_count: int
    timeout_count: int
    metrics: NodeMetrics


class Cluster:
    cluster_name: str
    invalid_node_count: int
    transaction_count: int
    retry_count: int
    nodes: list[Node]


class MetricsListeners:
    enable_listener: callable
    snapshot_listener: callable
    node_close_listener: callable
    disable_listener: callable


class MetricsPolicy:
    metrics_listeners: MetricsListeners
    report_dir: str
    report_size_limit: int
    interval: int
    latency_columns: int
    latency_shift: int
