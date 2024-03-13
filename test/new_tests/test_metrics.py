from aerospike import Client
from aerospike_helpers.metrics import MetricsPolicy


class TestMetrics:
    def test_enable_metrics(self, as_connection: Client):
        as_connection.enable_metrics()

    def test_enable_metrics_with_default_metrics_policy(self, as_connection: Client):
        policy = MetricsPolicy()
        as_connection.enable_metrics(policy=policy)

    def test_disable_metrics(self, as_connection: Client):
        as_connection.disable_metrics()
