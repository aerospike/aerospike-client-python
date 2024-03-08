from aerospike import Client


class TestMetrics:
    def test_disable_metrics(self, as_connection: Client):
        as_connection.disable_metrics()
