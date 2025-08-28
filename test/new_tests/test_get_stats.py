import pytest
from aerospike_helpers.metrics import ClusterStats, NodeStats, ConnectionStats


@pytest.mark.usefixtures("as_connection")
class TestGetStats(object):
    def test_get_stats(self):
        cluster_stats = self.as_connection.get_stats()
        assert isinstance(cluster_stats, ClusterStats)

        # Test class API
        assert isinstance(cluster_stats.nodes, list)
        assert isinstance(cluster_stats.retry_count, int)
        assert isinstance(cluster_stats.thread_pool_queued_tasks, int)
        assert isinstance(cluster_stats.recover_queue_size, int)

        for single_node_stats in cluster_stats.nodes:
            assert isinstance(single_node_stats, NodeStats)
            assert isinstance(single_node_stats.name, str)
            assert isinstance(single_node_stats.address, str)
            assert isinstance(single_node_stats.port, int)
            assert isinstance(single_node_stats.conns, ConnectionStats)
            assert isinstance(single_node_stats.error_count, int)
            assert isinstance(single_node_stats.timeout_count, int)
            assert isinstance(single_node_stats.key_busy_count, int)

    def test_invalid_args(self):
        with pytest.raises(TypeError):
            self.as_connection.get_stats("")
