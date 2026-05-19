import aerospike
import pytest


@pytest.mark.usefixtures("connection_config")
class TestSharedMemory:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.__class__.connection_config["shm"] = {}
        self.__class__.connection_config["use_shared_connection"] = True

    def test_one_client(self):
        client = aerospike.client(self.connection_config)

        assert client.is_connected()

        client.close()

    def test_multiple_clients(self):

        client1 = aerospike.client(self.connection_config)
        assert client1.is_connected()

        client2 = aerospike.client(self.connection_config)
        assert client2.is_connected()

        client1.close()
        client2.close()
