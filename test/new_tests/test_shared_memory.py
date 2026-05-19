import aerospike
import pytest


@pytest.mark.usefixtures("connection_config")
class TestSharedMemory:
    def test_one_client(self):
        self.connection_config["use_shared_connection"] = True
        client = aerospike.client(self.connection_config)

        assert client.is_connected()

        client.close()

    def test_multiple_clients(self):
        self.connection_config["use_shared_connection"] = True

        client1 = aerospike.client(self.connection_config)
        assert client1.is_connected()

        client2 = aerospike.client(self.connection_config)
        assert client2.is_connected()

        client1.close()
        client2.close()
