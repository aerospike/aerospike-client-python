import aerospike
import pytest


@pytest.mark.usefixtures("connection_config")
class TestSharedMemory:
    def test_one_client(self):
        self.connection_config["use_shared_connection"] = True
        client = aerospike.client(self.connection_config)

        assert client.is_connected()

        client.close()
