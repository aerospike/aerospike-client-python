import aerospike
from .test_base_class import TestBaseClass


class TestDynamicConfig:
    def test_basic_functionality(self):
        config = TestBaseClass.get_connection_config()
        provider = aerospike.ConfigProvider("./dyn_config.yml")
        config["config_provider"] = provider
        client = aerospike.client(config)

        key = ("test", "demo", 1)
        client.put(key)

        # "Send key" is enabled in dynamic config
        # The key should be returned here
        rec, _, _ = client.get(key)
        assert rec[2] is not None

        client.remove(key)
        client.close()
