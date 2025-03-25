import aerospike
from .test_base_class import TestBaseClass


class TestDynamicConfig:
    def test_basic_functionality(self):
        config = TestBaseClass.get_connection_config()
        provider = aerospike.ConfigProvider("./dyn_config.yml")
        config["config_provider"] = provider
        client = aerospike.client(config)

        client.close()
