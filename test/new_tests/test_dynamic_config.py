import aerospike
from aerospike import exception as e
from .test_base_class import TestBaseClass
import pytest


class TestDynamicConfig:
    def test_basic_functionality(self):
        config = TestBaseClass.get_connection_config()
        provider = aerospike.ConfigProvider("./dyn_config.yml")
        config["config_provider"] = provider
        aerospike.set_log_level(aerospike.LOG_LEVEL_TRACE)
        client = aerospike.client(config)

        key = ("test", "demo", 1)
        client.put(key, {"a": 1})

        # "Send key" is enabled in dynamic config
        # The key should be returned here
        rec, _, _ = client.get(key)
        assert rec[2] is not None

        # Cleanup
        client.remove(key)
        client.close()

    def test_api_invalid_provider(self):
        config = TestBaseClass.get_connection_config()
        config["config_provider"] = 0
        # TODO: return a more useful error msg.
        with pytest.raises(e.ParamError):
            aerospike.client(config)
