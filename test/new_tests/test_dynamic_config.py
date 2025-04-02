import aerospike
from aerospike import exception as e
from .test_base_class import TestBaseClass
import pytest


class TestDynamicConfig:
    def test_config_provider_class(self):
        provider = aerospike.ConfigProvider(path="path", interval=20)

        assert provider.path == "path"
        assert provider.interval == 20
        # Fields should be read only
        with pytest.raises(AttributeError):
            provider.path = "invalid"
        with pytest.raises(AttributeError):
            provider.interval = 10

        # We should be able to reinitialize with no issues
        provider.__init__()

    def test_basic_functionality(self):
        config = TestBaseClass.get_connection_config()
        provider = aerospike.ConfigProvider("./dyn_config.yml")
        config["config_provider"] = provider
        # We want to check that the config file we pass in is valid
        # The C client prints logs showing that it detects changes to the dynamic config file
        aerospike.set_log_level(aerospike.LOG_LEVEL_TRACE)
        client = aerospike.client(config)

        # TODO: make sure pk doesn't exist in server
        key = ("test", "demo", 1)
        client.put(key, {"a": 1})

        # "Send key" is enabled in dynamic config
        # The key should be returned here
        query = client.query("test", "demo")
        recs = query.results()
        assert len(recs) == 1
        # Check that record key tuple does not have a primary key
        first_record = recs[0]
        first_record_key = first_record[0]
        assert first_record_key[2] is not None

        # Cleanup
        client.remove(key)
        client.close()

    def test_api_invalid_provider(self):
        config = TestBaseClass.get_connection_config()
        config["config_provider"] = 0
        with pytest.raises(e.ParamError) as excinfo:
            aerospike.client(config)
        assert excinfo.value.msg == "config_provider must be an aerospike.ConfigProvider class instance"
