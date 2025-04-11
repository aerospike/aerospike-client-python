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

    def test_config_provider_class_invalid_args(self):
        with pytest.raises(ValueError) as excinfo:
            aerospike.ConfigProvider("path", interval=2**32)
        assert excinfo.value.args[0] == "interval is too large for an unsigned 32-bit integer"

    @pytest.fixture
    def functional_test_setup(self):
        config = TestBaseClass.get_connection_config()
        setup_client = aerospike.client(config)
        self.key = ("test", "demo", 1)
        try:
            setup_client.remove(self.key)
        except e.RecordNotFound:
            pass

        yield

        setup_client.remove(self.key)
        setup_client.close()

    def test_basic_functionality(self, functional_test_setup):
        config = TestBaseClass.get_connection_config()
        provider = aerospike.ConfigProvider("./dyn_config.yml")
        config["config_provider"] = provider
        # We want to check that the config file we pass in is valid
        # The C client prints logs showing that it detects changes to the dynamic config file
        # aerospike.set_log_level(aerospike.LOG_LEVEL_TRACE)

        client = aerospike.client(config)

        client.put(self.key, {"a": 1})

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
        client.close()

    def test_api_invalid_provider(self):
        config = TestBaseClass.get_connection_config()
        config["config_provider"] = 0
        with pytest.raises(e.ParamError) as excinfo:
            aerospike.client(config)
        assert excinfo.value.msg == "config_provider must be an aerospike.ConfigProvider class instance. "\
            "But a int was received instead"
