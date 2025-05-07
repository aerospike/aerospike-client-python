import aerospike
from aerospike import exception as e
from .test_base_class import TestBaseClass
import pytest
import os


class TestDynamicConfig:
    def test_config_provider_defaults(self):
        provider = aerospike.ConfigProvider(path="path")
        assert provider.interval == 60

    def test_config_provider_class(self):
        provider = aerospike.ConfigProvider(path="path", interval=30)

        assert provider.path == "path"
        assert provider.interval == 30
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

    # If not using env var, use the config provider instead
    @pytest.mark.parametrize("use_env_var", [False, True])
    # Dynamic config file should take precedence over both client config defaults and programmatically set values
    def test_dyn_config_file_has_highest_precedence(self, functional_test_setup, use_env_var: bool):
        config = TestBaseClass.get_connection_config()
        # Uncomment if we want to check that the config file we pass in is valid
        # The C client prints logs showing that it detects changes to the dynamic config file
        # aerospike.set_log_level(aerospike.LOG_LEVEL_TRACE)
        DYN_CONFIG_PATH = "./dyn_config.yml"
        if use_env_var:
            AEROSPIKE_CLIENT_CONFIG_URL = "AEROSPIKE_CLIENT_CONFIG_URL"
            os.environ[AEROSPIKE_CLIENT_CONFIG_URL] = DYN_CONFIG_PATH
        else:
            provider = aerospike.ConfigProvider(DYN_CONFIG_PATH)
            config["config_provider"] = provider

        write_policy = {"key": aerospike.POLICY_KEY_SEND}
        config["policies"]["write"] = write_policy
        client = aerospike.client(config)

        client.put(self.key, bins={"a": 1}, policy=write_policy)

        # "Send key" is disabled in dynamic config
        # The key should not be returned here
        query = client.query("test", "demo")
        recs = query.results()
        assert len(recs) == 1
        # Check that record key tuple has a primary key
        first_record = recs[0]
        first_record_key = first_record[0]
        assert first_record_key[2] is None

        # Cleanup
        client.close()
        if use_env_var:
            del os.environ[AEROSPIKE_CLIENT_CONFIG_URL]

    def test_api_invalid_provider(self):
        config = TestBaseClass.get_connection_config()
        config["config_provider"] = 0
        with pytest.raises(e.ParamError) as excinfo:
            aerospike.client(config)
        assert excinfo.value.msg == "config_provider must be an aerospike.ConfigProvider class instance. "\
            "But a int was received instead"
