import aerospike
from aerospike import exception as e
from .test_base_class import TestBaseClass
import pytest
import os
import glob

DYN_CONFIG_PATH = "./dyn_config.yml"
METRICS_LOG_FILES = "./metrics-*.log"


class TestDynamicConfig:
    def test_config_provider_defaults(self):
        provider = aerospike.ConfigProvider(path="path")
        assert provider.interval == 60000

    def test_config_provider_class(self):
        provider = aerospike.ConfigProvider(path="path", interval=30000)

        assert provider.path == "path"
        assert provider.interval == 30000
        # Fields should be read only
        with pytest.raises(AttributeError):
            provider.path = "invalid"
        with pytest.raises(AttributeError):
            provider.interval = 10000

    def test_config_provider_class_invalid_args(self):
        # See comment in test_mrt_api.py's test_transaction_class test case
        # for why Windows throws OverflowError instead of ValueError
        with pytest.raises((ValueError, OverflowError)):
            aerospike.ConfigProvider("path", interval=2**32)

    # We want to check that the config file we pass in is valid
    # The C client prints logs showing that it detects changes to the dynamic config file
    # We also want to check that enable/disable metrics prints out warning logs when dyn config is enabled
    @pytest.fixture
    def show_more_logs(self):
        aerospike.set_log_level(aerospike.LOG_LEVEL_TRACE)

        yield

        # TODO: currently there is no way to restore the log handler and level before running this test
        # These are the defaults in the implementation
        # aerospike.set_log_level(aerospike.LOG_LEVEL_ERROR)

    @pytest.fixture
    def cleanup_metrics_logs(self):
        yield

        metrics_log_filenames = glob.glob(METRICS_LOG_FILES)
        for item in metrics_log_filenames:
            os.remove(item)

    @pytest.fixture
    def functional_test_setup(self, show_more_logs, cleanup_metrics_logs):
        config = TestBaseClass.get_connection_config()
        setup_client = aerospike.client(config)
        self.key = ("test", "demo", 1)
        try:
            setup_client.remove(self.key)
        except e.RecordNotFound:
            pass

        yield

        # Close file descriptors for metrics log files before removing the files
        self.client.close()

        setup_client.remove(self.key)
        setup_client.close()

    # If not using env var, use the config provider instead
    @pytest.mark.parametrize("use_env_var", [False, True])
    # Dynamic config file should take precedence over both client config defaults and programmatically set values
    def test_dyn_config_file_has_highest_precedence(self, functional_test_setup, use_env_var: bool):
        config = TestBaseClass.get_connection_config()
        if use_env_var:
            AEROSPIKE_CLIENT_CONFIG_URL = "AEROSPIKE_CLIENT_CONFIG_URL"
            os.environ[AEROSPIKE_CLIENT_CONFIG_URL] = DYN_CONFIG_PATH
        else:
            provider = aerospike.ConfigProvider(DYN_CONFIG_PATH)
            config["config_provider"] = provider

        write_policy = {"key": aerospike.POLICY_KEY_SEND}
        config["policies"]["write"] = write_policy
        self.client = aerospike.client(config)

        self.client.put(self.key, bins={"a": 1}, policy=write_policy)

        # "Send key" is disabled in dynamic config
        # The key should not be returned here
        query = self.client.query("test", "demo")
        recs = query.results()
        assert len(recs) == 1
        # Check that record key tuple does not have a primary key
        first_record = recs[0]
        first_record_key = first_record[0]
        assert first_record_key[2] is None

        # Cleanup
        if use_env_var:
            del os.environ[AEROSPIKE_CLIENT_CONFIG_URL]

    def test_enable_metrics_cannot_override_dyn_config(self, show_more_logs):
        config = TestBaseClass.get_connection_config()
        config["config_provider"] = aerospike.ConfigProvider("./dyn_config_metrics_disabled.yml")
        client = aerospike.client(config)

        client.enable_metrics()

        # Cleanup
        client.close()

    def test_disable_metrics_cannot_override_dyn_config(self, show_more_logs, cleanup_metrics_logs):
        config = TestBaseClass.get_connection_config()
        config["config_provider"] = aerospike.ConfigProvider(DYN_CONFIG_PATH)
        client = aerospike.client(config)

        client.disable_metrics()

        client.close()

    def test_api_invalid_provider(self):
        config = TestBaseClass.get_connection_config()
        config["config_provider"] = 0
        with pytest.raises(e.ParamError) as excinfo:
            aerospike.client(config)
        assert excinfo.value.msg == "config_provider must be an aerospike.ConfigProvider class instance. "\
            "But a int was received instead"
