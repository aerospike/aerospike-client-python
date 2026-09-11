import aerospike
from aerospike import exception as e
from .conftest import expect_records_to_have_user_key_stored, AEROSPIKE_CLIENT_CONFIG_URL, DYN_CONFIG_PATH
from .test_base_class import TestBaseClass
import pytest
import os
import glob

METRICS_LOG_FILES = "./metrics-*.log"


class TestDynamicConfig:
    def test_config_provider_defaults(self):
        provider = aerospike.ConfigProvider(path="path")
        assert provider.interval == 5000

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
        aerospike.set_log_level(aerospike.LOG_LEVEL_ERROR)

    @pytest.fixture
    def cleanup_metrics_logs(self):
        yield

        metrics_log_filenames = glob.glob(METRICS_LOG_FILES)
        for item in metrics_log_filenames:
            os.remove(item)

    @pytest.fixture
    def functional_test_setup(self, request, show_more_logs, cleanup_metrics_logs):
        config = TestBaseClass.get_connection_config()
        setup_client = aerospike.client(config)
        self.key = ("test", "demo", 1)
        try:
            setup_client.remove(self.key)
        except e.RecordNotFound:
            pass

        yield request.param

        # Close file descriptors for metrics log files before removing the files
        self.client.close()

        setup_client.remove(self.key)
        setup_client.close()

        if request.param is True:
            del os.environ[AEROSPIKE_CLIENT_CONFIG_URL]

    # Decide whether env var should be used or not to read dynamic config file.
    # If not using env var, use the config provider instead
    # Manually tested that setting send_key to false in the dynamic config yaml causes this test to fail.
    @pytest.mark.parametrize("functional_test_setup", [False, True], indirect=True)
    def test_dyn_config_file_works(self, functional_test_setup):
        config = TestBaseClass.get_connection_config()
        if functional_test_setup is True:
            os.environ[AEROSPIKE_CLIENT_CONFIG_URL] = DYN_CONFIG_PATH
        else:
            provider = aerospike.ConfigProvider(DYN_CONFIG_PATH)
            config["config_provider"] = provider

        self.client = aerospike.client(config)

        self.client.put(self.key, bins={"a": 1})

        # "Send key" is enabled in dynamic config
        # The key should be returned here
        expect_records_to_have_user_key_stored(self.client, set_name="demo")

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
