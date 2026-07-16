import pytest
from .test_base_class import TestBaseClass
import aerospike
from .conftest import KEYS, BIN_NAME, expect_records_to_have_user_key_stored, AEROSPIKE_CLIENT_CONFIG_URL, DYN_CONFIG_PATH, WRITE_OPS
from aerospike_helpers.operations import operations
import os


KEY = KEYS[0]


@pytest.mark.parametrize(
    "insert_records",
    [1],
    indirect=True
)
@pytest.mark.usefixtures("insert_records")
@pytest.mark.parametrize("override_dynamic_config", [False, True])
class TestSendKeyUnionPrecedence:
    @pytest.fixture(autouse=True)
    def set_key_option(self, request, override_dynamic_config: bool):
        self.config = TestBaseClass.get_connection_config()
        if override_dynamic_config:
            os.environ[AEROSPIKE_CLIENT_CONFIG_URL] = DYN_CONFIG_PATH
        else:
            if request.param not in self.config["policies"]:
                self.config["policies"][request.param] = {}
            self.config["policies"][request.param]["key"] = True

        yield

        if override_dynamic_config:
            del os.environ[AEROSPIKE_CLIENT_CONFIG_URL]

    @pytest.mark.parametrize("set_key_option", ["write"], indirect=True)
    def test_client_config_overrides_command_level_write_policy(self):
        client = aerospike.client(self.config)

        client.put(KEY, bins={BIN_NAME: "a"})

        expect_records_to_have_user_key_stored(client, KEY[2])

    @pytest.mark.parametrize("set_key_option", ["operate"], indirect=True)
    def test_client_config_overrides_command_level_operate_policy(self):
        client = aerospike.client(self.config)

        ops = WRITE_OPS
        client.operate(KEY, ops)

        expect_records_to_have_user_key_stored(client, KEY[2])

    udf_to_load = "query_apply.lua"

    @pytest.mark.parametrize("set_key_option", ["apply"], indirect=True)
    def test_client_config_overrides_command_level_apply_policy(self, connection_with_udf):
        client = aerospike.client(self.config)

        client.apply(KEY, "query_apply", "mark_as_applied_one_arg", ["a"])

        expect_records_to_have_user_key_stored(client, KEY[2])

    @pytest.mark.parametrize("set_key_option", ["batch_write"], indirect=True)
    def test_client_config_overrides_command_level_batch_write_policy(self):
        client = aerospike.client(self.config)

        ops = [
            operations.write(BIN_NAME, "a")
        ]
        client.batch_operate([KEY], ops)

        expect_records_to_have_user_key_stored(client, KEY[2])

    @pytest.mark.parametrize("set_key_option", ["batch_apply"], indirect=True)
    def test_client_config_overrides_command_level_batch_apply_policy(self, connection_with_udf):
        client = aerospike.client(self.config)

        client.batch_apply([KEY], "query_apply", "mark_as_applied_one_arg", ["a"])

        expect_records_to_have_user_key_stored(client, KEY[2])
