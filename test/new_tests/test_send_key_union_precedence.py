import pytest
from .test_base_class import TestBaseClass
import aerospike
from .conftest import KEYS, BIN_NAME, expect_records_to_have_user_key_stored
from aerospike_helpers.operations import operations


config = TestBaseClass.get_connection_config()


# TODO: is this applied properly?
@pytest.mark.parametrize(
    "clean_test_background",
    [1],
    indirect=True
)
class TestSendKeyUnionPrecedence:
    @pytest.fixture(autouse=True)
    def setup(self, clean_test_background):
        pass

    def test_client_config_overrides_command_level_write_policy(self):
        config["policies"]["write"]["key"] = True
        client = aerospike.client(config)

        KEY = KEYS[0]
        client.put(KEY, bins={BIN_NAME: "a"})

        expect_records_to_have_user_key_stored(client, KEY[2])

    def test_client_config_overrides_command_level_operate_policy(self):
        config["policies"]["operate"]["key"] = True
        client = aerospike.client(config)

        KEY = KEYS[0]
        ops = [
            operations.write(BIN_NAME, "a")
        ]
        client.operate(KEY, ops)

        expect_records_to_have_user_key_stored(client, KEY[2])

    def test_client_config_overrides_command_level_apply_policy(self):
        config["policies"]["apply"]["key"] = True
        client = aerospike.client(config)
        # TODO: this needs to be set in a fixture...
        client.udf_put("query_apply.lua")

        KEY = KEYS[0]

        client.apply(KEY, "query_apply", "mark_as_applied_one_arg", ["a"])

        expect_records_to_have_user_key_stored(client, KEY[2])

    def test_client_config_overrides_command_level_batch_write_policy(self):
        config["policies"]["batch_write"] = {
            "key": True
        }
        client = aerospike.client(config)

        KEY = KEYS[0]
        ops = [
            operations.write(BIN_NAME, "a")
        ]
        client.batch_operate([KEY], ops)

        expect_records_to_have_user_key_stored(client, KEY[2])

    def test_client_config_overrides_command_level_batch_apply_policy(self):
        config["policies"]["batch_apply"] = {
            "key": True
        }
        client = aerospike.client(config)
        client.udf_put("query_apply.lua")

        KEY = KEYS[0]

        client.batch_apply([KEY], "query_apply", "mark_as_applied_one_arg", ["a"])

        expect_records_to_have_user_key_stored(client, KEY[2])
