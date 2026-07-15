import pytest
from .test_base_class import TestBaseClass
import aerospike
from .conftest import KEYS, BIN_NAME, expect_records_to_have_user_key_stored
from aerospike_helpers.operations import operations


config = TestBaseClass.get_connection_config()
KEY = KEYS[0]


@pytest.mark.parametrize(
    "insert_records",
    [1],
    indirect=True
)
@pytest.mark.usefixtures("insert_records")
class TestSendKeyUnionPrecedence:
    def test_client_config_overrides_command_level_write_policy(self):
        config["policies"]["write"]["key"] = True
        client = aerospike.client(config)

        client.put(KEY, bins={BIN_NAME: "a"})

        expect_records_to_have_user_key_stored(client, KEY[2])

    def test_client_config_overrides_command_level_operate_policy(self):
        config["policies"]["operate"]["key"] = True
        client = aerospike.client(config)

        ops = [
            operations.write(BIN_NAME, "a")
        ]
        client.operate(KEY, ops)

        expect_records_to_have_user_key_stored(client, KEY[2])

    udf_to_load = "example.lua"

    def test_client_config_overrides_command_level_apply_policy(self, connection_with_udf):
        config["policies"]["apply"]["key"] = True
        client = aerospike.client(config)


        client.apply(KEY, "query_apply", "mark_as_applied_one_arg", ["a"])

        expect_records_to_have_user_key_stored(client, KEY[2])

    def test_client_config_overrides_command_level_batch_write_policy(self):
        config["policies"]["batch_write"] = {
            "key": True
        }
        client = aerospike.client(config)

        ops = [
            operations.write(BIN_NAME, "a")
        ]
        client.batch_operate([KEY], ops)

        expect_records_to_have_user_key_stored(client, KEY[2])

    def test_client_config_overrides_command_level_batch_apply_policy(self, connection_with_udf):
        config["policies"]["batch_apply"] = {
            "key": True
        }
        client = aerospike.client(config)

        client.batch_apply([KEY], "query_apply", "mark_as_applied_one_arg", ["a"])

        expect_records_to_have_user_key_stored(client, KEY[2])
