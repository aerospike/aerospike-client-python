import pytest
from .test_base_class import TestBaseClass
import aerospike
from .conftest import TEST_NS, TEST_SET, BIN_NAME, expect_records_to_have_user_key_stored, AEROSPIKE_CLIENT_CONFIG_URL, DYN_CONFIG_PATH, WRITE_OPS
from aerospike_helpers.operations import operations
import os


@pytest.mark.parametrize(
    "insert_records",
    [{"record_count": 1, "make_set_unique": True}],
    indirect=True
)
@pytest.mark.usefixtures("insert_records")
class TestSendKeyUnionPrecedence:
    @pytest.fixture(autouse=True)
    def set_key_option(self, request):
        self.config = TestBaseClass.get_connection_config()
        policy_name, override_dynamic_config = request.param
        if override_dynamic_config:
            os.environ[AEROSPIKE_CLIENT_CONFIG_URL] = DYN_CONFIG_PATH
        else:
            if policy_name not in self.config["policies"]:
                self.config["policies"][policy_name] = {}
            self.config["policies"][policy_name]["key"] = True

        yield

        if override_dynamic_config:
            del os.environ[AEROSPIKE_CLIENT_CONFIG_URL]

    @pytest.mark.parametrize("set_key_option", [("write", False), ("write", True)], indirect=True)
    def test_client_config_overrides_command_level_write_policy(self):
        client = aerospike.client(self.config)

        client.put(self.keys[0], bins={BIN_NAME: "a"}, policy={"key": aerospike.POLICY_KEY_DIGEST})

        expect_records_to_have_user_key_stored(client, self.set_name)

    @pytest.mark.parametrize("set_key_option", [("operate", False)], indirect=True)
    def test_client_config_overrides_command_level_operate_policy(self):
        client = aerospike.client(self.config)

        ops = WRITE_OPS
        client.operate(self.keys[0], ops, policy={"key": aerospike.POLICY_KEY_DIGEST})

        expect_records_to_have_user_key_stored(client, self.set_name)

    udf_to_load = pytest.mark.parametrize(
        "connection_with_udf",
        [
            "query_apply.lua"
        ],
        indirect=True
    )

    @pytest.mark.parametrize("set_key_option", [("apply", False)], indirect=True)
    @udf_to_load
    def test_client_config_overrides_command_level_apply_policy(self, connection_with_udf):
        client = aerospike.client(self.config)

        client.apply(self.keys[0], "query_apply", "mark_as_applied_one_arg", ["a"], policy={"key": aerospike.POLICY_KEY_DIGEST})

        expect_records_to_have_user_key_stored(client, self.set_name)

    @pytest.mark.parametrize("set_key_option", [("batch_write", False), ("batch_write", True)], indirect=True)
    def test_client_config_overrides_command_level_batch_write_policy(self):
        client = aerospike.client(self.config)

        ops = [
            operations.write(BIN_NAME, "a")
        ]
        client.batch_operate([self.keys[0]], ops, policy_batch_write={"key": aerospike.POLICY_KEY_DIGEST})

        expect_records_to_have_user_key_stored(client, self.set_name)

    @pytest.mark.parametrize("set_key_option", [("batch_apply", False)], indirect=True)
    @udf_to_load
    def test_client_config_overrides_command_level_batch_apply_policy(self, connection_with_udf):
        client = aerospike.client(self.config)

        client.batch_apply([self.keys[0]], "query_apply", "mark_as_applied_one_arg", ["a"], policy_batch_apply={"key": aerospike.POLICY_KEY_DIGEST})

        expect_records_to_have_user_key_stored(client, self.set_name)
