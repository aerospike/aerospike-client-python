import pytest
from aerospike import exception as e

import aerospike
from .test_base_class import TestBaseClass

# This is for test_validate_keys
# INVALID_OPTION_KEY cannot be read for the client-config policies test case as a class variable,
# so these were made global
INVALID_OPTION_KEY = "a"

class TestInvalidClientConfig(object):
    def test_no_config(self):
        with pytest.raises(e.ParamError) as err:
            aerospike.client()
        assert "No config argument" in err.value.msg
        # Test exception chaining
        assert type(err.value.__context__) == TypeError

    def test_config_not_dict(self):
        with pytest.raises(e.ParamError) as err:
            aerospike.client([])
        assert "Config must be a dict" in err.value.msg

    def test_no_host_in_config(self):
        with pytest.raises(e.ParamError) as err:
            aerospike.client({})
        assert "Hosts must be a list" in err.value.msg

    def test_wrong_host_type(self):
        with pytest.raises(e.ParamError) as err:
            aerospike.client({"hosts": (())})
        assert "Hosts must be a list" in err.value.msg

    def test_empty_host_in_config(self):
        with pytest.raises(e.ParamError) as err:
            aerospike.client({"hosts": []})
        assert "Hosts must not be empty" in err.value.msg

    def test_invalid_host_in_list(self):
        with pytest.raises(e.ParamError) as err:
            aerospike.client({"hosts": [("localhost", 3000), ()]})
        assert "Invalid host" in err.value.msg

    def test_lua_user_path_too_long(self):
        with pytest.raises(e.ParamError) as err:
            aerospike.client({"hosts": [("localhost", 3000)], "lua": {"user_path": "a" * 256}})
        assert "Lua user path too long" in err.value.msg

    def test_non_callable_serializer(self):
        with pytest.raises(e.ParamError) as err:
            aerospike.client({"hosts": [("localhost", 3000)], "serialization": (5, lambda x: 5)})
        assert "Serializer must be callable" in err.value.msg

    def test_non_callable_deserializer(self):
        with pytest.raises(e.ParamError) as err:
            aerospike.client({"hosts": [("localhost", 3000)], "serialization": (lambda x: 5, 5)})
        assert "Deserializer must be callable" in err.value.msg

    def test_negative_threshold_value(self):
        with pytest.raises(e.ParamError) as err:
            aerospike.client({"hosts": [("localhost", 3000)], "compression_threshold": -1})
        assert "integer value for config.policy.write.compression_threshold exceeds ULONG_MAX" in err.value.msg

    @pytest.mark.parametrize("policy", ["read", "write", "operate", "batch", "scan", "query", "apply", "remove"])
    @pytest.mark.parametrize(
        "key, value",
        [("total_timeout", "5"), ("socket_timeout", "5"), ("max_retries", "5"), ("sleep_between_retries", "5")],
    )
    def test_invalid_subpolicy_base_types(self, policy, key, value):
        subpolicy = {key: value}
        with pytest.raises(e.ParamError):
            aerospike.client({"hosts": [("localhost", 3000)], "policies": {policy: subpolicy}})

    @pytest.mark.parametrize("key, value", [("deserialize", "nope"), ("key", "send"), ("replica", "maybe?")])
    def test_invalid_read_policy_types(self, key, value):
        subpolicy = {key: value}
        with pytest.raises(e.ParamError):
            aerospike.client({"hosts": [("localhost", 3000)], "policies": {"read": subpolicy}})

    @pytest.mark.parametrize(
        "key, value",
        [
            ("key", "send"),  # should be int
            ("exists", "exists"),  # should be int
            ("gen", "should be a constant integer"),  # should be int
            ("commit_level", "committed"),  # should be int
            ("durable_delete", "durable"),  # should be bool
        ],
    )
    def test_invalid_write_policy_types(self, key, value):
        subpolicy = {key: value}
        with pytest.raises(e.ParamError):
            aerospike.client({"hosts": [("localhost", 3000)], "policies": {"write": subpolicy}})

    @pytest.mark.parametrize(
        "key, value",
        [
            ("key", "send"),  # should be int
            ("gen", "should be a constant integer"),  # should be int
            ("replica", "maybe?"),  # should be int
            ("commit_level", "committed"),  # should be int
            ("durable_delete", "durable"),  # should be bool
        ],
    )
    def test_invalid_operat_policy_types(self, key, value):
        subpolicy = {key: value}
        with pytest.raises(e.ParamError):
            aerospike.client({"hosts": [("localhost", 3000)], "policies": {"operate": subpolicy}})

    @pytest.mark.parametrize(
        "key, value",
        [
            ("concurrent", "concurrent"),  # should be bool
            ("allow_inline", "False"),  # should be bool
            ("deserialize", "False"),  # should be bool
        ],
    )
    def test_invalid_batch_policy_types(self, key, value):
        subpolicy = {key: value}
        with pytest.raises(e.ParamError):
            aerospike.client({"hosts": [("localhost", 3000)], "policies": {"batch": subpolicy}})

    @pytest.mark.parametrize("key, value", [("durable_delete", "durable")])  # should be bool
    def test_invalid_scan_policy_types(self, key, value):
        subpolicy = {key: value}
        with pytest.raises(e.ParamError):
            aerospike.client({"hosts": [("localhost", 3000)], "policies": {"scan": subpolicy}})

    #  Keep this parametrized in case query gets additional policies
    @pytest.mark.parametrize(
        "key, value",
        [
            ("deserialize", "False"),  # should be a bool
        ],
    )
    def test_invalid_query_policy_types(self, key, value):
        subpolicy = {key: value}
        with pytest.raises(e.ParamError):
            aerospike.client({"hosts": [("localhost", 3000)], "policies": {"query": subpolicy}})

    @pytest.mark.parametrize(
        "key, value",
        [
            # ("gen", "should be a constant integer"),  # gen removed from apply policies by C client 5.0
            ("replica", "maybe?"),  # should be int
            ("commit_level", "committed"),  # should be int
            ("key", "send"),  # should be an int eg. aerospike.POLICY_KEY_SEND
            ("durable_delete", "durable"),  # should be bool
        ],
    )
    def test_invalid_apply_policy_types(self, key, value):
        subpolicy = {key: value}
        with pytest.raises(e.ParamError):
            aerospike.client({"hosts": [("localhost", 3000)], "policies": {"apply": subpolicy}})

    @pytest.mark.parametrize(
        "key, value",
        [
            ("key", "send"),  # should be an int eg. aerospike.POLICY_KEY_SEND
            ("durable_delete", "durable"),  # should be bool
            ("gen", "should be a constant integer"),  # should be int
            ("replica", "maybe?"),  # should be int
            ("commit_level", "committed"),  # should be int
        ],
    )
    def test_invalid_remove_policy_types(self, key, value):
        subpolicy = {key: value}
        with pytest.raises(e.ParamError):
            aerospike.client({"hosts": [("localhost", 3000)], "policies": {"remove": subpolicy}})

    def test_validate_keys_invalid_value(self):
        config = {
            "validate_keys": "True",
            "host": [("127.0.0.1", 3000)]
        }
        with pytest.raises(e.ParamError) as excinfo:
            aerospike.client(config)
        assert excinfo.value.msg == 'config[\"validate_keys\"] must be a boolean'

    @pytest.mark.parametrize(
        "invalid_config, is_policy",
        [
            ({INVALID_OPTION_KEY: 1}, False),
            ({"lua": {INVALID_OPTION_KEY: 1}}, False),
            ({"tls": {INVALID_OPTION_KEY: 1}}, False),
            ({"shm": {INVALID_OPTION_KEY: 1}}, False),
            ({"policies": {INVALID_OPTION_KEY: 1}}, False),
            # Generate client configs for all types of policies
            *[
                ({"policies": {policy_name: {INVALID_OPTION_KEY: 1}}}, True)
                for policy_name in
                [
                    "read",
                    "write",
                    "apply",
                    "remove",
                    "query",
                    "scan",
                    "operate",
                    "info",
                    "admin",
                    "batch_apply",
                    "batch_remove",
                    "batch_write",
                    "batch",
                    "batch_parent_write",
                    "txn_verify",
                    "txn_roll"
                ]
            ]
        ]
    )
    def test_validate_keys(self, invalid_config: dict, is_policy: bool):
        config = TestBaseClass.get_connection_config()
        config.update(invalid_config)
        config["validate_keys"] = True

        with pytest.raises(e.ParamError) as excinfo:
            aerospike.client(config)

        adjective = "policy" if is_policy else "client config"
        EXPECTED_INVALID_KEY_ERR_MSG = '\"{}\" is an invalid {} dictionary key'
        assert excinfo.value.msg == EXPECTED_INVALID_KEY_ERR_MSG.format(INVALID_OPTION_KEY, adjective)
