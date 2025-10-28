import pytest
from aerospike import exception as e
import time

from aerospike_helpers.operations import operations, bitwise_operations, map_operations, list_operations, hll_operations
from aerospike_helpers.batch import records as br
from .test_base_class import TestBaseClass
from contextlib import nullcontext
import aerospike
from typing import Callable

SKIP_MSG = "read_touch_ttl_percent only supported on server 7.1 or higher"
KEY = ("test", "demo", 1)
OPS_LIST = [
    operations.read(bin_name="a")
]
INVALID_POLICY = {"a": "key"}
BATCHRECORDS_WITH_INVALID_BATCH_READ_POLICY = br.BatchRecords(
    [
        br.Read(
            key=("test", "demo", 1),
            ops=[
                operations.read("count"),
            ],
            policy=INVALID_POLICY
        )
    ]
)
OPS_LIST_WITH_INVALID_BIT_POLICY = [
    bitwise_operations.bit_not("bin", bit_offset=0, bit_size=0, policy={"a": "key"})
]
OPS_LIST_WITH_INVALID_MAP_POLICY = [
    map_operations.map_put("bin", "map_key", "map_value", map_policy={"a": "key"})
]
OPS_LIST_WITH_INVALID_LIST_POLICY = [
    list_operations.list_append("bin_name", "list_item_value", policy={"a": "key"})
]
OPS_LIST_WITH_INVALID_HLL_POLICY = [
    hll_operations.hll_add("bin_name", values=[1, 2, 3], policy={"a": "key"})
]


class TestReadTouchTTLPercent:
    @pytest.fixture(autouse=True)
    def setup(self, as_connection):
        ttl = 2
        self.as_connection.put(KEY, bins={"a": 1}, meta={"ttl": ttl})
        self.policy = {
            "read_touch_ttl_percent": 50
        }
        self.invalid_policy = {
            "read_touch_ttl_percent": "1"
        }
        self.delay = ttl / 2 + 0.1

        yield

        # Some tests call the client remove API
        try:
            self.as_connection.remove(KEY)
        except e.RecordNotFound:
            pass

    def test_read_invalid(self):
        with pytest.raises(e.ParamError) as excinfo:
            self.as_connection.get(KEY, self.invalid_policy)
        assert excinfo.value.msg == "read_touch_ttl_percent is invalid"

    def test_operate_invalid(self):
        ops = [
            operations.read("a")
        ]
        with pytest.raises(e.ParamError) as excinfo:
            self.as_connection.operate(KEY, ops, policy=self.invalid_policy)
        assert excinfo.value.msg == "read_touch_ttl_percent is invalid"

    def test_batch_invalid(self):
        keys = [
            KEY
        ]
        with pytest.raises(e.ParamError) as excinfo:
            self.as_connection.batch_read(keys, policy=self.invalid_policy)
        assert excinfo.value.msg == "read_touch_ttl_percent is invalid"

    def test_get(self):
        if (TestBaseClass.major_ver, TestBaseClass.minor_ver) < (7, 1):
            pytest.skip(SKIP_MSG)
        time.sleep(self.delay)
        # By this time, the record's ttl should be less than 1 second left
        # Reset record TTL
        self.as_connection.get(KEY, policy=self.policy)
        time.sleep(self.delay)
        # Record should not have expired
        self.as_connection.get(KEY)

    def test_operate(self):
        if (TestBaseClass.major_ver, TestBaseClass.minor_ver) < (7, 1):
            pytest.skip(SKIP_MSG)
        time.sleep(self.delay)
        ops = [
            operations.read("a")
        ]
        self.as_connection.operate(KEY, ops, policy=self.policy)
        time.sleep(self.delay)
        self.as_connection.get(KEY)

    def test_batch(self):
        if (TestBaseClass.major_ver, TestBaseClass.minor_ver) < (7, 1):
            pytest.skip(SKIP_MSG)
        time.sleep(self.delay)
        keys = [
            KEY
        ]
        self.as_connection.batch_read(keys, policy=self.policy)
        time.sleep(self.delay)
        self.as_connection.get(KEY)

    def test_batch_write(self):
        if (TestBaseClass.major_ver, TestBaseClass.minor_ver) < (7, 1):
            pytest.skip(SKIP_MSG)
        batch_records = br.BatchRecords(
            [
                br.Read(
                    key=KEY,
                    ops=[
                        operations.read("a"),
                    ],
                    policy=self.policy
                )
            ]
        )
        time.sleep(self.delay)
        self.as_connection.batch_write(batch_records)
        time.sleep(self.delay)
        self.as_connection.get(KEY)

    # Test all policy code paths for invalid policy keys
    # This codepath is only for command (e.g transaction)-level policies. Config level policies
    # have a separate codepath.
    # Read and write command policies with invalid keys already covered by other test cases, so
    # we don't include test cases for them here.
    @pytest.mark.parametrize(
        "api_method, kwargs, context_if_validate_keys_is_false",
        [
            (aerospike.Client.operate, {"key": KEY, "list": OPS_LIST, "policy": INVALID_POLICY}, nullcontext()),
            # User doesn't exist
            (aerospike.Client.admin_query_user_info, {"user": "asdf", "policy": INVALID_POLICY}, pytest.raises((e.InvalidUser, e.SecurityNotSupported))),
            (aerospike.Client.info_all, {"command": "status", "policy": INVALID_POLICY}, nullcontext()),
            # UDF doesn't exist on server
            (aerospike.Client.apply, {"key": KEY, "module": "module", "function": "function", "args": [], "policy": INVALID_POLICY}, pytest.raises(e.UDFError)),
            (aerospike.Scan.results, {"policy": INVALID_POLICY}, nullcontext()),
            (aerospike.Query.results, {"policy": INVALID_POLICY}, nullcontext()),
            (aerospike.Client.remove, {"key": KEY, "policy": INVALID_POLICY}, nullcontext()),
            # Batch policy
            (aerospike.Client.batch_read, {"keys": [KEY], "policy": INVALID_POLICY}, nullcontext()),
            # Batch write policy
            (aerospike.Client.batch_operate, {"keys": [KEY], "ops": OPS_LIST, "policy_batch": INVALID_POLICY}, nullcontext()),
            # Batch read policy
            (aerospike.Client.batch_write, {"batch_records": BATCHRECORDS_WITH_INVALID_BATCH_READ_POLICY}, nullcontext()),
            # Batch apply policy
            (aerospike.Client.batch_apply, {"keys": [KEY], "module": "module", "function": "function", "args": [], "policy_batch_apply": INVALID_POLICY}, nullcontext()),
            # Batch remove policy
            (aerospike.Client.batch_remove, {"keys": [KEY], "policy_batch_remove": INVALID_POLICY}, nullcontext()),
            # If not validating keys, we don't really care what server error is reported
            # We only care that we are not validating keys if the feature flag is disabled
            # Bit policy
            (aerospike.Client.operate, {"key": KEY, "list": OPS_LIST_WITH_INVALID_BIT_POLICY}, pytest.raises(e.ServerError)),
            # The map and list operations create a new bin with a new list/map if it doesn't exist
            # Map policy
            (aerospike.Client.operate, {"key": KEY, "list": OPS_LIST_WITH_INVALID_MAP_POLICY}, nullcontext()),
            # List policy
            (aerospike.Client.operate, {"key": KEY, "list": OPS_LIST_WITH_INVALID_LIST_POLICY}, nullcontext()),
            # HLL policy
            (aerospike.Client.operate, {"key": KEY, "list": OPS_LIST_WITH_INVALID_HLL_POLICY}, pytest.raises(e.ServerError)),
        ]
    )
    def test_invalid_policy_keys(self, api_method: Callable, kwargs: dict, context_if_validate_keys_is_false):
        if self.config["validate_keys"]:
            context = pytest.raises(e.ParamError)
            EXPECTED_ERROR_MESSAGE = '\"a\" is an invalid policy dictionary key'
        else:
            context = context_if_validate_keys_is_false

        if api_method.__objclass__ == aerospike.Scan:
            invoker = self.as_connection.scan("test", "demo")
        elif api_method.__objclass__ == aerospike.Query:
            invoker = self.as_connection.query("test", "demo")
        else:
            invoker = self.as_connection

        with context as excinfo:
            api_method(invoker, **kwargs)

        if self.config["validate_keys"]:
            assert EXPECTED_ERROR_MESSAGE in excinfo.value.msg
