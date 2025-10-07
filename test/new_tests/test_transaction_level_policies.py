import pytest
from aerospike import exception as e
import time

from aerospike_helpers.operations import operations, bitwise_operations, map_operations, list_operations, hll_operations
from aerospike_helpers.batch import records as br
from .test_base_class import TestBaseClass

SKIP_MSG = "read_touch_ttl_percent only supported on server 7.1 or higher"


class TestReadTouchTTLPercent:
    @pytest.fixture(autouse=True)
    def setup(self, as_connection):
        self.key = ("test", "demo", 1)
        ttl = 2
        self.as_connection.put(self.key, bins={"a": 1}, meta={"ttl": ttl})
        self.policy = {
            "read_touch_ttl_percent": 50
        }
        self.invalid_policy = {
            "read_touch_ttl_percent": "1"
        }
        self.delay = ttl / 2 + 0.1

        yield

        self.as_connection.remove(self.key)

    def test_read_invalid(self):
        with pytest.raises(e.ParamError) as excinfo:
            self.as_connection.get(self.key, self.invalid_policy)
        assert excinfo.value.msg == "read_touch_ttl_percent is invalid"

    def test_operate_invalid(self):
        ops = [
            operations.read("a")
        ]
        with pytest.raises(e.ParamError) as excinfo:
            self.as_connection.operate(self.key, ops, policy=self.invalid_policy)
        assert excinfo.value.msg == "read_touch_ttl_percent is invalid"

    def test_batch_invalid(self):
        keys = [
            self.key
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
        self.as_connection.get(self.key, policy=self.policy)
        time.sleep(self.delay)
        # Record should not have expired
        self.as_connection.get(self.key)

    def test_operate(self):
        if (TestBaseClass.major_ver, TestBaseClass.minor_ver) < (7, 1):
            pytest.skip(SKIP_MSG)
        time.sleep(self.delay)
        ops = [
            operations.read("a")
        ]
        self.as_connection.operate(self.key, ops, policy=self.policy)
        time.sleep(self.delay)
        self.as_connection.get(self.key)

    def test_batch(self):
        if (TestBaseClass.major_ver, TestBaseClass.minor_ver) < (7, 1):
            pytest.skip(SKIP_MSG)
        time.sleep(self.delay)
        keys = [
            self.key
        ]
        self.as_connection.batch_read(keys, policy=self.policy)
        time.sleep(self.delay)
        self.as_connection.get(self.key)

    def test_batch_write(self):
        if (TestBaseClass.major_ver, TestBaseClass.minor_ver) < (7, 1):
            pytest.skip(SKIP_MSG)
        batch_records = br.BatchRecords(
            [
                br.Read(
                    key=self.key,
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
        self.as_connection.get(self.key)

    # Test all policy code paths for invalid policy keys
    # This codepath is only for command (e.g transaction)-level policies. Config level policies
    # have a separate codepath.
    # Read and write command policies with invalid keys already covered by other test cases, so
    # we don't include test cases for them here.
    def test_invalid_policy_keys(self):
        EXPECTED_ERROR_MESSAGE = '\"a\" is an invalid policy dictionary key'

        # Operate policy
        ops = [
            operations.read(bin_name="a")
        ]
        with pytest.raises(e.ParamError) as excinfo:
            self.as_connection.operate(self.key, list=ops, policy={"a": "key"})
        assert excinfo.value.msg == EXPECTED_ERROR_MESSAGE

        with pytest.raises(e.ParamError) as excinfo:
            self.as_connection.admin_query_user_info(user="asdf", policy={"a": "key"})
        assert excinfo.value.msg == EXPECTED_ERROR_MESSAGE

        # Info policy
        with pytest.raises(e.ParamError) as excinfo:
            self.as_connection.info_all(command="status", policy={"a": "key"})
        assert excinfo.value.msg == EXPECTED_ERROR_MESSAGE

        # Apply policy
        with pytest.raises(e.ParamError) as excinfo:
            self.as_connection.apply(self.key, "module", "function", args=[], policy={"a": "key"})
        assert excinfo.value.msg == EXPECTED_ERROR_MESSAGE

        # Scan policy
        scan = self.as_connection.scan("test", "demo")
        with pytest.raises(e.ParamError) as excinfo:
            scan.results(policy={"a": "key"})
        assert excinfo.value.msg == EXPECTED_ERROR_MESSAGE

        # Query policy
        query = self.as_connection.query("test", "demo")
        with pytest.raises(e.ParamError) as excinfo:
            query.results(policy={"a": "key"})
        assert excinfo.value.msg == EXPECTED_ERROR_MESSAGE

        # Remove policy
        with pytest.raises(e.ParamError) as excinfo:
            self.as_connection.remove(self.key, policy={"a": "key"})
        assert excinfo.value.msg == EXPECTED_ERROR_MESSAGE

        # Batch policy
        with pytest.raises(e.ParamError) as excinfo:
            self.as_connection.batch_read(keys=[self.key], policy={"a": "key"})
        assert excinfo.value.msg == EXPECTED_ERROR_MESSAGE

        # Batch write policy
        with pytest.raises(e.ParamError) as excinfo:
            self.as_connection.batch_operate(keys=[self.key], ops=ops, policy_batch_write={"a": "key"})
        assert excinfo.value.msg == EXPECTED_ERROR_MESSAGE

        # Batch read policy
        batch_records = br.BatchRecords(
            [
                br.Read(
                    key=("test", "demo", 1),
                    ops=[
                        operations.read("count"),
                    ],
                    policy={
                        "a": "key"
                    }
                )
            ]
        )
        with pytest.raises(e.ParamError) as excinfo:
            self.as_connection.batch_write(batch_records)
        assert excinfo.value.msg == EXPECTED_ERROR_MESSAGE

        # Batch apply policy
        with pytest.raises(e.ParamError) as excinfo:
            self.as_connection.batch_apply(keys=[self.key], module="module", function="func", args=[], policy_batch_apply={"a": "key"})
        assert excinfo.value.msg == EXPECTED_ERROR_MESSAGE

        # Batch remove policy
        with pytest.raises(e.ParamError) as excinfo:
            self.as_connection.batch_remove(keys=[self.key], policy_batch_remove={"a": "key"})
        assert excinfo.value.msg == EXPECTED_ERROR_MESSAGE

        # Bit policy
        ops = [
            bitwise_operations.bit_not("bin", bit_offset=0, bit_size=0, policy={"a": "key"})
        ]
        with pytest.raises(e.ParamError) as excinfo:
            self.as_connection.operate(self.key, list=ops)
        assert excinfo.value.msg == EXPECTED_ERROR_MESSAGE

        # Map policy
        ops = [
            map_operations.map_put("bin", "map_key", "map_value", map_policy={"a": "key"})
        ]
        with pytest.raises(e.ParamError) as excinfo:
            self.as_connection.operate(self.key, list=ops)
        assert excinfo.value.msg == EXPECTED_ERROR_MESSAGE

        # List policy
        ops = [
            list_operations.list_append("bin_name", "list_item_value", policy={"a": "key"})
        ]
        with pytest.raises(e.ParamError) as excinfo:
            self.as_connection.operate(self.key, list=ops)
        assert excinfo.value.msg == EXPECTED_ERROR_MESSAGE

        # HLL policy
        ops = [
            hll_operations.hll_add("bin_name", values=[1, 2, 3], policy={"a": "key"})
        ]
        with pytest.raises(e.ParamError) as excinfo:
            self.as_connection.operate(self.key, list=ops)
        assert excinfo.value.msg == EXPECTED_ERROR_MESSAGE
