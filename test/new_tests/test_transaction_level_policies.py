import pytest
from aerospike import exception as e
import time

from aerospike_helpers.operations import operations
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

    def test_invalid_policy_keys(self):
        with pytest.raises(e.ParamError) as excinfo:
            self.as_connection.get(self.key, policy={"invalid": "key"})
        assert excinfo.value.msg == ""
