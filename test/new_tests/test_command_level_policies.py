import pytest
from aerospike import exception as e
import aerospike
import time

from aerospike_helpers.batch import records as br
from .test_base_class import TestBaseClass
from aerospike_helpers.operations import operations
from .conftest import verify_record_ttl

SKIP_MSG = "read_touch_ttl_percent only supported on server 7.1 or higher"
KEY = ("test", "demo", 1)


@pytest.mark.usefixtures("as_connection")
class CommandLevelTTL:
    NEW_TTL = 3000
    POLICY = {"ttl": NEW_TTL}

    pytestmark = pytest.mark.parametrize(
        "kwargs_with_ttl",
        [
            {"meta": POLICY},
            {"policy": POLICY},
        ]
    )

    def test_write_policy(self, kwargs_with_ttl):
        self.as_connection.put(KEY, bins={"a": 1}, **kwargs_with_ttl)
        verify_record_ttl(self.client, KEY, expected_ttl=self.NEW_TTL)

    def test_operate_policy(self, kwargs_with_ttl):
        ops = [
            operations.write(bin_name="a", write_item=1)
        ]
        self.as_connection.operate(KEY, list=ops, **kwargs_with_ttl)
        verify_record_ttl(self.client, KEY, expected_ttl=self.NEW_TTL)

    def test_batch_write_policy(self):
        ops = [
            operations.write(bin_name="a", write_item=1)
        ]
        self.as_connection.batch_operate(keys=[KEY], ops=ops, policy_batch_write=self.POLICY)

        verify_record_ttl(self.client, KEY, expected_ttl=self.NEW_TTL)

    def test_scan_policy(self):
        ops = [
            operations.write(bin_name="a", write_item=1)
        ]
        scan = self.as_connection.scan("test", "demo")
        scan.add_ops(ops)
        scan.results(policy=self.POLICY)

        verify_record_ttl(self.client, KEY, expected_ttl=self.NEW_TTL)


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
