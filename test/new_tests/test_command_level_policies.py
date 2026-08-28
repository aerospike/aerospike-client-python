import pytest
from aerospike import exception as e
import aerospike
import time

from aerospike_helpers.batch import records as br
from .test_base_class import TestBaseClass
from aerospike_helpers.operations import operations
from .conftest import verify_record_ttl, TEST_NS, TEST_SET

SKIP_MSG = "read_touch_ttl_percent only supported on server 7.1 or higher"
KEY = (TEST_NS, TEST_SET, 1)


@pytest.mark.usefixtures("as_connection")
class CommandLevelTTL:
    NEW_TTL = 3000
    POLICY = {"ttl": NEW_TTL}

    meta_and_policy_params = pytest.mark.parametrize(
        "kwargs_with_ttl",
        [
            {"meta": POLICY},
            {"policy": POLICY},
        ]
    )

    @meta_and_policy_params
    def test_write_policy(self, kwargs_with_ttl):
        self.as_connection.put(KEY, bins={"a": 1}, **kwargs_with_ttl)
        verify_record_ttl(self.client, KEY, expected_ttl=self.NEW_TTL)

    @meta_and_policy_params
    def test_operate_policy(self, kwargs_with_ttl):
        ops = [
            operations.write(bin_name="a", write_item=1)
        ]
        self.as_connection.operate(KEY, list=ops, **kwargs_with_ttl)
        verify_record_ttl(self.client, KEY, expected_ttl=self.NEW_TTL)

    OPS = [
        operations.write(bin_name="a", write_item=1)
    ]

    def test_batch_operate(self):
        ops = [
            operations.write(bin_name="a", write_item=1)
        ]
        self.as_connection.batch_operate(keys=[KEY], ops=self.OPS, policy_batch_write=self.POLICY)

        verify_record_ttl(self.client, KEY, expected_ttl=self.NEW_TTL)

    # Don't bother testing for DeprecationWarnings here since running Python with -W error flag can
    # cause ClientError to be raised. It's too complicated to check both cases
    @pytest.mark.filterwarnings("ignore::DeprecationWarning")
    @meta_and_policy_params
    def test_batch_write(self, kwargs_with_ttl):
        batch_records = br.BatchRecords([
            br.Write(KEY, ops=self.OPS, **kwargs_with_ttl)
        ])
        try:
            self.as_connection.batch_write(batch_records)
        except e.ClientError as exc:
            # ClientError can be raised if the user runs Python with warnings treated as errors.
            assert exc.msg == "meta[\"ttl\"] is deprecated and will be removed in the next client major release"

        verify_record_ttl(self.client, KEY, expected_ttl=self.NEW_TTL)

    # This test case is more important when warnings are converted into errors
    @pytest.mark.filterwarnings("ignore::DeprecationWarning")
    def test_batch_write_with_read_br_raises_deprecation_warning(self):
        batch_records = br.BatchRecords([
            br.Read(KEY, meta={"ttl": 100})
        ])
        try:
            self.client.batch_write(batch_records)
        except e.ClientError as exc:
            assert exc.msg == "meta[\"ttl\"] is deprecated and will be removed in the next client major release"

    def test_scan_policy(self):
        ops = [
            operations.write(bin_name="a", write_item=1)
        ]
        scan = self.as_connection.scan("test", "demo")
        scan.add_ops(ops)
        scan.results(policy=self.POLICY)

        verify_record_ttl(self.client, KEY, expected_ttl=self.NEW_TTL)


TTL = 2

@pytest.mark.parametrize(
    "insert_records",
    [{"record_count": 1, "make_set_unique": False, "batch_write_command_policy": {"ttl": TTL}}],
    indirect=True
)
@pytest.mark.usefixtures("insert_records")
class TestReadTouchTTLPercent:
    @pytest.fixture(autouse=True)
    def setup(self, as_connection):
        self.policy = {
            "read_touch_ttl_percent": 50
        }
        self.invalid_policy = {
            "read_touch_ttl_percent": "1"
        }
        self.delay = TTL / 2 + 0.1

        yield

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
        self.as_connection.get(self.keys[0], policy=self.policy)
        time.sleep(self.delay)
        # Record should not have expired
        self.as_connection.get(self.keys[0])

    def test_operate(self):
        if (TestBaseClass.major_ver, TestBaseClass.minor_ver) < (7, 1):
            pytest.skip(SKIP_MSG)
        time.sleep(self.delay)
        ops = [
            operations.read("a")
        ]
        self.as_connection.operate(self.keys[0], ops, policy=self.policy)
        time.sleep(self.delay)
        self.as_connection.get(self.keys[0])

    def test_batch(self):
        if (TestBaseClass.major_ver, TestBaseClass.minor_ver) < (7, 1):
            pytest.skip(SKIP_MSG)
        time.sleep(self.delay)
        self.as_connection.batch_read(self.keys, policy=self.policy)
        time.sleep(self.delay)
        self.as_connection.get(self.keys[0])

    def test_batch_write(self):
        if (TestBaseClass.major_ver, TestBaseClass.minor_ver) < (7, 1):
            pytest.skip(SKIP_MSG)
        batch_records = br.BatchRecords(
            [
                br.Read(
                    key=self.keys[0],
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
        self.as_connection.get(self.keys[0])
