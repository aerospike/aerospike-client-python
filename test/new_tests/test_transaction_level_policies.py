import pytest
from aerospike import exception as e

from aerospike_helpers.operations import operations


class TestTransactionLevelPolicies:
    @pytest.fixture(autouse=True)
    def setup(self, as_connection):
        self.key = ("test", "demo", 1)
        self.invalid_policy = {
            "read_touch_ttl_percent": "1"
        }

        yield

    def test_read_invalid_read_touch_ttl_percent(self):
        with pytest.raises(e.ParamError) as excinfo:
            self.as_connection.get(self.key, self.invalid_policy)
        assert excinfo.value.msg == "read_touch_ttl_percent is invalid"

    def test_operate_invalid_read_touch_ttl_percent(self):
        ops = [
            operations.read("a")
        ]
        with pytest.raises(e.ParamError) as excinfo:
            self.as_connection.operate(self.key, ops, policy=self.invalid_policy)
        assert excinfo.value.msg == "read_touch_ttl_percent is invalid"

    def test_batch_invalid_read_touch_ttl_percent(self):
        keys = [
            self.key
        ]
        with pytest.raises(e.ParamError) as excinfo:
            self.as_connection.batch_read(keys, policy=self.invalid_policy)
        assert excinfo.value.msg == "read_touch_ttl_percent is invalid"
