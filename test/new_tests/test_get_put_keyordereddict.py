from aerospike import KeyOrderedDict
import pytest
from aerospike import exception as e


class TestGetPutOrderedDict:
    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        yield
        try:
            self.as_connection.remove(self.key)
        except e.AerospikeError:
            pass

    def test_get_put_keyordereddict(self):
        bins = {
            "dict": KeyOrderedDict({"f": 6, "e": 5, "d": 4})
        }
        self.key = ("test", "demo", 1)
        self.as_connection.put(self.key, bins)

        _, _, res = self.as_connection.get(self.key)
        assert res["dict"] == KeyOrderedDict({"f": 6, "e": 5, "d": 4})
        assert type(res["dict"]) == KeyOrderedDict
