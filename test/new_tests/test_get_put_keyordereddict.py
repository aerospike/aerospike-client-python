from aerospike import KeyOrderedDict
import pytest


class TestGetPutOrderedDict:
    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        pass

    def test_get_put_keyordereddict(self):
        bins = {
            "dict": KeyOrderedDict({"f": 6, "e": 5, "d": 4})
        }
        key = ("test", "demo", 1)
        self.as_connection.put(key, bins)

        _, _, res = self.as_connection.get(key)
        assert res["dict"] == KeyOrderedDict({"f": 6, "e": 5, "d": 4})
        assert type(res["dict"]) == KeyOrderedDict
