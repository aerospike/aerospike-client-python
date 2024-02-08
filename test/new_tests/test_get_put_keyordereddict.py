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

    @pytest.mark.parametrize(
        "bin_value",
        [
            KeyOrderedDict({"f": 6, "e": 5, "d": 4}),
            [
                KeyOrderedDict({"f": 6, "e": 5, "d": 4})
            ]
        ],
        ids=[
            "bin-level",
            "nested"
        ]
    )
    def test_get_put_keyordereddict(self, bin_value):
        bins = {
            "dict": bin_value
        }
        self.key = ("test", "demo", 1)
        self.as_connection.put(self.key, bins)

        _, _, res = self.as_connection.get(self.key)

        assert res["dict"] == bin_value
        if type(res["dict"]) == list:
            assert type(res["dict"][0]) == KeyOrderedDict
        else:
            assert type(res["dict"]) == KeyOrderedDict
