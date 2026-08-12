import pytest
from .conftest import TEST_NS, TEST_SET
from aerospike import exception as e
import aerospike


@pytest.mark.usefixtures("as_connection")
class TestInvalidMapKeys:
    @pytest.mark.parametrize(
        "invalid_map_key",
        [
            None,
            True,
            [1],
            {"a": 1},
            4.0,
            aerospike.GeoJSON({"type": "Point", "coordinates": [-122.096449, 37.421868]})
        ]
    )
    def test_passing_invalid_map_keys_raises_exc(self, invalid_map_key):
        KEY = (TEST_NS, TEST_SET, 1)
        invalid_map_in_server = {
            4.0: 1
        }
        # Python client checks for valid key types,
        # since C client doesn't raise a specific enough error in as_map_set
        with pytest.raises(e.ParamError):
            self.as_connection.put(KEY, bins={"map": invalid_map_in_server})
