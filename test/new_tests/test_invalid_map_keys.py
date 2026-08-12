import pytest
from .conftest import TEST_NS, TEST_SET
from aerospike import exception as e


@pytest.mark.usefixtures("as_connection")
class TestInvalidMapKeys:
    def test_passing_invalid_map_keys_raises_exc(self):
        KEY = (TEST_NS, TEST_SET, 1)
        invalid_map_in_server = {
            4.0: 1
        }
        # TODO: should raise ParamError
        # Python client can check if valid key, since C client doesn't raise a specific enough error in as_map_set
        with pytest.raises(e.ClientError):
            self.as_connection.put(KEY, bins={"map": invalid_map_in_server})
