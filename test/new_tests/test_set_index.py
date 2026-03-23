import pytest
import aerospike
from .index_helpers import ensure_dropped_index


INDEX_NAME = "index_name"

@pytest.mark.usefixtures("as_connection")
class TestSetIndex:
    @pytest.fixture(autouse=True)
    def setup(self):
        yield
        ensure_dropped_index(self.as_connection, None, INDEX_NAME)

    @pytest.mark.parametrize(
        "index_create_method",
        [
            aerospike.Client.index_single_value_create,
            aerospike.Client.index_map_keys_create,
            aerospike.Client.index_map_values_create,
            aerospike.Client.index_list_create,
        ]
    )
    def test_create_set_index(self):
        # TODO: reuse code from other PR
        self.as_connection.index_create_method(None, "demo", "number", aerospike.INDEX_NUMERIC, INDEX_NAME)
