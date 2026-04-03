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

    def test_create_set_index(self):
        self.as_connection.index_set_create("test", "demo", INDEX_NAME)
