import pytest
import aerospike
from aerospike import exception as e
import warnings

INDEX_NAME = "deprecated_index"

@pytest.mark.usefixtures("as_connection")
class TestDeprecatedIndexCreationMethods:
    @pytest.fixture(autouse=True)
    def setup(self):
        yield
        self.as_connection.index_remove("test", INDEX_NAME)

    @pytest.mark.parametrize(
        "index_create_method",
        [
            aerospike.Client.index_blob_create,
            aerospike.Client.index_integer_create,
            aerospike.Client.index_string_create,
            aerospike.Client.index_geo2dsphere_create,
        ]
    )
    def test_deprecated_index_creation_methods(self, index_create_method):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter(action="always", category=DeprecationWarning)
            with pytest.raises(e.ParamError):
                index_create_method(self.as_connection, 1, "demo", "bin_name", INDEX_NAME)
        assert len(w) == 1
