import pytest
import aerospike
from aerospike import exception as e
import warnings


@pytest.mark.usefixtures("as_connection")
class TestDeprecatedIndexCreationMethods:
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
        with pytest.warns(DeprecationWarning):
            with pytest.raises(e.ParamError):
                index_create_method(self.as_connection, 1, "demo", "bin_name", "deprecated_index")
