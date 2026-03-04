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
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter(action="always", category=DeprecationWarning)
            with pytest.raises(e.ParamError):
                index_create_method(self.as_connection, 1, "demo", "bin_name", "deprecated_index")
        assert len(w) == 1

    def test_index_cdt_create_raises_warning(self):
        with pytest.warns(DeprecationWarning):
            with pytest.raises(e.ParamError):
                self.as_connection.index_cdt_create("test", "demo", "bin", aerospike.INDEX_TYPE_DEFAULT, aerospike.INDEX_NUMERIC, 2)

    def test_neg_cdtindex_with_no_paramters(self):
        """
        Invoke index_cdt_create() without any mandatory parameters.
        """
        with pytest.warns(DeprecationWarning):
            with pytest.raises(TypeError) as typeError:
                self.as_connection.index_cdt_create()
            assert "argument 'ns' (pos 1)" in str(typeError.value)

    @pytest.mark.parametrize(
        "ctx",
        [
            None,
            # Invalid type
            {"ctx": 1}
        ]
    )
    def test_neg_cdtindex_with_invalid_ctx(self, ctx):
        with pytest.warns(DeprecationWarning):
            with pytest.raises(e.ParamError):
                self.as_connection.index_cdt_create(
                    "test",
                    "demo",
                    "string_list",
                    aerospike.INDEX_TYPE_LIST,
                    aerospike.INDEX_STRING,
                    "test_string_list_cdt_index",
                    # Ctx must be a list
                    ctx
                )
