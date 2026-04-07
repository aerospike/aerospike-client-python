import pytest
from .conftest import BIN_NAME, BASIC_READ_BIN_OPS, READ_AND_WRITE_OPS, NON_EXISTENT_BIN_NAME, WRITE_OPS, MAP_BIN_NAME, expected_number_bin_values
import aerospike
from aerospike_helpers.operations import map_operations
from aerospike import Query
from aerospike import exception as e
from .test_base_class import TestBaseClass
from contextlib import nullcontext


class TestQueryBinProjection:
    pytestmark = [
        pytest.mark.parametrize(
            "query",
            [
                aerospike.Client.scan,
                aerospike.Client.query
            ],
            indirect=True
        )
    ]

    def test_query_foreach(self, query):
        bin_values = set()
        def callback(record):
            bin_values.add(record[2][BIN_NAME])

        query.add_ops(BASIC_READ_BIN_OPS)
        query.foreach(callback)

        assert bin_values == expected_number_bin_values

    def test_query_results(self, query):
        query.add_ops(BASIC_READ_BIN_OPS)
        records = query.results()

        bin_values = [record[2][BIN_NAME] for record in records]
        assert len(bin_values) == len(set(bin_values)) and set(bin_values) == expected_number_bin_values

    MAP_GET_BY_KEY_OP = [
        map_operations.map_get_by_key(MAP_BIN_NAME, "a", aerospike.MAP_RETURN_VALUE)
    ]

    @pytest.fixture()
    def client_should_fail_if_server_version_less_than_8_1_2(self, as_connection, request):
        # For bin projection, the server can convert complex (e.g map) read operations into a regular bin read
        # Since the server doesn't fail, the client has to check the server version and raise an error on its end.
        if (TestBaseClass.major_ver, TestBaseClass.minor_ver, TestBaseClass.patch_ver) >= (8, 1, 2):
            request.cls.expected_context_for_pos_tests = nullcontext()
        else:
            # InvalidRequest, BinIncompatibleTypes are exceptions that have been raised
            request.cls.expected_context_for_pos_tests = pytest.raises(e.ParamError)

    def test_query_nested_results(self, query, client_should_fail_if_server_version_less_than_8_1_2):
        query.add_ops(self.MAP_GET_BY_KEY_OP)
        with self.expected_context_for_pos_tests:
            records = query.results()

            bin_values = [record[2][MAP_BIN_NAME] for record in records]
            assert len(bin_values) == len(set(bin_values)) and set(bin_values) == expected_number_bin_values

    # Negative tests

    def noop_callback(record):
        pass

    @pytest.mark.parametrize(
        "api_method, args",
        [
            ("results", []),
            ("foreach", [noop_callback])
        ]
    )
    @pytest.mark.parametrize(
        "ops",
        [
            READ_AND_WRITE_OPS,
            WRITE_OPS
        ]
    )
    def test_add_write_ops_in_foreground_query(self, query, api_method, args, ops):
        query.add_ops(ops)
        with pytest.raises(e.ParamError):
            getattr(query, api_method)(*args)

    def test_select_bins_then_add_ops_then_foreground_query(self, query):
        # Filter out the only bin in the record
        query.select(NON_EXISTENT_BIN_NAME)
        with pytest.warns(DeprecationWarning):
            query.add_ops(BASIC_READ_BIN_OPS)

        with self.expected_context_for_pos_tests:
            records = query.results()

            # The "filtered out" bin should still be returned
            for _, _, bins in records:
                assert BIN_NAME in bins

    def test_add_ops_then_select_bins_then_foreground_query(self, query):
        query.add_ops(BASIC_READ_BIN_OPS)
        # Filter out the only bin in the record
        with pytest.warns(DeprecationWarning):
            query.select(NON_EXISTENT_BIN_NAME)

        with self.expected_context_for_pos_tests:
            records = query.results()

            # The "filtered out" bin should still be returned
            for _, _, bins in records:
                assert BIN_NAME in bins
