import pytest
from .conftest import BIN_NAME, READ_OPS, READ_AND_WRITE_OPS, NON_EXISTENT_BIN_NAME, WRITE_OPS, query, MAP_BIN_NAME, expected_number_bin_values
import aerospike
from aerospike_helpers.operations import map_operations
from aerospike import Query
from aerospike import exception as e


class TestQueryBinProjection:
    def test_query_foreach(self, query):
        bin_values = set()
        def callback(record):
            bin_values.add(record[2][BIN_NAME])

        query.add_ops(READ_OPS)
        query.foreach(callback)
        assert bin_values == expected_number_bin_values

    def test_query_results(self, query):
        query.add_ops(READ_OPS)
        records = query.results()
        bin_values = [record[2][BIN_NAME] for record in records]
        assert len(bin_values) == len(set(bin_values)) and set(bin_values) == expected_number_bin_values

    NESTED_READ_OP = [
        map_operations.map_get_by_key(MAP_BIN_NAME, "a", aerospike.MAP_RETURN_VALUE)
    ]

    def test_query_nested_results(self, query):
        query.add_ops(self.NESTED_READ_OP)
        records = query.results()
        bin_values = [record[2][MAP_BIN_NAME] for record in records]
        assert len(bin_values) == len(set(bin_values)) and set(bin_values) == expected_number_bin_values

    # Negative tests

    def noop_callback(record):
        pass

    @pytest.mark.parametrize(
        "api_method, args",
        [
            (Query.results, []),
            (Query.foreach, [noop_callback])
        ]
    )
    @pytest.mark.parametrize(
        "ops",
        [
            READ_AND_WRITE_OPS,
            WRITE_OPS
        ]
    )
    def test_add_write_ops(self, query, api_method, args, ops):
        query.add_ops(ops)
        with pytest.raises(e.ParamError):
            api_method(query, *args)

    def test_select_bins_then_add_ops_then_foreground_query(self, query):
        # Filter out the only bin in the record
        query.select(NON_EXISTENT_BIN_NAME)
        with pytest.warns(DeprecationWarning):
            query.add_ops(READ_OPS)
        records = query.results()

        # The "filtered out" bin should still be returned
        for _, _, bins in records:
            assert BIN_NAME in bins

    def test_add_ops_then_select_bins_then_foreground_query(self, query):
        query.add_ops(READ_OPS)
        # Filter out the only bin in the record
        with pytest.warns(DeprecationWarning):
            query.select(NON_EXISTENT_BIN_NAME)
        records = query.results()

        # The "filtered out" bin should still be returned
        for _, _, bins in records:
            assert BIN_NAME in bins
