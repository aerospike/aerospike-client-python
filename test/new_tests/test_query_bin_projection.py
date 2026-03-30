import pytest
from .conftest import TEST_NS, TEST_SET, BIN_NAME, READ_OPS, READ_AND_WRITE_OPS, NON_EXISTENT_BIN_NAME, WRITE_OPS
from aerospike_helpers.operations import operations
from aerospike import Query
from aerospike import exception as e


class TestQueryBinProjection:
    @pytest.fixture(autouse=True)
    def query(self, clean_test_background):
        query = self.as_connection.query(TEST_NS, TEST_SET)
        yield query

    def test_query_foreach(self, query):
        bin_values = set()
        def callback(record):
            bin_values.add(record[2][BIN_NAME])

        query.add_ops(READ_OPS)
        query.foreach(callback)
        for i in range(len(bin_values)):
            assert i in bin_values

    # TODO: scale down tests maybe
    def test_query_results(self, query):
        query.add_ops(READ_OPS)
        # TODO: records are not necessarily in order
        records = query.results()
        for record in records:
            assert type(record[2][BIN_NAME]) == int

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

        # The only bin should still be returned
        for _, _, bins in records:
            assert BIN_NAME in bins

    def test_add_ops_then_select_bins_then_foreground_query(self, query):
        query.add_ops(READ_OPS)
        # Filter out the only bin in the record
        with pytest.warns(DeprecationWarning):
            query.select(NON_EXISTENT_BIN_NAME)
        records = query.results()

        # The only bin should still be returned
        for _, _, bins in records:
            assert BIN_NAME in bins
