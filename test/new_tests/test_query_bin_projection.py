import pytest
from .conftest import TEST_NS, TEST_SET, BIN_NAME, READ_OPS
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

    # TODO: Missing test cases for path expression projection

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
    def test_add_write_ops_to_foreground_query(self, query, api_method, args):
        ops = [
            operations.write(BIN_NAME, 3)
        ]
        query.add_ops(ops)
        with pytest.raises(e.ParamError):
            api_method(query, *args)

    def test_select_bins_and_then_bin_projection(self, query):
        query.select(BIN_NAME)
        with pytest.raises(e.ParamError):
            query.add_ops(READ_OPS)

    def test_bin_projection_and_then_select_bins(self, query):
        query.add_ops(READ_OPS)
        with pytest.raises(e.ParamError):
            query.select(BIN_NAME)
