import pytest
from conftest import TEST_NS, TEST_SET, BIN_NAME, clean_test_background
from aerospike_helpers.operations import operations
from aerospike import Query
from aerospike import exception as e


@pytest.mark.usefixtures("as_connection")
class TestQueryBinProjection:
    @pytest.fixture(autouse=True)
    def query(self, clean_test_background):
        query = self.as_connection.query(TEST_NS, TEST_SET)
        yield query

    READ_OPS = [
        operations.read()
    ]

    def test_query_foreach(self, query):
        records = []
        def callback(record):
            records.append(record)

        query.add_ops(self.READ_OPS)
        records = query.foreach(callback)
        for i, record in enumerate(records):
            assert record[2][BIN_NAME] == i

    def test_query_results(self, query):
        query.add_ops(self.READ_OPS)
        records = query.results()
        for i, record in enumerate(records):
            assert record[2][BIN_NAME] == i

    # TODO: Missing test cases for path expression projection

    # Negative tests

    def noop_callback(record):
        pass

    @pytest.mark.parametrize(
        "api_method, args",
        [
            Query.results, [],
            Query.foreach, [noop_callback]
        ]
    )
    def test_add_write_ops_to_foreground_query(self, query, api_method, args):
        ops = [
            operations.write(BIN_NAME, 3)
        ]
        query.add_ops(ops)
        with pytest.raises(e.ParamError):
            api_method(query, *args)

    def test_execute_background(self, query):
        query.add_ops(self.READ_OPS)

        with pytest.raises(e.ParamError):
            query.execute_background(self.READ_OPS)

    def test_select_bins_and_then_bin_projection(self, query):
        query.select(BIN_NAME)
        with pytest.raises(e.ParamError):
            query.add_ops(self.READ_OPS)

    def test_bin_projection_and_then_select_bins(self, query):
        query.add_ops(self.READ_OPS)
        with pytest.raises(e.ParamError):
            query.select(BIN_NAME)
