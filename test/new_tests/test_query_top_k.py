# -*- coding: utf-8 -*-
import pytest

import aerospike
from aerospike import exception as e
from aerospike_helpers.operations import operations

# Every case here is rejected before the query is sent, so the server does not
# need Top-K support. A connected client is still required to build a Query.

BIN = "score"


def noop_callback(record):
    pass


@pytest.mark.usefixtures("as_connection")
class TestQueryTopKValidation:
    @pytest.fixture
    def query(self):
        return self.as_connection.query("test", "demo")

    @pytest.mark.parametrize(
        "bin_name",
        [
            "",
            "a" * 16,
            "a\0b",
        ],
    )
    def test_order_by_invalid_bin_name(self, query, bin_name):
        with pytest.raises(e.ParamError) as excinfo:
            query.order_by(bin_name, aerospike.QueryOrderByType.INTEGER, aerospike.QueryOrder.ASCENDING)
        assert excinfo.value.msg == "Top-K order-by bin name must be 1-15 UTF-8 bytes without NUL"

    def test_order_by_bin_name_counts_utf8_bytes(self, query):
        # 8 two-byte characters is 16 UTF-8 bytes.
        with pytest.raises(e.ParamError):
            query.order_by("é" * 8, aerospike.QueryOrderByType.INTEGER, aerospike.QueryOrder.ASCENDING)

    def test_order_by_missing_type(self, query):
        with pytest.raises(e.ParamError) as excinfo:
            query.order_by(BIN)
        assert excinfo.value.msg == (
            "orderBy requires a type declaration (INTEGER, DOUBLE, STRING, or BYTES); Aerospike has no schema"
        )

    def test_order_by_invalid_type(self, query):
        with pytest.raises(e.ParamError) as excinfo:
            query.order_by(BIN, 9, aerospike.QueryOrder.ASCENDING)
        assert excinfo.value.msg == "orderBy type must be one of INTEGER, DOUBLE, STRING, BYTES; got 9"

    @pytest.mark.parametrize(
        "kwargs",
        [
            {},
            {"direction": 2},
        ],
    )
    def test_order_by_missing_or_invalid_direction(self, query, kwargs):
        with pytest.raises(e.ParamError) as excinfo:
            query.order_by(BIN, aerospike.QueryOrderByType.INTEGER, **kwargs)
        assert excinfo.value.msg == "orderBy direction must be ASC or DESC"

    @pytest.mark.parametrize(
        "bin_type",
        [
            aerospike.QueryOrderByType.INTEGER,
            aerospike.QueryOrderByType.DOUBLE,
            aerospike.QueryOrderByType.BYTES,
        ],
    )
    def test_order_by_flags_require_string(self, query, bin_type):
        with pytest.raises(e.ParamError) as excinfo:
            query.order_by(BIN, bin_type, aerospike.QueryOrder.ASCENDING, aerospike.QueryOrderByFlags.CASE_INSENSITIVE)
        assert excinfo.value.msg == "orderBy flags are only valid with type STRING"

    def test_order_by_string_flags_accepted(self, query):
        returned = query.order_by(
            BIN, aerospike.QueryOrderByType.STRING, aerospike.QueryOrder.DESCENDING, aerospike.QueryOrderByFlags.CASE_INSENSITIVE
        )
        assert returned is query

    def test_order_by_again_keeps_top_k(self, query):
        query.top_k = 10
        query.order_by(BIN, aerospike.QueryOrderByType.INTEGER, aerospike.QueryOrder.ASCENDING)
        query.order_by("other", aerospike.QueryOrderByType.DOUBLE, aerospike.QueryOrder.DESCENDING)
        assert query.top_k == 10

    @pytest.mark.parametrize("api_method, args", [("results", []), ("foreach", [noop_callback])])
    def test_top_k_out_of_range(self, query, api_method, args):
        query.order_by(BIN, aerospike.QueryOrderByType.INTEGER, aerospike.QueryOrder.ASCENDING)
        query.top_k = 1001
        with pytest.raises(e.ParamError) as excinfo:
            getattr(query, api_method)(*args)
        assert excinfo.value.msg == "topK must be in [1, 1000]; got 1001"

    def test_order_by_without_top_k_is_left_to_the_server(self, query):
        query.order_by(BIN, aerospike.QueryOrderByType.INTEGER, aerospike.QueryOrder.ASCENDING)
        with pytest.raises(e.ParamError) as excinfo:
            query.results()
        assert not excinfo.value.msg.startswith("topK must be in")

    @pytest.mark.parametrize("api_method, args", [("results", []), ("foreach", [noop_callback])])
    def test_top_k_with_max_records(self, query, api_method, args):
        query.order_by(BIN, aerospike.QueryOrderByType.INTEGER, aerospike.QueryOrder.ASCENDING)
        query.top_k = 10
        query.max_records = 100
        with pytest.raises(e.ParamError) as excinfo:
            getattr(query, api_method)(*args)
        assert excinfo.value.msg == "Top-K is incompatible with maxRecords"

    def test_top_k_on_background_query(self, query):
        query.add_ops([operations.write("bin", 1)])
        query.order_by(BIN, aerospike.QueryOrderByType.INTEGER, aerospike.QueryOrder.ASCENDING)
        query.top_k = 10
        with pytest.raises(e.ParamError) as excinfo:
            query.execute_background()
        assert excinfo.value.msg == (
            "orderBy/topK is only valid on foreground (SELECT-style) queries; "
            "background (UPDATE-style) queries do not produce a result stream"
        )

    def test_top_k_with_aggregate_udf(self, query):
        query.apply("stream_example", "count")
        query.order_by(BIN, aerospike.QueryOrderByType.INTEGER, aerospike.QueryOrder.ASCENDING)
        query.top_k = 10
        with pytest.raises(e.ParamError) as excinfo:
            query.results()
        assert excinfo.value.msg == "orderBy is incompatible with aggregate UDFs; use the UDF's own reduce stage"

    @pytest.mark.parametrize(
        "partition_filter",
        [
            {"begin": 0, "count": 1, "digest": {"init": 1, "value": "a" * 20}},
            {
                "begin": 0,
                "count": 1,
                "partition_status": {
                    "done": False,
                    "retry": True,
                    0: (0, True, False, bytearray(20), 0),
                },
            },
            {
                "begin": 0,
                "count": 1,
                "partition_status": {
                    "done": False,
                    "retry": True,
                    0: (0, False, False, bytearray(20), 42),
                },
            },
        ],
    )
    def test_top_k_with_resumed_partition_filter(self, query, partition_filter):
        query.order_by(BIN, aerospike.QueryOrderByType.INTEGER, aerospike.QueryOrder.ASCENDING)
        query.top_k = 10
        with pytest.raises(e.ParamError) as excinfo:
            query.results({"partition_filter": partition_filter})
        assert excinfo.value.msg == "Top-K can't resume a partition filter from an earlier query"

    def test_order_by_bin_not_in_select_projection(self, query):
        query.select("report", "location")
        query.order_by("occurred", aerospike.QueryOrderByType.INTEGER, aerospike.QueryOrder.DESCENDING)
        query.top_k = 20
        with pytest.raises(e.ParamError) as excinfo:
            query.results()
        assert excinfo.value.msg == (
            "orderBy bin 'occurred' is not in projection; add it to setOperations or remove projection"
        )

    def test_order_by_bin_not_in_ops_projection(self, query):
        query.add_ops([operations.read("report")])
        query.order_by("occurred", aerospike.QueryOrderByType.INTEGER, aerospike.QueryOrder.DESCENDING)
        query.top_k = 20
        with pytest.raises(e.ParamError) as excinfo:
            query.results()
        assert excinfo.value.msg == (
            "orderBy bin 'occurred' is not in projection; add it to setOperations or remove projection"
        )

    def test_order_by_bin_produced_by_more_than_one_op(self, query):
        query.add_ops([operations.read("occurred"), operations.read("occurred")])
        query.order_by("occurred", aerospike.QueryOrderByType.INTEGER, aerospike.QueryOrder.DESCENDING)
        query.top_k = 20
        with pytest.raises(e.ParamError) as excinfo:
            query.results()
        assert excinfo.value.msg == "orderBy bin 'occurred' is produced by more than one operation"
