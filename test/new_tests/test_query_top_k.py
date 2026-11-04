# -*- coding: utf-8 -*-
import pytest

import aerospike
from aerospike import exception as e
from aerospike_helpers import Vector
from aerospike_helpers.operations import hll_operations, operations

# Every case here is rejected before the query is sent, so the server does not
# need Top-K support. A connected client is still required to build a Query.

BIN = "score"
TYPE_REQUIRED_MSG = "orderBy requires a type declaration (INTEGER, DOUBLE, or BYTES); Aerospike has no schema"


def noop_callback(record):
    pass


@pytest.mark.usefixtures("as_connection")
class TestQueryTopKValidation:
    @pytest.fixture
    def query(self):
        return self.as_connection.query("test", "demo")

    def test_order_by_bin_name_with_nul(self, query):
        with pytest.raises(e.ParamError) as excinfo:
            query.order_by("a\0b", aerospike.QueryOrderByType.INTEGER, aerospike.QueryOrder.ASCENDING)
        assert excinfo.value.msg == "Top-K order-by bin name must be 1-15 UTF-8 bytes without NUL"

    # 8 two-byte characters is 16 UTF-8 bytes.
    @pytest.mark.parametrize("bin_name", ["", "a" * 16, "é" * 8])
    def test_order_by_invalid_bin_name_length(self, query, bin_name):
        query.order_by(bin_name, aerospike.QueryOrderByType.INTEGER, aerospike.QueryOrder.ASCENDING)
        query.top_k = 10
        with pytest.raises(e.ParamError) as excinfo:
            query.results()
        assert excinfo.value.msg == "Top-K order-by bin name must be 1-15 UTF-8 bytes without NUL"

    def test_order_by_bin_name_must_be_str(self, query):
        with pytest.raises(TypeError, match=r"^order_by\(\) argument 1 must be str, not bytes$"):
            query.order_by(b"score", aerospike.QueryOrderByType.INTEGER, aerospike.QueryOrder.ASCENDING)

    def test_order_by_missing_type(self, query):
        with pytest.raises(e.ParamError) as excinfo:
            query.order_by(BIN)
        assert excinfo.value.msg == TYPE_REQUIRED_MSG

    @pytest.mark.parametrize(
        "bin_type, expected_msg",
        [
            # The C client treats a type of 0 as missing.
            (0, TYPE_REQUIRED_MSG),
            (9, "orderBy type must be INTEGER, DOUBLE, or BYTES; got 9"),
            (-1, "orderBy type must be INTEGER, DOUBLE, or BYTES; got -1"),
            (aerospike.QueryOrderByType.STRING, "orderBy type STRING is not supported yet; use INTEGER, DOUBLE, or BYTES"),
        ],
    )
    def test_order_by_invalid_type(self, query, bin_type, expected_msg):
        query.order_by(BIN, bin_type, aerospike.QueryOrder.ASCENDING)
        query.top_k = 10
        with pytest.raises(e.ParamError) as excinfo:
            query.results()
        assert excinfo.value.msg == expected_msg

    @pytest.mark.parametrize("bin_type", [2**32 + 1, -(2**31) - 1])
    def test_order_by_type_out_of_range(self, query, bin_type):
        with pytest.raises(OverflowError, match=r"^Python int too large to convert to C int$"):
            query.order_by(BIN, bin_type, aerospike.QueryOrder.ASCENDING)

    @pytest.mark.parametrize("direction", [2**32, -(2**31) - 1])
    def test_order_by_direction_out_of_range(self, query, direction):
        with pytest.raises(OverflowError, match=r"^Python int too large to convert to C int$"):
            query.order_by(BIN, aerospike.QueryOrderByType.INTEGER, direction)

    def test_order_by_type_must_be_int(self, query):
        with pytest.raises(TypeError, match=r"^order_by\(\) argument 2 must be int, not str$"):
            query.order_by(BIN, "INTEGER", aerospike.QueryOrder.ASCENDING)

    def test_order_by_missing_direction(self, query):
        with pytest.raises(e.ParamError) as excinfo:
            query.order_by(BIN, aerospike.QueryOrderByType.INTEGER)
        assert excinfo.value.msg == "orderBy direction must be ASC or DESC"

    @pytest.mark.parametrize("direction", [2, -1])
    def test_order_by_invalid_direction(self, query, direction):
        query.order_by(BIN, aerospike.QueryOrderByType.INTEGER, direction)
        query.top_k = 10
        with pytest.raises(e.ParamError) as excinfo:
            query.results()
        assert excinfo.value.msg == "orderBy direction must be ASC or DESC"

    # STRING is not supported yet, so the C client rejects every flag, including unknown flag bits, as
    # requiring STRING.
    @pytest.mark.parametrize(
        "bin_type",
        [aerospike.QueryOrderByType.INTEGER, aerospike.QueryOrderByType.DOUBLE, aerospike.QueryOrderByType.BYTES],
    )
    @pytest.mark.parametrize("flags", [aerospike.QueryOrderByFlags.CASE_INSENSITIVE, 2, -1])
    def test_order_by_flags_require_string(self, query, bin_type, flags):
        query.order_by(BIN, bin_type, aerospike.QueryOrder.ASCENDING, flags)
        query.top_k = 10
        with pytest.raises(e.ParamError) as excinfo:
            query.results()
        assert excinfo.value.msg == "orderBy flags are only valid with type STRING"

    def test_order_by_flags_out_of_range(self, query):
        # On Windows, C long is 32 bits, so the value overflows long before it is checked against int.
        with pytest.raises(
            OverflowError,
            match=r"^(signed integer is greater than maximum|Python int too large to convert to C long)$",
        ):
            query.order_by(BIN, aerospike.QueryOrderByType.STRING, aerospike.QueryOrder.ASCENDING, 2**32 + 1)

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

    @pytest.mark.parametrize("api_method, args", [("results", []), ("foreach", [noop_callback])])
    def test_top_k_zero(self, query, api_method, args):
        query.order_by(BIN, aerospike.QueryOrderByType.INTEGER, aerospike.QueryOrder.ASCENDING)
        query.top_k = 0
        with pytest.raises(e.ParamError) as excinfo:
            getattr(query, api_method)(*args)
        assert excinfo.value.msg == "topK must be in [1, 1000]; got 0"

    def test_top_k_max_uint32_is_stored(self, query):
        query.top_k = 2**32 - 1
        assert query.top_k == 2**32 - 1

    @pytest.mark.parametrize("top_k", [-1, 2**32, 2**64])
    def test_top_k_outside_uint32(self, query, top_k):
        with pytest.raises(OverflowError) as excinfo:
            query.top_k = top_k
        assert str(excinfo.value) == f"top_k must be in [0, 4294967295]; got {top_k}"
        assert query.top_k == 0

    @pytest.mark.parametrize("top_k", [1.5, "10", None])
    def test_top_k_must_be_int(self, query, top_k):
        with pytest.raises(TypeError, match=r"object cannot be interpreted as an integer$"):
            query.top_k = top_k

    def test_top_k_cannot_be_deleted(self, query):
        with pytest.raises(AttributeError, match=r"^top_k can't be deleted$"):
            del query.top_k

    @pytest.mark.usefixtures("skip_if_server_older_than_8_2_1")
    def test_order_by_without_top_k_is_left_to_the_server(self, query):
        query.order_by(BIN, aerospike.QueryOrderByType.INTEGER, aerospike.QueryOrder.ASCENDING)
        with pytest.raises(e.InvalidRequest):
            query.results()

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
            "orderBy bin 'occurred' is not in projection; add it to the projection or remove the projection"
        )

    def test_order_by_bin_not_in_ops_projection(self, query):
        query.add_ops([operations.read("report")])
        query.order_by("occurred", aerospike.QueryOrderByType.INTEGER, aerospike.QueryOrder.DESCENDING)
        query.top_k = 20
        with pytest.raises(e.ParamError) as excinfo:
            query.results()
        assert excinfo.value.msg == (
            "orderBy bin 'occurred' is not in projection; add it to the projection or remove the projection"
        )

    def test_order_by_bin_produced_by_more_than_one_op(self, query):
        query.add_ops([operations.read("occurred"), operations.read("occurred")])
        query.order_by("occurred", aerospike.QueryOrderByType.INTEGER, aerospike.QueryOrder.DESCENDING)
        query.top_k = 20
        with pytest.raises(e.ParamError) as excinfo:
            query.results()
        assert excinfo.value.msg == (
            "orderBy bin 'occurred' is produced by more than one operation"
        )

    def test_then_order_by_returns_query(self, query):
        query.order_by(BIN, aerospike.QueryOrderByType.INTEGER, aerospike.QueryOrder.DESCENDING)
        returned = query.then_order_by(
            "name", aerospike.QueryOrderByType.STRING, aerospike.QueryOrder.ASCENDING, aerospike.QueryOrderByFlags.CASE_INSENSITIVE
        )
        assert returned is query

    @pytest.mark.parametrize(
        "args, expected_msg",
        [
            (("a\0b", aerospike.QueryOrderByType.INTEGER, aerospike.QueryOrder.ASCENDING),
             "Top-K order-by bin name must be 1-15 UTF-8 bytes without NUL"),
            (("ts",), TYPE_REQUIRED_MSG),
            (("ts", aerospike.QueryOrderByType.INTEGER), "orderBy direction must be ASC or DESC"),
        ],
    )
    def test_then_order_by_invalid_args(self, query, args, expected_msg):
        query.order_by(BIN, aerospike.QueryOrderByType.INTEGER, aerospike.QueryOrder.DESCENDING)
        with pytest.raises(e.ParamError) as excinfo:
            query.then_order_by(*args)
        assert excinfo.value.msg == expected_msg

    @pytest.mark.parametrize(
        "args, expected_msg",
        [
            (("a" * 16, aerospike.QueryOrderByType.INTEGER, aerospike.QueryOrder.ASCENDING),
             "Top-K order-by bin name must be 1-15 UTF-8 bytes without NUL"),
            (("ts", 9, aerospike.QueryOrder.ASCENDING), "orderBy type must be INTEGER, DOUBLE, or BYTES; got 9"),
            (("ts", aerospike.QueryOrderByType.INTEGER, 2), "orderBy direction must be ASC or DESC"),
            (("ts", aerospike.QueryOrderByType.INTEGER, aerospike.QueryOrder.ASCENDING,
              aerospike.QueryOrderByFlags.CASE_INSENSITIVE),
             "orderBy flags are only valid with type STRING"),
        ],
    )
    def test_then_order_by_invalid_key_at_execution(self, query, args, expected_msg):
        query.order_by(BIN, aerospike.QueryOrderByType.INTEGER, aerospike.QueryOrder.DESCENDING)
        query.then_order_by(*args)
        query.top_k = 10
        with pytest.raises(e.ParamError) as excinfo:
            query.results()
        assert excinfo.value.msg == expected_msg

    def test_then_order_by_missing_bin_names_method(self, query):
        with pytest.raises(TypeError, match=r"^then_order_by\(\) missing required argument 'bin'"):
            query.then_order_by()

    @pytest.mark.parametrize("api_method, args", [("results", []), ("foreach", [noop_callback])])
    def test_then_order_by_third_key(self, query, api_method, args):
        query.order_by(BIN, aerospike.QueryOrderByType.INTEGER, aerospike.QueryOrder.DESCENDING)
        query.then_order_by("ts", aerospike.QueryOrderByType.INTEGER, aerospike.QueryOrder.ASCENDING)
        query.then_order_by("name", aerospike.QueryOrderByType.STRING, aerospike.QueryOrder.ASCENDING)
        query.top_k = 10
        with pytest.raises(e.ParamError) as excinfo:
            getattr(query, api_method)(*args)
        assert excinfo.value.msg == "orderBy supports at most two keys"

    def test_order_by_replaces_both_keys(self, query):
        query.order_by(BIN, aerospike.QueryOrderByType.INTEGER, aerospike.QueryOrder.DESCENDING)
        query.then_order_by("ts", aerospike.QueryOrderByType.INTEGER, aerospike.QueryOrder.ASCENDING)
        query.then_order_by("name", aerospike.QueryOrderByType.STRING, aerospike.QueryOrder.ASCENDING)
        query.order_by("occurred", aerospike.QueryOrderByType.INTEGER, aerospike.QueryOrder.DESCENDING)
        query.select("occurred")
        query.top_k = 10
        # The key count error is gone, and "ts" no longer has to be in the projection.
        query.max_records = 100
        with pytest.raises(e.ParamError) as excinfo:
            query.results()
        assert excinfo.value.msg == "Top-K is incompatible with maxRecords"

    @pytest.mark.parametrize("order_by_later", [False, True])
    @pytest.mark.parametrize("top_k", [None, 20])
    def test_then_order_by_before_order_by(self, query, order_by_later, top_k):
        query.then_order_by("occurred", aerospike.QueryOrderByType.INTEGER, aerospike.QueryOrder.DESCENDING)
        if order_by_later:
            query.order_by(BIN, aerospike.QueryOrderByType.INTEGER, aerospike.QueryOrder.DESCENDING)
        if top_k is not None:
            query.top_k = top_k
        with pytest.raises(e.ParamError) as excinfo:
            query.results()
        assert excinfo.value.msg == "thenOrderBy can't be called before orderBy"

    def test_then_order_by_bin_not_in_projection(self, query):
        query.select(BIN)
        query.order_by(BIN, aerospike.QueryOrderByType.INTEGER, aerospike.QueryOrder.DESCENDING)
        query.then_order_by("ts", aerospike.QueryOrderByType.INTEGER, aerospike.QueryOrder.ASCENDING)
        query.top_k = 20
        with pytest.raises(e.ParamError) as excinfo:
            query.results()
        assert excinfo.value.msg == (
            "orderBy bin 'ts' is not in projection; add it to the projection or remove the projection"
        )


class TestQueryTopKIntegration:
    SET_NAME = "demo-top-k"
    # (score, ts) pairs. The two records with score 3 tie on the first key.
    RECORDS = [(1, 50), (3, 20), (2, 40), (3, 10)]

    @pytest.fixture(autouse=True)
    def setup(self, as_connection, skip_if_server_older_than_8_2_1):
        self.keys = [("test", self.SET_NAME, i) for i in range(len(self.RECORDS))]
        for key, (score, ts) in zip(self.keys, self.RECORDS):
            as_connection.put(key, {BIN: score, "ts": ts})
        yield
        for key in self.keys:
            as_connection.remove(key)

    def test_then_order_by_breaks_ties(self):
        query = self.as_connection.query("test", self.SET_NAME)
        query.order_by(BIN, aerospike.QueryOrderByType.INTEGER, aerospike.QueryOrder.DESCENDING)
        query.then_order_by("ts", aerospike.QueryOrderByType.INTEGER, aerospike.QueryOrder.ASCENDING)
        query.top_k = 3
        records = query.results()
        ordered = [(bins[BIN], bins["ts"]) for _, _, bins in records]
        assert ordered == [(3, 10), (3, 20), (2, 40)]


class TestQueryTopKBytes:
    SET_NAME = "demo-top-k-bytes"
    # The integer is not a bytes value, so it sorts as NIL.
    VALUES = [b"\x02", b"\x01\xff", b"\x01", b"\x00\x05", 7]

    @pytest.fixture(autouse=True)
    def setup(self, as_connection, skip_if_server_older_than_8_2_1):
        self.keys = [("test", self.SET_NAME, i) for i in range(len(self.VALUES))]
        for key, value in zip(self.keys, self.VALUES):
            as_connection.put(key, {BIN: value})
        yield
        for key in self.keys:
            as_connection.remove(key)

    @pytest.mark.parametrize(
        "direction, expected",
        [
            (aerospike.QueryOrder.ASCENDING, [b"\x00\x05", b"\x01", b"\x01\xff", b"\x02", 7]),
            (aerospike.QueryOrder.DESCENDING, [b"\x02", b"\x01\xff", b"\x01", b"\x00\x05", 7]),
        ],
    )
    def test_order_by_bytes(self, direction, expected):
        query = self.as_connection.query("test", self.SET_NAME)
        query.order_by(BIN, aerospike.QueryOrderByType.BYTES, direction)
        query.top_k = len(self.VALUES)
        records = query.results()
        assert [bins[BIN] for _, _, bins in records] == expected


class TestQueryTopKBytesWithVectorAndHll:
    SET_NAME = "demo-top-k-bytes-mixed"
    BLOBS = [b"\x02", b"\x01", b"\x00\x05"]

    @pytest.fixture(autouse=True)
    def setup(self, as_connection, skip_if_server_older_than_8_2_1):
        self.keys = [("test", self.SET_NAME, i) for i in range(len(self.BLOBS) + 3)]
        for key, blob in zip(self.keys, self.BLOBS):
            as_connection.put(key, {BIN: blob})
        vector_key, hll_key, int_key = self.keys[len(self.BLOBS):]
        as_connection.put(vector_key, {BIN: Vector.of_float32([0.5])})
        as_connection.operate(hll_key, [hll_operations.hll_init(BIN, 4)])
        as_connection.put(int_key, {BIN: 7})
        yield
        for key in self.keys:
            as_connection.remove(key)

    # Vector and HyperLogLog bins are blobs too, but only a plain blob is ranked by its
    # bytes. The others sort as NIL, after every plain blob, in no particular order.
    @pytest.mark.parametrize(
        "direction, expected",
        [
            (aerospike.QueryOrder.ASCENDING, [b"\x00\x05", b"\x01", b"\x02"]),
            (aerospike.QueryOrder.DESCENDING, [b"\x02", b"\x01", b"\x00\x05"]),
        ],
    )
    def test_vector_and_hll_sort_as_nil(self, direction, expected):
        query = self.as_connection.query("test", self.SET_NAME)
        query.order_by(BIN, aerospike.QueryOrderByType.BYTES, direction)
        query.top_k = len(self.keys)
        values = [bins[BIN] for _, _, bins in query.results()]

        # Vector and HyperLogLog subclass bytes, so compare the types as well.
        assert [type(value) for value in values[: len(expected)]] == [bytes] * len(expected)
        assert values[: len(expected)] == expected
        assert sorted(type(value).__name__ for value in values[len(expected):]) == ["HyperLogLog", "Vector", "int"]

    @pytest.mark.parametrize("direction", [aerospike.QueryOrder.ASCENDING, aerospike.QueryOrder.DESCENDING])
    def test_top_k_of_plain_blobs_skips_vector_and_hll(self, direction):
        query = self.as_connection.query("test", self.SET_NAME)
        query.order_by(BIN, aerospike.QueryOrderByType.BYTES, direction)
        query.top_k = len(self.BLOBS)
        values = [bins[BIN] for _, _, bins in query.results()]
        assert [type(value) for value in values] == [bytes] * len(self.BLOBS)
