# -*- coding: utf-8 -*-
# Tests for the Vector Phase 1 general client test plan that the other vector and Top-K test files don't
# already cover. Each test names its plan ID, and the tests in each class are in plan order.
import functools
import math
import os
import random
import struct
import sys
import time
from array import array
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Iterator, Optional

import numpy as np
import pytest

import aerospike
import aerospike_helpers.expressions as exp
from aerospike import exception as e
from aerospike import predicates as p
from aerospike_helpers import Vector, cdt_ctx
from aerospike_helpers.batch import records as br
from aerospike_helpers.expressions import vector as vector_exp
from aerospike_helpers.operations import expression_operations as exp_ops
from aerospike_helpers.operations import hll_operations, list_operations, map_operations, operations

from .as_status_codes import AerospikeStatus
from .index_helpers import ensure_dropped_index, wait_for_index
from .server_config_helpers import (
    SERVICE_CONTEXT,
    TEST_NAMESPACE_CONTEXT,
    get_server_config,
    set_server_config,
    set_server_dynamic_config,
)
from .test_base_class import TestBaseClass
from .test_expressions_vector import dot_product, euclidean_squared
from .test_vector import MAX_DIMENSIONS, NAN_FLOAT32_VECTOR, TestVectorUnit

NAMESPACE = "test"
FLT_MAX = 3.4028234663852886e38
DETAIL_POLICY = {"error_detail_verbosity": aerospike.ERROR_DETAIL_MESSAGE}
EUCLIDEAN_SQUARED = vector_exp.VectorDistanceMetric.EUCLIDEAN_SQUARED
DOT_PRODUCT = vector_exp.VectorDistanceMetric.DOT_PRODUCT
COSINE_SIMILARITY = vector_exp.VectorDistanceMetric.COSINE_SIMILARITY
ASC = aerospike.Query.Order.ASC
DESC = aerospike.Query.Order.DESC
INTEGER = aerospike.Query.BinDataType.INTEGER
DOUBLE = aerospike.Query.BinDataType.DOUBLE
BYTES = aerospike.Query.BinDataType.BYTES
APPEND_DETAIL = "cannot append/prepend to a vector bin - write the whole vector"


def distance(metric: vector_exp.VectorDistanceMetric, query_vector: Vector, bin_name: str = "embedding") -> list:
    return vector_exp.VectorDistance(metric, query_vector, exp.VectorBin(bin_name)).compile()


def order_top_k(
    query: aerospike.Query,
    bin_name: str,
    bin_type: int,
    direction: int,
    top_k: int,
    then_order_by: Optional[tuple] = None,
) -> None:
    query.order_by(bin_name, bin_type, direction)
    if then_order_by:
        query.then_order_by(*then_order_by)
    query.top_k = top_k


def assert_server_detail(error: e.AerospikeError, detail: str) -> None:
    # The C client prefixes the server's detail with the node address.
    assert error.msg.endswith(f" {detail}")


# ---------------------------------------------------------------------------
# Unit tests - no server required.
# ---------------------------------------------------------------------------


class TestGeneralPlanUnit:
    # Vector Phase 1 general test plan: AV-05
    # Every finite binary16 value given as a float64, and the midpoint between each pair of neighbours,
    # which is a tie that rounds to the even one. NumPy narrows to float32 and rounds to nearest even, as
    # of_float16() does. test_numpy_arrays_round_trip_bit_for_bit in test_vector.py starts from float16
    # values, so it doesn't round.
    @pytest.mark.parametrize("as_list", [True, False], ids=["list", "numpy"])
    def test_float16_from_numbers_matches_reference(self, as_list: bool):
        finite = TestVectorUnit._finite_float16_array().astype(np.float64)
        neighbours = np.unique(finite)
        midpoints = (neighbours[:-1] + neighbours[1:]) / 2
        source = np.concatenate([finite, midpoints])
        expected = source.astype(np.float32).astype(np.float16).view(np.uint16)

        values = source
        if as_list:
            values = source.tolist()
        vector = Vector.of_float16(values)
        assert np.array_equal(np.frombuffer(vector.float16_bits(), dtype=np.uint16), expected)

    # Vector Phase 1 general test plan: AV-06
    def test_from_bytes_trailing_bytes_raise(self):
        raw = bytes(Vector.of_float32([0.5, 1.5])) + b"\x00"
        with pytest.raises(
            ValueError, match=r"^Vector buffer has bytes after the declared dimensions: need 14, got 15$"
        ):
            Vector.from_bytes(raw)

    # Vector Phase 1 general test plan: AV-12
    @pytest.mark.parametrize(
        "value, expected",
        [
            (16777217, 16777216.0),
            (16777219, 16777220.0),
            (1 + 2**-24, 1.0),
            (3.4028235e38, FLT_MAX),
            (-3.4028235e38, -FLT_MAX),
        ],
    )
    def test_float32_elements_round_to_nearest(self, value: float, expected: float):
        assert list(Vector.of_float32([value]).value) == [expected]

    # Vector Phase 1 general test plan: AV-12
    # Halfway between FLT_MAX and the next power of two is the smallest value that rounds to infinity.
    # test_overflowing_float32_rejected in test_vector.py uses values far past it.
    @pytest.mark.parametrize("value", [FLT_MAX + 2**103, -(FLT_MAX + 2**103)])
    def test_float32_element_rounding_to_infinity_is_refused(self, value: float):
        with pytest.raises(ValueError, match=r"^Vector elements must be finite \(NaN/Infinity not allowed\)$"):
            Vector.of_float32([value])


# ---------------------------------------------------------------------------
# Top-K checks before sending. A connected client is needed to build a Query, but the server doesn't need
# Top-K support.
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("as_connection")
class TestGeneralPlanTopKChecks:
    @pytest.fixture
    def query(self) -> aerospike.Query:
        return self.as_connection.query(NAMESPACE, "demo")

    # Vector Phase 1 general test plan: AT-05
    @pytest.mark.parametrize("api_method", ["results", "foreach"])
    def test_same_bin_for_both_order_keys(self, query: aerospike.Query, api_method: str):
        query.order_by("score", INTEGER, ASC)
        query.then_order_by("score", DOUBLE, DESC)
        query.top_k = 10
        args = []
        if api_method == "foreach":
            args = [lambda record: None]
        with pytest.raises(e.ParamError) as excinfo:
            getattr(query, api_method)(*args)
        assert excinfo.value.msg == "order-by names bin 'score' more than once"

    # Vector Phase 1 general test plan: AT-07
    # The names pass the name check, so the query is refused at the later maxRecords check instead.
    @pytest.mark.parametrize("bin_name", ["a" * 15, "é" * 7 + "a"], ids=["ascii", "utf-8"])
    def test_15_byte_order_bin_name_is_accepted(self, query: aerospike.Query, bin_name: str):
        assert len(bin_name.encode("utf-8")) == 15
        query.order_by(bin_name, INTEGER, ASC)
        query.top_k = 10
        query.max_records = 100
        with pytest.raises(e.ParamError) as excinfo:
            query.results()
        assert excinfo.value.msg == "Top-K is incompatible with maxRecords"

    # Vector Phase 1 general test plan: AT-14
    # Python has no missing direction or type default either: omitting one raises ParamError
    # (test_order_by_missing_type and test_order_by_missing_direction in test_query_top_k.py).
    @pytest.mark.parametrize("knn", [False, True], ids=["plain", "knn"])
    @pytest.mark.parametrize("method", ["order_by", "then_order_by"])
    @pytest.mark.parametrize(
        "bin_type, direction, argument", [(None, ASC, 2), (DOUBLE, None, 3)], ids=["type", "direction"]
    )
    def test_null_order_type_or_direction_is_refused(
        self,
        query: aerospike.Query,
        knn: bool,
        method: str,
        bin_type: Optional[int],
        direction: Optional[int],
        argument: int,
    ):
        bin_name = "score"
        if knn:
            bin_name = "dist"
            query.add_ops([exp_ops.expression_read("dist", distance(EUCLIDEAN_SQUARED, Vector.of_float32([0.5])))])
        if method == "then_order_by":
            query.order_by("first", INTEGER, ASC)
        with pytest.raises(TypeError, match=rf"^{method}\(\) argument {argument} must be int, not None$"):
            getattr(query, method)(bin_name, bin_type, direction)


# ---------------------------------------------------------------------------
# Integration tests - require server version 8.2.1 or later.
# ---------------------------------------------------------------------------


class TestGeneralPlanIntegration:
    @pytest.fixture(autouse=True)
    def setup(self, request: pytest.FixtureRequest, as_connection: aerospike.Client, skip_if_server_older_than_8_2_1):
        self.set_name = f"gp_{request.node.originalname[5:40]}"
        self.keys = []
        yield
        for key in self.keys:
            try:
                as_connection.remove(key)
            except e.RecordNotFound:
                pass

    def _key(self, user_key: Any, set_name: Optional[str] = None) -> tuple:
        key = (NAMESPACE, set_name or self.set_name, user_key)
        self.keys.append(key)
        return key

    def _put(self, user_key: Any, bins: dict, policy: Optional[dict] = None) -> tuple:
        key = self._key(user_key)
        self.as_connection.put(key, bins, policy=policy)
        return key

    def _expression_read(self, key: tuple, expression: list, flags: int = aerospike.EXP_READ_DEFAULT) -> Any:
        _, _, bins = self.as_connection.operate(key, [exp_ops.expression_read("r", expression, flags)])
        return bins["r"]

    def _query(self, ops: Optional[list] = None) -> aerospike.Query:
        query = self.as_connection.query(NAMESPACE, self.set_name)
        if ops:
            query.add_ops(ops)
        return query

    @pytest.fixture
    def vector_udf(self) -> Iterator[str]:
        self.as_connection.udf_put("vector_udf.lua", 0)
        yield "vector_udf"
        self.as_connection.udf_remove("vector_udf.lua")

    # Vector Phase 1 general test plan: AV-02
    @pytest.mark.parametrize(
        "vector",
        [
            Vector.of_float16_bits([0x0000, 0x8000, 0x0001, 0x8001, 0x03FF, 0x0400, 0x7BFF, 0xFBFF]),
            Vector.of_float32([-0.0, 2**-149, -(2**-149), 2**-126 - 2**-149, 2**-126, FLT_MAX, -FLT_MAX]),
            Vector.of_float64(
                [-0.0, 5e-324, -5e-324, sys.float_info.min, sys.float_info.max, -sys.float_info.max]
            ),
            Vector.of_int32([-(2**31), 2**31 - 1, 0, -1]),
        ],
        ids=["float16", "float32", "float64", "int32"],
    )
    def test_edge_values_round_trip(self, vector: Vector):
        key = self._put(user_key="edge", bins={"embedding": vector})
        result = self.as_connection.get(key)[2]["embedding"]
        assert isinstance(result, Vector)
        assert bytes(result) == bytes(vector)

    # Vector Phase 1 general test plan: AV-11
    def test_buffers_and_lists_are_not_stored_as_vectors(self):
        vector = Vector.of_float32([1.0, 2.0])
        bins = {"bytes": bytes(vector), "bytearray": bytearray(vector), "list": list(vector.value), "vector": vector}
        key = self._put(user_key="types", bins=bins)
        ops = [exp_ops.expression_read(name, exp.BinType(name).compile()) for name in bins]
        _, _, types = self.as_connection.operate(key, ops)
        assert types == {
            "bytes": aerospike.AS_BYTES_BLOB,
            "bytearray": aerospike.AS_BYTES_BLOB,
            "list": aerospike.AS_BYTES_LIST,
            "vector": aerospike.AS_BYTES_VECTOR,
        }
        stored = self.as_connection.get(key)[2]
        assert type(stored["bytes"]) is bytes
        assert type(stored["vector"]) is Vector

    # Vector Phase 1 general test plan: AV-11
    # Python has no native array type to store, so the client refuses these instead of storing them.
    @pytest.mark.parametrize(
        "value", [array("f", [1.0, 2.0]), np.array([1.0, 2.0], dtype=np.float32)], ids=["array", "numpy"]
    )
    def test_float_arrays_are_refused(self, value: Any):
        key = self._key("float-array")
        with pytest.raises(e.ParamError) as excinfo:
            self.as_connection.put(key, {"embedding": value})
        assert excinfo.value.msg == "Cannot serialize: SERIALIZER_NONE selected"

    # Vector Phase 1 general test plan: AW-02
    # A get without compression is test_put_get_max_dimensions in test_vector.py.
    @pytest.mark.parametrize("compress", [False, True])
    @pytest.mark.parametrize("factory, max_dims", MAX_DIMENSIONS)
    def test_largest_vector_through_get_and_scan(self, factory: str, max_dims: int, compress: bool):
        vector = getattr(Vector, factory)([i % 100 for i in range(max_dims)])
        key = self._put(user_key="largest", bins={"embedding": vector})
        policy = {"compress": compress}
        if compress:
            _, _, bins = self.as_connection.get(key, policy=policy)
            assert bytes(bins["embedding"]) == bytes(vector)
        records = self.as_connection.scan(NAMESPACE, self.set_name).results(policy)
        assert [bytes(bins["embedding"]) for _, _, bins in records] == [bytes(vector)]

    # Vector Phase 1 general test plan: AW-03
    # The other read paths are in test_vector.py (PB-05, PC-05) and test_expressions_vector.py (PE-07), and a
    # transaction read is in TestGeneralPlanStrongConsistency. A scan has no deserialize policy.
    def test_reads_without_deserialize_return_vector(self):
        vector = Vector.of_float32([1.0, -2.0])
        key = self._put(user_key="raw", bins={"embedding": vector, "list": [1, 2]})
        policy = {"deserialize": False}
        reads = [operations.read("embedding"), operations.read("list")]
        results = [
            self.as_connection.get(key, policy=policy)[2],
            self.as_connection.operate(key, reads, policy=policy)[2],
            self.as_connection.batch_read([key], policy=policy).batch_records[0].record[2],
            self._query().results(policy)[0][2],
        ]
        for bins in results:
            assert bins["list"] == b"\x92\x01\x02"
            assert isinstance(bins["embedding"], Vector)
            assert bytes(bins["embedding"]) == bytes(vector)

    # Vector Phase 1 general test plan: AW-04
    # The other write paths are in test_vector.py (PB-04, PC-05).
    def test_expression_write_stores_vector(self):
        vector = Vector.of_int32([3, -4, 5])
        key = self._put(user_key="expression-write", bins={"embedding": vector})
        self.as_connection.operate(key, [exp_ops.expression_write("copy", exp.VectorBin("embedding").compile())])
        assert self._expression_read(key, exp.BinType("copy").compile()) == aerospike.AS_BYTES_VECTOR
        copy = self.as_connection.get(key)[2]["copy"]
        assert isinstance(copy, Vector)
        assert bytes(copy) == bytes(vector)

    # Vector Phase 1 general test plan: AW-05
    def test_vector_read_and_written_back(self):
        written = {
            "f16": Vector.of_float16([0.5, -1.5]),
            "f32": Vector.of_float32([0.1, 0.2]),
            "i32": Vector.of_int32([7, -8]),
            "f64": Vector.of_float64([1e-300, 2.5]),
            "n": 1,
        }
        keys = [self._put(user_key=i, bins=written) for i in range(3)]

        _, _, bins = self.as_connection.get(keys[0])
        self.as_connection.put(keys[0], bins)

        _, meta, bins = self.as_connection.get(keys[1])
        self.as_connection.put(keys[1], bins, meta={"gen": meta["gen"]}, policy={"gen": aerospike.POLICY_GEN_EQ})

        read = self.as_connection.batch_read([keys[2]])
        bins = read.batch_records[0].record[2]
        write_back = br.Write(
            keys[2],
            [operations.write(name, value) for name, value in bins.items()],
            policy={"key": aerospike.POLICY_KEY_SEND, "exists": aerospike.POLICY_EXISTS_REPLACE},
        )
        result = self.as_connection.batch_write(br.BatchRecords([write_back]))
        assert [record.result for record in result.batch_records] == [AerospikeStatus.AEROSPIKE_OK]

        for key in keys:
            stored = self.as_connection.get(key)[2]
            assert stored == written
            assert all(type(stored[name]) is Vector for name in ("f16", "f32", "i32", "f64"))

    # Vector Phase 1 general test plan: AW-06
    @pytest.mark.parametrize("command", ["query", "scan"])
    def test_projection_paging_and_metadata_only(self, command: str):
        vectors = {i: Vector.of_float32([i, -i]) for i in range(7)}
        for i, vector in vectors.items():
            self._put(user_key=i, bins={"n": i, "embedding": vector, "other": "x"})

        def new_job() -> Any:
            return getattr(self.as_connection, command)(NAMESPACE, self.set_name)

        projected = new_job()
        projected.select("n", "embedding")
        records = projected.results()
        assert sorted((bins["n"], bytes(bins["embedding"])) for _, _, bins in records) == [
            (i, bytes(vector)) for i, vector in vectors.items()
        ]
        assert all(set(bins) == {"n", "embedding"} for _, _, bins in records)

        paged = new_job()
        paged.paginate()
        page_policy = {}
        if command == "query":
            paged.max_records = 3
        else:
            page_policy["max_records"] = 3
        seen = []
        while not paged.is_done():
            page = paged.results(page_policy)
            seen += [bins["n"] for _, _, bins in page]
            for _, _, bins in page:
                assert bytes(bins["embedding"]) == bytes(vectors[bins["n"]])
        assert sorted(seen) == list(vectors)

        metadata_only = []
        new_job().foreach(metadata_only.append, options={"nobins": True})
        assert len(metadata_only) == len(vectors)
        assert all(bins == {} for _, _, bins in metadata_only)

    # Vector Phase 1 general test plan: AW-07
    def test_scan_with_last_update_time_filter(self):
        older = self._put(user_key="older", bins={"embedding": Vector.of_float32([1.0])})
        time.sleep(0.01)
        vector = Vector.of_float32([2.0, 3.0])
        newer = self._put(user_key="newer", bins={"embedding": vector})
        threshold = self._expression_read(newer, exp.LastUpdateTime().compile())
        assert self._expression_read(older, exp.LastUpdateTime().compile()) < threshold

        policy = {"expressions": exp.GE(exp.LastUpdateTime(), threshold).compile()}
        records = self.as_connection.scan(NAMESPACE, self.set_name).results(policy)
        assert [bytes(bins["embedding"]) for _, _, bins in records] == [bytes(vector)]

    # Vector Phase 1 general test plan: AW-08
    def test_vector_bin_replaced_cleared_and_deleted(self):
        key = self._put(user_key="replace", bins={"embedding": Vector.of_float32([1.0, 2.0]), "n": 1})
        replacements = [Vector.of_int32([1, 2, 3]), Vector.of_float16([0.5]), 7, None]
        for gen, value in enumerate(replacements, start=2):
            self.as_connection.put(key, {"embedding": value}, policy={"ttl": aerospike.TTL_NEVER_EXPIRE})
            _, meta, bins = self.as_connection.get(key)
            expected = {"n": 1}
            if value is not None:
                expected["embedding"] = value
            assert bins == expected
            assert meta == {"gen": gen, "ttl": aerospike.TTL_NEVER_EXPIRE}
        self.as_connection.remove(key)
        with pytest.raises(e.RecordNotFound):
            self.as_connection.get(key)

    # Vector Phase 1 general test plan: AW-09
    def test_several_vector_bins_with_other_bins(self):
        bins = {
            "a": Vector.of_float32([0.25, 0.5]),
            "b": Vector.of_int32([-(2**31)]),
            "c": Vector.of_float16([1.5, -2.0, 4.0]),
            "d": Vector.of_float64([1e-300]),
            "n": 5,
            "s": "x",
            "l": [1, 2],
            "m": {"k": 1.5},
        }
        key = self._put(user_key="several", bins=bins)
        assert self.as_connection.get(key)[2] == bins

        replacement = Vector.of_float32([9.0])
        self.as_connection.operate(key, [operations.write("a", replacement)])
        assert self.as_connection.get(key)[2] == bins | {"a": replacement}

    # Vector Phase 1 general test plan: AS-01
    # A vector in a list or map literal keeps its type, so the server refuses it like a CDT operation does
    # (test_vector_in_cdt_operation_is_rejected in test_vector.py).
    @pytest.mark.parametrize(
        "operation",
        [
            exp_ops.expression_write(
                "l",
                exp.ListAppendItems(
                    ctx=None, policy=None, value=[Vector.of_float32([0.5])], bin=exp.ListBin("l")
                ).compile(),
            ),
            exp_ops.expression_write(
                "m",
                exp.MapPutItems(
                    ctx=None, policy=None, map={"b": Vector.of_float32([0.5])}, bin=exp.MapBin("m")
                ).compile(),
            ),
            exp_ops.expression_read("r", exp.Eq(exp.ListBin("l"), [Vector.of_float32([0.5])]).compile()),
        ],
        ids=["list_append_items", "map_put_items", "list_literal"],
    )
    def test_vector_in_expression_collection_literal_is_refused(self, operation: dict):
        bins = {"l": [1], "m": {"a": 1}}
        key = self._put(user_key="literal", bins=bins)
        with pytest.raises(e.InvalidRequest):
            self.as_connection.operate(key, [operation])
        assert self.as_connection.get(key)[2] == bins

    # Vector Phase 1 general test plan: AS-01
    # A vector passed directly as an expression value keeps its type too. The product specification gives a
    # parameter error (4) for a list or map, but 8.2.1.0-66 returns rc 26 for a list or map expression operation.
    # Expressions have no vector literal, so it refuses a vector value anywhere else in an expression. Writing a
    # vector bin from another vector bin is test_expression_write_stores_vector (AW-04).
    @pytest.mark.parametrize(
        "bin_name, expression, error, detail",
        [
            (
                "l", exp.ListAppend(ctx=None, policy=None, value=Vector.of_float32([0.5]), bin=exp.ListBin("l")),
                e.OpNotApplicable, "list_append: list/map cannot hold a vector",
            ),
            (
                "m",
                exp.MapPut(ctx=None, policy=None, key="b", value=Vector.of_float32([0.5]), bin=exp.MapBin("m")),
                e.OpNotApplicable, "map_add: invalid value - list/map cannot hold a vector",
            ),
            (
                "v", exp.Let(exp.Def("x", Vector.of_float32([0.5])), exp.Var("x")),
                e.InvalidRequest, "invalid expression in operation request",
            ),
        ],
        ids=["list_append", "map_put", "bin_write"],
    )
    def test_vector_as_expression_value_is_refused(
        self, bin_name: str, expression: Any, error: type[e.AerospikeError], detail: str
    ):
        bins = {"l": [1], "m": {"a": 1}}
        key = self._put(user_key="value", bins=bins)
        with pytest.raises(error) as excinfo:
            self.as_connection.operate(
                key, [exp_ops.expression_write(bin_name, expression.compile())], policy=DETAIL_POLICY
            )
        assert_server_detail(excinfo.value, detail)
        assert self.as_connection.get(key)[2] == bins

    # Vector Phase 1 general test plan: AS-02
    # Append, prepend and increment alone are in test_vector.py (PB-07).
    @pytest.mark.parametrize(
        "operation, detail",
        [
            (list_operations.list_size("embedding"), "list read requires list bin, got bin type vector"),
            (list_operations.list_append("embedding", 1), "list modify requires list bin, got bin type vector"),
            (map_operations.map_size("embedding"), "map read requires map bin, got bin type vector"),
            (map_operations.map_put("embedding", key="k", value=1), "map modify requires map bin, got bin type vector"),
            (operations.append("embedding", b"\x00"), APPEND_DETAIL),
            (operations.prepend("embedding", b"\x00"), APPEND_DETAIL),
            (operations.increment("embedding", 1), "cannot increment a vector bin"),
        ],
        ids=["list_size", "list_append", "map_size", "map_put", "append", "prepend", "increment"],
    )
    def test_operation_on_vector_bin_with_another_write(self, operation: dict, detail: str):
        bins = {"embedding": Vector.of_float32([0.5, 1.5]), "n": 1}
        key = self._put(user_key="modify", bins=bins)
        with pytest.raises(e.BinIncompatibleType) as excinfo:
            self.as_connection.operate(key, [operations.write("n", 2), operation], policy=DETAIL_POLICY)
        assert_server_detail(excinfo.value, detail)
        assert self.as_connection.get(key)[2] == bins

    # Vector Phase 1 general test plan: AS-04
    # The client has no vector index type, so the vector-type index is sent as a raw info command.
    def test_no_index_holds_vector_values(self):
        vector = Vector.of_float32([0.5, 1.5])
        self._put(user_key="vector", bins={"embedding": vector})
        self._put(user_key="blob", bins={"embedding": bytes(vector)})
        index_name = "gp_vector_blob_index"
        ensure_dropped_index(self.as_connection, NAMESPACE, index_name)

        command = f"sindex-create:ns={NAMESPACE};set={self.set_name};indexname={index_name};bin=embedding;type=vector"
        with pytest.raises(e.InvalidRequest) as excinfo:
            self.as_connection.info_random_node(command)
        assert "bad 'type' - must be one of 'integer', 'string', 'blob', 'geo2dsphere'" in excinfo.value.msg

        with pytest.raises(e.InvalidRequest) as excinfo:
            self.as_connection.index_expr_create(
                NAMESPACE, self.set_name, index_type=aerospike.INDEX_TYPE_DEFAULT, index_datatype=aerospike.INDEX_BLOB,
                expressions=exp.VectorBin("embedding").compile(), name=index_name,
            )
        assert "bad 'exp' - expression type 'vector' does not match expected type 'blob'" in excinfo.value.msg

        self.as_connection.index_single_value_create(
            NAMESPACE, self.set_name, bin="embedding", index_datatype=aerospike.INDEX_BLOB, name=index_name
        )
        try:
            wait_for_index(self.as_connection, NAMESPACE, index_name)
            query = self._query()
            query.where(p.equals("embedding", bytes(vector)))
            records = query.results()
        finally:
            ensure_dropped_index(self.as_connection, NAMESPACE, index_name)
        assert [type(bins["embedding"]) for _, _, bins in records] == [bytes]

    # Vector Phase 1 general test plan: AX-01
    # 2·e and -3·e at the last dimension.
    @pytest.mark.parametrize("factory, max_dims", [entry for entry in MAX_DIMENSIONS if entry[0] != "of_float16_bits"])
    def test_one_hot_distances_are_exact(self, factory: str, max_dims: int):
        expected = {"sq": 25.0, "dot": -6.0, "cos": -1.0}
        for dimensions in [1, 3, 4, 5, 7, 8, 9, 15, 16, 17, 33, max_dims]:
            stored = [0] * dimensions
            stored[-1] = 2
            query_values = [0] * dimensions
            query_values[-1] = -3
            query_vector = getattr(Vector, factory)(query_values)
            key = self._put(user_key=dimensions, bins={"embedding": getattr(Vector, factory)(stored)})
            ops = [
                exp_ops.expression_read(name, distance(metric, query_vector))
                for name, metric in (("sq", EUCLIDEAN_SQUARED), ("dot", DOT_PRODUCT), ("cos", COSINE_SIMILARITY))
            ]
            _, _, bins = self.as_connection.operate(key, ops)
            assert bins == expected, f"{dimensions} dimensions"

    # Vector Phase 1 general test plan: AX-04
    @pytest.mark.parametrize(
        "comparison, expected",
        [(exp.GT, [3]), (exp.GE, [2, 3]), (exp.LT, [1]), (exp.LE, [1, 2])],
        ids=["GT", "GE", "LT", "LE"],
    )
    def test_filter_at_distance_threshold(self, comparison: type, expected: list):
        # The squared distances to the zero vector are 1, 2 and 3.
        for i, values in {1: [1, 0, 0], 2: [1, 1, 0], 3: [1, 1, 1]}.items():
            self._put(user_key=i, bins={"id": i, "embedding": Vector.of_float32(values)})
        dist = vector_exp.VectorDistance(EUCLIDEAN_SQUARED, Vector.of_float32([0, 0, 0]), "embedding")
        policy = {"expressions": comparison(dist, 2.0).compile()}
        records = self._query().results(policy)
        assert sorted(bins["id"] for _, _, bins in records) == expected

    # Vector Phase 1 general test plan: AX-05
    def test_same_distance_on_every_path(self):
        random.seed(5)
        key = self._put(user_key=0, bins={"embedding": Vector.of_float32([random.uniform(-1, 1) for _ in range(16)])})
        query_vector = Vector.of_float32([random.uniform(-1, 1) for _ in range(16)])
        dist = distance(COSINE_SIMILARITY, query_vector)
        read_dist = exp_ops.expression_read("dist", dist)

        distances = [self.as_connection.operate(key, [read_dist])[2]["dist"]]
        distances.append(self.as_connection.batch_operate([key], [read_dist]).batch_records[0].record[2]["dist"])
        batch = self.as_connection.batch_write(br.BatchRecords([br.Read(key, [read_dist])]))
        distances.append(batch.batch_records[0].record[2]["dist"])
        distances.append(self._query([read_dist]).results()[0][2]["dist"])
        top_k = self._query([read_dist])
        top_k.order_by("dist", DOUBLE, DESC)
        top_k.top_k = 1
        distances.append(top_k.results()[0][2]["dist"])
        assert distances == [distances[0]] * 5

    # Vector Phase 1 general test plan: AX-06
    def test_distance_compared_with_integer_literal(self):
        key = self._put(user_key=0, bins={"embedding": Vector.of_float32([1.0, 2.0])})
        dist = vector_exp.VectorDistance(EUCLIDEAN_SQUARED, Vector.of_float32([0.0, 0.0]), "embedding")
        filter_policy: dict = DETAIL_POLICY | {"expressions": exp.GT(dist, 5).compile()}
        with pytest.raises(e.InvalidRequest) as excinfo:
            self.as_connection.get(key, policy=filter_policy)
        assert_server_detail(excinfo.value, "invalid filter expression in request")
        with pytest.raises(e.InvalidRequest) as excinfo:
            self.as_connection.operate(
                key, [exp_ops.expression_read("r", exp.GT(dist, 5).compile())], policy=DETAIL_POLICY
            )
        assert_server_detail(excinfo.value, "invalid expression in operation request")

    # Vector Phase 1 general test plan: AX-06
    # bytes.__new__() skips the client's checks. An empty query vector is refused by the client
    # (test_dimensions_out_of_bounds_raise in test_vector.py).
    @pytest.mark.parametrize(
        "raw",
        [b"", bytes(Vector.of_float32([1.0, 2.0]))[:5], struct.pack("<BBI2f", 2, Vector.ElementType.FLOAT32, 2, 1, 2)],
        ids=["empty", "short-header", "version-2"],
    )
    def test_malformed_query_vector(self, raw: bytes):
        key = self._put(user_key=0, bins={"embedding": Vector.of_float32([1.0, 2.0])})
        dist = distance(EUCLIDEAN_SQUARED, bytes.__new__(Vector, raw))
        with pytest.raises(e.InvalidRequest) as excinfo:
            self.as_connection.operate(key, [exp_ops.expression_read("r", dist)], policy=DETAIL_POLICY)
        assert_server_detail(excinfo.value, "vector: query vector literal is not a valid vector")

    # Vector Phase 1 general test plan: AX-07
    # FLOAT32 products overflow a float, so the server sums them in a double. The squared difference
    # overflows even so, and ranks as +Inf. FLOAT64 overflow is in test_expressions_vector.py (PE-05).
    def test_float32_distance_overflow(self):
        huge = Vector.of_float32([3e38, 3e38])
        key = self._put(user_key="huge", bins={"id": "huge", "embedding": huge})
        self._put(user_key="small", bins={"id": "small", "embedding": Vector.of_float32([1.0, 1.0])})
        element = list(huge.value)[0]
        assert self._expression_read(key, distance(DOT_PRODUCT, huge)) == pytest.approx(2 * element * element)

        negated = Vector.of_float32([-3e38, -3e38])
        assert self._expression_read(key, distance(EUCLIDEAN_SQUARED, negated)) == math.inf
        read_dist = exp_ops.expression_read("dist", distance(EUCLIDEAN_SQUARED, negated))
        query = self._query([operations.read("id"), read_dist])
        query.order_by("dist", DOUBLE, ASC)
        query.top_k = 2
        assert [(bins["id"], bins["dist"]) for _, _, bins in query.results()][-1] == ("huge", math.inf)

    # (factory, stored, query, distance under float, distance under double): each dot product loses its
    # last term when summed in a float.
    PRECISION_CASES = [
        (Vector.of_float16, [2048.0, 1.0], [2048.0, 2**-14], 2.0**22, 2**22 + 2**-14),
        (Vector.of_int32, [16777217], [1], 16777216.0, 16777217.0),
        (Vector.of_float32, [1 + 2**-12], [1 + 2**-12], 1 + 2**-11, 1 + 2**-11 + 2**-24),
        (Vector.of_float64, [1 + 2**-30], [1 + 2**-30], 1 + 2**-29, 1 + 2**-29),
    ]
    PRECISION_IDS = ["float16", "int32", "float32", "float64"]

    def _dot_product_at_precision(self, factory: Callable, stored: list, query_values: list) -> float:
        key = self._put(user_key="precision", bins={"embedding": factory(stored)})
        return self._expression_read(key, distance(DOT_PRODUCT, factory(query_values)))

    # Vector Phase 1 general test plan: AX-08
    @pytest.mark.parametrize("precision", ["float", "double"])
    @pytest.mark.parametrize("factory, stored, query_values, as_float, as_double", PRECISION_CASES, ids=PRECISION_IDS)
    def test_distance_follows_precision_setting(
        self, precision: str, factory: Callable, stored: list, query_values: list, as_float: float, as_double: float
    ):
        with set_server_dynamic_config(
            self.as_connection, context=SERVICE_CONTEXT, name="vector-distance-precision", value=precision
        ):
            result = self._dot_product_at_precision(factory, stored, query_values)
        expected = as_float
        if precision == "double":
            expected = as_double
        assert result == expected

    # Vector Phase 1 general test plan: AX-08
    # The setting flips while one Top-K query runs, so every distance it returns comes from one precision.
    def test_precision_change_during_query(self):
        stored = Vector.of_float32([1 + 2**-12])
        keys = [self._key(i) for i in range(2000)]
        self.as_connection.batch_write(
            br.BatchRecords([br.Write(key, [operations.write("embedding", stored)]) for key in keys])
        )
        query = self._query([exp_ops.expression_read("dist", distance(DOT_PRODUCT, stored))])
        query.order_by("dist", DOUBLE, DESC)
        query.top_k = 1000

        setting = "vector-distance-precision"
        original = get_server_config(self.as_connection, context=SERVICE_CONTEXT, name=setting)
        try:
            set_server_config(self.as_connection, context=SERVICE_CONTEXT, name=setting, value="float")
        except e.RoleViolation:
            pytest.skip(f"set-config of {setting} needs the sys-admin privilege")
        done = False

        def flip() -> None:
            value = "double"
            while not done:
                set_server_config(self.as_connection, context=SERVICE_CONTEXT, name=setting, value=value)
                value = "float" if value == "double" else "double"

        try:
            with ThreadPoolExecutor(max_workers=1) as pool:
                flipper = pool.submit(flip)
                try:
                    runs = []
                    for _ in range(10):
                        records = query.results()
                        runs.append({bins["dist"] for _, _, bins in records})
                finally:
                    done = True
                flipper.result()
        finally:
            set_server_config(self.as_connection, context=SERVICE_CONTEXT, name=setting, value=original)
        assert all(len(distances) == 1 for distances in runs)

    # Vector Phase 1 general test plan: AX-09
    def test_largest_query_vector_literal(self):
        first = Vector.of_float16([(i % 7) - 3 for i in range(131072)])
        second = Vector.of_float16([(i % 5) - 2 for i in range(131072)])
        assert len(first) == 262150
        key = self._put(user_key="literal", bins={"first": first, "second": second})
        expected = dot_product(list(first.value), list(second.value))
        first_against_second = self._expression_read(key, distance(DOT_PRODUCT, first, bin_name="second"))
        second_against_first = self._expression_read(key, distance(DOT_PRODUCT, second, bin_name="first"))
        assert first_against_second == second_against_first == expected

    # Vector Phase 1 general test plan: AX-11
    def test_vector_expressions_on_every_node(self):
        vectors = {i: Vector.of_float32([i, -i, 0.5]) for i in range(64)}
        for i, vector in vectors.items():
            self._put(user_key=i, bins={"id": i, "embedding": vector})
        query_vector = Vector.of_float32([1.0, 2.0, 3.0])
        dist = vector_exp.VectorDistance(EUCLIDEAN_SQUARED, query_vector, "embedding")
        query = self._query(
            [
                operations.read("id"),
                exp_ops.expression_read("copy", exp.VectorBin("embedding").compile()),
                exp_ops.expression_read("dist", dist.compile()),
            ]
        )
        records = query.results({"expressions": exp.GE(dist, 0.0).compile()})

        assert sorted(bins["id"] for _, _, bins in records) == list(vectors)
        query_values = list(query_vector.value)
        for _, _, bins in records:
            assert bytes(bins["copy"]) == bytes(vectors[bins["id"]])
            assert bins["dist"] == pytest.approx(euclidean_squared(list(vectors[bins["id"]].value), query_values))
        for error, response in self.as_connection.info_all("status").values():
            assert error is None
            assert response.strip() == "ok"

    # Vector Phase 1 general test plan: AX-12
    @pytest.mark.parametrize(
        "bin_expression, literal",
        [(exp.BlobBin("embedding"), None), (exp.ListBin("embedding"), [1]), (exp.MapBin("embedding"), {"a": 1})],
        ids=["blob", "list", "map"],
    )
    def test_other_bin_type_expression_on_vector_bin(self, bin_expression: Any, literal: Any):
        vector = Vector.of_float32([0.5, 1.5])
        key = self._put(user_key=0, bins={"embedding": vector})
        with pytest.raises(e.OpNotApplicable):
            self._expression_read(key, bin_expression.compile())
        if literal is None:
            literal = bytes(vector)
        with pytest.raises(e.FilteredOut):
            self.as_connection.get(key, policy={"expressions": exp.Eq(bin_expression, literal).compile()})

    # Vector Phase 1 general test plan: AX-15
    def test_batch_and_query_with_vector_that_cannot_be_compared(self):
        vectors = {0: [0.0, 0.0], 1: [1.0, 1.0], 2: [2.0, 2.0]}
        keys = [
            self._put(user_key=i, bins={"id": i, "embedding": Vector.of_float32(values)})
            for i, values in vectors.items()
        ]
        keys.append(self._put(user_key=3, bins={"id": 3, "embedding": Vector.of_float32([1.0, 2.0, 3.0])}))
        query_vector = Vector.of_float32([0.0, 0.0])
        dist = vector_exp.VectorDistance(EUCLIDEAN_SQUARED, query_vector, "embedding")
        written = Vector.of_float16([0.5])

        batch = self.as_connection.batch_operate(
            keys,
            [
                operations.write("written", written),
                operations.read("embedding"),
                exp_ops.expression_read("dist", dist.compile()),
            ],
        )
        assert [record.result for record in batch.batch_records] == [0, 0, 0, e.OpNotApplicable.code]
        for record in batch.batch_records[:3]:
            bins = record.record[2]
            assert isinstance(bins["embedding"], Vector)
            assert bins["dist"] == euclidean_squared(list(bins["embedding"].value), [0.0, 0.0])
        for key in keys[:3]:
            stored = self.as_connection.get(key)[2]["written"]
            assert type(stored) is Vector
            assert bytes(stored) == bytes(written)

        filtered = self.as_connection.batch_read(keys, policy={"expressions": exp.LT(dist, 1.0).compile()})
        assert [record.result for record in filtered.batch_records] == [0] + [e.FilteredOut.code] * 3

        expected_ids_by_flags = {aerospike.EXP_READ_DEFAULT: [0, 1, 2], aerospike.EXP_READ_EVAL_NO_FAIL: [0, 1, 2, 3]}
        for command in ("query", "scan"):
            for flags, expected_ids in expected_ids_by_flags.items():
                job = getattr(self.as_connection, command)(NAMESPACE, self.set_name)
                read_dist = exp_ops.expression_read("dist", dist.compile(), flags)
                job.add_ops([operations.read("id"), operations.read("embedding"), read_dist])
                records = sorted(job.results(), key=lambda record: record[2]["id"])
                assert [bins["id"] for _, _, bins in records] == expected_ids, f"{command}, flags {flags}"
                assert all(isinstance(bins["embedding"], Vector) for _, _, bins in records)
                # A query returns the distance as None, but the C client doesn't ask the server to respond to
                # every operation of a scan, so a scan leaves it out.
                if flags == aerospike.EXP_READ_EVAL_NO_FAIL:
                    assert records[3][2].get("dist") is None

    # Vector Phase 1 general test plan: AT-10, AP-05
    # A new partition filter is accepted, and the records are ranked within its partitions. Python has no
    # single-node query, so only the partition range is tested. A resumed filter is refused
    # (test_top_k_with_resumed_partition_filter in test_query_top_k.py).
    @pytest.mark.parametrize("count", [4096, 2048])
    def test_top_k_with_new_partition_filter(self, count: int):
        partition_ids = {}
        for i in range(40):
            self._put(user_key=i, bins={"n": i})
            partition_ids[i] = self.as_connection.get_key_partition_id(ns=NAMESPACE, set=self.set_name, key=i)
        in_range = [i for i, part_id in partition_ids.items() if part_id < count]
        expected = sorted(in_range, reverse=True)[:5]
        partition_filter = {"partition_filter": {"begin": 0, "count": count}}
        query = self._query()
        query.order_by("n", INTEGER, DESC)
        query.top_k = 5

        records = query.results(partition_filter)
        assert [bins["n"] for _, _, bins in records] == expected
        streamed = []
        query.foreach(lambda part_id, record: streamed.append((part_id, record)), partition_filter)
        assert [record[2]["n"] for _, record in streamed] == expected
        assert all(part_id == partition_ids[record[2]["n"]] for part_id, record in streamed)
        # A Top-K query can't be resumed, so it doesn't keep the partition status.
        assert query.get_partitions_status() == {}
        records = query.results(partition_filter)
        assert [bins["n"] for _, _, bins in records] == expected

    # Vector Phase 1 general test plan: AT-11
    # As for any query, the selected bins are ignored with a deprecation warning.
    def test_top_k_with_selected_bins_and_operations(self):
        for i in range(5):
            self._put(user_key=i, bins={"n": i, "other": "x"})
        query = self._query([operations.read("n")])
        with pytest.warns(DeprecationWarning, match=r"^Operations and bin names are mutually exclusive\."):
            query.select("other")
        query.order_by("n", INTEGER, DESC)
        query.top_k = 3
        records = query.results()
        assert [bins for _, _, bins in records] == [{"n": 4}, {"n": 3}, {"n": 2}]

    def _ranked(
        self,
        bin_name: str,
        bin_type: int,
        direction: int,
        top_k: int,
        then_order_by: Optional[tuple] = None,
        ops: Optional[list] = None,
    ) -> list:
        query = self._query(ops)
        order_top_k(query, bin_name, bin_type, direction, top_k, then_order_by=then_order_by)
        return query.results()

    # Vector Phase 1 general test plan: AO-01
    @pytest.mark.parametrize("direction", [ASC, DESC])
    def test_order_by_integer_extremes(self, direction: int):
        values = [2**63 - 1, -(2**63), 0, 1, -1, 2**62, -(2**62)]
        for i, value in enumerate(values):
            self._put(user_key=i, bins={"n": value})
        records = self._ranked("n", INTEGER, direction, len(values))
        assert [bins["n"] for _, _, bins in records] == sorted(values, reverse=direction == DESC)

    # Vector Phase 1 general test plan: AO-01, AO-02
    # The order of NaN, infinities and zeros is in test_query_top_k.py (PQ-02). This adds the largest
    # doubles, and a record without the bin, which ranks last in either direction, even after NaN in ASC.
    @pytest.mark.parametrize(
        "direction, expected",
        [
            (ASC, [-sys.float_info.max, 0.5, sys.float_info.max, math.nan, None]),
            (DESC, [math.nan, sys.float_info.max, 0.5, -sys.float_info.max, None]),
        ],
    )
    def test_order_by_double_extremes_and_missing_bin(self, direction: int, expected: list):
        values = [sys.float_info.max, math.nan, 0.5, None, -sys.float_info.max]
        for i, value in enumerate(values):
            bins = {"i": i}
            if value is not None:
                bins["n"] = value
            self._put(user_key=i, bins=bins)
        records = self._ranked("n", DOUBLE, direction, len(values))
        returned = [bins.get("n") for _, _, bins in records]
        assert returned == pytest.approx(expected, nan_ok=True, rel=0, abs=0)

    # Vector Phase 1 general test plan: AO-04
    # Records whose n isn't an integer rank last and count toward k. With one key their ties are broken by
    # ascending digest, and with two keys by the second key. A string and a missing n are in
    # test_query_top_k.py (PQ-05), and vector and HLL values of a BYTES key in TestQueryTopKBytesWithVectorAndHll.
    @pytest.mark.parametrize("direction", [ASC, DESC])
    def test_order_by_integer_with_other_value_types(self, direction: int):
        integers = [5, 1, 3]
        others = [1.5, [1], {"a": 1}, Vector.of_float32([1.0])]
        m_values = list(range(len(integers) + len(others) + 1))
        random.Random(4).shuffle(m_values)
        keys = []
        for i, value in enumerate(integers + others):
            bins = {"m": m_values[i]}
            if value is not None:
                bins["n"] = value
            keys.append(self._put(user_key=i, bins=bins))
        hll_key = self._key("hll")
        self.as_connection.operate(
            hll_key, [operations.write("m", m_values[-1]), hll_operations.hll_init("n", index_bit_count=4)]
        )
        keys.append(hll_key)
        other_keys = keys[len(integers):]

        top_k = len(integers) + 2
        records = self._ranked("n", INTEGER, direction, top_k)
        assert len(records) == top_k
        assert [bins["n"] for _, _, bins in records[: len(integers)]] == sorted(integers, reverse=direction == DESC)
        lowest_digests = sorted(aerospike.calc_digest(*key) for key in other_keys)[:2]
        assert [key[3] for key, _, _ in records[len(integers):]] == lowest_digests

        records = self._ranked("n", INTEGER, direction, len(keys), then_order_by=("m", INTEGER, ASC))
        other_m = [bins["m"] for _, _, bins in records[len(integers):]]
        assert other_m == sorted(other_m)
        assert len(other_m) == len(other_keys)

    # Vector Phase 1 general test plan: AO-05
    # A single run's digest tie-break is in test_query_top_k.py (PQ-02). This checks repeated runs agree.
    def test_ties_at_k_boundary_are_stable(self):
        keys = [self._put(user_key=i, bins={"n": 1}) for i in range(10)]
        self._put(user_key="top", bins={"n": 2})
        digests = [aerospike.calc_digest(*key) for key in keys]
        expected = sorted(digests)[:3]
        runs = []
        for _ in range(5):
            records = self._ranked("n", INTEGER, DESC, 4)
            ties = records[1:]
            runs.append([key[3] for key, _, _ in ties])
        assert runs == [expected] * 5

    # Vector Phase 1 general test plan: AO-10
    def test_order_by_computed_value_then_stored_bin(self):
        for n in range(15):
            self._put(user_key=n, bins={"n": n})
        offset = exp.Abs(exp.Sub(exp.IntBin("n"), 7)).compile()
        query = self._query([operations.read("n"), exp_ops.expression_read("offset", offset)])
        query.order_by("offset", INTEGER, ASC)
        query.then_order_by("n", INTEGER, DESC)
        query.top_k = 5
        ranked = [(bins["offset"], bins["n"]) for _, _, bins in query.results()]
        assert ranked == [(0, 7), (1, 8), (1, 6), (2, 9), (2, 5)]

    # Vector Phase 1 general test plan: AO-11
    # Checks random Top-K queries against a ranking computed in Python:
    # 1. Write 60 records with a group bin of 0-2, and an integer i, double d and bytes b bin that are each
    #    missing 10% of the time. The values are drawn from small ranges so that records tie.
    # 2. Run 20 queries, each with one or two of i, d and b as order keys in a random direction, a random
    #    top_k from 1 to 69 (above 60 returns every record), and an optional filter on one group.
    # 3. compare() ranks records the way the server should: by each order key in turn, with a missing bin
    #    last in either direction, then by digest, ascending. Each query must return the top_k of the
    #    filtered records in that order.
    # A failure reports the seed. Set TOP_K_RANDOM_SEED to it to replay the same records and queries.
    def test_random_order_key_combinations(self):
        seed = int(os.environ.get("TOP_K_RANDOM_SEED", random.randrange(2**32)))
        rng = random.Random(seed)
        records = {}
        for i in range(60):
            bins = {"group": rng.randrange(3)}
            if rng.random() < 0.9:
                bins["i"] = rng.randrange(-5, 5)
            if rng.random() < 0.9:
                bins["d"] = rng.choice([-1.5, 0.0, 0.25, 2.0, 1e10])
            if rng.random() < 0.9:
                bins["b"] = bytes(rng.randrange(256) for _ in range(rng.randrange(3)))
            records[self._put(user_key=i, bins=bins)] = bins
        key_types = {"i": INTEGER, "d": DOUBLE, "b": BYTES}

        def compare(first: tuple, second: tuple, order_keys: list) -> int:
            for bin_name, direction in order_keys:
                a = records[first].get(bin_name)
                b = records[second].get(bin_name)
                if a is None or b is None:
                    if a is None and b is None:
                        continue
                    return 1 if a is None else -1
                if a != b:
                    ascending = -1 if a < b else 1
                    return ascending if direction == ASC else -ascending
            first_digest = aerospike.calc_digest(*first)
            second_digest = aerospike.calc_digest(*second)
            return -1 if first_digest < second_digest else 1

        for combination in range(20):
            bin_names = rng.sample(list(key_types), rng.choice([1, 2]))
            order_keys = [(bin_name, rng.choice([ASC, DESC])) for bin_name in bin_names]
            top_k = rng.randrange(1, 70)
            group = rng.choice([None, 0, 1, 2])
            candidates = [key for key, bins in records.items() if group is None or bins["group"] == group]
            expected = sorted(candidates, key=functools.cmp_to_key(lambda a, b: compare(a, b, order_keys)))[:top_k]

            query = self._query()
            query.order_by(order_keys[0][0], key_types[order_keys[0][0]], order_keys[0][1])
            if len(order_keys) == 2:
                query.then_order_by(order_keys[1][0], key_types[order_keys[1][0]], order_keys[1][1])
            query.top_k = top_k
            policy = {}
            if group is not None:
                policy["expressions"] = exp.Eq(exp.IntBin("group"), group).compile()
            returned = [key[3] for key, _, _ in query.results(policy)]
            assert returned == [aerospike.calc_digest(*key) for key in expected], (
                f"seed {seed}, combination {combination}: order keys {order_keys}, top_k {top_k}, group {group}"
            )

    # Vector Phase 1 general test plan: AO-12
    def test_distance_order_unaffected_by_projection(self):
        random.seed(12)
        for i in range(20):
            embedding = Vector.of_float32([random.uniform(-1, 1) for _ in range(4)])
            self._put(user_key=i, bins={"id": i, "embedding": embedding})
        dist = exp_ops.expression_read("dist", distance(EUCLIDEAN_SQUARED, Vector.of_float32([0.5, -0.25, 0.75, 0.1])))

        records = self._ranked("dist", DOUBLE, ASC, top_k=5, ops=[operations.read("id"), dist])
        without_vector = [(bins["id"], bins["dist"]) for _, _, bins in records]
        ops = [operations.read("id"), operations.read("embedding"), dist]
        records = self._ranked("dist", DOUBLE, ASC, top_k=5, ops=ops)
        with_vector = [(bins["id"], bins["dist"]) for _, _, bins in records]
        assert with_vector == without_vector
        assert [d for _, d in without_vector] == sorted(d for _, d in without_vector)

        records = self._ranked("id", INTEGER, DESC, top_k=5, ops=[operations.read("id"), dist])
        by_id = [(bins["id"], bins["dist"]) for _, _, bins in records]
        assert [i for i, _ in by_id] == [19, 18, 17, 16, 15]
        assert all(d >= 0 for _, d in by_id)

    # Vector Phase 1 general test plan: AP-01, AP-13
    # Run on an SC namespace too for AP-13. Secondary-index equality with a filter expression is in
    # test_query_top_k.py (PQ-07).
    @pytest.mark.parametrize("index", ["range", "expression", "cdt-context"])
    def test_top_k_with_secondary_index(self, index: str):
        records = {i: (i % 4, (i * 7) % 40) for i in range(40)}
        for i, (group, score) in records.items():
            self._put(user_key=i, bins={"group": group, "m": {"g": group}, "score": score})
        index_name = f"gp_top_k_{index.replace('-', '_')}"
        ensure_dropped_index(self.as_connection, NAMESPACE, index_name)
        query = self._query()
        if index == "range":
            self.as_connection.index_single_value_create(
                NAMESPACE, self.set_name, bin="group", index_datatype=aerospike.INDEX_NUMERIC, name=index_name
            )
            query.where(p.between("group", 1, 2))
        elif index == "expression":
            expression = exp.IntBin("group").compile()
            self.as_connection.index_expr_create(
                NAMESPACE, self.set_name, index_type=aerospike.INDEX_TYPE_DEFAULT,
                index_datatype=aerospike.INDEX_NUMERIC, expressions=expression, name=index_name,
            )
            query.where_with_expr(expression, p.between(None, 1, 2))
        else:
            ctx = [cdt_ctx.cdt_ctx_map_key("g")]
            self.as_connection.index_single_value_create(
                NAMESPACE, self.set_name, bin="m", index_datatype=aerospike.INDEX_NUMERIC, name=index_name, ctx=ctx
            )
            query.where(p.between("m", 1, 2), ctx)
        try:
            wait_for_index(self.as_connection, NAMESPACE, index_name)
            query.order_by("score", INTEGER, DESC)
            query.top_k = 5
            returned = [bins["score"] for _, _, bins in query.results()]
        finally:
            ensure_dropped_index(self.as_connection, NAMESPACE, index_name)
        matching = [score for group, score in records.values() if group in (1, 2)]
        assert returned == sorted(matching, reverse=True)[:5]

    # Vector Phase 1 general test plan: AP-02
    def test_top_k_with_list_index_matching_several_values(self):
        for i in range(20):
            self._put(user_key=i, bins={"tags": [i % 3, i % 3 + 1, i % 3], "score": i})
        index_name = "gp_top_k_list"
        ensure_dropped_index(self.as_connection, NAMESPACE, index_name)
        self.as_connection.index_list_create(
            NAMESPACE, self.set_name, bin="tags", index_datatype=aerospike.INDEX_NUMERIC, name=index_name
        )
        try:
            wait_for_index(self.as_connection, NAMESPACE, index_name)
            query = self._query()
            query.where(p.range("tags", aerospike.INDEX_TYPE_LIST, 0, 3))
            query.order_by("score", INTEGER, DESC)
            query.top_k = 10
            returned = [bins["score"] for _, _, bins in query.results()]
        finally:
            ensure_dropped_index(self.as_connection, NAMESPACE, index_name)
        assert returned == list(range(19, 9, -1))

    # Vector Phase 1 general test plan: AP-03
    def test_top_k_over_whole_namespace(self):
        rank_bin = "gp_ns_rank"
        for i in range(5):
            self._put(user_key=i, bins={rank_bin: i})
        query = self.as_connection.query(NAMESPACE, None)
        order_top_k(query, bin_name=rank_bin, bin_type=INTEGER, direction=DESC, top_k=1000)
        records = query.results({"expressions": exp.BinExists(rank_bin).compile()})
        assert [bins[rank_bin] for _, _, bins in records] == [4, 3, 2, 1, 0]

    # Vector Phase 1 general test plan: AP-03
    def test_top_k_over_empty_set(self):
        query = self.as_connection.query(NAMESPACE, "gp_empty_set")
        order_top_k(query, bin_name="score", bin_type=INTEGER, direction=DESC, top_k=10)
        assert query.results() == []

    # Vector Phase 1 general test plan: AP-03
    def test_top_k_above_record_count(self):
        for i in range(5):
            self._put(user_key=i, bins={"score": i})
        records = self._ranked("score", INTEGER, DESC, 1000)
        assert [bins["score"] for _, _, bins in records] == [4, 3, 2, 1, 0]

    # Vector Phase 1 general test plan: AP-04
    @pytest.mark.parametrize("top_k", [1, 1000])
    def test_top_k_at_limits(self, top_k: int):
        scores = list(range(1100))
        random.Random(top_k).shuffle(scores)
        keys = [self._key(i) for i in range(len(scores))]
        self.as_connection.batch_write(
            br.BatchRecords([br.Write(key, [operations.write("score", score)]) for key, score in zip(keys, scores)])
        )
        records = self._ranked("score", INTEGER, DESC, top_k)
        assert [bins["score"] for _, _, bins in records] == list(range(1099, 1099 - top_k, -1))

    # Vector Phase 1 general test plan: AP-07
    def test_streams_stopped_at_first_record(self):
        for i in range(50):
            self._put(user_key=i, bins={"score": i})

        def first_record() -> list:
            records = []

            def stop(record: tuple) -> bool:
                records.append(record)
                return False

            query = self._query()
            query.order_by("score", INTEGER, DESC)
            query.top_k = 20
            query.foreach(stop)
            return records

        with ThreadPoolExecutor(max_workers=20) as pool:
            futures = [pool.submit(first_record) for _ in range(20)]
            outcomes = [future.result(timeout=60) for future in futures]
        assert [len(records) for records in outcomes] == [1] * 20
        records = self._ranked("score", INTEGER, DESC, 20)
        assert [bins["score"] for _, _, bins in records] == list(range(49, 29, -1))

    # Vector Phase 1 general test plan: AP-08
    @pytest.mark.parametrize(
        "replica",
        [
            aerospike.POLICY_REPLICA_SEQUENCE,
            aerospike.POLICY_REPLICA_MASTER,
            aerospike.POLICY_REPLICA_ANY,
            aerospike.POLICY_REPLICA_PREFER_RACK,
        ],
        ids=["sequence", "master", "any", "prefer-rack"],
    )
    def test_top_k_with_replica_policy(self, replica: int):
        for i in range(30):
            self._put(user_key=i, bins={"score": i})
        query = self._query()
        query.order_by("score", INTEGER, DESC)
        query.top_k = 10
        records = query.results({"replica": replica})
        assert [bins["score"] for _, _, bins in records] == list(range(29, 19, -1))

    # Vector Phase 1 general test plan: AP-08
    # The C client doesn't support the RANDOM replica policy on a query.
    def test_top_k_with_random_replica_policy_is_refused(self):
        query = self._query()
        query.order_by("score", INTEGER, DESC)
        query.top_k = 10
        with pytest.raises(e.ParamError) as excinfo:
            query.results({"replica": aerospike.POLICY_REPLICA_RANDOM})
        assert excinfo.value.msg == "Invalid replica: AS_POLICY_REPLICA_RANDOM"

    # Vector Phase 1 general test plan: AP-09
    @pytest.mark.parametrize("top_k", [5, 20])
    def test_top_k_records_are_exact_and_not_padded(self, top_k: int):
        generations = {}
        for i in range(8):
            key = self._put(user_key=i, bins={"score": i, "embedding": Vector.of_float32([i])})
            generations[bytes(aerospike.calc_digest(*key))] = (i, 1)
        for i in (6, 7):
            key = (NAMESPACE, self.set_name, i)
            self.as_connection.put(key, {"score": i})
            generations[bytes(aerospike.calc_digest(*key))] = (i, 2)

        records = self._ranked("score", INTEGER, DESC, top_k)
        assert len(records) == min(top_k, 8)
        for (_, _, _, digest), meta, bins in records:
            i, gen = generations[bytes(digest)]
            assert meta["gen"] == gen
            assert bins == {"score": i, "embedding": Vector.of_float32([i])}
        assert [bins["score"] for _, _, bins in records] == list(range(7, 7 - len(records), -1))

    # Vector Phase 1 general test plan: AU-02
    @pytest.mark.parametrize(
        "function, args, detail",
        [
            (
                "write_bin",
                ["embedding", bytes.__new__(Vector, NAN_FLOAT32_VECTOR)],
                "vector: elements must be finite (no NaN or infinity)",
            ),
            ("write_truncated_vector", ["embedding"], "vector: value size 3 smaller than 6-byte header"),
        ],
        ids=["nan", "malformed"],
    )
    def test_udf_writing_invalid_vector(self, vector_udf: str, function: str, args: list, detail: str):
        original = {"embedding": Vector.of_float32([9.0, 8.0])}
        key = self._put(user_key="udf", bins=original)
        with pytest.raises(e.InvalidRequest) as excinfo:
            self.as_connection.apply(key, module=vector_udf, function=function, args=args, policy=DETAIL_POLICY)
        assert_server_detail(excinfo.value, detail)
        assert self.as_connection.get(key)[2] == original

    # Vector Phase 1 general test plan: AU-04
    def test_batch_udf_with_vector_arguments(self, vector_udf: str):
        valid = Vector.of_float32([1.0, 2.0])
        arguments = [valid, bytes.__new__(Vector, NAN_FLOAT32_VECTOR), Vector.of_int32([3])]
        keys = [self._key(i) for i in range(len(arguments))]
        applies = [
            br.Apply(key, module=vector_udf, function="write_bin", args=["embedding", value])
            for key, value in zip(keys, arguments)
        ]
        batch = self.as_connection.batch_write(br.BatchRecords(applies))
        assert [record.result for record in batch.batch_records] == [0, e.InvalidRequest.code, 0]
        assert self.as_connection.get(keys[0])[2] == {"embedding": valid}
        assert self.as_connection.get(keys[2])[2] == {"embedding": arguments[2]}
        with pytest.raises(e.RecordNotFound):
            self.as_connection.get(keys[1])

    # Vector Phase 1 general test plan: AE-06
    # Adding an integer bin to a float bin can't be built.
    @pytest.mark.parametrize("top_k", [True, False], ids=["top-k", "plain"])
    def test_read_expression_the_server_cannot_build(self, top_k: bool):
        self._put(user_key=0, bins={"score": 1, "a": 1, "b": 1.5})
        unbuildable = exp.Add(exp.IntBin("a"), exp.FloatBin("b")).compile()
        query = self._query([operations.read("score"), exp_ops.expression_read("bad", unbuildable)])
        if top_k:
            query.order_by("score", INTEGER, DESC)
            query.top_k = 1
        with pytest.raises(e.InvalidRequest) as excinfo:
            query.results(DETAIL_POLICY)
        assert_server_detail(excinfo.value, "invalid expression in operation request")

    # Vector Phase 1 general test plan: AG-08
    # The C client returns a blob for a bytes type it doesn't know, and Python returns it as plain bytes.
    @pytest.mark.parametrize("bytes_type", [99, 200, 255])
    def test_unknown_bytes_type_is_returned_as_bytes(self, vector_udf: str, bytes_type: int):
        key = self._put(user_key="typed", bins={"a": 1})
        result = self.as_connection.apply(key, module=vector_udf, function="typed_bytes", args=[bytes_type])
        assert type(result) is bytes
        assert result == b"\x07\x09"


# ---------------------------------------------------------------------------
# Server settings - require server version 8.2.1 or later, and the sys-admin privilege to change them.
# ---------------------------------------------------------------------------


class TestGeneralPlanServerSettings:
    SET_NAME = "gp_server_settings"
    RECORD_COUNT = 20000

    @pytest.fixture(autouse=True, scope="class")
    def records(self, as_connection: aerospike.Client) -> Iterator[None]:
        keys = [(NAMESPACE, self.SET_NAME, i) for i in range(self.RECORD_COUNT)]
        for start in range(0, len(keys), 5000):
            chunk = keys[start:start + 5000]
            as_connection.batch_write(
                br.BatchRecords([br.Write(key, [operations.write("score", key[2])]) for key in chunk])
            )
        yield
        for start in range(0, len(keys), 5000):
            as_connection.batch_remove(keys[start:start + 5000])

    @pytest.fixture(autouse=True)
    def setup(self, skip_if_server_older_than_8_2_1):
        pass

    def _top_k_query(self) -> aerospike.Query:
        query = self.as_connection.query(NAMESPACE, self.SET_NAME)
        order_top_k(query, bin_name="score", bin_type=INTEGER, direction=DESC, top_k=10)
        return query

    EXPECTED = list(range(RECORD_COUNT - 1, RECORD_COUNT - 11, -1))

    # Vector Phase 1 general test plan: AE-02
    def test_rejection_without_error_details(self):
        query = self.as_connection.query(NAMESPACE, self.SET_NAME)
        query.order_by("score", INTEGER, DESC)
        details_off = set_server_dynamic_config(
            self.as_connection, context=SERVICE_CONTEXT, name="error-details-max-verbosity", value="off"
        )
        with details_off:
            with pytest.raises(e.InvalidRequest) as excinfo:
                query.results(DETAIL_POLICY)
        assert excinfo.value.msg.endswith(" AEROSPIKE_ERR_REQUEST_INVALID")

    def _max_run_time_of_1_ms(self) -> Any:
        return set_server_dynamic_config(
            self.as_connection, context=TEST_NAMESPACE_CONTEXT, name="query-max-run-time-ms", value="1"
        )

    # Vector Phase 1 general test plan: AE-07
    def test_top_k_past_query_max_run_time(self):
        with self._max_run_time_of_1_ms():
            with pytest.raises(e.TimeoutError):
                self._top_k_query().results({"max_retries": 0})

    # Vector Phase 1 general test plan: AE-07
    # Each retry rescans every node, so the client shouldn't retry a Top-K query the server stopped.
    @pytest.mark.xfail(strict=True, reason="The C client retries a Top-K query the server stopped (CLIENT-5778)")
    def test_top_k_past_query_max_run_time_is_not_retried(self):
        with self._max_run_time_of_1_ms():
            with pytest.raises(e.TimeoutError) as excinfo:
                self._top_k_query().results({"max_retries": 2})
        attempts = [line for line in excinfo.value.msg.splitlines() if ",AEROSPIKE_ERR_TIMEOUT" in line]
        assert len(attempts) <= 1

    # Vector Phase 1 general test plan: AE-08
    def test_top_k_jobs_over_the_limit(self):
        def run() -> Any:
            try:
                records = self._top_k_query().results({"max_retries": 0})
                return [bins["score"] for _, _, bins in records]
            except e.ForbiddenError as error:
                return error

        with set_server_dynamic_config(self.as_connection, context=SERVICE_CONTEXT, name="top-k-max-jobs", value="1"):
            with ThreadPoolExecutor(max_workers=16) as pool:
                futures = [pool.submit(run) for _ in range(16)]
                outcomes = [future.result(timeout=120) for future in futures]
        for outcome in outcomes:
            if isinstance(outcome, e.ForbiddenError):
                assert outcome.code == e.ForbiddenError.code
            else:
                assert outcome == self.EXPECTED
        records = self._top_k_query().results()
        assert [bins["score"] for _, _, bins in records] == self.EXPECTED


# ---------------------------------------------------------------------------
# Strong consistency - require server version 8.2.1 or later and a strong consistency namespace.
# ---------------------------------------------------------------------------


class TestGeneralPlanStrongConsistency:
    SET_NAME = "gp_strong_consistency"

    @pytest.fixture(autouse=True)
    def setup(self, as_connection: aerospike.Client, skip_if_server_older_than_8_2_1) -> Iterator[None]:
        if not TestBaseClass.strong_consistency_enabled:
            pytest.skip("Needs a strong consistency namespace")
        self.keys = [(NAMESPACE, self.SET_NAME, i) for i in range(3)]
        yield
        for key in self.keys:
            try:
                as_connection.remove(key)
            except e.RecordNotFound:
                pass

    # Vector Phase 1 general test plan: AW-03
    # The other read paths are listed at TestGeneralPlanIntegration::test_reads_without_deserialize_return_vector.
    def test_transaction_read_returns_vector(self):
        original = Vector.of_float32([9.0, 8.0])
        self.as_connection.put(self.keys[0], {"embedding": original})
        vector = Vector.of_float16([1.0, 2.0])
        txn = aerospike.Transaction()
        policy = {"txn": txn}
        try:
            self.as_connection.put(self.keys[0], {"embedding": vector}, policy=policy)
            read = self.as_connection.get(self.keys[0], policy=policy)[2]["embedding"]
            assert isinstance(read, Vector)
            assert bytes(read) == bytes(vector)
            assert bytes(self.as_connection.get(self.keys[0])[2]["embedding"]) == bytes(original)
        except Exception:
            self.as_connection.abort(transaction=txn)
            raise
        assert self.as_connection.commit(transaction=txn) == aerospike.COMMIT_OK
        assert bytes(self.as_connection.get(self.keys[0])[2]["embedding"]) == bytes(vector)

    # Vector Phase 1 general test plan: AW-13
    def test_linearizable_reads_return_last_written_vector(self):
        policy = {"read_mode_sc": aerospike.POLICY_READ_MODE_SC_LINEARIZE}
        for i in range(10):
            vector = Vector.of_float32([i, -i])
            self.as_connection.put(self.keys[0], {"embedding": vector})
            assert bytes(self.as_connection.get(self.keys[0], policy=policy)[2]["embedding"]) == bytes(vector)

    # Vector Phase 1 general test plan: AW-14
    # A query isn't part of a transaction, so it ignores the policy's txn (the txn docs in client.rst).
    def test_top_k_ranks_committed_vector(self):
        stored = {0: [10.0, 10.0], 1: [1.0, 1.0], 2: [5.0, 5.0]}
        for key, values in zip(self.keys, stored.values()):
            self.as_connection.put(key, {"id": key[2], "embedding": Vector.of_float32(values)})
        txn = aerospike.Transaction()

        def nearest(policy: Optional[dict] = None) -> int:
            query = self.as_connection.query(NAMESPACE, self.SET_NAME)
            dist = distance(EUCLIDEAN_SQUARED, Vector.of_float32([0.0, 0.0]))
            query.add_ops([operations.read("id"), exp_ops.expression_read("dist", dist)])
            query.order_by("dist", DOUBLE, ASC)
            query.top_k = 1
            return query.results(policy)[0][2]["id"]

        try:
            self.as_connection.put(self.keys[0], {"embedding": Vector.of_float32([0.0, 0.0])}, policy={"txn": txn})
            assert nearest() == 1
            assert nearest({"txn": txn}) == 1
        except Exception:
            self.as_connection.abort(transaction=txn)
            raise
        assert self.as_connection.commit(transaction=txn) == aerospike.COMMIT_OK
        assert nearest() == 0
