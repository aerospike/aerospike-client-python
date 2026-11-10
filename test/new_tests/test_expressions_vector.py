# -*- coding: utf-8 -*-
import math
import random
from typing import Optional

import pytest

import aerospike
import aerospike_helpers.expressions as exp
from aerospike_helpers import Vector
from aerospike_helpers.expressions import vector as vector_exp
from aerospike_helpers.operations import expression_operations as exp_ops
from aerospike_helpers.operations import operations as base_ops

# ---------------------------------------------------------------------------
# Unit tests - no server required. Verifies VectorBin/VectorDistance compile
# to the expected shape, purely offline (no as_connection fixture), since
# expression compile() is a pure-Python operation with no C-extension call
# involved.
# ---------------------------------------------------------------------------


class TestVectorExpressionsUnit(object):
    def test_vector_bin_compiles(self):
        compiled = exp.VectorBin("embedding").compile()
        assert compiled == [(aerospike_bin_op(), 10, {"bin": "embedding"}, 0)]

    @pytest.mark.parametrize(
        "metric,expected_op",
        [
            (vector_exp.VectorDistanceMetric.EUCLIDEAN_SQUARED, 52),
            (vector_exp.VectorDistanceMetric.DOT_PRODUCT, 53),
            (vector_exp.VectorDistanceMetric.COSINE_SIMILARITY, 54),
        ],
    )
    def test_vector_distance_compiles_to_expected_opcode(self, metric, expected_op):
        query_vector = Vector.of_float32([0.1, 0.2, 0.3, 0.4])
        compiled = vector_exp.VectorDistance(metric, query_vector, exp.VectorBin("embedding")).compile()

        # [0]: the VectorDistance op itself, with 2 children.
        assert compiled[0][0] == expected_op
        assert compiled[0][3] == 2

        # [1]: the query-vector bytes value (a plain VAL child).
        assert compiled[1][2]["val"] == bytes(query_vector)

        # [2]: the vector-bin child, generically typed as VECTOR (10).
        assert compiled[2][1] == 10
        assert compiled[2][2] == {"bin": "embedding"}

    def test_vector_distance_accepts_plain_bin_name(self):
        query_vector = Vector.of_float32([0.1, 0.2])
        compiled = vector_exp.VectorDistance(
            vector_exp.VectorDistanceMetric.EUCLIDEAN_SQUARED, query_vector, "embedding"
        ).compile()
        assert compiled[2][2] == {"bin": "embedding"}

    def test_unknown_metric_raises_value_error(self):
        query_vector = Vector.of_float32([0.1, 0.2])
        with pytest.raises(ValueError, match=r"^Unknown VectorDistanceMetric: 999$"):
            vector_exp.VectorDistance(999, query_vector, "embedding")

    @pytest.mark.parametrize("bad_query", [4, [0.1, 0.2], b"\x01\x03\x01\x00\x00\x00\x00\x00\x00\x00", None])
    def test_non_vector_query_raises_type_error(self, bad_query):
        with pytest.raises(TypeError, match=rf"^VectorDistance query must be a Vector; got {type(bad_query).__name__}$"):
            vector_exp.VectorDistance(vector_exp.VectorDistanceMetric.EUCLIDEAN_SQUARED, bad_query, "embedding")


def euclidean_squared(a, b):
    return sum((x - y) ** 2 for x, y in zip(a, b))


def dot_product(a, b):
    return sum(x * y for x, y in zip(a, b))


def cosine_similarity(a, b):
    return dot_product(a, b) / math.sqrt(dot_product(a, a) * dot_product(b, b))


def aerospike_bin_op():
    # _AS_EXP_CODE_BIN's Python-side op value, used by VectorBin/etc.
    from aerospike_helpers.expressions.resources import _ExprOp

    return _ExprOp.BIN


# ---------------------------------------------------------------------------
# Integration tests - require server version 8.2.1 or later for vectors and
# order_by/top_k.
# ---------------------------------------------------------------------------


class TestVectorExpressionsIntegration(object):
    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection, skip_if_server_older_than_8_2_1):
        self.test_ns = "test"
        self.test_set = "test_vector_exp"
        self.test_keys = []

        def teardown():
            for key in self.test_keys:
                try:
                    as_connection.remove(key)
                except aerospike.exception.RecordNotFound:
                    pass

        request.addfinalizer(teardown)

    def _put(self, i, embedding):
        key = (self.test_ns, self.test_set, i)
        self.test_keys.append(key)
        self.as_connection.put(key, {"id": i, "embedding": embedding})
        return key

    def test_vector_distance_as_filter_expression(self):
        # Cosine similarity of a vector against itself is 1.0.
        v = Vector.of_float32([0.6, 0.8, 0.0, 0.0])
        self._put("self-match", v)

        dist_expr = vector_exp.VectorDistance(
            vector_exp.VectorDistanceMetric.COSINE_SIMILARITY, v, exp.VectorBin("embedding")
        ).compile()

        # Project the distance as a named result bin via expression_read().
        _, _, res = self.as_connection.operate(
            (self.test_ns, self.test_set, "self-match"),
            [exp_ops.expression_read("dist", dist_expr)],
        )
        assert res["dist"] == pytest.approx(1.0, abs=1e-4)

    @pytest.mark.parametrize(
        "metric, expected",
        [
            (vector_exp.VectorDistanceMetric.EUCLIDEAN_SQUARED, 10.0),
            (vector_exp.VectorDistanceMetric.DOT_PRODUCT, 4.0),
            (vector_exp.VectorDistanceMetric.COSINE_SIMILARITY, 4 / 9),
        ],
    )
    def test_vector_distance_matches_hand_computed_value(self, metric, expected):
        # Both vectors have length 3: (1 - 2)^2 + (2 - 0)^2 + (2 - 1)^2 + (0 - 2)^2 = 10,
        # 1*2 + 2*0 + 2*1 + 0*2 = 4, and 4 / (3 * 3) = 4/9.
        key = self._put("hand-computed", Vector.of_float32([1, 2, 2, 0]))
        dist_expr = vector_exp.VectorDistance(
            metric, Vector.of_float32([2, 0, 1, 2]), exp.VectorBin("embedding")
        ).compile()

        _, _, bins = self.as_connection.operate(key, [exp_ops.expression_read("dist", dist_expr)])
        assert bins["dist"] == pytest.approx(expected, rel=1e-6)

    def _set_vector_distance_precision(self, precision: str) -> None:
        responses = self.as_connection.info_all(f"set-config:context=service;vector-distance-precision={precision}")
        for error, response in responses.values():
            assert error is None
            assert response.strip() == "ok"

    # Since server 8.2.1, a distance comes back at the precision of the server's accumulator. The setting is
    # dynamic, so each value is set on every node for the test and the original is restored afterwards.
    @pytest.fixture(params=["float", "double"])
    def vector_distance_precision(self, request) -> str:
        service_config = self.as_connection.info_random_node("get-config:context=service")
        prefix = "vector-distance-precision="
        original = next(item[len(prefix):] for item in service_config.split(";") if item.startswith(prefix))
        self._set_vector_distance_precision(request.param)
        yield request.param
        self._set_vector_distance_precision(original)

    # CLIENT-5845 test plan: PE-01
    @pytest.mark.parametrize(
        "metric, expected",
        [
            (vector_exp.VectorDistanceMetric.EUCLIDEAN_SQUARED, 27.0),
            (vector_exp.VectorDistanceMetric.DOT_PRODUCT, 32.0),
            (vector_exp.VectorDistanceMetric.COSINE_SIMILARITY, 32 / math.sqrt(14 * 77)),
        ],
    )
    def test_vector_distance_reference_values(self, vector_distance_precision, metric, expected):
        key = self._put("reference", Vector.of_float32([1, 2, 3]))
        dist = vector_exp.VectorDistance(metric, Vector.of_float32([4, 5, 6]), exp.VectorBin("embedding"))
        rel = 1e-12 if vector_distance_precision == "double" else 1e-6

        _, _, bins = self.as_connection.operate(key, [exp_ops.expression_read("dist", dist.compile())])
        assert bins["dist"] == pytest.approx(expected, rel=rel)

        margin = expected * rel
        in_range = exp.And(exp.GE(dist, expected - margin), exp.LE(dist, expected + margin)).compile()
        _, _, bins = self.as_connection.get(key, policy={"expressions": in_range})
        assert bins["embedding"] == Vector.of_float32([1, 2, 3])

        above = exp.GT(dist, expected + margin).compile()
        with pytest.raises(aerospike.exception.FilteredOut):
            self.as_connection.get(key, policy={"expressions": above})

    # CLIENT-5845 test plan: PE-01
    @pytest.mark.parametrize("stored, query", [([0, 0, 0], [4, 5, 6]), ([1, 2, 3], [0, 0, 0])])
    def test_cosine_similarity_with_zero_vector_is_zero(self, stored, query):
        key = self._put("zero", Vector.of_float32(stored))
        dist_expr = vector_exp.VectorDistance(
            vector_exp.VectorDistanceMetric.COSINE_SIMILARITY, Vector.of_float32(query), exp.VectorBin("embedding")
        ).compile()
        _, _, bins = self.as_connection.operate(key, [exp_ops.expression_read("dist", dist_expr)])
        assert bins["dist"] == 0.0

    # CLIENT-5845 test plan: PE-07
    def test_expression_read_of_vector_bin_returns_vector(self):
        vector = Vector.of_float32([0.25, -0.5, 1.0])
        key = self._put("vector-bin", vector)
        vector_bin = exp.VectorBin("embedding").compile()
        _, _, bins = self.as_connection.operate(key, [exp_ops.expression_read("copy", vector_bin)])
        assert isinstance(bins["copy"], Vector)
        assert bytes(bins["copy"]) == bytes(vector)

    @pytest.mark.parametrize(
        "metric, distance, direction",
        [
            (vector_exp.VectorDistanceMetric.EUCLIDEAN_SQUARED, euclidean_squared, aerospike.QueryOrder.ASCENDING),
            (vector_exp.VectorDistanceMetric.DOT_PRODUCT, dot_product, aerospike.QueryOrder.DESCENDING),
            (vector_exp.VectorDistanceMetric.COSINE_SIMILARITY, cosine_similarity, aerospike.QueryOrder.DESCENDING),
        ],
    )
    def test_knn_top_k_query_matches_bruteforce(self, metric, distance, direction):
        random.seed(1234)
        stored = {}
        for i in range(20):
            vector = Vector.of_float32([random.uniform(-1, 1) for _ in range(4)])
            stored[i] = list(vector.value)
            self._put(i, vector)

        query_vector = Vector.of_float32([0.5, -0.25, 0.75, 0.1])
        query_values = list(query_vector.value)
        expected = sorted(
            stored, key=lambda i: distance(stored[i], query_values), reverse=direction == aerospike.QueryOrder.DESCENDING
        )[:5]

        dist_expr = vector_exp.VectorDistance(metric, query_vector, exp.VectorBin("embedding")).compile()
        query = self.as_connection.query(self.test_ns, self.test_set)
        query.add_ops([base_ops.read("id"), exp_ops.expression_read("dist", dist_expr)])
        query.order_by("dist", aerospike.QueryOrderByType.DOUBLE, direction)
        query.top_k = 5
        records = query.results()

        assert [bins["id"] for _, _, bins in records] == expected
        for _, _, bins in records:
            assert bins["dist"] == pytest.approx(distance(stored[bins["id"]], query_values), rel=1e-5, abs=1e-6)

    def _top_k_distances(
        self,
        metric: vector_exp.VectorDistanceMetric,
        query_vector: Vector,
        direction: aerospike.QueryOrder,
        flags: int = aerospike.EXP_READ_DEFAULT,
    ) -> list[tuple[int, Optional[float]]]:
        dist_expr = vector_exp.VectorDistance(metric, query_vector, exp.VectorBin("embedding")).compile()
        query = self.as_connection.query(self.test_ns, self.test_set)
        query.add_ops([base_ops.read("id"), exp_ops.expression_read("dist", dist_expr, flags)])
        query.order_by("dist", aerospike.QueryOrderByType.DOUBLE, direction)
        query.top_k = len(self.test_keys)
        return [(bins["id"], bins["dist"]) for _, _, bins in query.results()]

    # CLIENT-5845 test plan: PE-05
    # The far record's squared distance overflows a double, so it is +Inf: last ascending and first
    # descending.
    @pytest.mark.parametrize("direction", [aerospike.QueryOrder.ASCENDING, aerospike.QueryOrder.DESCENDING])
    def test_top_k_ranks_overflowing_squared_distance_as_infinity(self, direction):
        stored = {0: [0.0, 0.0], 1: [1.0, 2.0], 2: [-3.0, 0.5], 3: [1e200, 1e200]}
        for i, values in stored.items():
            self._put(i, Vector.of_float64(values))
        query_values = [0.5, 0.5]

        def squared_distance(values: list[float]) -> float:
            return sum((x - y) * (x - y) for x, y in zip(values, query_values))

        descending = direction == aerospike.QueryOrder.DESCENDING
        expected = sorted(stored, key=lambda i: squared_distance(stored[i]), reverse=descending)
        assert expected[0 if descending else -1] == 3

        results = self._top_k_distances(
            vector_exp.VectorDistanceMetric.EUCLIDEAN_SQUARED, Vector.of_float64(query_values), direction
        )
        assert [i for i, _ in results] == expected
        assert dict(results)[3] == math.inf

    # CLIENT-5845 test plan: PE-03, PE-05
    # The huge record's dot product with the query overflows a double, so its distance is unknown: a
    # single-record read fails, and a query leaves the record out, or returns it with None and ranks it
    # last in either direction under EXP_READ_EVAL_NO_FAIL.
    @pytest.mark.parametrize("direction", [aerospike.QueryOrder.ASCENDING, aerospike.QueryOrder.DESCENDING])
    def test_top_k_with_overflowing_dot_product(self, direction):
        stored = {0: [1.0, 1.0], 1: [2.0, -1.0], 2: [-3.0, 0.5], 3: [1e200, 1e200]}
        for i, values in stored.items():
            self._put(i, Vector.of_float64(values))
        query_vector = Vector.of_float64([1e200, 1e200])
        metric = vector_exp.VectorDistanceMetric.DOT_PRODUCT

        dist_expr = vector_exp.VectorDistance(metric, query_vector, exp.VectorBin("embedding")).compile()
        with pytest.raises(aerospike.exception.OpNotApplicable):
            self.as_connection.operate((self.test_ns, self.test_set, 3), [exp_ops.expression_read("dist", dist_expr)])

        descending = direction == aerospike.QueryOrder.DESCENDING
        finite = sorted([0, 1, 2], key=lambda i: dot_product(stored[i], [1e200, 1e200]), reverse=descending)

        results = self._top_k_distances(metric, query_vector, direction)
        assert [i for i, _ in results] == finite

        results = self._top_k_distances(metric, query_vector, direction, aerospike.EXP_READ_EVAL_NO_FAIL)
        assert results[:-1] == [(i, pytest.approx(dot_product(stored[i], [1e200, 1e200]))) for i in finite]
        assert results[-1] == (3, None)
