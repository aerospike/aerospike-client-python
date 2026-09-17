# -*- coding: utf-8 -*-
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
        with pytest.raises(ValueError):
            vector_exp.VectorDistance(999, query_vector, "embedding")


def aerospike_bin_op():
    # _AS_EXP_CODE_BIN's Python-side op value, used by VectorBin/etc.
    from aerospike_helpers.expressions.resources import _ExprOp

    return _ExprOp.BIN


# ---------------------------------------------------------------------------
# Integration tests - require a running server with Vector Phase 1 milestone
# 1 (vector particle type) and milestone 2 (order_by/top_k) support. Skips
# (rather than hard version-gating, since there is no numbered server
# release yet at the time of writing) if the server rejects order_by/top_k
# as unsupported.
# ---------------------------------------------------------------------------


class TestVectorExpressionsIntegration(object):
    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
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
        try:
            _, _, res = self.as_connection.operate(
                (self.test_ns, self.test_set, "self-match"),
                [exp_ops.expression_read("dist", dist_expr)],
            )
        except aerospike.exception.InvalidRequest as ex:
            # Older/stock servers without Vector Phase 1's expression-engine
            # support (VECTOR-typed bin evaluation in rt_bin_translate)
            # reject this with AEROSPIKE_ERR_REQUEST_INVALID instead of
            # evaluating the expression.
            pytest.skip(f"Server does not support vector-bin expressions: {ex}")
        assert res["dist"] == pytest.approx(1.0, abs=1e-4)

    def test_knn_top_k_query_matches_bruteforce(self):
        import random

        random.seed(1234)
        truth = []
        for i in range(20):
            values = [random.uniform(-1, 1) for _ in range(4)]
            truth.append((i, values))
            self._put(i, Vector.of_float32(values))

        query_values = [0.5, 0.5, 0.5, 0.5]
        query_vector = Vector.of_float32(query_values)

        def euclidean_sq(a, b):
            return sum((x - y) ** 2 for x, y in zip(a, b))

        expected_ids = [t[0] for t in sorted(truth, key=lambda t: euclidean_sq(t[1], query_values))[:5]]

        dist_expr = vector_exp.VectorDistance(
            vector_exp.VectorDistanceMetric.EUCLIDEAN_SQUARED, query_vector, exp.VectorBin("embedding")
        ).compile()

        query = self.as_connection.query(self.test_ns, self.test_set)
        query.add_ops([base_ops.read("id"), exp_ops.expression_read("dist", dist_expr)])
        query.order_by("dist", aerospike.QUERY_ORDER_BY_DOUBLE, aerospike.QUERY_ORDER_ASCENDING)
        query.top_k = 5

        try:
            records = query.results()
        except aerospike.exception.ParamError as ex:
            if "order_by" in str(ex).lower() or "top_k" in str(ex).lower() or "top-k" in str(ex).lower():
                pytest.skip(f"Server does not support order_by/top_k: {ex}")
            raise

        assert len(records) == 5
        actual_ids = [bins["id"] for _, _, bins in records]
        assert actual_ids == expected_ids

        # Distances must come back sorted ascending (best match first) for
        # EUCLIDEAN_SQUARED, per VectorDistanceMetric's documented sort order.
        actual_dists = [bins["dist"] for _, _, bins in records]
        assert actual_dists == sorted(actual_dists)
