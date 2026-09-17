# -*- coding: utf-8 -*-
import pytest

import aerospike

# ---------------------------------------------------------------------------
# Query.min()/Query.max(): convenience methods built on order_by()/top_k(1)
# internally (see the vendored C client's aerospike_query_min()/
# aerospike_query_max()), to find the minimum/maximum value of a scalar bin
# across a query's result set. Uses the same server-support skip pattern as
# test_expressions_vector.py, since both methods ultimately just execute an
# order_by/top_k query.
# ---------------------------------------------------------------------------


def _skip_if_unsupported(callable_, *args, **kwargs):
    try:
        return callable_(*args, **kwargs)
    except aerospike.exception.ParamError as ex:
        msg = str(ex).lower()
        if "order_by" in msg or "top_k" in msg or "top-k" in msg:
            pytest.skip(f"Server does not support order_by/top_k: {ex}")
        raise


class TestQueryMinMaxIntegration(object):
    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        self.test_ns = "test"
        self.test_set = "test_query_min_max"
        self.test_keys = []

        for i in range(5):
            key = (self.test_ns, self.test_set, i)
            self.test_keys.append(key)
            as_connection.put(key, {"score": i * 10, "fscore": float(i) * 1.5})

        def teardown():
            for key in self.test_keys:
                try:
                    as_connection.remove(key)
                except aerospike.exception.RecordNotFound:
                    pass

        request.addfinalizer(teardown)

    def test_min_integer_bin(self):
        query = self.as_connection.query(self.test_ns, self.test_set)
        result = _skip_if_unsupported(query.min, "score", aerospike.QUERY_ORDER_BY_INTEGER)
        assert result == 0

    def test_max_integer_bin(self):
        query = self.as_connection.query(self.test_ns, self.test_set)
        result = _skip_if_unsupported(query.max, "score", aerospike.QUERY_ORDER_BY_INTEGER)
        assert result == 40

    def test_min_double_bin(self):
        query = self.as_connection.query(self.test_ns, self.test_set)
        result = _skip_if_unsupported(query.min, "fscore", aerospike.QUERY_ORDER_BY_DOUBLE)
        assert result == 0.0

    def test_max_double_bin(self):
        query = self.as_connection.query(self.test_ns, self.test_set)
        result = _skip_if_unsupported(query.max, "fscore", aerospike.QUERY_ORDER_BY_DOUBLE)
        assert result == 6.0

    def test_min_accepts_policy_argument(self):
        query = self.as_connection.query(self.test_ns, self.test_set)
        result = _skip_if_unsupported(
            query.min, "score", aerospike.QUERY_ORDER_BY_INTEGER, {"total_timeout": 2000}
        )
        assert result == 0

    def test_min_returns_none_for_empty_result_set(self):
        query = self.as_connection.query(self.test_ns, self.test_set + "-empty")
        result = _skip_if_unsupported(query.min, "score", aerospike.QUERY_ORDER_BY_INTEGER)
        assert result is None

    def test_max_returns_none_for_empty_result_set(self):
        query = self.as_connection.query(self.test_ns, self.test_set + "-empty")
        result = _skip_if_unsupported(query.max, "score", aerospike.QUERY_ORDER_BY_INTEGER)
        assert result is None

    def test_min_accepts_bin_already_covered_by_existing_select(self):
        query = self.as_connection.query(self.test_ns, self.test_set)
        query.select("score", "fscore")
        result = _skip_if_unsupported(query.min, "score", aerospike.QUERY_ORDER_BY_INTEGER)
        assert result == 0

    def test_min_rejects_bin_not_covered_by_existing_select(self):
        # This cross-field check happens client-side (in the vendored C
        # client) before any query is sent to the server, so it applies even
        # on servers without order_by/top_k support - no skip needed.
        query = self.as_connection.query(self.test_ns, self.test_set)
        query.select("fscore")
        with pytest.raises(aerospike.exception.ParamError):
            query.min("score", aerospike.QUERY_ORDER_BY_INTEGER)

    def test_min_bad_bin_type_raises_param_error(self):
        query = self.as_connection.query(self.test_ns, self.test_set)
        with pytest.raises(aerospike.exception.ParamError):
            query.min(123, aerospike.QUERY_ORDER_BY_INTEGER)

    def test_min_missing_required_args_raises_param_error(self):
        query = self.as_connection.query(self.test_ns, self.test_set)
        with pytest.raises(aerospike.exception.ParamError):
            query.min("score")

    def test_max_missing_required_args_raises_param_error(self):
        query = self.as_connection.query(self.test_ns, self.test_set)
        with pytest.raises(aerospike.exception.ParamError):
            query.max("score")
