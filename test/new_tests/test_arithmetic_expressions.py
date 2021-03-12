# -*- coding: utf-8 -*-

import pytest
import sys
from .test_base_class import TestBaseClass
from aerospike import exception as e
from .as_status_codes import AerospikeStatus
from aerospike_helpers import cdt_ctx
from aerospike_helpers.expressions import *
from aerospike_helpers.operations import operations
from math import sqrt, ceil, floor
from aerospike_helpers.expressions import arithmetic
from aerospike import exception as e

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)


class TestExpressions(TestBaseClass):

    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        self.test_ns = 'test'
        self.test_set = 'demo'

        self.key = ('test', u'demo', 5)
        self.rec = {
            'name': 'name5',
            'fbin': 5.0,
            'ibin': 5
        }
        self.as_connection.put(self.key, self.rec)

        def teardown():
            as_connection.remove(self.key)

        request.addfinalizer(teardown)

    def verify_expression(self, expr, expected):
        _, _, res = self.as_connection.get(self.key, policy={'expressions': expr})
        assert res == self.rec

    def verify_expression_neg(self, expr, expected):
        with pytest.raises(expected):
            _, _, res = self.as_connection.get(self.key, policy={'expressions': expr})

    @pytest.mark.parametrize("bin, val, check", [
        (IntBin("ibin"), [5], 10),
        (IntBin("ibin"), [5, 100], 110),
        (FloatBin("fbin"), [3.0], 8.0)
    ])
    def test_add_pos(self, bin, val, check):
        """
        Test arithemtic Add expression with correct parameters.
        """
        expr = Eq(arithmetic.Add(bin, *val),
                    check).compile()
        
        self.verify_expression(expr, self.rec)

    @pytest.mark.parametrize("bin, val, check, expected", [
        (IntBin("ibin"), [5], 25, e.FilteredOut),
        (IntBin("ibin"), [5.0], 10, e.InvalidRequest),
        (FloatBin("fbin"), [3], 8.0, e.InvalidRequest),
        (FloatBin("fbin"), ["bad_arg"], 8.0, e.InvalidRequest)
    ])
    def test_add_neg(self, bin, val, check, expected):
        """
        Test arithemtic Add expression expecting failure.
        """
        expr = Eq(arithmetic.Add(bin, *val),
                    check).compile()
        
        self.verify_expression_neg(expr, expected)

    @pytest.mark.parametrize("bin, val, check", [
        (IntBin("ibin"), [5], 0),
        (IntBin("ibin"), [5, 5], -5),
        (IntBin("ibin"), [IntBin("ibin")], 0),
        (FloatBin("fbin"), [3.0, 1.0], 1.0)
    ])
    def test_sub_pos(self, bin, val, check):
        """
        Test arithemtic Sub expression with correct parameters.
        """
        expr = Eq(arithmetic.Sub(bin, *val),
                    check).compile()
        
        self.verify_expression(expr, self.rec)

    @pytest.mark.parametrize("bin, val, check, expected", [
        (IntBin("ibin"), [5], 25, e.FilteredOut),
        (IntBin("ibin"), [5.0], 0, e.InvalidRequest),
        (FloatBin("fbin"), [3], 2.0, e.InvalidRequest),
        (FloatBin("fbin"), ["bad_arg"], 8.0, e.InvalidRequest)
    ])
    def test_sub_neg(self, bin, val, check, expected):
        """
        Test arithemtic Sub expression expecting failure.
        """
        expr = Eq(arithmetic.Sub(bin, *val),
                    check).compile()
        
        self.verify_expression_neg(expr, expected)

    @pytest.mark.parametrize("bin, val, check", [
        (IntBin("ibin"), [5], 25),
        (IntBin("ibin"), [5, 2], 50),
        (IntBin("ibin"), [IntBin("ibin")], 25),
        (FloatBin("fbin"), [3.0], 15.0)
    ])
    def test_mul_pos(self, bin, val, check):
        """
        Test arithemtic Mul expression with correct parameters.
        """
        expr = Eq(arithmetic.Mul(bin, *val),
                    check).compile()
        
        self.verify_expression(expr, self.rec)

    @pytest.mark.parametrize("bin, val, check, expected", [
        (IntBin("ibin"), [5], 5, e.FilteredOut),
        (IntBin("ibin"), [5.0], 25, e.InvalidRequest),
        (FloatBin("fbin"), [3], 15.0, e.InvalidRequest),
        (FloatBin("fbin"), ["bad_arg"], 8.0, e.InvalidRequest)
    ])
    def test_mul_neg(self, bin, val, check, expected):
        """
        Test arithemtic Mul expression expecting failure.
        """
        expr = Eq(arithmetic.Mul(bin, *val),
                    check).compile()
        
        self.verify_expression_neg(expr, expected)

    @pytest.mark.parametrize("bin, val, check", [
        (IntBin("ibin"), [5], 1),
        (FloatBin("fbin"), [5.0, 2.0], 0.5),
        (IntBin("ibin"), [IntBin("ibin")], 1),
        (FloatBin("fbin"), [3.0], 1.6666666666666667)
    ])
    def test_div_pos(self, bin, val, check):
        """
        Test arithemtic Div expression with correct parameters.
        """
        expr = Eq(arithmetic.Div(bin, *val),
                    check).compile()
        
        self.verify_expression(expr, self.rec)

    @pytest.mark.parametrize("bin, val, check, expected", [
        (IntBin("ibin"), [5], 25, e.FilteredOut),
        (IntBin("ibin"), [5.0], 25, e.InvalidRequest),
        (FloatBin("fbin"), [3], 15.0, e.InvalidRequest),
        (FloatBin("fbin"), ["bad_arg"], 8.0, e.InvalidRequest)
    ])
    def test_div_neg(self, bin, val, check, expected):
        """
        Test arithemtic Div expression expecting failure.
        """
        expr = Eq(arithmetic.Div(bin, *val),
                    check).compile()
        
        self.verify_expression_neg(expr, expected)

    @pytest.mark.parametrize("bin, val, check", [
        (FloatBin("fbin"), [2.0], 25.0),
        (FloatBin("fbin"), [FloatBin("fbin")], 3125.0)
    ])
    def test_pow_pos(self, bin, val, check):
        """
        Test arithemtic Pow expression with correct parameters.
        """
        expr = Eq(arithmetic.Pow(bin, *val),
                    check).compile()
        
        self.verify_expression(expr, self.rec)

    @pytest.mark.parametrize("bin, val, check, expected", [
        (IntBin("ibin"), [2], 25, e.InvalidRequest),
        (IntBin("ibin"), [IntBin("ibin")], 1, e.InvalidRequest),
        (FloatBin("fbin"), [2.0], 26.0, e.FilteredOut),
        (IntBin("ibin"), [5.0], 25, e.InvalidRequest),
        (FloatBin("fbin"), [3], 15.0, e.InvalidRequest),
        (FloatBin("fbin"), ["bad_arg"], 8.0, e.InvalidRequest)
    ])
    def test_pow_neg(self, bin, val, check, expected):
        """
        Test arithemtic Pow expression expecting failure.
        """
        expr = Eq(arithmetic.Pow(bin, *val),
                    check).compile()
        
        self.verify_expression_neg(expr, expected)

    @pytest.mark.parametrize("bin, val, check", [
        (arithmetic.Pow(FloatBin("fbin"), 4.0), [5.0], 4.0),
        (arithmetic.Pow(FloatBin("fbin"), 10.0), [FloatBin("fbin")], 10.0)
    ])
    def test_log_pos(self, bin, val, check):
        """
        Test arithemtic Log expression with correct parameters.
        """
        expr = Eq(arithmetic.Log(bin, *val),
                    check).compile()
        
        self.verify_expression(expr, self.rec)

    @pytest.mark.parametrize("bin, val, check, expected", [
        (IntBin("ibin"), [2], 25, e.InvalidRequest),
        (IntBin("ibin"), [IntBin("ibin")], 1, e.InvalidRequest),
        (FloatBin("fbin"), [2.0], 26.0, e.FilteredOut),
        (IntBin("ibin"), [5.0], 25, e.InvalidRequest),
        (FloatBin("fbin"), [3], 15.0, e.InvalidRequest),
        (FloatBin("fbin"), ["bad_arg"], 8.0, e.InvalidRequest)
    ])
    def test_log_neg(self, bin, val, check, expected):
        """
        Test arithemtic Log expression expecting failure.
        """
        expr = Eq(arithmetic.Log(bin, *val),
                    check).compile()
        
        self.verify_expression_neg(expr, expected)

    @pytest.mark.parametrize("bin, val, check", [
        (IntBin("ibin"), [2], 1),
        (IntBin("ibin"), [IntBin("ibin")], 0),
    ])
    def test_mod_pos(self, bin, val, check):
        """
        Test arithemtic Mod expression with correct parameters.
        """
        expr = Eq(arithmetic.Mod(bin, *val),
                    check).compile()
        
        self.verify_expression(expr, self.rec)

    @pytest.mark.parametrize("bin, val, check, expected", [
        (FloatBin("fbin"), [4.0], [5.0], e.InvalidRequest),
        (FloatBin("fbin"), [10.0], [FloatBin("fbin")], e.InvalidRequest),
        (IntBin("ibin"), [2], 0, e.FilteredOut),
        (IntBin("ibin"), [5.0], 25, e.InvalidRequest),
        (FloatBin("fbin"), [3], 15.0, e.InvalidRequest),
        (FloatBin("fbin"), ["bad_arg"], 8.0, e.InvalidRequest)
    ])
    def test_mod_neg(self, bin, val, check, expected):
        """
        Test arithemtic Mod expression expecting failure.
        """
        expr = Eq(arithmetic.Mod(bin, *val),
                    check).compile()
        
        self.verify_expression_neg(expr, expected)

    @pytest.mark.parametrize("val, check", [
        (8.953, 8.0),
        (arithmetic.Add(FloatBin("fbin"), 4.5), 9.0)
    ])
    def test_floor_pos(self, val, check):
        """
        Test arithemtic Floor expression with correct parameters.
        """
        expr = Eq(arithmetic.Floor(val),
                    check).compile()
        
        self.verify_expression(expr, self.rec)

    @pytest.mark.parametrize("val, check, expected", [
        (IntBin("ibin"), 25, e.InvalidRequest),
        (1, 1, e.InvalidRequest),
        (FloatBin("fbin"), 26.0, e.FilteredOut),
        ("bad_arg", 8.0, e.InvalidRequest)
    ])
    def test_floor_neg(self, val, check, expected):
        """
        Test arithemtic Floor expression expecting failure.
        """
        expr = Eq(arithmetic.Floor(val),
                    check).compile()
        
        self.verify_expression_neg(expr, expected)

    @pytest.mark.parametrize("val, check", [
        (8.953, 9.0),
        (arithmetic.Add(FloatBin("fbin"), 4.5), 10.0)
    ])
    def test_ceil_pos(self, val, check):
        """
        Test arithemtic Ceil expression with correct parameters.
        """
        expr = Eq(arithmetic.Ceil(val),
                    check).compile()
        
        self.verify_expression(expr, self.rec)

    @pytest.mark.parametrize("val, check, expected", [
        (IntBin("ibin"), 25, e.InvalidRequest),
        (1, 1, e.InvalidRequest),
        (FloatBin("fbin"), 26.0, e.FilteredOut),
        ("bad_arg", 8.0, e.InvalidRequest)
    ])
    def test_ceil_neg(self, val, check, expected):
        """
        Test arithemtic Ceil expression expecting failure.
        """
        expr = Eq(arithmetic.Ceil(val),
                    check).compile()
        
        self.verify_expression_neg(expr, expected)

    @pytest.mark.parametrize("val, check", [
        (8.953, 8),
        (FloatBin("fbin"), 5)
    ])
    def test_toint_pos(self, val, check):
        """
        Test arithemtic ToInt expression with correct parameters.
        """
        expr = Eq(arithmetic.ToInt(val),
                    check).compile()
        
        self.verify_expression(expr, self.rec)

    @pytest.mark.parametrize("val, check, expected", [
        (IntBin("ibin"), 5, e.InvalidRequest),
        (1, 1, e.InvalidRequest),
        (FloatBin("fbin"), 26, e.FilteredOut),
        ("bad_arg", 8.0, e.InvalidRequest)
    ])
    def test_toint_neg(self, val, check, expected):
        """
        Test arithemtic ToInt expression expecting failure.
        """
        expr = Eq(arithmetic.ToInt(val),
                    check).compile()
        
        self.verify_expression_neg(expr, expected)

    @pytest.mark.parametrize("val, check", [
        (8, 8.0),
        (IntBin("ibin"), 5.0)
    ])
    def test_tofloat_pos(self, val, check):
        """
        Test arithemtic ToFloat expression with correct parameters.
        """
        expr = Eq(arithmetic.ToFloat(val),
                    check).compile()
        
        self.verify_expression(expr, self.rec)

    @pytest.mark.parametrize("val, check, expected", [
        (FloatBin("fbin"), 5.0, e.InvalidRequest),
        (1.0, 1.0, e.InvalidRequest),
        (IntBin("ibin"), 6.0, e.FilteredOut),
        ("bad_arg", 8.0, e.InvalidRequest)
    ])
    def test_tofloat_neg(self, val, check, expected):
        """
        Test arithemtic ToFloat expression expecting failure.
        """
        expr = Eq(arithmetic.ToFloat(val),
                    check).compile()
        
        self.verify_expression_neg(expr, expected)

    @pytest.mark.parametrize("bin, val, check", [
        (IntBin("ibin"), [5, 25, 4, 35, 64, 1, 23, 2, 2, 2], 1),
        (IntBin("ibin"), [arithmetic.Add(IntBin("ibin"), 5)], 5),
        (IntBin("ibin"), [IntBin("ibin")], 5),
        (FloatBin("fbin"), [6.0, 20.0], 5.0)
    ])
    def test_min_pos(self, bin, val, check):
        """
        Test arithemtic Min expression with correct parameters.
        """
        expr = Eq(arithmetic.Min(bin, *val),
                    check).compile()
        
        self.verify_expression(expr, self.rec)

    @pytest.mark.parametrize("bin, val, check, expected", [
        (IntBin("ibin"), [5], 25, e.FilteredOut),
        (IntBin("ibin"), [5.0], 0, e.InvalidRequest),
        (FloatBin("fbin"), [3], 2.0, e.InvalidRequest),
        (FloatBin("fbin"), ["bad_arg"], 8.0, e.InvalidRequest)
    ])
    def test_min_neg(self, bin, val, check, expected):
        """
        Test arithemtic Min expression expecting failure.
        """
        expr = Eq(arithmetic.Min(bin, *val),
                    check).compile()
        
        self.verify_expression_neg(expr, expected)

    @pytest.mark.parametrize("bin, val, check", [
        (IntBin("ibin"), [5, 25, 4, 35, 64, 1, 23, 2, 2, 2], 64),
        (IntBin("ibin"), [arithmetic.Add(IntBin("ibin"), 5)], 10),
        (IntBin("ibin"), [IntBin("ibin")], 5),
        (FloatBin("fbin"), [6.0, 20.0], 20.0)
    ])
    def test_max_pos(self, bin, val, check):
        """
        Test arithemtic Max expression with correct parameters.
        """
        expr = Eq(arithmetic.Max(bin, *val),
                    check).compile()
        
        self.verify_expression(expr, self.rec)

    @pytest.mark.parametrize("bin, val, check, expected", [
        (IntBin("ibin"), [6], 5, e.FilteredOut),
        (IntBin("ibin"), [5.0], 0, e.InvalidRequest),
        (FloatBin("fbin"), [3], 2.0, e.InvalidRequest),
        (FloatBin("fbin"), ["bad_arg"], 8.0, e.InvalidRequest)
    ])
    def test_max_neg(self, bin, val, check, expected):
        """
        Test arithemtic Max expression expecting failure.
        """
        expr = Eq(arithmetic.Max(bin, *val),
                    check).compile()
        
        self.verify_expression_neg(expr, expected)