# -*- coding: utf-8 -*-

import sys
import math

import pytest

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)

from aerospike_helpers.expressions import *
from aerospike_helpers.operations import operations
from aerospike_helpers.expressions import arithmetic
from aerospike import exception as e
from .test_base_class import TestBaseClass

class TestExpressionsArithmetic(TestBaseClass):

    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        if self.server_version < [5, 6]:
            pytest.mark.xfail(reason="Servers older than 5.6 do not support arithmetic expressions.")
            pytest.xfail()
        
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
        Test arithmetic Add expression with correct parameters.
        """
        expr = Eq(Add(bin, *val),
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
        Test arithmetic Add expression expecting failure.
        """
        expr = Eq(Add(bin, *val),
                    check).compile()
        
        self.verify_expression_neg(expr, expected)

    @pytest.mark.parametrize("expression", [
        Eq(IntBin("ibin") + Add(IntBin("ibin"), IntBin("ibin")) + 5, 20),
        Eq(FloatBin("fbin") + Add(FloatBin("fbin"), FloatBin("fbin")) + 5.0, 20.0),
    ])
    def test_add_overloaded_pos(self, expression):
        """
        Test arithmetic Add expression with correct parameters.
        """
        self.verify_expression(expression.compile(), self.rec)

    @pytest.mark.parametrize("expression, expected", [
        (Eq(IntBin("ibin") + Add(IntBin("ibin"), IntBin("ibin")) + 5, 15), e.FilteredOut),
        (Eq(IntBin("ibin") + Add(FloatBin("fbin"), FloatBin("fbin")) + 5.0, 20.0), e.InvalidRequest)
    ])
    def test_add_overloaded_neg(self, expression, expected):
        """
        Test arithmetic Add expression with incorrect parameters.
        """
        self.verify_expression_neg(expression.compile(), expected)

    @pytest.mark.parametrize("bin, val, check", [
        (IntBin("ibin"), [5], 0),
        (IntBin("ibin"), [5, 5], -5),
        (IntBin("ibin"), [IntBin("ibin")], 0),
        (FloatBin("fbin"), [3.0, 1.0], 1.0)
    ])
    def test_sub_pos(self, bin, val, check):
        """
        Test arithmetic Sub expression with correct parameters.
        """
        expr = Eq(Sub(bin, *val),
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
        Test arithmetic Sub expression expecting failure.
        """
        expr = Eq(Sub(bin, *val),
                    check).compile()
        
        self.verify_expression_neg(expr, expected)

    @pytest.mark.parametrize("expression", [
        Eq(IntBin("ibin") - Sub(IntBin("ibin"), IntBin("ibin")) - 5, -10),
        Eq(FloatBin("fbin") - Sub(FloatBin("fbin"), FloatBin("fbin")) - 5.0, -10.0)
    ])
    def test_sub_overloaded_pos(self, expression):
        """
        Test arithmetic Sub expression with correct parameters.
        """
        self.verify_expression(expression.compile(), self.rec)

    @pytest.mark.parametrize("expression, expected", [
        (Eq(IntBin("ibin") - Sub(IntBin("ibin"), IntBin("ibin")) - 5, -5), e.FilteredOut),
        (Eq(FloatBin("fbin") - Sub(IntBin("ibin"), FloatBin("fbin")) - 5.0, -10.0), e.InvalidRequest)
    ])
    def test_sub_overloaded_neg(self, expression, expected):
        """
        Test arithmetic Sub expression with incorrect parameters.
        """
        self.verify_expression_neg(expression.compile(), expected)

    @pytest.mark.parametrize("bin, val, check", [
        (IntBin("ibin"), [5], 25),
        (IntBin("ibin"), [5, 2], 50),
        (IntBin("ibin"), [IntBin("ibin")], 25),
        (FloatBin("fbin"), [3.0], 15.0)
    ])
    def test_mul_pos(self, bin, val, check):
        """
        Test arithmetic Mul expression with correct parameters.
        """
        expr = Eq(Mul(bin, *val),
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
        Test arithmetic Mul expression expecting failure.
        """
        expr = Eq(Mul(bin, *val),
                    check).compile()
        
        self.verify_expression_neg(expr, expected)

    @pytest.mark.parametrize("expression", [
        Eq(IntBin("ibin") * Mul(IntBin("ibin"), IntBin("ibin")) * IntBin("ibin"), 625),
        Eq(FloatBin("fbin") * Mul(FloatBin("fbin"), FloatBin("fbin")) * 5.0, 625.0)
    ])
    def test_mul_overloaded_pos(self, expression):
        """
        Test arithmetic Mul expression with correct parameters.
        """
        self.verify_expression(expression.compile(), self.rec)

    @pytest.mark.parametrize("expression, expected", [
        (Eq(IntBin("ibin") * Mul(IntBin("ibin"), IntBin("ibin")) * IntBin("ibin"), 5), e.FilteredOut),
        (Eq(IntBin("ibin") * Mul(FloatBin("fbin"), FloatBin("fbin")) * 5.0, 5.0), e.InvalidRequest)
    ])
    def test_mul_overloaded_neg(self, expression, expected):
        """
        Test arithmetic Mul expression with incorrect parameters.
        """
        self.verify_expression_neg(expression.compile(), expected)

    @pytest.mark.parametrize("bin, val, check", [
        (IntBin("ibin"), [5], 1),
        (FloatBin("fbin"), [5.0, 2.0], 0.5),
        (IntBin("ibin"), [IntBin("ibin")], 1),
        (FloatBin("fbin"), [3.0], 1.6666666666666667)
    ])
    def test_div_pos(self, bin, val, check):
        """
        Test arithmetic Div expression with correct parameters.
        """
        expr = Eq(Div(bin, *val),
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
        Test arithmetic Div expression expecting failure.
        """
        expr = Eq(Div(bin, *val),
                    check).compile()
        
        self.verify_expression_neg(expr, expected)

    @pytest.mark.parametrize("expression", [
        Eq(IntBin("ibin") * 100 / Div(IntBin("ibin"), IntBin("ibin")) / IntBin("ibin"), 4),
        Eq(FloatBin("fbin") * 100.0 / Div(FloatBin("fbin"), FloatBin("fbin")) / 5.0, 4.0)
    ])
    def test_div_overloaded_pos(self, expression):
        """
        Test arithmetic Div expression with correct parameters.
        """
        self.verify_expression(expression.compile(), self.rec)

    @pytest.mark.parametrize("expression, expected", [ # Note that the parens in Div() do not overload precedence with /.
        (Eq(IntBin("ibin") * 100 / Div(IntBin("ibin"), IntBin("ibin")) / IntBin("ibin"), 3), e.FilteredOut),
        (Eq(IntBin("ibin") * 100.0 / Div(FloatBin("fbin"), FloatBin("fbin")) / 5.0, 4.0), e.InvalidRequest)
    ])
    def test_div_overloaded_neg(self, expression, expected):
        """
        Test arithmetic Div expression with incorrect parameters.
        """
        self.verify_expression_neg(expression.compile(), expected)

    @pytest.mark.parametrize("expression", [
        Eq(FloatBin("fbin") * 5.1 // 5.0, 5.0),
        Eq(FloatBin("fbin") // FloatBin("fbin") // FloatBin("fbin"), 0.0)
    ])
    def test_floor_div_overloaded_pos(self, expression):
        """
        Test arithmetic // expression with correct parameters.
        """
        self.verify_expression(expression.compile(), self.rec)

    @pytest.mark.parametrize("expression, expected", [
        (Eq(FloatBin("fbin") * 5.1 // 5.0, 5.1), e.FilteredOut),
        (Eq(IntBin("ibin") * 5.1 // 5.0, 5.0), e.InvalidRequest),
        (Eq(IntBin("ibin") * 5 // 5, 5), e.InvalidRequest)
    ])
    def test_floor_div_overloaded_neg(self, expression, expected):
        """
        Test arithmetic // expression with incorrect parameters.
        """
        self.verify_expression_neg(expression.compile(), expected)

    @pytest.mark.parametrize("bin, val, check", [
        (FloatBin("fbin"), [2.0], 25.0),
        (FloatBin("fbin"), [FloatBin("fbin")], 3125.0)
    ])
    def test_pow_pos(self, bin, val, check):
        """
        Test arithmetic Pow expression with correct parameters.
        """
        expr = Eq(Pow(bin, *val),
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
        Test arithmetic Pow expression expecting failure.
        """
        expr = Eq(Pow(bin, *val),
                    check).compile()
        
        self.verify_expression_neg(expr, expected)

    @pytest.mark.parametrize("expression", [
        Eq(FloatBin("fbin") ** FloatBin("fbin"), 3125.0),
        Eq(FloatBin("fbin") ** 5.0, 3125.0),
    ])
    def test_pow_overloaded_pos(self, expression):
        """
        Test arithmetic Pow expression with correct parameters.
        """
        self.verify_expression(expression.compile(), self.rec)

    @pytest.mark.parametrize("expression, expected", [
        (Eq(FloatBin("fbin") ** FloatBin("fbin"), 50.0), e.FilteredOut),
        (Eq(FloatBin("fbin") ** 5, 3125.0), e.InvalidRequest),
        (Eq(IntBin("ibin") ** 5.0, 3125.0), e.InvalidRequest)
    ])
    def test_pow_overloaded_neg(self, expression, expected):
        """
        Test arithmetic Pow expression with incorrect parameters.
        """
        self.verify_expression_neg(expression.compile(), expected)

    @pytest.mark.parametrize("bin, val, check", [
        (Pow(FloatBin("fbin"), 4.0), [5.0], 4.0),
        (Pow(FloatBin("fbin"), 10.0), [FloatBin("fbin")], 10.0)
    ])
    def test_log_pos(self, bin, val, check):
        """
        Test arithmetic Log expression with correct parameters.
        """
        expr = Eq(Log(bin, *val),
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
        Test arithmetic Log expression expecting failure.
        """
        expr = Eq(Log(bin, *val),
                    check).compile()
        
        self.verify_expression_neg(expr, expected)

    @pytest.mark.parametrize("bin, val, check", [
        (IntBin("ibin"), [2], 1),
        (IntBin("ibin"), [IntBin("ibin")], 0),
    ])
    def test_mod_pos(self, bin, val, check):
        """
        Test arithmetic Mod expression with correct parameters.
        """
        expr = Eq(Mod(bin, *val),
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
        Test arithmetic Mod expression expecting failure.
        """
        expr = Eq(Mod(bin, *val),
                    check).compile()
        
        self.verify_expression_neg(expr, expected)

    @pytest.mark.parametrize("expression", [
        Eq(IntBin("ibin") % IntBin("ibin"), 0),
        Eq(IntBin("ibin") % 2, 1),
    ])
    def test_mod_overloaded_pos(self, expression):
        """
        Test arithmetic Mod expression with correct parameters.
        """
        self.verify_expression(expression.compile(), self.rec)

    @pytest.mark.parametrize("expression, expected", [
        (Eq(IntBin("ibin") % IntBin("ibin"), 1), e.FilteredOut),
        (Eq(FloatBin("fbin") % 5, 0.0), e.InvalidRequest),
        (Eq(IntBin("ibin") % 5.0, 0.0), e.InvalidRequest)
    ])
    def test_mod_overloaded_neg(self, expression, expected):
        """
        Test arithmetic Mod expression with incorrect parameters.
        """
        self.verify_expression_neg(expression.compile(), expected)

    @pytest.mark.parametrize("expression", [
        Eq(abs(Sub(FloatBin("fbin"))), 5.0),
        Eq(abs(FloatBin("fbin")), 5.0),
        Eq(abs(-5.0), 5.0),
        Eq(abs(Sub(IntBin("ibin"))), 5),
        Eq(abs(IntBin("ibin")), 5),
        Eq(abs(-5), 5),
        # using the expr object
        Eq(Abs(Sub(FloatBin("fbin"))), 5.0),
        Eq(Abs(FloatBin("fbin")), 5.0),
        Eq(Abs(-5.0), 5.0),
        Eq(Abs(Sub(IntBin("ibin"))), 5),
        Eq(Abs(IntBin("ibin")), 5),
        Eq(Abs(-5), 5)
    ])
    def test_abs_pos(self, expression):
        """
        Test arithmetic Abs expression with correct parameters.
        """
        self.verify_expression(expression.compile(), self.rec)

    @pytest.mark.parametrize("expression, expected", [
        (Eq(Abs(Sub(FloatBin("fbin"))), -5.0), e.FilteredOut),
        (Eq(Abs(Sub(StrBin("bad_bin"))), -5.0), e.InvalidRequest),
        # using the expr object
        (Eq(Abs(Sub(FloatBin("fbin"))), -5.0), e.FilteredOut),
        (Eq(Abs(Sub(StrBin("bad_bin"))), -5.0), e.InvalidRequest)
    ])
    def test_abs_neg(self, expression, expected):
        """
        Test arithmetic Abs expression with incorrect parameters.
        """
        self.verify_expression_neg(expression.compile(), expected)

    @pytest.mark.parametrize("val, check", [
        (8.953, 8.0),
        (Add(FloatBin("fbin"), 4.5), 9.0)
    ])
    def test_floor_pos(self, val, check):
        """
        Test arithmetic Floor expression with correct parameters.
        """
        expr = Eq(Floor(val),
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
        Test arithmetic Floor expression expecting failure.
        """
        expr = Eq(Floor(val),
                    check).compile()
        
        self.verify_expression_neg(expr, expected)

    @pytest.mark.parametrize("expression", [
        Eq(math.floor(FloatBin("fbin") + 1.6), 6.0)
    ])
    def test_floor_overloaded_pos(self, expression):
        """
        Test arithmetic Floor expression with correct parameters.
        """
        self.verify_expression(expression.compile(), self.rec)

    @pytest.mark.parametrize("expression, expected", [
        (Eq(math.floor(FloatBin("fbin") + 1.6), 7.0), e.FilteredOut),
        (Eq(math.floor(IntBin("ibin")), 5), e.InvalidRequest)
    ])
    def test_floor_overloaded_neg(self, expression, expected):
        """
        Test arithmetic Floor expression with incorrect parameters.
        """
        self.verify_expression_neg(expression.compile(), expected)

    @pytest.mark.parametrize("val, check", [
        (8.953, 9.0),
        (Add(FloatBin("fbin"), 4.5), 10.0)
    ])
    def test_ceil_pos(self, val, check):
        """
        Test arithmetic Ceil expression with correct parameters.
        """
        expr = Eq(Ceil(val),
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
        Test arithmetic Ceil expression expecting failure.
        """
        expr = Eq(Ceil(val),
                    check).compile()
        
        self.verify_expression_neg(expr, expected)

    @pytest.mark.parametrize("expression", [
        Eq(math.ceil(FloatBin("fbin") + 1.6), 7.0)
    ])
    def test_ceil_overloaded_pos(self, expression):
        """
        Test arithmetic Ceil expression with correct parameters.
        """
        self.verify_expression(expression.compile(), self.rec)

    @pytest.mark.parametrize("expression, expected", [
        (Eq(math.ceil(FloatBin("fbin") + 1.6), 6.0), e.FilteredOut),
        (Eq(math.ceil(IntBin("ibin")), 5), e.InvalidRequest)
    ])
    def test_ceil_overloaded_neg(self, expression, expected):
        """
        Test arithmetic Ceil expression with incorrect parameters.
        """
        self.verify_expression_neg(expression.compile(), expected)

    @pytest.mark.parametrize("val, check", [
        (8.953, 8),
        (FloatBin("fbin"), 5)
    ])
    def test_toint_pos(self, val, check):
        """
        Test arithmetic ToInt expression with correct parameters.
        """
        expr = Eq(ToInt(val),
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
        Test arithmetic ToInt expression expecting failure.
        """
        expr = Eq(ToInt(val),
                    check).compile()
        
        self.verify_expression_neg(expr, expected)

    @pytest.mark.parametrize("val, check", [
        (8, 8.0),
        (IntBin("ibin"), 5.0)
    ])
    def test_tofloat_pos(self, val, check):
        """
        Test arithmetic ToFloat expression with correct parameters.
        """
        expr = Eq(ToFloat(val),
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
        Test arithmetic ToFloat expression expecting failure.
        """
        expr = Eq(ToFloat(val),
                    check).compile()
        
        self.verify_expression_neg(expr, expected)

    @pytest.mark.parametrize("bin, val, check", [
        (IntBin("ibin"), [5, 25, 4, 35, 64, 1, 23, 2, 2, 2], 1),
        (IntBin("ibin"), [Add(IntBin("ibin"), 5)], 5),
        (IntBin("ibin"), [IntBin("ibin")], 5),
        (FloatBin("fbin"), [6.0, 20.0], 5.0)
    ])
    def test_min_pos(self, bin, val, check):
        """
        Test arithmetic Min expression with correct parameters.
        """
        expr = Eq(Min(bin, *val),
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
        Test arithmetic Min expression expecting failure.
        """
        expr = Eq(Min(bin, *val),
                    check).compile()
        
        self.verify_expression_neg(expr, expected)

    @pytest.mark.parametrize("bin, val, check", [
        (IntBin("ibin"), [5, 25, 4, 35, 64, 1, 23, 2, 2, 2], 64),
        (IntBin("ibin"), [Add(IntBin("ibin"), 5)], 10),
        (IntBin("ibin"), [IntBin("ibin")], 5),
        (FloatBin("fbin"), [6.0, 20.0], 20.0)
    ])
    def test_max_pos(self, bin, val, check):
        """
        Test arithmetic Max expression with correct parameters.
        """
        expr = Eq(Max(bin, *val),
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
        Test arithmetic Max expression expecting failure.
        """
        expr = Eq(Max(bin, *val),
                    check).compile()
        
        self.verify_expression_neg(expr, expected)