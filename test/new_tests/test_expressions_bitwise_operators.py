# -*- coding: utf-8 -*-

import sys

import pytest

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)

from aerospike_helpers.expressions import *
from aerospike import exception as e
from .test_base_class import TestBaseClass


class TestExpressionsBitOps(TestBaseClass):

    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        if self.server_version < [5, 6]:
            pytest.mark.xfail(reason="Servers older than 5.6 do not support bitwise operator expressions.")
            pytest.xfail()
        
        self.test_ns = 'test'
        self.test_set = 'demo'

        self.key = ('test', u'demo', 5)
        self.rec = {
            '1bin':  0xFFFF, #(255 * 2), 65535, FFFF, 1111111111111111
            '0bin':  0x0000, #(0 * 2),   0,     0,    0000000000000000
            '10bin': 0xAAAA, #(170 * 2), 43690, AAAA, 1010101010101010
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
        (IntBin("10bin"), [0xFF00], 0xAA00),
        (IntBin("1bin"), [0x0000], 0x0000),
        (IntBin("0bin"), [0xFF00], 0x0000),
    ])
    def test_int_and_pos(self, bin, val, check):
        """
        Test IntAnd expression with correct parameters.
        """
        expr = Eq(IntAnd(bin, *val),
                    check).compile()
        
        self.verify_expression(expr, self.rec)

    @pytest.mark.parametrize("bin, val, check, expected", [
        (IntBin("10bin"), [0xFF00], 0xAAF0, e.FilteredOut),
        (IntBin("1bin"), ["bad_arg"], 0x0000, e.InvalidRequest)
    ])
    def test_int_and_neg(self, bin, val, expected, check):
        """
        Test IntAnd expression with incorrect parameters.
        """
        expr = Eq(IntAnd(bin, *val),
                    check).compile()
        
        self.verify_expression_neg(expr, expected)

    @pytest.mark.parametrize("bin, val, check", [
        (IntBin("10bin"), [0xFF00], 0xFFAA),
        (IntBin("1bin"), [0x0000], 0xFFFF),
        (IntBin("0bin"), [0xFF00], 0xFF00),
    ])
    def test_int_or_pos(self, bin, val, check):
        """
        Test IntOr expression with correct parameters.
        """
        expr = Eq(IntOr(bin, *val),
                    check).compile()
        
        self.verify_expression(expr, self.rec)

    @pytest.mark.parametrize("bin, val, check, expected", [
        (IntBin("10bin"), [0xFF00], 0xAAF0, e.FilteredOut),
        (IntBin("1bin"), ["bad_arg"], 0x0000, e.InvalidRequest)
    ])
    def test_int_or_neg(self, bin, val, expected, check):
        """
        Test IntOr expression with incorrect parameters.
        """
        expr = Eq(IntOr(bin, *val),
                    check).compile()
        
        self.verify_expression_neg(expr, expected)

    @pytest.mark.parametrize("bin, val, check", [
        (IntBin("10bin"), [0xFF00], 0x55AA),
        (IntBin("1bin"), [0x0000], 0xFFFF),
        (IntBin("0bin"), [0xFF00], 0xFF00),
    ])
    def test_int_xor_pos(self, bin, val, check):
        """
        Test IntXOr expression with correct parameters.
        """
        expr = Eq(IntXOr(bin, *val),
                    check).compile()
        
        self.verify_expression(expr, self.rec)

    @pytest.mark.parametrize("bin, val, check, expected", [
        (IntBin("10bin"), [0xFF00], 0xAAF0, e.FilteredOut),
        (IntBin("1bin"), ["bad_arg"], 0x0000, e.InvalidRequest)
    ])
    def test_int_xor_neg(self, bin, val, expected, check):
        """
        Test IntXOr expression with incorrect parameters.
        """
        expr = Eq(IntXOr(bin, *val),
                    check).compile()
        
        self.verify_expression_neg(expr, expected)

    @pytest.mark.parametrize("bin, check", [
        (IntBin("10bin"), ~ 0xAAAA),
        (IntBin("1bin"), ~ 0xFFFF),
        (IntBin("0bin"), ~ 0x0000),
    ])
    def test_int_not_pos(self, bin, check):
        """
        Test IntNot expression with correct parameters.
        """
        expr = Eq(IntNot(bin),
                    check).compile()
        
        self.verify_expression(expr, self.rec)

    @pytest.mark.parametrize("bin, check, expected", [
        (IntBin("10bin"), 0xAAF0, e.FilteredOut),
        ("bad_arg", 0x0000, e.InvalidRequest)
    ])
    def test_int_not_neg(self, bin, expected, check):
        """
        Test IntNot expression with incorrect parameters.
        """
        expr = Eq(IntNot(bin),
                    check).compile()
        
        self.verify_expression_neg(expr, expected)

    @pytest.mark.parametrize("bin, val, check", [
        (IntBin("10bin"), [2], 0xAAAA << 2),
        (IntBin("1bin"), [4], 0xFFFF << 4),
        (IntBin("0bin"), [1], 0x0000 << 1),
    ])
    def test_int_left_shift_pos(self, bin, val, check):
        """
        Test IntLeftShift expression with correct parameters.
        """
        expr = Eq(IntLeftShift(bin, *val),
                    check).compile()
        
        self.verify_expression(expr, self.rec)

    @pytest.mark.parametrize("bin, val, check, expected", [
        (IntBin("10bin"), [2], 0xAAF0, e.FilteredOut),
		# InvalidRequest because shift is not type checked (expression).
        (IntBin("1bin"), ["bad_arg"], 0x0000, e.InvalidRequest)
    ])
    def test_int_left_shift_neg(self, bin, val, expected, check):
        """
        Test IntLeftShift expression with incorrect parameters.
        """
        expr = Eq(IntLeftShift(bin, *val),
                    check).compile()
        
        self.verify_expression_neg(expr, expected)

    @pytest.mark.parametrize("bin, val, check", [
        (IntBin("10bin"), [2], 0xAAAA >> 2),
        (IntBin("1bin"), [4], 0xFFFF >> 4),
        (IntBin("0bin"), [1], 0x0000 >> 1),
        #(-255, [1], -255 >> 1), TODO add case distinct from arithmetic shift
    ])
    def test_int_right_shift_pos(self, bin, val, check):
        """
        Test IntRightShift expression with correct parameters.
        """
        expr = Eq(IntRightShift(bin, *val),
                    check).compile()
        
        self.verify_expression(expr, self.rec)

    @pytest.mark.parametrize("bin, val, check, expected", [
        (IntBin("10bin"), [2], 0xAAF0, e.FilteredOut),
		# InvalidRequest because shift is not type checked (expression).
        (IntBin("1bin"), ["bad_arg"], 0xFFFF, e.InvalidRequest)
    ])
    def test_int_right_shift_neg(self, bin, val, expected, check):
        """
        Test IntRightShift expression with incorrect parameters.
        """
        expr = Eq(IntRightShift(bin, *val),
                    check).compile()
        
        self.verify_expression_neg(expr, expected)

    @pytest.mark.parametrize("bin, val, check", [
        (IntBin("10bin"), [2], 0xAAAA >> 2),
        (IntBin("1bin"), [4], 0xFFFF >> 4),
        (IntBin("0bin"), [1], 0x0000 >> 1),
        #TODO add case distinct from logical shift
    ])
    def test_int_right_arithmetic_shift_pos(self, bin, val, check):
        """
        Test IntArithmeticRightShift expression with correct parameters.
        """
        expr = Eq(IntArithmeticRightShift(bin, *val),
                    check).compile()
        
        self.verify_expression(expr, self.rec)

    @pytest.mark.parametrize("bin, val, check, expected", [
        (IntBin("10bin"), [2], 0xAAF0, e.FilteredOut),
        # InvalidRequest because shift is not type checked (expression).
        (IntBin("1bin"), ["bad_arg"], 0xFFFF, e.InvalidRequest)
    ])
    def test_int_right_arithmetic_shift_neg(self, bin, val, expected, check):
        """
        Test IntArithmeticRightShift expression with incorrect parameters.
        """
        expr = Eq(IntArithmeticRightShift(bin, *val),
                    check).compile()
        
        self.verify_expression_neg(expr, expected)

    @pytest.mark.parametrize("bin, check", [
        (IntBin("10bin"), 8),
        (IntBin("1bin"), 16),
        (IntBin("0bin"), 0),
    ])
    def test_int_count_pos(self, bin, check):
        """
        Test IntCount expression with correct parameters.
        """
        expr = Eq(IntCount(bin),
                    check).compile()
        
        self.verify_expression(expr, self.rec)

    @pytest.mark.parametrize("bin, check, expected", [
        (IntBin("10bin"), 10, e.FilteredOut),
        ("bad_arg", 0, e.InvalidRequest)
    ])
    def test_int_count_neg(self, bin, expected, check):
        """
        Test IntCount expression with incorrect parameters.
        """
        expr = Eq(IntCount(bin),
                    check).compile()
        
        self.verify_expression_neg(expr, expected)

    @pytest.mark.parametrize("bin, val, check", [ #NOTE: Aerospike ints are 64bit.
        (1, [True], 63),
        (IntBin("1bin"), [True], 48),
        (IntBin("0bin"), [False], 0),
    ])
    def test_int_left_scan_pos(self, bin, val, check):
        """
        Test IntLeftScan expression with correct parameters.
        """
        expr = Eq(IntLeftScan(bin, *val),
                    check).compile()
        
        self.verify_expression(expr, self.rec)

    @pytest.mark.parametrize("bin, val, check, expected", [
        (IntBin("0bin"), [True], 0, e.FilteredOut),
        (IntBin("1bin"), ["bad_arg"], 0x0000, e.InvalidRequest)
    ])
    def test_int_left_scan_neg(self, bin, val, expected, check):
        """
        Test IntLeftScan expression with incorrect parameters.
        """
        expr = Eq(IntLeftScan(bin, *val),
                    check).compile()
        
        self.verify_expression_neg(expr, expected)

    @pytest.mark.parametrize("bin, val, check", [ #NOTE: Aerospike ints are 64bit.
        (1, [True], 63),
        (IntBin("1bin"), [True], 63),
        (IntBin("0bin"), [False], 63),
    ])
    def test_int_right_scan_pos(self, bin, val, check):
        """
        Test IntRightScan expression with correct parameters.
        """
        expr = Eq(IntRightScan(bin, *val),
                    check).compile()
        
        self.verify_expression(expr, self.rec)

    @pytest.mark.parametrize("bin, val, check, expected", [
        (IntBin("0bin"), [True], 0, e.FilteredOut),
        (IntBin("1bin"), ["bad_arg"], 0x0000, e.InvalidRequest)
    ])
    def test_int_right_scan_neg(self, bin, val, expected, check):
        """
        Test IntRightScan expression with incorrect parameters.
        """
        expr = Eq(IntRightScan(bin, *val),
                    check).compile()
        
        self.verify_expression_neg(expr, expected)