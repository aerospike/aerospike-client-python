# -*- coding: utf-8 -*-

import pytest
import sys
from .test_base_class import TestBaseClass
from aerospike import exception as e
from .as_status_codes import AerospikeStatus
from aerospike_helpers import cdt_ctx
from aerospike_helpers.expressions import *
from aerospike_helpers.operations import map_operations
from aerospike_helpers.operations import list_operations
from aerospike_helpers.operations import hll_operations
from aerospike_helpers.operations import operations
from math import sqrt, ceil, floor

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)

# Constants
_NUM_RECORDS = 9

GEO_POLY = aerospike.GeoJSON(
                            {"type": "Polygon",
                            "coordinates": [[[-122.500000, 37.000000],
                                            [-121.000000, 37.000000],
                                            [-121.000000, 38.080000],
                                            [-122.500000, 38.080000],
                                            [-122.500000, 37.000000]]]})


def verify_multiple_expression_result(client, test_ns, test_set, expr, op_bin, expected):
    keys = [(test_ns, test_set, i) for i in range(_NUM_RECORDS + 1)]

    # batch get
    res = [rec for rec in client.get_many(keys, policy={'expressions': expr}) if rec[2]]

    assert len(res) == expected


class TestUsrDefinedClass():

    __test__ = False

    def __init__(self, i):
        self.data = i


class TestExpressions(TestBaseClass):

    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        self.test_ns = 'test'
        self.test_set = 'demo'

        for i in range(_NUM_RECORDS):
            key = ('test', u'demo', i)
            rec = {'1bits_bin': bytearray([1] * 8)}
            self.as_connection.put(key, rec)

        def teardown():
            for i in range(_NUM_RECORDS):
                key = ('test', u'demo', i)
                as_connection.remove(key)

        request.addfinalizer(teardown)

    @pytest.mark.parametrize("policy, bytes_size, flags, bin, expected", [
        (None, 10, None, '1bits_bin', bytearray([1])),
        ({'bit_write_flags': aerospike.BIT_WRITE_UPDATE_ONLY}, 10, None, '1bits_bin', bytearray([1])),
        (None, 10, aerospike.BIT_RESIZE_FROM_FRONT, '1bits_bin', bytearray([0]))
    ])
    def test_bit_resize_pos(self, policy, bytes_size, flags, bin, expected):
        """
        Test BitResize expression.
        """

        expr = Eq(
                    BitGet(8, 8, 
                        BitResize(policy, bytes_size, flags, bin)),
                    expected
                )

        verify_multiple_expression_result(self.as_connection, self.test_ns, self.test_set, expr.compile(), bin, _NUM_RECORDS)

    @pytest.mark.parametrize("policy, byte_offset, byte_size, bin, expected", [
        (None, 0, 1, '1bits_bin', bytearray([0] * 1)),
        ({'bit_write_flags': aerospike.BIT_WRITE_UPDATE_ONLY}, 0, 1, '1bits_bin', bytearray([0] * 1))
    ])
    def test_bit_remove_ops_pos(self, policy, byte_offset, byte_size, bin, expected):
        """
        Test BitRemove expression.
        """

        expr = Eq(
                    BitRemove(policy, byte_offset, byte_size, bin),
                    bytearray([1] * 7)
                )

        verify_multiple_expression_result(self.as_connection, self.test_ns, self.test_set, expr.compile(), bin, _NUM_RECORDS)

    @pytest.mark.parametrize("policy", [
        (None),
        ({'bit_write_flags': aerospike.BIT_WRITE_UPDATE_ONLY})
    ])
    def test_BitInsert_pos(self, policy):
        """
        Test BitInsert expression.
        """

        expr = Eq(
                    BitInsert(policy, 1, bytearray([3]), '1bits_bin'),
                    bytearray([1, 3, 1, 1, 1, 1, 1, 1, 1])
                )

        verify_multiple_expression_result(self.as_connection, self.test_ns, self.test_set, expr.compile(), '1bits_bin', _NUM_RECORDS)

    @pytest.mark.parametrize("policy", [
        (None),
        ({'bit_write_flags': aerospike.BIT_WRITE_UPDATE_ONLY})
    ])
    def test_bit_set_pos(self, policy):
        """
        Test BitSet expression.
        """

        expr = Eq(
                    BitSet(policy, 7, 1, bytearray([255]),
                        BitSet(policy, 0, 8 * 8, bytearray([0] * 8), '1bits_bin')),
                    bytearray([1] + [0] * 7)
                )

        verify_multiple_expression_result(self.as_connection, self.test_ns, self.test_set, expr.compile(), '1bits_bin', _NUM_RECORDS)

    @pytest.mark.parametrize("policy", [
        (None),
        ({'bit_write_flags': aerospike.BIT_WRITE_UPDATE_ONLY})
    ])
    def test_BitOr_pos(self, policy):
        """
        Test BitOr expression.
        """

        expr = Eq(
                    BitOr(policy, 0, 8, bytearray([8]), '1bits_bin'),
                    bytearray([9] + [1] * 7)
                )

        verify_multiple_expression_result(self.as_connection, self.test_ns, self.test_set, expr.compile(), '1bits_bin', _NUM_RECORDS)

    @pytest.mark.parametrize("policy", [
        (None),
        ({'bit_write_flags': aerospike.BIT_WRITE_UPDATE_ONLY})
    ])
    def test_BitXor_pos(self, policy):
        """
        Test BitXor expression.
        """

        expr = Eq(
                    BitXor(policy, 0, 8, bytearray([1]), '1bits_bin'),
                    bytearray([0] + [1] * 7)
                )

        verify_multiple_expression_result(self.as_connection, self.test_ns, self.test_set, expr.compile(), '1bits_bin', _NUM_RECORDS)

    @pytest.mark.parametrize("policy", [
        (None),
        ({'bit_write_flags': aerospike.BIT_WRITE_UPDATE_ONLY})
    ])
    def test_bit_and_pos(self, policy):
        """
        Test BitAnd expression.
        """

        expr = Eq(
                    BitAnd(policy, 0, 8, bytearray([0]), '1bits_bin'),
                    bytearray([0] + [1] * 7)
                )

        verify_multiple_expression_result(self.as_connection, self.test_ns, self.test_set, expr.compile(), '1bits_bin', _NUM_RECORDS)

    @pytest.mark.parametrize("policy", [
        (None),
        ({'bit_write_flags': aerospike.BIT_WRITE_UPDATE_ONLY})
    ])
    def test_bit_not_pos(self, policy):
        """
        Test BitNot expression.
        """

        expr = Eq(
                    BitNot(policy, 0, 64, '1bits_bin'),
                    bytearray([254] * 8)
                )

        verify_multiple_expression_result(self.as_connection, self.test_ns, self.test_set, expr.compile(), '1bits_bin', _NUM_RECORDS)

    @pytest.mark.parametrize("policy", [
        (None),
        ({'bit_write_flags': aerospike.BIT_WRITE_UPDATE_ONLY})
    ])
    def test_bit_left_shift_pos(self, policy):
        """
        Test BitLeftShift expression.
        """

        expr = Eq(
                    BitLeftShift(policy, 0, 8, 3, '1bits_bin'),
                    bytearray([8] + [1] * 7)
                )

        verify_multiple_expression_result(self.as_connection, self.test_ns, self.test_set, expr.compile(), '1bits_bin', _NUM_RECORDS)

    @pytest.mark.parametrize("policy", [
        (None),
        ({'bit_write_flags': aerospike.BIT_WRITE_UPDATE_ONLY})
    ])
    def test_bit_right_shift_pos(self, policy):
        """
        Test BitRightShift expression.
        """

        expr = Eq(
                    BitRightShift(policy, 0, 8, 1, 
                        BitLeftShift(None, 0, 8, 3, '1bits_bin')),
                    bytearray([4] + [1] * 7)
                )

        verify_multiple_expression_result(self.as_connection, self.test_ns, self.test_set, expr.compile(), '1bits_bin', _NUM_RECORDS)

    @pytest.mark.parametrize("policy, bit_offset, bit_size, value, action, bin, expected", [
        (None, 8, 8, 1, aerospike.BIT_OVERFLOW_FAIL, '1bits_bin', bytearray([1] + [2] + [1] * 6)),
        ({'bit_write_flags': aerospike.BIT_WRITE_UPDATE_ONLY}, 8, 8, 1, aerospike.BIT_OVERFLOW_FAIL, '1bits_bin', bytearray([1] + [2] + [1] * 6))
    ])
    def test_bit_add_pos(self, policy, bit_offset, bit_size, value, action, bin, expected):
        """
        Test BitAdd expression.
        """

        expr = Eq(
                    BitAdd(policy, bit_offset, bit_size, value, action, bin),
                    expected
                )

        verify_multiple_expression_result(self.as_connection, self.test_ns, self.test_set, expr.compile(), bin, _NUM_RECORDS)

    @pytest.mark.parametrize("policy, bit_offset, bit_size, value, action, bin, expected", [
        (None, 8, 8, 1, aerospike.BIT_OVERFLOW_FAIL, '1bits_bin', bytearray([1] + [0] + [1] * 6)),
        ({'bit_write_flags': aerospike.BIT_WRITE_UPDATE_ONLY}, 8, 8, 1, aerospike.BIT_OVERFLOW_FAIL, '1bits_bin', bytearray([1] + [0] + [1] * 6))
    ])
    def test_bit_subtract_pos(self, policy, bit_offset, bit_size, value, action, bin, expected):
        """
        Test BitSubtract expression.
        """

        expr = Eq(
                    BitSubtract(policy, bit_offset, bit_size, value, action, bin),
                    expected
                )

        verify_multiple_expression_result(self.as_connection, self.test_ns, self.test_set, expr.compile(), bin, _NUM_RECORDS)

    @pytest.mark.parametrize("policy", [
        (None),
        ({'bit_write_flags': aerospike.BIT_WRITE_UPDATE_ONLY})
    ])
    def test_bit_set_int_pos(self, policy):
        """
        Test BitSetInt expression.
        """

        expr = Eq(
                    BitSetInt(policy, 7, 1, 0, '1bits_bin'),
                    bytearray([0] + [1] * 7)
                )

        verify_multiple_expression_result(self.as_connection, self.test_ns, self.test_set, expr.compile(), '1bits_bin', _NUM_RECORDS)

    @pytest.mark.parametrize("bit_offset, bit_size, bin, expected", [
        (8, 8, '1bits_bin', bytearray([1]))
    ])
    def test_bit_get_pos(self, bit_offset, bit_size, bin, expected):
        """
        Test BitGet expression.
        """

        expr = Eq(
                    BitGet(bit_offset, bit_size, bin),
                    expected
                )

        verify_multiple_expression_result(self.as_connection, self.test_ns, self.test_set, expr.compile(), bin, _NUM_RECORDS)

    @pytest.mark.parametrize("bit_offset, bit_size, bin, expected", [
        (16, 8 * 3, '1bits_bin', 3)
    ])
    def test_bit_count_pos(self, bit_offset, bit_size, bin, expected):
        """
        Test BitCount expression.
        """

        expr = Eq(
                    BitCount(bit_offset, bit_size, bin),
                    expected
                )

        verify_multiple_expression_result(self.as_connection, self.test_ns, self.test_set, expr.compile(), bin, _NUM_RECORDS)

    @pytest.mark.parametrize("bit_offset, bit_size, value, bin, expected", [
        (0, 8, True, '1bits_bin', 7)
    ])
    def test_bit_left_scan_pos(self, bit_offset, bit_size, value, bin, expected):
        """
        Test BitLeftScan expression.
        """

        expr = Eq(
                    BitLeftScan(bit_offset, bit_size, value, bin),
                    expected
                )

        verify_multiple_expression_result(self.as_connection, self.test_ns, self.test_set, expr.compile(), bin, _NUM_RECORDS)

    @pytest.mark.parametrize("bit_offset, bit_size, value, bin, expected", [
        (0, 8, True, '1bits_bin', 7)
    ])
    def test_bit_right_scan_pos(self, bit_offset, bit_size, value, bin, expected):
        """
        Test BitRightScan expression.
        """

        expr = Eq(
                    BitRightScan(bit_offset, bit_size, value, bin),
                    expected
                )

        verify_multiple_expression_result(self.as_connection, self.test_ns, self.test_set, expr.compile(), bin, _NUM_RECORDS)

    @pytest.mark.parametrize("bit_offset, bit_size, bin, expected", [
        (0, 8, '1bits_bin', 1)
    ])
    def test_bit_get_int_pos(self, bit_offset, bit_size, bin, expected):
        """
        Test BitGetInt expression.
        """

        expr = Eq(
                    BitGetInt(bit_offset, bit_size, True, bin),
                    expected
                )

        verify_multiple_expression_result(self.as_connection, self.test_ns, self.test_set, expr.compile(), bin, _NUM_RECORDS)