# -*- coding: utf-8 -*-

import pytest
from .test_base_class import TestBaseClass
from aerospike_helpers.expressions import (
    BitAdd,
    BitAnd,
    BitCount,
    BitGet,
    BitGetInt,
    BitInsert,
    BitLeftScan,
    BitLeftShift,
    BitNot,
    BitOr,
    BitRemove,
    BitResize,
    BitRightScan,
    BitRightShift,
    BitSet,
    BitSetInt,
    BitSubtract,
    BitXor,
    BitB64Encode,
    Eq,
)
from aerospike_helpers.operations import expression_operations as expr_ops
from .conftest import expect_server_version_earlier_than_8_1_3_to_fail

import aerospike
from . import as_errors

# Constants
_NUM_RECORDS = 9

GEO_POLY = aerospike.GeoJSON(
    {
        "type": "Polygon",
        "coordinates": [
            [
                [-122.500000, 37.000000],
                [-121.000000, 37.000000],
                [-121.000000, 38.080000],
                [-122.500000, 38.080000],
                [-122.500000, 37.000000],
            ]
        ],
    }
)


def verify_multiple_expression_result(client, test_ns, test_set, expr, op_bin, expected):
    keys = [(test_ns, test_set, i) for i in range(_NUM_RECORDS)]

    # batch get
    res = [br for br in client.batch_read(keys, policy={"expressions": expr}).batch_records
           if br.result != as_errors.AEROSPIKE_FILTERED_OUT]

    assert len(res) == expected


class TestUsrDefinedClass:

    __test__ = False

    def __init__(self, i):
        self.data = i


BASE64_BYTES = b'1234'
import base64


class TestExpressions(TestBaseClass):
    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        self.test_ns = "test"
        self.test_set = "demo"

        for i in range(_NUM_RECORDS):
            key = ("test", "demo", i)
            self.rec = {
                "1bits_bin": bytearray([1] * 8),
                "base64_bytes": BASE64_BYTES
            }
            self.as_connection.put(key, self.rec)

        def teardown():
            for i in range(_NUM_RECORDS):
                key = ("test", "demo", i)
                as_connection.remove(key)

        request.addfinalizer(teardown)

    @pytest.mark.parametrize(
        "policy, bytes_size, flags, bin, expected",
        [
            (None, 10, None, "1bits_bin", bytearray([1])),
            ({"bit_write_flags": aerospike.BIT_WRITE_UPDATE_ONLY}, 10, None, "1bits_bin", bytearray([1])),
            (None, 10, aerospike.BIT_RESIZE_FROM_FRONT, "1bits_bin", bytearray([0])),
        ],
    )
    def test_bit_resize_pos(self, policy, bytes_size, flags, bin, expected):
        """
        Test BitResize expression.
        """

        expr = Eq(BitGet(8, 8, BitResize(policy, bytes_size, flags, bin)), expected)

        verify_multiple_expression_result(
            self.as_connection, self.test_ns, self.test_set, expr.compile(), bin, _NUM_RECORDS
        )

    @pytest.mark.parametrize(
        "policy, byte_offset, byte_size, bin, expected",
        [
            (None, 0, 1, "1bits_bin", bytearray([0] * 1)),
            ({"bit_write_flags": aerospike.BIT_WRITE_UPDATE_ONLY}, 0, 1, "1bits_bin", bytearray([0] * 1)),
        ],
    )
    def test_bit_remove_ops_pos(self, policy, byte_offset, byte_size, bin, expected):
        """
        Test BitRemove expression.
        """

        expr = Eq(BitRemove(policy, byte_offset, byte_size, bin), bytearray([1] * 7))

        verify_multiple_expression_result(
            self.as_connection, self.test_ns, self.test_set, expr.compile(), bin, _NUM_RECORDS
        )

    @pytest.mark.parametrize("policy", [(None), ({"bit_write_flags": aerospike.BIT_WRITE_UPDATE_ONLY})])
    def test_BitInsert_pos(self, policy):
        """
        Test BitInsert expression.
        """

        expr = Eq(BitInsert(policy, 1, bytearray([3]), "1bits_bin"), bytearray([1, 3, 1, 1, 1, 1, 1, 1, 1]))

        verify_multiple_expression_result(
            self.as_connection, self.test_ns, self.test_set, expr.compile(), "1bits_bin", _NUM_RECORDS
        )

    @pytest.mark.parametrize("policy", [(None), ({"bit_write_flags": aerospike.BIT_WRITE_UPDATE_ONLY})])
    def test_bit_set_pos(self, policy):
        """
        Test BitSet expression.
        """

        expr = Eq(
            BitSet(policy, 7, 1, bytearray([255]), BitSet(policy, 0, 8 * 8, bytearray([0] * 8), "1bits_bin")),
            bytearray([1] + [0] * 7),
        )

        verify_multiple_expression_result(
            self.as_connection, self.test_ns, self.test_set, expr.compile(), "1bits_bin", _NUM_RECORDS
        )

    @pytest.mark.parametrize("policy", [(None), ({"bit_write_flags": aerospike.BIT_WRITE_UPDATE_ONLY})])
    def test_BitOr_pos(self, policy):
        """
        Test BitOr expression.
        """

        expr = Eq(BitOr(policy, 0, 8, bytearray([8]), "1bits_bin"), bytearray([9] + [1] * 7))

        verify_multiple_expression_result(
            self.as_connection, self.test_ns, self.test_set, expr.compile(), "1bits_bin", _NUM_RECORDS
        )

    @pytest.mark.parametrize("policy", [(None), ({"bit_write_flags": aerospike.BIT_WRITE_UPDATE_ONLY})])
    def test_BitXor_pos(self, policy):
        """
        Test BitXor expression.
        """

        expr = Eq(BitXor(policy, 0, 8, bytearray([1]), "1bits_bin"), bytearray([0] + [1] * 7))

        verify_multiple_expression_result(
            self.as_connection, self.test_ns, self.test_set, expr.compile(), "1bits_bin", _NUM_RECORDS
        )

    @pytest.mark.parametrize("policy", [(None), ({"bit_write_flags": aerospike.BIT_WRITE_UPDATE_ONLY})])
    def test_bit_and_pos(self, policy):
        """
        Test BitAnd expression.
        """

        expr = Eq(BitAnd(policy, 0, 8, bytearray([0]), "1bits_bin"), bytearray([0] + [1] * 7))

        verify_multiple_expression_result(
            self.as_connection, self.test_ns, self.test_set, expr.compile(), "1bits_bin", _NUM_RECORDS
        )

    @pytest.mark.parametrize("policy", [(None), ({"bit_write_flags": aerospike.BIT_WRITE_UPDATE_ONLY})])
    def test_bit_not_pos(self, policy):
        """
        Test BitNot expression.
        """

        expr = Eq(BitNot(policy, 0, 64, "1bits_bin"), bytearray([254] * 8))

        verify_multiple_expression_result(
            self.as_connection, self.test_ns, self.test_set, expr.compile(), "1bits_bin", _NUM_RECORDS
        )

    @pytest.mark.parametrize("policy", [(None), ({"bit_write_flags": aerospike.BIT_WRITE_UPDATE_ONLY})])
    def test_bit_left_shift_pos(self, policy):
        """
        Test BitLeftShift expression.
        """

        expr = Eq(BitLeftShift(policy, 0, 8, 3, "1bits_bin"), bytearray([8] + [1] * 7))

        verify_multiple_expression_result(
            self.as_connection, self.test_ns, self.test_set, expr.compile(), "1bits_bin", _NUM_RECORDS
        )

    @pytest.mark.parametrize("policy", [(None), ({"bit_write_flags": aerospike.BIT_WRITE_UPDATE_ONLY})])
    def test_bit_right_shift_pos(self, policy):
        """
        Test BitRightShift expression.
        """

        expr = Eq(BitRightShift(policy, 0, 8, 1, BitLeftShift(None, 0, 8, 3, "1bits_bin")), bytearray([4] + [1] * 7))

        verify_multiple_expression_result(
            self.as_connection, self.test_ns, self.test_set, expr.compile(), "1bits_bin", _NUM_RECORDS
        )

    @pytest.mark.parametrize(
        "policy, bit_offset, bit_size, value, action, bin, expected",
        [
            (None, 8, 8, 1, aerospike.BIT_OVERFLOW_FAIL, "1bits_bin", bytearray([1] + [2] + [1] * 6)),
            (
                {"bit_write_flags": aerospike.BIT_WRITE_UPDATE_ONLY},
                8,
                8,
                1,
                aerospike.BIT_OVERFLOW_FAIL,
                "1bits_bin",
                bytearray([1] + [2] + [1] * 6),
            ),
        ],
    )
    def test_bit_add_pos(self, policy, bit_offset, bit_size, value, action, bin, expected):
        """
        Test BitAdd expression.
        """

        expr = Eq(BitAdd(policy, bit_offset, bit_size, value, action, bin), expected)

        verify_multiple_expression_result(
            self.as_connection, self.test_ns, self.test_set, expr.compile(), bin, _NUM_RECORDS
        )

    @pytest.mark.parametrize(
        "policy, bit_offset, bit_size, value, action, bin, expected",
        [
            (None, 8, 8, 1, aerospike.BIT_OVERFLOW_FAIL, "1bits_bin", bytearray([1] + [0] + [1] * 6)),
            (
                {"bit_write_flags": aerospike.BIT_WRITE_UPDATE_ONLY},
                8,
                8,
                1,
                aerospike.BIT_OVERFLOW_FAIL,
                "1bits_bin",
                bytearray([1] + [0] + [1] * 6),
            ),
        ],
    )
    def test_bit_subtract_pos(self, policy, bit_offset, bit_size, value, action, bin, expected):
        """
        Test BitSubtract expression.
        """

        expr = Eq(BitSubtract(policy, bit_offset, bit_size, value, action, bin), expected)

        verify_multiple_expression_result(
            self.as_connection, self.test_ns, self.test_set, expr.compile(), bin, _NUM_RECORDS
        )

    @pytest.mark.parametrize("policy", [(None), ({"bit_write_flags": aerospike.BIT_WRITE_UPDATE_ONLY})])
    def test_bit_set_int_pos(self, policy):
        """
        Test BitSetInt expression.
        """

        expr = Eq(BitSetInt(policy, 7, 1, 0, "1bits_bin"), bytearray([0] + [1] * 7))

        verify_multiple_expression_result(
            self.as_connection, self.test_ns, self.test_set, expr.compile(), "1bits_bin", _NUM_RECORDS
        )

    @pytest.mark.parametrize("bit_offset, bit_size, bin, expected", [(8, 8, "1bits_bin", bytearray([1]))])
    def test_bit_get_pos(self, bit_offset, bit_size, bin, expected):
        """
        Test BitGet expression.
        """

        expr = Eq(BitGet(bit_offset, bit_size, bin), expected)

        verify_multiple_expression_result(
            self.as_connection, self.test_ns, self.test_set, expr.compile(), bin, _NUM_RECORDS
        )

    @pytest.mark.parametrize("bit_offset, bit_size, bin, expected", [(16, 8 * 3, "1bits_bin", 3)])
    def test_bit_count_pos(self, bit_offset, bit_size, bin, expected):
        """
        Test BitCount expression.
        """

        expr = Eq(BitCount(bit_offset, bit_size, bin), expected)

        verify_multiple_expression_result(
            self.as_connection, self.test_ns, self.test_set, expr.compile(), bin, _NUM_RECORDS
        )

    @pytest.mark.parametrize("bit_offset, bit_size, value, bin, expected", [(0, 8, True, "1bits_bin", 7)])
    def test_bit_left_scan_pos(self, bit_offset, bit_size, value, bin, expected):
        """
        Test BitLeftScan expression.
        """

        expr = Eq(BitLeftScan(bit_offset, bit_size, value, bin), expected)

        verify_multiple_expression_result(
            self.as_connection, self.test_ns, self.test_set, expr.compile(), bin, _NUM_RECORDS
        )

    @pytest.mark.parametrize("bit_offset, bit_size, value, bin, expected", [(0, 8, True, "1bits_bin", 7)])
    def test_bit_right_scan_pos(self, bit_offset, bit_size, value, bin, expected):
        """
        Test BitRightScan expression.
        """

        expr = Eq(BitRightScan(bit_offset, bit_size, value, bin), expected)

        verify_multiple_expression_result(
            self.as_connection, self.test_ns, self.test_set, expr.compile(), bin, _NUM_RECORDS
        )

    @pytest.mark.parametrize("bit_offset, bit_size, bin, expected", [(0, 8, "1bits_bin", 1)])
    def test_bit_get_int_pos(self, bit_offset, bit_size, bin, expected):
        """
        Test BitGetInt expression.
        """

        expr = Eq(BitGetInt(bit_offset, bit_size, True, bin), expected)

        verify_multiple_expression_result(
            self.as_connection, self.test_ns, self.test_set, expr.compile(), bin, _NUM_RECORDS
        )

    @pytest.mark.parametrize(
        "byte_offset, byte_size, invert_size, expected",
        [
            (0, None, False, base64.b64encode(BASE64_BYTES).decode("utf-8"))
        ]
    )
    @expect_server_version_earlier_than_8_1_3_to_fail
    @pytest.mark.usefixtures("expect_earlier_than_server_version_to_fail")
    def test_bit_b64_encode(self, byte_offset, byte_size, invert_size, expected):
        bin = "base64_bytes"
        expr = BitB64Encode(byte_offset, byte_size, invert_size, bin).compile()
        ops = [
            expr_ops.expression_read(bin, expr)
        ]
        key = ("test", "demo", 1)

        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(key, ops)
            assert bins[bin] == expected
