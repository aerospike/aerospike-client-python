##########################################################################
# Copyright 2013-2021 Aerospike, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
##########################################################################
'''
Bitwise expressions contain expressions for performing bitwise operations.
Most of these operations are equivalent to the
:mod:`Bitwise Operations API <aerospike_helpers.operations.bitwise_operations>`
for binary data.

Example::

    import aerospike_helpers.expressions as exp
    # Let blob bin "c" == bytearray([3] * 5).
    # Count set bits starting at 3rd byte in bin "c" to get count of 6.
    expr = exp.BitCount(16, 8 * 3, exp.BlobBin("c")).compile()
'''

#from __future__ import annotations
from itertools import chain
from typing import List, Optional, Tuple, Union, Dict, Any
import aerospike
from aerospike_helpers import cdt_ctx
from aerospike_helpers.expressions.resources import _GenericExpr
from aerospike_helpers.expressions.resources import _BaseExpr
from aerospike_helpers.expressions.resources import _ExprOp
from aerospike_helpers.expressions.resources import ResultType
from aerospike_helpers.expressions.resources import _Keys
from aerospike_helpers.expressions.base import BlobBin

########################
# Bit Modify Expressions
########################

TypeBitValue = Union[bytes, bytearray]
TypeBinName = Union[_BaseExpr, str]
TypePolicy = Union[Dict[str, Any], None]


class BitResize(_BaseExpr):
    """Create an expression that performs a bit_resize operation."""
    _op = aerospike.OP_BIT_RESIZE

    def __init__(self, policy: 'TypePolicy', byte_size: int, flags: int, bin: 'TypeBinName'):
        """ Args:
                policy (TypePolicy): Optional dictionary of :ref:`Bit policies <aerospike_bit_policies>`.
                byte_size (int): Number of bytes the resulting blob should occupy.
                flags (int): One or a combination of bit resize flags.
                bin (TypeBinName): A :class:`~aerospike_helpers.expressions.base.BlobBin` expression.

            :return: Blob value expression of resized blob bin.

            Example::

                # Blob bin "c" == bytearray([1] * 5).
                # Resize blob bin "c" from the front so that the returned value is bytearray([0] * 5 + [1] * 5).
                expr = exp.BitResize(None, 10, aerospike.BIT_RESIZE_FROM_FRONT, exp.BlobBin("c")).compile()
        """
        self._children= (
            byte_size,
            _GenericExpr(_ExprOp._AS_EXP_BIT_FLAGS, 0, {_Keys.VALUE_KEY: policy['bit_write_flags']} if policy is not None and 'bit_write_flags' in policy else {_Keys.VALUE_KEY: 0}),
            _GenericExpr(_ExprOp._AS_EXP_BIT_FLAGS, 0, {_Keys.VALUE_KEY: flags} if flags is not None else {_Keys.VALUE_KEY: 0}),
            bin if isinstance(bin, _BaseExpr) else BlobBin(bin)
        )


class BitInsert(_BaseExpr):
    """Create an expression that performs a bit_insert operation."""
    _op = aerospike.OP_BIT_INSERT

    def __init__(self, policy: 'TypePolicy', byte_offset: int, value: 'TypeBitValue', bin: 'TypeBinName'):
        """ Args:
                policy (TypePolicy): Optional dictionary of :ref:`Bit policies <aerospike_bit_policies>`.
                byte_offset (int): Integer byte index of where to insert the value.
                value (TypeBitValue): A bytes value or blob value expression to insert.
                bin (TypeBinName): A :class:`~aerospike_helpers.expressions.base.BlobBin` expression.

            :return: Resulting blob containing the inserted bytes.

            Example::

                # Let blob bin "c" == bytearray([1] * 5).
                # Insert 3 so that returned value is bytearray([1, 3, 1, 1, 1, 1]).
                expr = exp.BitInsert(None, 1, bytearray([3]), exp.BlobBin("c")).compile()
        """
        self._children= (
            byte_offset,
            value,
            _GenericExpr(_ExprOp._AS_EXP_BIT_FLAGS, 0, {_Keys.VALUE_KEY: policy['bit_write_flags']} if policy is not None and 'bit_write_flags' in policy else {_Keys.VALUE_KEY: 0}),
            bin if isinstance(bin, _BaseExpr) else BlobBin(bin)
        )


class BitRemove(_BaseExpr):
    """Create an expression that performs a bit_remove operation."""
    _op = aerospike.OP_BIT_REMOVE

    def __init__(self, policy: 'TypePolicy', byte_offset: int, byte_size: int, bin: 'TypeBinName'):
        """ Args:
                policy (TypePolicy): Optional dictionary of :ref:`Bit policies <aerospike_bit_policies>`.
                byte_offset (int): Byte index of where to start removing from.
                byte_size (int): Number of bytes to remove.
                bin (TypeBinName): A :class:`~aerospike_helpers.expressions.base.BlobBin` expression.

            :return: Resulting blob containing the remaining bytes.

            Example::

                # Let blob bin "c" == bytearray([1] * 5).
                # Remove 1 element so that the returned value is bytearray([1] * 4).
                expr = exp.BitRemove(None, 1, 1, exp.BlobBin("c")).compile()
        """
        self._children= (
            byte_offset,
            byte_size,
            _GenericExpr(_ExprOp._AS_EXP_BIT_FLAGS, 0, {_Keys.VALUE_KEY: policy['bit_write_flags']} if policy is not None and 'bit_write_flags' in policy else {_Keys.VALUE_KEY: 0}),
            bin if isinstance(bin, _BaseExpr) else BlobBin(bin)
        )


class BitSet(_BaseExpr):
    """Create an expression that performs a bit_set operation."""
    _op = aerospike.OP_BIT_SET

    def __init__(self, policy: 'TypePolicy', bit_offset: int, bit_size: int, value: 'TypeBitValue', bin: 'TypeBinName'):
        """ Args:
                policy (TypePolicy): Optional dictionary of :ref:`Bit policies <aerospike_bit_policies>`.
                bit_offset (int): Bit index of where to start overwriting.
                bit_size (int): Number of bits to overwrite.
                value (TypeBitValue): Bytes value or blob expression containing bytes to write.
                bin (TypeBinName): A :class:`~aerospike_helpers.expressions.base.BlobBin` expression.

            :return: Resulting blob expression with the bits overwritten.

            Example::

                # Let blob bin "c" == bytearray([0] * 5).
                # Set bit at offset 7 with size 1 bits to 1 to make the returned value bytearray([1, 0, 0, 0, 0]).
                expr = exp.BitSet(None, 7, 1, bytearray([255]), exp.BlobBin("c")).compile()
        """
        self._children= (
            bit_offset,
            bit_size,
            value,
            _GenericExpr(_ExprOp._AS_EXP_BIT_FLAGS, 0, {_Keys.VALUE_KEY: policy['bit_write_flags']} if policy is not None and 'bit_write_flags' in policy else {_Keys.VALUE_KEY: 0}),
            bin if isinstance(bin, _BaseExpr) else BlobBin(bin)
        )


class BitOr(_BaseExpr):
    """Create an expression that performs a bit_or operation."""
    _op = aerospike.OP_BIT_OR

    def __init__(self, policy: 'TypePolicy', bit_offset: int, bit_size: int, value: 'TypeBitValue', bin: 'TypeBinName'):
        """ Args:
                policy (TypePolicy): Optional dictionary of :ref:`Bit policies <aerospike_bit_policies>`.
                bit_offset (int): Bit index of where to start operation.
                bit_size (int): Number of bits to be operated on.
                value (TypeBitValue): Bytes value or blob expression containing bytes to use in operation.
                bin (TypeBinName): A :class:`~aerospike_helpers.expressions.base.BlobBin` expression.

            :return: Resulting blob with the bits operated on.

            Example::

                # Let blob bin "c" == bytearray([1] * 5).
                # bitwise Or `8` with the first byte of blob bin c so that the returned value is bytearray([9, 1, 1, 1, 1]).
                expr = exp.BitOr(None, 0, 8, bytearray([8]), exp.BlobBin("c")).compile()
        """
        self._children= (
            bit_offset,
            bit_size,
            value,
            _GenericExpr(_ExprOp._AS_EXP_BIT_FLAGS, 0, {_Keys.VALUE_KEY: policy['bit_write_flags']} if policy is not None and 'bit_write_flags' in policy else {_Keys.VALUE_KEY: 0}),
            bin if isinstance(bin, _BaseExpr) else BlobBin(bin)
        )


class BitXor(_BaseExpr):
    """Create an expression that performs a bit_xor operation."""
    _op = aerospike.OP_BIT_XOR

    def __init__(self, policy: 'TypePolicy', bit_offset: int, bit_size: int, value: 'TypeBitValue', bin: 'TypeBinName'):
        """ Args:
                policy (TypePolicy): Optional dictionary of :ref:`Bit policies <aerospike_bit_policies>`.
                bit_offset (int): Bit index of where to start operation.
                bit_size (int): Number of bits to be operated on.
                value (TypeBitValue): Bytes value or blob expression containing bytes to use in operation.
                bin (TypeBinName): A :class:`~aerospike_helpers.expressions.base.BlobBin` expression.

            :return: Resulting blob with the bits operated on.

            Example::

                # Let blob bin "c" == bytearray([1] * 5).
                # bitwise Xor `1` with the first byte of blob bin c so that the returned value is bytearray([0, 1, 1, 1, 1]).
                expr = exp.BitXor(None, 0, 8, bytearray([1]), exp.BlobBin("c")).compile()
        """
        self._children= (
            bit_offset,
            bit_size,
            value,
            _GenericExpr(_ExprOp._AS_EXP_BIT_FLAGS, 0, {_Keys.VALUE_KEY: policy['bit_write_flags']} if policy is not None and 'bit_write_flags' in policy else {_Keys.VALUE_KEY: 0}),
            bin if isinstance(bin, _BaseExpr) else BlobBin(bin)
        )


class BitAnd(_BaseExpr):
    """Create an expression that performs a bit_and operation."""
    _op = aerospike.OP_BIT_AND

    def __init__(self, policy: 'TypePolicy', bit_offset: int, bit_size: int, value: 'TypeBitValue', bin: 'TypeBinName'):
        """ Args:
                policy (TypePolicy): Optional dictionary of :ref:`Bit policies <aerospike_bit_policies>`.
                bit_offset (int): Bit index of where to start operation.
                bit_size (int): Number of bits to be operated on.
                value (TypeBitValue): Bytes value or blob expression containing bytes to use in operation.
                bin (TypeBinName): A :class:`~aerospike_helpers.expressions.base.BlobBin` expression.

            :return: Resulting blob with the bits operated on.

            Example::

                # Let blob bin "c" == bytearray([1] * 5).
                # bitwise and `0` with the first byte of blob bin c so that the returned value is bytearray([0, 5, 5, 5, 5]).
                expr = exp.BitAnd(None, 0, 8, bytearray([0]), exp.BlobBin("c")).compile()
        """
        self._children= (
            bit_offset,
            bit_size,
            value,
            _GenericExpr(_ExprOp._AS_EXP_BIT_FLAGS, 0, {_Keys.VALUE_KEY: policy['bit_write_flags']} if policy is not None and 'bit_write_flags' in policy else {_Keys.VALUE_KEY: 0}),
            bin if isinstance(bin, _BaseExpr) else BlobBin(bin)
        )


class BitNot(_BaseExpr):
    """Create an expression that performs a bit_not operation."""
    _op = aerospike.OP_BIT_NOT

    def __init__(self, policy: 'TypePolicy', bit_offset: int, bit_size: int, bin: 'TypeBinName'):
        """ Args:
                policy (TypePolicy): Optional dictionary of :ref:`Bit policies <aerospike_bit_policies>`.
                bit_offset (int): Bit index of where to start operation.
                bit_size (int): Number of bits to be operated on.
                bin (TypeBinName): A :class:`~aerospike_helpers.expressions.base.BlobBin` expression.

            :return: Resulting blob with the bits operated on.

            Example::

                # Let blob bin "c" == bytearray([255] * 5).
                # bitwise, not, all of "c" to get bytearray([254] * 5).
                expr = exp.BitNot(None, 0, 40, exp.BlobBin("c")).compile()
        """
        self._children= (
            bit_offset,
            bit_size,
            _GenericExpr(_ExprOp._AS_EXP_BIT_FLAGS, 0, {_Keys.VALUE_KEY: policy['bit_write_flags']} if policy is not None and 'bit_write_flags' in policy else {_Keys.VALUE_KEY: 0}),
            bin if isinstance(bin, _BaseExpr) else BlobBin(bin)
        )


class BitLeftShift(_BaseExpr):
    """Create an expression that performs a bit_lshift operation."""
    _op = aerospike.OP_BIT_LSHIFT

    def __init__(self, policy: 'TypePolicy', bit_offset: int, bit_size: int, shift: int, bin: 'TypeBinName'):
        """ Args:
                policy (TypePolicy): Optional dictionary of :ref:`Bit policies <aerospike_bit_policies>`.
                bit_offset (int): Bit index of where to start operation.
                bit_size (int): Number of bits to be operated on.
                shift (int): Number of bits to shift by.
                bin (TypeBinName): A :class:`~aerospike_helpers.expressions.base.BlobBin` expression.

            :return: Resulting blob with the bits operated on.

            Example::

                # Let blob bin "c" == bytearray([1] * 5).
                # Bit left shift the first byte of bin "c" to get bytearray([8, 1, 1, 1, 1]).
                expr = exp.BitLeftShift(None, 0, 8, 3, exp.BlobBin("c")).compile()
        """
        self._children= (
            bit_offset,
            bit_size,
            shift,
            _GenericExpr(_ExprOp._AS_EXP_BIT_FLAGS, 0, {_Keys.VALUE_KEY: policy['bit_write_flags']} if policy is not None and 'bit_write_flags' in policy else {_Keys.VALUE_KEY: 0}),
            bin if isinstance(bin, _BaseExpr) else BlobBin(bin)
        )


class BitRightShift(_BaseExpr):
    """Create an expression that performs a bit_rshift operation."""
    _op = aerospike.OP_BIT_RSHIFT

    def __init__(self, policy: 'TypePolicy', bit_offset: int, bit_size: int, shift: int, bin: 'TypeBinName'):
        """ Args:
                policy (TypePolicy): Optional dictionary of :ref:`Bit policies <aerospike_bit_policies>`.
                bit_offset (int): Bit index of where to start operation.
                bit_size (int): Number of bits to be operated on.
                shift (int): Number of bits to shift by.
                bin (TypeBinName): A :class:`~aerospike_helpers.expressions.base.BlobBin` expression.

            :return: Resulting blob with the bits operated on.

            Example::

                # Let blob bin "c" == bytearray([8] * 5).
                # Bit left shift the first byte of bin "c" to get bytearray([4, 8, 8, 8, 8]).
                expr = exp.BitRightShift(None, 0, 8, 1, exp.BlobBin("c")).compile()
        """
        self._children= (
            bit_offset,
            bit_size,
            shift,
            _GenericExpr(_ExprOp._AS_EXP_BIT_FLAGS, 0, {_Keys.VALUE_KEY: policy['bit_write_flags']} if policy is not None and 'bit_write_flags' in policy else {_Keys.VALUE_KEY: 0}),
            bin if isinstance(bin, _BaseExpr) else BlobBin(bin)
        )


class BitAdd(_BaseExpr):
    """Create an expression that performs a bit_add operation.
       Note: integers are stored big-endian.
    """
    _op = aerospike.OP_BIT_ADD

    def __init__(self, policy: 'TypePolicy', bit_offset: int, bit_size: int, value: int, action: int, bin: 'TypeBinName'):
        """ Args:
                policy (TypePolicy): Optional dictionary of :ref:`Bit policies <aerospike_bit_policies>`.
                bit_offset (int): Bit index of where to start operation.
                bit_size (int): Number of bits to be operated on.
                value (int): Integer value or expression for value to add.
                action (int): An aerospike bit overflow action.
                bin (TypeBinName): A :class:`~aerospike_helpers.expressions.base.BlobBin` expression.

            :return: resulting blob with the bits operated on.

            Example::

                # Let blob bin "c" == bytearray([1] * 5).
                # Bit add the second byte of bin "c" to get bytearray([1, 2, 1, 1, 1])
                expr = exp.BitAdd(None, 8, 8, 1, aerospike.BIT_OVERFLOW_FAIL).compile()
        """
        self._children= (
            bit_offset,
            bit_size,
            value,
            _GenericExpr(_ExprOp._AS_EXP_BIT_FLAGS, 0, {_Keys.VALUE_KEY: policy['bit_write_flags']} if policy is not None and 'bit_write_flags' in policy else {_Keys.VALUE_KEY: 0}),
            _GenericExpr(_ExprOp._AS_EXP_BIT_FLAGS, 0, {_Keys.VALUE_KEY: action} if action is not None else {_Keys.VALUE_KEY: 0}),
            bin if isinstance(bin, _BaseExpr) else BlobBin(bin)
        )


class BitSubtract(_BaseExpr):
    """ Create an expression that performs a bit_subtract operation.
        Note: integers are stored big-endian.
    """
    _op = aerospike.OP_BIT_SUBTRACT

    def __init__(self, policy: 'TypePolicy', bit_offset: int, bit_size: int, value: int, action: int, bin: 'TypeBinName'):
        """ Args:
                policy (TypePolicy): Optional dictionary of :ref:`Bit policies <aerospike_bit_policies>`.
                bit_offset (int): Bit index of where to start operation.
                bit_size (int): Number of bits to be operated on.
                value (int): Integer value or expression for value to add.
                action (int): An aerospike bit overflow action.
                bin (TypeBinName): A :class:`~aerospike_helpers.expressions.base.BlobBin` expression.

            :return: resulting blob with the bits operated on.

            Example::

                # Let blob bin "c" == bytearray([1] * 5).
                # Bit subtract the second byte of bin "c" to get bytearray([1, 0, 1, 1, 1])
                expr = exp.BitSubtract(None, 8, 8, 1, aerospike.BIT_OVERFLOW_FAIL).compile()
        """
        self._children= (
            bit_offset,
            bit_size,
            value,
            _GenericExpr(_ExprOp._AS_EXP_BIT_FLAGS, 0, {_Keys.VALUE_KEY: policy['bit_write_flags']} if policy is not None and 'bit_write_flags' in policy else {_Keys.VALUE_KEY: 0}),
            _GenericExpr(_ExprOp._AS_EXP_BIT_FLAGS, 0, {_Keys.VALUE_KEY: action} if action is not None else {_Keys.VALUE_KEY: 0}),
            bin if isinstance(bin, _BaseExpr) else BlobBin(bin)
        )


class BitSetInt(_BaseExpr):
    """ Create an expression that performs a bit_set_int operation.
        Note: integers are stored big-endian.
    """
    _op = aerospike.OP_BIT_SET_INT

    def __init__(self, policy: 'TypePolicy', bit_offset: int, bit_size: int, value: int, bin: 'TypeBinName'):
        """ Args:
                policy (TypePolicy): Optional dictionary of :ref:`Bit policies <aerospike_bit_policies>`.
                bit_offset (int): Bit index of where to start writing.
                bit_size (int): Number of bits to overwrite.
                value (int): Integer value or integer expression containing value to write.
                bin (TypeBinName): A :class:`~aerospike_helpers.expressions.base.BlobBin` expression.

            :return: Resulting blob expression with the bits overwritten.

            Example::

                # Let blob bin "c" == bytearray([0] * 5).
                # Set bit at offset 7 with size 1 bytes to 1 to make the returned value bytearray([1, 0, 0, 0, 0]).
                expr = exp.BitSetInt(None, 7, 1, 1, exp.BlobBin("c")).compile()
        """
        self._children= (
            bit_offset,
            bit_size,
            value,
            _GenericExpr(_ExprOp._AS_EXP_BIT_FLAGS, 0, {_Keys.VALUE_KEY: policy['bit_write_flags']} if policy is not None and 'bit_write_flags' in policy else {_Keys.VALUE_KEY: 0}),
            bin if isinstance(bin, _BaseExpr) else BlobBin(bin)
        )


######################
# Bit Read Expressions
######################


class BitGet(_BaseExpr):
    """Create an expression that performs a bit_get operation."""
    _op = aerospike.OP_BIT_GET

    def __init__(self, bit_offset: int, bit_size: int, bin: 'TypeBinName'):
        """ Args:
                bit_offset (int): Bit index of where to start reading.
                bit_size (int): Number of bits to get.
                bin (TypeBinName): A :class:`~aerospike_helpers.expressions.base.BlobBin` expression.

            :return: Blob, bit_size bits rounded up to the nearest byte size.

            Example::

                # Let blob bin "c" == bytearray([1, 2, 3, 4, 5).
                # Get 2 from bin "c".
                expr = exp.BitGet(8, 8, exp.BlobBin("c")).compile()
        """
        self._children= (
            bit_offset,
            bit_size,
            bin if isinstance(bin, _BaseExpr) else BlobBin(bin)
        )


class BitCount(_BaseExpr):
    """Create an expression that performs a bit_count operation."""
    _op = aerospike.OP_BIT_COUNT

    def __init__(self, bit_offset: int, bit_size: int, bin: 'TypeBinName'):
        """ Args:
                bit_offset (int): Bit index of where to start reading.
                bit_size (int): Number of bits to count.
                bin (TypeBinName): A :class:`~aerospike_helpers.expressions.base.BlobBin` expression.

            :return: Blob, bit_size bits rounded up to the nearest byte size.

            Example::

                # Let blob bin "c" == bytearray([3] * 5).
                # Count set bits starting at 3rd byte in bin "c" to get count of 6.
                expr = exp.BitCount(16, 8 * 3, exp.BlobBin("c")).compile()
        """
        self._children= (
            bit_offset,
            bit_size,
            bin if isinstance(bin, _BaseExpr) else BlobBin(bin)
        )


class BitLeftScan(_BaseExpr):
    """Create an expression that performs a bit_lscan operation."""
    _op = aerospike.OP_BIT_LSCAN

    def __init__(self, bit_offset: int, bit_size: int, value: bool, bin: 'TypeBinName'):
        """ Args:
                bit_offset (int): Bit index of where to start reading.
                bit_size (int): Number of bits to read.
                value bool: Bit value to check for.
                bin (TypeBinName): A :class:`~aerospike_helpers.expressions.base.BlobBin` expression.

            :return: Index of the left most bit starting from bit_offset set to value. Returns -1 if not found.

            Example::

                # Let blob bin "c" == bytearray([3] * 5).
                # Scan the first byte of bin "c" for the first bit set to 1. (should get 6)
                expr = exp.BitLeftScan(0, 8, True, exp.BlobBin("c")).compile()
        """
        self._children= (
            bit_offset,
            bit_size,
            value,
            bin if isinstance(bin, _BaseExpr) else BlobBin(bin)
        )


class BitRightScan(_BaseExpr):
    """Create an expression that performs a bit_rscan operation."""
    _op = aerospike.OP_BIT_RSCAN

    def __init__(self, bit_offset: int, bit_size: int, value: bool, bin: 'TypeBinName'):
        """ Args:
                bit_offset (int): Bit index of where to start reading.
                bit_size (int): Number of bits to read.
                value bool: Bit value to check for.
                bin (TypeBinName): A :class:`~aerospike_helpers.expressions.base.BlobBin` expression.

            :return: Index of the right most bit starting from bit_offset set to value. Returns -1 if not found.

            Example::

                # Let blob bin "c" == bytearray([3] * 5).
                # Scan the first byte of bin "c" for the right most bit set to 1. (should get 7)
                expr = exp.BitRightScan(0, 8, True, exp.BlobBin("c")).compile()
        """
        self._children= (
            bit_offset,
            bit_size,
            value,
            bin if isinstance(bin, _BaseExpr) else BlobBin(bin)
        )


class BitGetInt(_BaseExpr):
    """Create an expression that performs a bit_get_int operation."""
    _op = aerospike.OP_BIT_GET_INT

    def __init__(self, bit_offset: int, bit_size: int, sign: bool, bin: 'TypeBinName'):
        """ Args:
                bit_offset (int): Bit index of where to start reading.
                bit_size (int): Number of bits to get.
                sign bool: True for signed, False for unsigned.
                bin (TypeBinName): A :class:`~aerospike_helpers.expressions.base.BlobBin` expression.

            :return: Integer expression.

            Example::

                # Let blob bin "c" == bytearray([1, 2, 3, 4, 5).
                # Get 2 as an integer from bin "c".
                expr = exp.BitGetInt(8, 8, True, exp.BlobBin("c")).compile()
        """
        self._children= (
            bit_offset,
            bit_size,
            1 if sign else 0,
            bin if isinstance(bin, _BaseExpr) else BlobBin(bin)
        )
