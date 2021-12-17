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
Bitwise operator expressions provide support for bitwise operators like `&` and `>>` in Aerospike expressions.

Example::

    import aerospike_helpers.expressions as exp
    # Let int bin "a" == 0xAAAA.
    # Use bitwise and to apply a mask 0xFF00 to 0xAAAA and check for 0xAA00.
    expr = exp.Eq(exp.IntAnd(IntBin("a"), 0xFF00), 0xAA00).compile()
'''

#from __future__ import annotations
from typing import Union

import aerospike
from aerospike_helpers.expressions.resources import _GenericExpr
from aerospike_helpers.expressions.resources import _BaseExpr
from aerospike_helpers.expressions.resources import _ExprOp

##############################
# Bitwise Operator Expressions
##############################

TypeInteger = Union[_BaseExpr, int]
TypeBool = Union[_BaseExpr, bool]

class IntAnd(_BaseExpr):
    """Create integer "and" (&) operator expression that is applied to two or more integers.
       All arguments must resolve to integers.

       Requires server version 5.6.0+.
    """
    _op = _ExprOp.INT_AND

    def __init__(self, *exprs: 'TypeInteger'):
        """ Args:
                `*exprs` (TypeInteger): A variable amount of integer expressions or values to be bitwise ANDed.

            :return: (integer value)

            Example::

                # for int bin "a", a & 0xff == 0x11
                expr = exp.Eq(IntAnd(exp.IntBin("a"), 0xff), 0x11).compile()
        """
        self._children = exprs + (_GenericExpr(_ExprOp._AS_EXP_CODE_END_OF_VA_ARGS, 0, {}),)


class IntOr(_BaseExpr):
    """Create integer "or" (|) operator expression that is applied to two or more integers.
       All arguments must resolve to integers.

       Requires server version 5.6.0+.
    """
    _op = _ExprOp.INT_OR

    def __init__(self, *exprs: 'TypeInteger'):
        """ Args:
                `*exprs` (TypeInteger): A variable amount of integer expressions or values to be bitwise ORed.

            :return: (integer value)

            Example::

                # for int bin "a", a | 0x10 not == 0
                expr = exp.NE(exp.IntOr(IntBin("a"), 0x10), 0).compile()
        """
        self._children = exprs + (_GenericExpr(_ExprOp._AS_EXP_CODE_END_OF_VA_ARGS, 0, {}),)


class IntXOr(_BaseExpr):
    """Create integer "xor" (^) operator that is applied to two or more integers.
       All arguments must resolve to integers.

       Requires server version 5.6.0+.
    """
    _op = _ExprOp.INT_XOR

    def __init__(self, *exprs: 'TypeInteger'):
        """ Args:
                `*exprs` (TypeInteger): A variable amount of integer expressions or values to be bitwise XORed.

            :return: (integer value)

            Example::

                # for int bin "a", "b", a ^ b == 16
                expr = exp.Eq(exp.IntXOr(exp.IntBin("a"), exp.IntBin("b")), 16).compile()
        """
        self._children = exprs + (_GenericExpr(_ExprOp._AS_EXP_CODE_END_OF_VA_ARGS, 0, {}),)


class IntNot(_BaseExpr):
    """Create integer "not" (~) operator.

       Requires server version 5.6.0+.
    """
    _op = _ExprOp.INT_NOT

    def __init__(self, expr: 'TypeInteger'):
        """ Args:
                `expr` (TypeInteger): An integer value or expression to be bitwise negated.

            :return: (integer value)

            Example::

                # for int bin "a", ~ a == 7
                expr = exp.Eq(exp.IntNot(IntBin("a")), 7).compile()
        """
        self._children = (expr,)


class IntLeftShift(_BaseExpr):
    """Create integer "left shift" (<<) operator.

       Requires server version 5.6.0+.
    """
    _op = _ExprOp.INT_LSHIFT

    def __init__(self, value: 'TypeInteger', shift: 'TypeInteger'):
        """ Args:
                `value` (TypeInteger): An integer value or expression to be left shifted.
                `shift` (TypeInteger): An integer value or expression for number of bits to left shift `value` by.

            :return: (integer value)

            Example::

                # for int bin "a", a << 8 > 0xff
                expr = exp.GT(exp.IntLeftShift(exp.IntBin("a"), 8), 0xff).compile()
        """
        self._children = (value, shift)


class IntRightShift(_BaseExpr):
    """Create integer "logical right shift" (>>>) operator.

       Requires server version 5.6.0+.
    """
    _op = _ExprOp.INT_RSHIFT

    def __init__(self, value: 'TypeInteger', shift: 'TypeInteger'):
        """ Args:
                `value` (TypeInteger): An integer value or expression to be right shifted.
                `shift` (TypeInteger): An integer value or expression for number of bits to right shift `value` by.

            :return: (integer value)

            Example::

                # for int bin "a", a >>> 8 > 0xff
                expr = exp.GT(exp.IntRightShift(exp.IntBin("a"), 8), 0xff).compile()
        """
        self._children = (value, shift)


class IntArithmeticRightShift(_BaseExpr):
    """Create integer "arithmetic right shift" (>>) operator.

       Requires server version 5.6.0+.
    """
    _op = _ExprOp.INT_ARSHIFT

    def __init__(self, value: 'TypeInteger', shift: 'TypeInteger'):
        """ Args:
                `value` (TypeInteger): An integer value or expression to be right shifted.
                `shift` (TypeInteger): An integer value or expression for number of bits to right shift `value` by.

            :return: (integer value)

            Example::

                # for int bin "a", a >> 8 > 0xff
                expr = exp.GT(exp.IntArithmeticRightShift(exp.IntBin("a"), 8), 0xff).compile()
        """
        self._children = (value, shift)


class IntCount(_BaseExpr):
    """Create expression that returns count of integer bits that are set to 1.

       Requires server version 5.6.0+.
    """
    _op = _ExprOp.INT_COUNT

    def __init__(self, value: 'TypeInteger'):
        """ Args:
                `value` (TypeInteger): An integer value or expression to have bits counted.

            :return: (integer value)

            Example::

                # for int bin "a", count(a) == 4
                expr = exp.Eq(exp.IntCount(exp.IntBin("a")), 4).compile()
        """
        self._children = (value,)


class IntLeftScan(_BaseExpr):
    """ Create expression that scans integer bits from left (most significant bit) to
       right (least significant bit), looking for a search bit value. When the
       search value is found, the index of that bit (where the most significant bit is
       index 0) is returned. If "search" is true, the scan will search for the bit
       value 1. If "search" is false it will search for bit value 0.

       Requires server version 5.6.0+.
    """
    _op = _ExprOp.INT_LSCAN

    def __init__(self, value: 'TypeInteger', search: 'TypeBool'):
        """ Args:
                `value` (TypeInteger): An integer value or expression to be scanned.
                `search` (TypeBool): A bool expression or value to scan for.

            :return: (integer value)

            Example::

                # for int bin "a", lscan(a, True) == 4
                expr = exp.GT(lscan(exp.IntBin("a"), True), 4).compile()
        """
        self._children = (value, search)


class IntRightScan(_BaseExpr):
    """
       Create expression that scans integer bits from right (least significant bit) to
       left (most significant bit), looking for a search bit value. When the
       search value is found, the index of that bit (where the most significant bit is
       index 0) is returned. If "search" is true, the scan will search for the bit
       value 1. If "search" is false it will search for bit value 0.

       Requires server version 5.6.0+.
    """
    _op = _ExprOp.INT_RSCAN

    def __init__(self, value: 'TypeInteger', search: 'TypeBool'):
        """ Args:
                `value` (TypeInteger): An integer value or expression to be scanned.
                `search` (TypeBool): A bool expression or value to scan for.

            :return: (integer value)

            Example::

                # for int bin "a", rscan(a, True) == 4
                expr = exp.GT(exp.IntRightScan(exp.IntBin("a"), True), 4).compile()
        """
        self._children = (value, search)
