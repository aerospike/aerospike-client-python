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
Base expressions include operators, bin, and meta data related expressions.

Example::

    import aerospike_helpers.expressions.base as exp
    # See if integer bin "bin_name" contains a value equal to 10.
    expr = exp.Eq(exp.IntBin("bin_name"), 10).compile()
'''

from itertools import chain
from typing import List, Optional, Tuple, Union, Dict, Any
import aerospike
from aerospike_helpers import cdt_ctx
from aerospike_helpers.expressions.resources import _GenericExpr
from aerospike_helpers.expressions.resources import _BaseExpr
from aerospike_helpers.expressions.resources import _ExprOp
from aerospike_helpers.expressions.resources import ResultType
from aerospike_helpers.expressions.resources import _Keys


TypeComparisonArg = Union[_BaseExpr, Any]
TypeGeo = Union[_BaseExpr, aerospike.GeoJSON]
TypeNumber = Union[_BaseExpr, int, float]
TypeFloat = Union[_BaseExpr, float]
TypeInteger = Union[_BaseExpr, int]


########################
# Arithemtic Expressions
########################


class Add(_BaseExpr):
    """Create an add, (+) expression."""
    _op = _ExprOp.ADD

    def __init__(self, *args: TypeNumber):
        """ Create an add, (+) expression.
            All arguments must be the same type (integer or float).
            Requires server version 5.6.0+.

        Args:
            `*args` (TypeNumber): Variable amount of float or integer expressions or values to be added together.

        :return: (integer or float value).

        Example::

            # Integer bin "a" + "b" == 11
            expr = Eq(Add(IntBin("a"), IntBin("b")), 11).compile()
        """        
        args + (_GenericExpr(_ExprOp._AS_EXP_CODE_END_OF_VA_ARGS, 0, {}),)


class Sub(_BaseExpr):
    """Create an sub, (-) expression."""
    _op = _ExprOp.SUB

    def __init__(self, *args: TypeNumber):
        """ Create "subtract" (-) operator that applies to a variable number of expressions.
            If only one argument is provided, return the negation of that argument.
            Otherwise, return the sum of the 2nd to Nth argument subtracted from the 1st
            argument. All arguments must resolve to the same type (integer or float).
            Requires server version 5.6.0+.

        Args:
            `*args` (TypeNumber): Variable amount of float or integer expressions or values to be subtracted.

        :return: (integer or float value)

        Example::

            # Integer bin "a" - "b" == 11
            expr = Eq(Sub(IntBin("a"), IntBin("b")), 11).compile()
        """        
        args + (_GenericExpr(_ExprOp._AS_EXP_CODE_END_OF_VA_ARGS, 0, {}),)


class Mul(_BaseExpr):
    """Create a multiply, (*) expression."""
    _op = _ExprOp.MUL

    def __init__(self, *args: TypeNumber):
        """ Create "multiply" (*) operator that applies to a variable number of expressions.
            Return the product of all arguments. If only one argument is supplied, return
            that argument. All arguments must resolve to the same type (integer or float).
            Requires server version 5.6.0+.

        Args:
            `*args` (TypeNumber): Variable amount of float or integer expressions or values to be multiplied.

        :return: (integer or float value)

        Example::

            # Integer bin "a" * "b" >= 11
            expr = GE(Mul(IntBin("a"), IntBin("b")), 11).compile()
        """        
        args + (_GenericExpr(_ExprOp._AS_EXP_CODE_END_OF_VA_ARGS, 0, {}),)


class Div(_BaseExpr):
    """Create a divide, (/) expression."""
    _op = _ExprOp.DIV

    def __init__(self, *args: TypeNumber):
        """ Create "divide" (/) operator that applies to a variable number of expressions.
            If there is only one argument, returns the reciprocal for that argument.
            Otherwise, return the first argument divided by the product of the rest.
            All arguments must resolve to the same type (integer or float).
            Requires server version 5.6.0+.

        Args:
            `*args` (TypeNumber): Variable amount of float or integer expressions or values to be divided.

        :return: (integer or float value)

        Example::

            # Integer bin "a" / "b" / "c" >= 11
            expr = GE(Mul(IntBin("a"), IntBin("b"), IntBin("b")), 11).compile()
        """        
        args + (_GenericExpr(_ExprOp._AS_EXP_CODE_END_OF_VA_ARGS, 0, {}),)


class Pow(_BaseExpr):
    """Create a pow, (**) expression."""
    _op = _ExprOp.POW

    def __init__(self, base: TypeFloat, exponent: TypeFloat):
        """ Create "pow" operator that raises a "base" to the "exponent" power.
            All arguments must resolve to floats.
            Requires server version 5.6.0+.

        Args:
            base (TypeFloat): Base value.
            exponent (TypeFloat): Exponent value.

        :return: (float value)

        Example::

            # 2.0 ** Float bin "a" == 16.0
            expr = Eq(Pow(2, FloatBin("a")), 16.0).compile()
        """        
        self._children = (base, exponent)


class Log(_BaseExpr):
    """Create a log operator expression."""
    _op = _ExprOp.LOG

    def __init__(self, num: TypeFloat, base: TypeFloat):
        """ Create "log" operator for logarithm of "num" with base "base".
            All arguments must resolve to floats.
            Requires server version 5.6.0+.

        Args:
            num (TypeFloat): Number value.
            base (TypeFloat): Base value.

        :return: (float value)

        Example::

            # For float bin "a", log("a", 2.0) == 16.0
            expr = Eq(Log(FloatBin("a"), 2), 16.0).compile()
        """        
        self._children = (num, base)


class Mod(_BaseExpr):
    """Create a mod, (%) expression."""
    _op = _ExprOp.MOD

    def __init__(self, numerator: TypeInteger, denominator: TypeInteger):
        """ Create "modulo" (%) operator that determines the remainder of "numerator"
            divided by "denominator". All arguments must resolve to integers.
            Requires server version 5.6.0+.

        Args:
            numerator (TypeInteger): Numerator value.
            denominator (TypeInteger): Denominator value.

        :return: (integer value)

        Example::

            # For int bin "a", mod("a", 10) == 0
            expr = Eq(Log(IntBin("a"), 10), 0).compile()
        """        
        self._children = (numerator, denominator)


class Abs(_BaseExpr):
    """Create an absoloute value operator expression."""
    _op = _ExprOp.ABS

    def __init__(self, value: TypeNumber):
        """ Create operator that returns absolute value of a number.
            All arguments must resolve to integer or float.
            Requires server version 5.6.0+.

        Args:
            value (TypeNumber): Number to take absolute value of.

        :return: (number value)

        Example::

            # For int bin "a", abs("a") == 1
            expr = Eq(Abs(IntBin("a")), 1).compile()
        """        
        self._children = (value,)


class Floor(_BaseExpr):
    """Create a floor operator expression."""
    _op = _ExprOp.FLOOR

    def __init__(self, value: TypeFloat):
        """ Create expression that rounds a floating point number down
            to the closest integer value.
            Requires server version 5.6.0+.

        Args:
            value (TypeFloat): Number to take floor of.

        :return: (integer value)

        Example::

            # Floor(2.25) == 2
            expr = Eq(Floor(2.25), 3).compile()
        """        
        self._children = (value,)


class Ceil(_BaseExpr):
    """Create a ceiling operator expression."""
    _op = _ExprOp.CEIL

    def __init__(self, value: TypeFloat):
        """ Create expression that rounds a floating point number up
            to the closest integer value.
            Requires server version 5.6.0+.

        Args:
            value (TypeFloat): Number to take ceiling of.

        :return: (integer value)

        Example::

            # Ceil(2.25) == 3
            expr = Eq(Ceil(2.25), 3).compile()
        """        
        self._children = (value,)