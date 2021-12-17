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
Arithmetic expressions provide arithmetic operator support for Aerospike expressions.

Example::

    import aerospike_helpers.expressions as exp
    # Add integer bin "a" to integer bin "b" and see if the result is > 20.
    expr = exp.GT(exp.Add(exp.IntBin("a"), exp.IntBin("b")), 20).compile()
'''

#from __future__ import annotations
from typing import Union

import aerospike
from aerospike_helpers.expressions.resources import _GenericExpr
from aerospike_helpers.expressions.resources import _BaseExpr
from aerospike_helpers.expressions.resources import _ExprOp

TypeNumber = Union[_BaseExpr, int, float]
TypeFloat = Union[_BaseExpr, float]
TypeInteger = Union[_BaseExpr, int]


########################
# Arithmetic Expressions
########################


class Add(_BaseExpr):
    """ Create an add, (+) expression.
        All arguments must be the same type (integer or float).

        Add is also available via operator overloading using `+`
        and any subclass of _BaseExpr. See the second example.

        Requires server version 5.6.0+.
    """
    _op = _ExprOp.ADD

    def __init__(self, *args: 'TypeNumber'):
        """ Args:
            `*args` (TypeNumber): Variable amount of float or integer expressions or values to be added together.

        :return: (integer or float value).

        Example::

            # Integer bin "a" + "b" == 11
            expr = exp.Eq(exp.Add(exp.IntBin("a"), exp.IntBin("b")), 11).compile()

            # Using operator overloading.
            expr = exp.Eq(exp.IntBin("a") + exp.IntBin("b"), 11).compile()

        """
        self._children = args + (_GenericExpr(_ExprOp._AS_EXP_CODE_END_OF_VA_ARGS, 0, {}),)


class Sub(_BaseExpr):
    """
        Create "subtract" (-) operator that applies to a variable number of expressions.
        If only one argument is provided, return the negation of that argument.
        Otherwise, return the sum of the 2nd to Nth argument subtracted from the 1st
        argument. All arguments must resolve to the same type (integer or float).

        Sub is also available via operator overloading using `-`
        and any subclass of _BaseExpr. See the second example.

        Requires server version 5.6.0+.
    """
    _op = _ExprOp.SUB

    def __init__(self, *args: 'TypeNumber'):
        """ Args:
            `*args` (TypeNumber): Variable amount of float or integer expressions or values to be subtracted.

        :return: (integer or float value)

        Example::

            # Integer bin "a" - "b" == 11
            expr = exp.Eq(exp.Sub(exp.IntBin("a"), exp.IntBin("b")), 11).compile()

            # Using operator overloading.
            expr = exp.Eq(exp.IntBin("a") - exp.IntBin("b"), 11).compile()
        """
        self._children = args + (_GenericExpr(_ExprOp._AS_EXP_CODE_END_OF_VA_ARGS, 0, {}),)


class Mul(_BaseExpr):
    """
        Create "multiply" (*) operator that applies to a variable number of expressions.
        Return the product of all arguments. If only one argument is supplied, return
        that argument. All arguments must resolve to the same type (integer or float).

        Mul is also available via operator overloading using `*`
        and any subclass of _BaseExpr. See the second example.

        Requires server version 5.6.0+.
    """
    _op = _ExprOp.MUL

    def __init__(self, *args: 'TypeNumber'):
        """ Args:
            `*args` (TypeNumber): Variable amount of float or integer expressions or values to be multiplied.

        :return: (integer or float value)

        Example::

            # Integer bin "a" * "b" >= 11
            expr = exp.GE(exp.Mul(exp.IntBin("a"), exp.IntBin("b")), 11).compile()

            # Using operator overloading.
            expr = exp.GE(exp.IntBin("a") * exp.IntBin("b"), 11).compile()
        """
        self._children = args + (_GenericExpr(_ExprOp._AS_EXP_CODE_END_OF_VA_ARGS, 0, {}),)


class Div(_BaseExpr):
    """ Create "divide" (/) operator that applies to a variable number of expressions.
        If there is only one argument, returns the reciprocal for that argument.
        Otherwise, return the first argument divided by the product of the rest.
        All arguments must resolve to the same type (integer or float).

        Div is also available via operator overloading using `/`
        and any subclass of _BaseExpr. See the second example.

        Floor div is also avaliable via `//` but must be used with floats.

        Requires server version 5.6.0+.
    """

    _op = _ExprOp.DIV

    def __init__(self, *args: 'TypeNumber'):
        """ Args:
            `*args` (TypeNumber): Variable amount of float or integer expressions or values to be divided.

        :return: (integer or float value)

        Example::

            # Integer bin "a" / "b" / "c" >= 11
            expr = exp.GE(exp.Div(exp.IntBin("a"), exp.IntBin("b"), exp.IntBin("c")), 11).compile()

            # Using operator overloading.
            expr = exp.GE(exp.IntBin("a") / exp.IntBin("b") / exp.IntBin("c"), 11).compile()

            # Float bin "a" // "b" // "c" >= 11.0
            expr = exp.GE(exp.FloatBin("a") // exp.FloatBin("b") // exp.FloatBin("c"), 11.0).compile()
        """
        self._children = args + (_GenericExpr(_ExprOp._AS_EXP_CODE_END_OF_VA_ARGS, 0, {}),)


class Pow(_BaseExpr):
    """
        Create "pow" operator that raises a "base" to the "exponent" power.
        All arguments must resolve to floats.

        Pow is also available via operator overloading using `**`
        and any subclass of _BaseExpr. See the second example.

        Requires server version 5.6.0+.
    """
    _op = _ExprOp.POW

    def __init__(self, base: 'TypeFloat', exponent: 'TypeFloat'):
        """ Args:
            base (TypeFloat): Float expression or value base.
            exponent (TypeFloat): Float expression or value exponent.

        :return: (float value)

        Example::

            # Float bin "a" ** 2.0 == 16.0
            expr = exp.Eq(exp.Pow(exp.FloatBin("a"), 2.0), 16.0).compile()

            # Using operator overloading.
            expr = exp.Eq(exp.FloatBin("a") ** 2.0, 16.0).compile()
        """
        self._children = (base, exponent)


class Log(_BaseExpr):
    """
        Create "log" operator for logarithm of "num" with base "base".
        All arguments must resolve to floats.

        Requires server version 5.6.0+.
    """
    _op = _ExprOp.LOG

    def __init__(self, num: 'TypeFloat', base: 'TypeFloat'):
        """ Args:
            num (TypeFloat): Float expression or value number.
            base (TypeFloat): Float expression or value base.

        :return: (float value)

        Example::

            # For float bin "a", log("a", 2.0) == 16.0
            expr = exp.Eq(exp.Log(exp.FloatBin("a"), 2.0), 16.0).compile()
        """
        self._children = (num, base)


class Mod(_BaseExpr):
    """
       Create "modulo" (%) operator that determines the remainder of "numerator"
       divided by "denominator". All arguments must resolve to integers.

       Mod is also available via operator overloading using `%`
       and any subclass of _BaseExpr. See the second example.

       Requires server version 5.6.0+.
    """
    _op = _ExprOp.MOD

    def __init__(self, numerator: 'TypeInteger', denominator: 'TypeInteger'):
        """ Args:
            numerator (TypeInteger): Integer expression or value numerator.
            denominator (TypeInteger): Integer expression or value denominator.

        :return: (integer value)

        Example::

            # For int bin "a" % 10 == 0
            expr = exp.Eq(exp.Mod(exp.IntBin("a"), 10), 0).compile()

            # Using operator overloading.
            expr = exp.Eq(exp.IntBin("a") % 10, 0).compile()
        """
        self._children = (numerator, denominator)


class Abs(_BaseExpr):
    """
       Create operator that returns absolute value of a number.
       All arguments must resolve to integer or float.

       Abs is also available via operator overloading using the bultin
       abs() function and any subclass of _BaseExpr. See the second example.

       Requires server version 5.6.0+.
    """
    _op = _ExprOp.ABS

    def __init__(self, value: 'TypeNumber'):
        """ Args:
            value (TypeNumber): Float or integer expression or value to take absolute value of.

        :return: (number value)

        Example::

            # For int bin "a", abs("a") == 1
            expr = exp.Eq(exp.Abs(exp.IntBin("a")), 1).compile()

            # Using operator overloading
            expr = exp.Eq(abs(exp.IntBin("a")), 1).compile()
        """
        self._children = (value,)


class Floor(_BaseExpr):
    """
        Create floor expression that rounds a floating point number down
        to the closest integer value.

        Floor is also available via operator overloading using the math.floor()
        function and any subclass of _BaseExpr. See the second example.

        Requires server version 5.6.0+.
    """
    _op = _ExprOp.FLOOR

    def __init__(self, value: 'TypeFloat'):
        """ Args:
            value (TypeFloat): Float expression or value to take floor of.

        :return: (float value)

        Example::

            # Floor(2.25) == 2.0
            expr = exp.Eq(exp.Floor(2.25), 2.0).compile()

            # Using operator overloading
            expr = exp.Eq(math.floor(2.25), 2.0).compile()
        """
        self._children = (value,)


class Ceil(_BaseExpr):
    """
       Create ceil expression that rounds a floating point number up
       to the closest integer value.

       Ceil is also available via operator overloading using the math.ceil()
       function and any subclass of _BaseExpr. See the second example.

       Requires server version 5.6.0+.
    """
    _op = _ExprOp.CEIL

    def __init__(self, value: 'TypeFloat'):
        """ Args:
            value (TypeFloat): Float expression or value to take ceiling of.

        :return: (float value)

        Example::

            # Ceil(2.25) == 3.0
            expr = exp.Eq(exp.Ceil(2.25), 3.0).compile()

            # Using operator overloading
            expr = exp.Eq(math.ceil(2.25), 3.0).compile()
        """
        self._children = (value,)


class ToInt(_BaseExpr):
    """Create expression that converts a float to an integer.

       Requires server version 5.6.0+.
    """
    _op = _ExprOp.TO_INT

    def __init__(self, value: 'TypeFloat'):
        """ Args:
            value (TypeFloat): Float expression or value to convert to int.

        :return: (integer value)

        Example::

            #For float bin "a", int(exp.FloatBin("a")) == 2
            expr = exp.Eq(exp.ToInt(exp.FloatBin("a")), 2).compile()
        """
        self._children = (value,)


class ToFloat(_BaseExpr):
    """Create expression that converts an integer to a float.

       Requires server version 5.6.0+.
    """
    _op = _ExprOp.TO_FLOAT

    def __init__(self, value: 'TypeInteger'):
        """ Args:
            value (TypeInteger): Integer expression or value to convert to float.

        :return: (float value)

        Example::

            #For int bin "a", float(exp.IntBin("a")) == 2
            expr = exp.Eq(exp.ToFloat(exp.IntBin("a")), 2).compile()
        """
        self._children = (value,)


class Min(_BaseExpr):
    """
       Create expression that returns the minimum value in a variable number of expressions.
       All arguments must be the same type (integer or float).

       Requires server version 5.6.0+.
    """
    _op = _ExprOp.MIN

    def __init__(self, *args: 'TypeNumber'):
        """ Args:
            `*args` (TypeNumber): Variable amount of float or integer expressions or values from which to find the minimum value.

        :return: (integer or float value).

        Example::

            # for integer bins a, b, c, min(a, b, c) > 0
            expr = exp.GT(exp.Min(exp.IntBin("a"), exp.IntBin("b"), exp.IntBin("c")), 0).compile()
        """
        self._children = args + (_GenericExpr(_ExprOp._AS_EXP_CODE_END_OF_VA_ARGS, 0, {}),)


class Max(_BaseExpr):
    """Create expression that returns the maximum value in a variable number of expressions.
       All arguments must be the same type (integer or float).

       Requires server version 5.6.0+.
    """
    _op = _ExprOp.MAX

    def __init__(self, *args: 'TypeNumber'):
        """ Args:
            `*args` (TypeNumber): Variable amount of float or integer expressions or values from which to find the maximum value.

        :return: (integer or float value).

        Example::

            # for integer bins a, b, c, max(a, b, c) > 100
            expr = exp.GT(exp.Max(exp.IntBin("a"), exp.IntBin("b"), exp.IntBin("c")), 100).compile()
        """
        self._children = args + (_GenericExpr(_ExprOp._AS_EXP_CODE_END_OF_VA_ARGS, 0, {}),)
