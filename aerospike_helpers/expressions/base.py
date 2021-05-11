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


###################
# Value Expressions
###################


class Unknown(_BaseExpr):
    """ Create an 'Unknown' expression. Used to intentionally fail an expression operation
        such as expression_read or expression_write with error code 26, OpNotApplicable.
        When evaluated, the Unkown expression return the unknown-value returned by other failed expressions.
        These failures can be ignored with the expression flag aerospike.EXP_READ_EVAL_NO_FAIL.
        This expression is most useful as a case in a conditional expression used in an expression operation.
    """
    _op = _ExprOp.UNKNOWN

    def __init__(self):
        """ Create an 'Unknown' expression. Used to intentionally fail an expression.
            The failure can be ignored with EXP_WRITE_EVAL_NO_FAIL or
            EXP_READ_NO_FAIL see :ref:`aerospike_expression_write_flags`.
        
            :return (unknown value)

            Example::

                # If IntBin("balance") >= 50, get "balance" + 50.
                # Otherwise, fail the expression via Unknown().
                # This sort of expression is useful with expression operations
                # expression_read() and expression_write().
                Let(Def("bal", IntBin("balance")),
                    Cond(
                        GE(Var("bal"), 50),
                            Add(Var("bal"), 50),
                        Unknown())
                )
        """
        super().__init__()


########################
# Record Key Expressions
########################


class _Key(_BaseExpr):
    _op = _ExprOp.REC_KEY


class KeyInt(_Key):
    """ Create an expression that returns the key as an integer. Returns the unknown-value if
        the key is not an integer.
    """
    _rt = ResultType.INTEGER

    def __init__(self):
        """ Create an expression that returns the key as an integer. Returns the unknown-value if
            the key is not an integer.
        
            :return (integer value): Integer value of the key if the key is an integer.

            Example::

                # Integer record key >= 10000.
                expr = GE(KeyInt(), 10000).compile()
        """
        super().__init__()


class KeyStr(_Key):
    """ Create an expression that returns the key as a string. Returns the unknown-value if
        the key is not a string.
    """
    _rt = ResultType.STRING

    def __init__(self):
        """ Create an expression that returns the key as a string. Returns the unknown-value if
            the key is not a string.
        
            :return (string value): string value of the key if the key is an string.

            Example::

                # string record key == "aaa".
                expr = Eq(KeyStr(), "aaa").compile()
        """ 
        super().__init__()


class KeyBlob(_Key):
    """ Create an expression that returns the key as a blob. Returns the unknown-value if
        the key is not a blob.
    """
    _rt = ResultType.BLOB

    def __init__(self):
        """ Create an expression that returns the key as a blob. Returns the unknown-value if
            the key is not a blob.
        
            :return (blob value): Blob value of the key if the key is a blob.

            Example::

                # blob record key <= bytearray([0x65, 0x65]).
                expr = GE(KeyBlob(), bytearray([0x65, 0x65])).compile()
        """ 
        super().__init__()


class KeyExists(_BaseExpr):
    """ Create an expression that returns if the primary key is stored in the record storage
        data as a boolean expression. This would occur on record write, when write policies set the `key` field to
        aerospike.POLICY_KEY_SEND.
    """
    _op = _ExprOp.META_KEY_EXISTS
    _rt = ResultType.BOOLEAN

    def __init__(self):
        """ Create an expression that returns if the primary key is stored in the record storage
            data as a boolean expression. This would occur on record write, when write policies set the `key` field to
            aerospike.POLICY_KEY_SEND.
        
            :return (boolean value): True if the record has a stored key, false otherwise.

            Example::

                # Key exists in record meta data.
                expr = KeyExists().compile()
        """ 
        super().__init__()


#################
# Bin Expressions
#################


class BoolBin(_BaseExpr):
    """ Create an expression that returns a bin as a boolean. Returns the unknown-value
        if the bin is not a boolean.
    """
    _op = _ExprOp.BIN
    _rt = ResultType.BOOLEAN

    def __init__(self, bin: str):
        """ Create an expression that returns a bin as a boolean. Returns the unknown-value
            if the bin is not a boolean.

            Args:
                bin (str): Bin name.

            :return: (boolean bin)
        
            Example::

                # Boolean bin "a" is True.
                expr = BoolBin("a").compile()
        """        
        self._fixed = {_Keys.BIN_KEY: bin}


class IntBin(_BaseExpr):
    """ Create an expression that returns a bin as an integer. Returns the unknown-value
        if the bin is not an integer.
    """
    _op = _ExprOp.BIN
    _rt = ResultType.INTEGER

    def __init__(self, bin: str):
        """ Create an expression that returns a bin as an integer. Returns the unknown-value
            if the bin is not an integer.

            Args:
                bin (str): Bin name.

            :return: (integer bin)
        
            Example::

                # Integer bin "a" == 200.
                expr = Eq(IntBin("a"), 200).compile()
        """        
        self._fixed = {_Keys.BIN_KEY: bin}


class StrBin(_BaseExpr):
    """ Create an expression that returns a bin as a string. Returns the unknown-value
        if the bin is not a string.
    """
    _op = _ExprOp.BIN
    _rt = ResultType.STRING

    def __init__(self, bin: str):
        """ Create an expression that returns a bin as a string. Returns the unknown-value
            if the bin is not a string.

            Args:
                bin (str): Bin name.

            :return: (string bin)
        
            Example::

                # String bin "a" == "xyz".
                expr = Eq(StrBin("a"), "xyz").compile()
        """        
        self._fixed = {_Keys.BIN_KEY: bin}


class FloatBin(_BaseExpr):
    """ Create an expression that returns a bin as a float. Returns the unknown-value
        if the bin is not a float.
    """
    _op = _ExprOp.BIN
    _rt = ResultType.FLOAT

    def __init__(self, bin: str):
        """ Create an expression that returns a bin as a float. Returns the unknown-value
            if the bin is not a float.

            Args:
                bin (str): Bin name.

            :return: (float bin)
        
            Example::

                # Float bin "a" > 2.71.
                expr = GT(FloatBin("a"), 2.71).compile()
        """        
        self._fixed = {_Keys.BIN_KEY: bin}


class BlobBin(_BaseExpr):
    """ Create an expression that returns a bin as a blob. Returns the unknown-value
        if the bin is not a blob.
    """
    _op = _ExprOp.BIN
    _rt = ResultType.BLOB

    def __init__(self, bin: str):
        """ Create an expression that returns a bin as a blob. Returns the unknown-value
            if the bin is not a blob.

            Args:
                bin (str): Bin name.

            :return (blob bin)
        
            Example::

                #. Blob bin "a" == bytearray([0x65, 0x65])
                expr = Eq(BlobBin("a"), bytearray([0x65, 0x65])).compile()
        """        
        self._fixed = {_Keys.BIN_KEY: bin}


class GeoBin(_BaseExpr):
    """ Create an expression that returns a bin as a geojson. Returns the unknown-value
        if the bin is not a geojson.
    """
    _op = _ExprOp.BIN
    _rt = ResultType.GEOJSON

    def __init__(self, bin: str):
        """ Create an expression that returns a bin as a geojson. Returns the unknown-value
            if the bin is not a geojson.

            Args:
                bin (str): Bin name.

            :return (geojson bin)
        
            Example::

                #GeoJSON bin "a" contained by GeoJSON bin "b".
                expr = CmpGeo(GeoBin("a"), GeoBin("b")).compile()
        """        
        self._fixed = {_Keys.BIN_KEY: bin}


class ListBin(_BaseExpr):
    """ Create an expression that returns a bin as a list. Returns the unknown-value
        if the bin is not a list.
    """
    _op = _ExprOp.BIN
    _rt = ResultType.LIST

    def __init__(self, bin: str):
        """ Create an expression that returns a bin as a list. Returns the unknown-value
            if the bin is not a list.

            Args:
                bin (str): Bin name.

            :return (list bin)
        
            Example::

                # List bin "a" contains at least one item with value "abc".
                expr = GT(ListGetByValue(None, aerospike.LIST_RETURN_COUNT, 
                            ResultType.INTEGER, "abc", ListBin("a")), 
                        0).compile()
        """        
        self._fixed = {_Keys.BIN_KEY: bin}


class MapBin(_BaseExpr):
    """ Create an expression that returns a bin as a map. Returns the unknown-value
        if the bin is not a map.
    """
    _op = _ExprOp.BIN
    _rt = ResultType.MAP

    def __init__(self, bin: str):
        """ Create an expression that returns a bin as a map. Returns the unknown-value
            if the bin is not a map.

            Args:
                bin (str): Bin name.

            :return (map bin)
        
            Example::

                # Map bin "a" size > 7.
                expr = GT(MapSize(None, MapBin("a")), 7).compile()
        """        
        self._fixed = {_Keys.BIN_KEY: bin}


class HLLBin(_BaseExpr):
    """ Create an expression that returns a bin as a HyperLogLog. Returns the unknown-value
        if the bin is not a HyperLogLog.
    """
    _op = _ExprOp.BIN
    _rt = ResultType.HLL

    def __init__(self, bin: str):
        """ Create an expression that returns a bin as a HyperLogLog. Returns the unknown-value
            if the bin is not a HyperLogLog.

            Args:
                bin (str): Bin name.

            :return (HyperLogLog bin)
        
            Example::

                # Does HLL bin "a" have a hll_count > 1000000.
                expr = GT(HllGetCount(HllBin("a"), 1000000)).compile()
        """        
        self._fixed = {_Keys.BIN_KEY: bin}


class BinExists(_BaseExpr):
    """Create an expression that returns True if bin exists."""
    _op = _ExprOp.BIN_EXISTS
    _rt = ResultType.BOOLEAN

    def __init__(self, bin: str):
        """ Create an expression that returns True if bin exists.

            Args:
                bin (str): bin name.

            :return (boolean value): True if bin exists, False otherwise.
        
            Example::

                #Bin "a" exists in record.
                expr = BinExists("a").compile()
        """        
        self._fixed = {_Keys.BIN_KEY: bin}


class BinType(_BaseExpr):
    """ Create an expression that returns the type of a bin
        as one of the aerospike :ref:`bin types <aerospike_bin_types>`
    """
    _op = _ExprOp.BIN_TYPE
    _rt = ResultType.INTEGER

    def __init__(self, bin: str):
        """ Create an expression that returns the type of a bin
            as one of the aerospike :ref:`bin types <aerospike_bin_types>`.

            Args:
                bin (str): bin name.

            :return (integer value): returns the bin type.
        
            Example::

                # bin "a" == type string.
                expr = Eq(BinType("a"), aerospike.AS_BYTES_STRING).compile()
        """        
        self._fixed = {_Keys.BIN_KEY: bin}


####################
# Record Expressions
####################


class SetName(_BaseExpr):
    """ Create an expression that returns record set name string.
        This expression usually evaluates quickly because record
        meta data is cached in memory.
    """
    _op = _ExprOp.META_SET_NAME
    _rt = ResultType.STRING

    def __init__(self):
        """ Create an expression that returns record set name string.
            This expression usually evaluates quickly because record
            meta data is cached in memory.

            :return (string value): Name of the set this record belongs to.
        
            Example::

                # Record set name == "myset".
                expr = Eq(SetName(), "myset").compile()
        """        
        super().__init__()


class DeviceSize(_BaseExpr):
    """ Create an expression that returns record size on disk. If server storage-engine is
        memory, then zero is returned. This expression usually evaluates quickly
        because record meta data is cached in memory.
    """
    _op = _ExprOp.META_DEVICE_SIZE
    _rt = ResultType.INTEGER

    def __init__(self):
        """ Create an expression that returns record size on disk. If server storage-engine is
            memory, then zero is returned. This expression usually evaluates quickly
            because record meta data is cached in memory.

            :return (integer value): Uncompressed storage size of the record.
        
            Example::

                # Record device size >= 100 KB.
                expr = GE(DeviceSize(), 100 * 1024).compile()
        """        
        super().__init__()


class LastUpdateTime(_BaseExpr):
    """ Create an expression that the returns record last update time expressed as 64 bit
        integer nanoseconds since 1970-01-01 epoch.
    """
    _op = _ExprOp.META_LAST_UPDATE_TIME
    _rt = ResultType.INTEGER

    def __init__(self):
        """ Create an expression that the returns record last update time expressed as 64 bit
            integer nanoseconds since 1970-01-01 epoch.

            :return (integer value): When the record was last updated.
        
            Example::

                # Record last update time >= 2020-01-15.
                expr = GE(LastUpdateTime(), 1577836800).compile()
        """        
        super().__init__()


class SinceUpdateTime(_BaseExpr):
    """ Create an expression that returns milliseconds since the record was last updated.
        This expression usually evaluates quickly because record meta data is cached in memory.
    """
    _op = _ExprOp.META_SINCE_UPDATE_TIME
    _rt = ResultType.INTEGER

    def __init__(self):
        """ Create an expression that returns milliseconds since the record was last updated.
            This expression usually evaluates quickly because record meta data is cached in memory.

            :return (integer value): Number of milliseconds since last updated.
        
            Example::

                # Record last updated more than 2 hours ago.
                expr = GT(SinceUpdateTime(), 2 * 60 * 1000).compile()
        """        
        super().__init__()    


class VoidTime(_BaseExpr):
    """ Create an expression that returns record expiration time expressed as 64 bit
        integer nanoseconds since 1970-01-01 epoch.
    """
    _op = _ExprOp.META_VOID_TIME
    _rt = ResultType.INTEGER

    def __init__(self):
        """ Create an expression that returns record expiration time expressed as 64 bit
            integer nanoseconds since 1970-01-01 epoch.

            :return (integer value): Expiration time in nanoseconds since 1970-01-01.
        
            Example::

                # Record expires on 2021-01-01.
                expr = And(
                        GE(VoidTime(), 1609459200),
                        LT(VoidTime(), 1609545600)).compile()
        """        
        super().__init__()  


class TTL(_BaseExpr):
    """ Create an expression that returns record expiration time (time to live) in integer
        seconds.
    """
    _op = _ExprOp.META_TTL
    _rt = ResultType.INTEGER

    def __init__(self):
        """ Create an expression that returns record expiration time (time to live) in integer
            seconds.

            :return (integer value): Number of seconds till the record will expire,
                                    returns -1 if the record never expires.
        
            Example::

                # Record expires in less than 1 hour.
                expr = LT(TTL(), 60 * 60).compile()
        """
        super().__init__()  


class IsTombstone(_BaseExpr):
    """ Create an expression that returns if record has been deleted and is still in
        tombstone state. This expression usually evaluates quickly because record
        meta data is cached in memory. NOTE: this is only applicable for XDR filter expressions.
    """
    _op = _ExprOp.META_IS_TOMBSTONE
    _rt = ResultType.BOOLEAN

    def __init__(self):
        """ Create an expression that returns if record has been deleted and is still in
            tombstone state. This expression usually evaluates quickly because record
            meta data is cached in memory. NOTE: this is only applicable for XDR filter expressions.

            :return (boolean value): True if the record is a tombstone, false otherwise.
        
            Example::

                # Detect deleted records that are in tombstone state.
                expr = IsTombstone().compile()
        """
        super().__init__() 


class DigestMod(_BaseExpr):
    """Create an expression that returns record digest modulo as integer."""
    _op = _ExprOp.META_DIGEST_MOD
    _rt = ResultType.INTEGER

    def __init__(self, mod: int):
        """ Create an expression that returns record digest modulo as integer.

            Args:
                mod (int): Divisor used to divide the digest to get a remainder.

            :return (integer value): Value in range 0 and mod (exclusive).
        
            Example::

                # Records that have digest(key) % 3 == 1.
                expr = Eq(DigestMod(3), 1).compile()
        """        
        self._fixed = {_Keys.VALUE_KEY: mod}


########################
# Comparison Expressions
########################


class Eq(_BaseExpr):
    """Create an equals, (==) expression."""
    _op = _ExprOp.EQ

    def __init__(self, expr0: TypeComparisonArg, expr1: TypeComparisonArg):
        """ Create an equals, (==) expression.

        Args:
            expr0 (TypeComparisonArg): Left argument to `==`.
            expr1 (TypeComparisonArg): Right argument to `==`.

        :return: (boolean value)

        Example::

            # Integer bin "a" == 11
            expr = Eq(IntBin("a"), 11).compile()
        """        
        self._children = (expr0, expr1)


class NE(_BaseExpr):
    """Create a not equals (not ==) expressions."""
    _op = _ExprOp.NE

    def __init__(self, expr0: TypeComparisonArg, expr1: TypeComparisonArg):
        """ Create a not equals (not ==) expressions.

            Args:
                expr0 (TypeComparisonArg): Left argument to `not ==`.
                expr1 (TypeComparisonArg): Right argument to `not ==`.
        
            :return: (boolean value)

            Example::

                # Integer bin "a" not == 13.
                expr = NE(IntBin("a"), 13).compile()
        """                 
        self._children = (expr0, expr1)


class GT(_BaseExpr):
    """Create a greater than (>) expression."""
    _op = _ExprOp.GT

    def __init__(self, expr0: TypeComparisonArg, expr1: TypeComparisonArg):
        """ Create a greater than (>) expression.

            Args:
                expr0 (TypeComparisonArg): Left argument to `>`.
                expr1 (TypeComparisonArg): Right argument to `>`.
        
            :return: (boolean value)

            Example::

                # Integer bin "a" > 8.
                expr = GT(IntBin("a"), 8).compile()
        """
        self._children = (expr0, expr1)


class GE(_BaseExpr):
    """Create a greater than or equal to (>=) expression."""
    _op = _ExprOp.GE

    def __init__(self, expr0: TypeComparisonArg, expr1: TypeComparisonArg):
        """ Create a greater than or equal to (>=) expression.

            Args:
                expr0 (TypeComparisonArg): Left argument to `>=`.
                expr1 (TypeComparisonArg): Right argument to `>=`.
        
            :return: (boolean value)

            Example::

                # Integer bin "a" >= 88.
                expr = GE(IntBin("a"), 88).compile()
        """
        self._children = (expr0, expr1)


class LT(_BaseExpr):
    """Create a less than (<) expression."""
    _op = _ExprOp.LT

    def __init__(self, expr0: TypeComparisonArg, expr1: TypeComparisonArg):
        """ Create a less than (<) expression.

            Args:
                expr0 (TypeComparisonArg): Left argument to `<`.
                expr1 (TypeComparisonArg): Right argument to `<`.
        
            :return: (boolean value)

            Example::

                # Integer bin "a" < 1000.
                expr = LT(IntBin("a"), 1000).compile()
        """
        self._children = (expr0, expr1)


class LE(_BaseExpr):
    """Create a less than or equal to (<=) expression."""
    _op = _ExprOp.LE

    def __init__(self, expr0: TypeComparisonArg, expr1: TypeComparisonArg):
        """ Create a less than or equal to (<=) expression.

            Args:
                expr0 (TypeComparisonArg): Left argument to `<=`.
                expr1 (TypeComparisonArg): Right argument to `<=`.
        
            :return: (boolean value)

            Example::

                # Integer bin "a" <= 1.
                expr = LE(IntBin("a"), 1).compile()
        """
        self._children = (expr0, expr1)


class CmpRegex(_BaseExpr):
    """ Create an expression that performs a regex match on a string bin or value expression."""
    _op = _ExprOp.CMP_REGEX

    def __init__(self, options: int, regex_str: str, cmp_str: Union[_BaseExpr, str]):
        """ Create an expression that performs a regex match on a string bin or value expression.

            Args:
                options (int) :ref:`regex_constants`: One of the aerospike regex constants, :ref:`regex_constants`.
                regex_str (str): POSIX regex string.
                cmp_str (Union[_BaseExpr, str]): String expression to compare against.
        
            :return: (boolean value)

            Example::

                # Select string bin "a" that starts with "prefix" and ends with "suffix".
                # Ignore case and do not match newline.
                expr = CmpRegex(aerospike.REGEX_ICASE | aerospike.REGEX_NEWLINE, "prefix.*suffix", BinStr("a")).compile()
        """        
        self._children = (cmp_str,)
        self._fixed = {_Keys.REGEX_OPTIONS_KEY: options, _Keys.VALUE_KEY: regex_str}


class CmpGeo(_BaseExpr):
    """Create a point within region or region contains point expression."""
    _op = _ExprOp.CMP_GEO

    def __init__(self, expr0: TypeGeo, expr1: TypeGeo):
        """ Create a point within region or region contains point expression.

            Args:
                expr0 (TypeGeo): Left expression in comparrison.
                expr1 (TypeGeo): Right expression in comparrison.
        
            :return: (boolean value)

            Example::

                # Geo bin "point" is within geo bin "region".
                expr = CmpGeo(GeoBin("point"), GeoBin("region")).compile()
        """        
        self._children = (expr0, expr1)


#####################
# Logical Expressions
#####################


class Not(_BaseExpr):
    """Create a "not" (not) operator expression."""
    _op = _ExprOp.NOT

    def __init__(self, *exprs):
        """ Create a "not" (not) operator expression.

            Args:
                `*exprs` (_BaseExpr): Variable amount of expressions to be negated.
        
            :return: (boolean value)

            Example::

                # not (a == 0 or a == 10)
                expr = Not(Or(
                            Eq(IntBin("a"), 0),
                            Eq(IntBin("a"), 10))).compile()
        """        
        self._children = exprs


class And(_BaseExpr):
    """Create an "and" operator that applies to a variable amount of expressions."""
    _op = _ExprOp.AND

    def __init__(self, *exprs: _BaseExpr):
        """ Create an "and" operator that applies to a variable amount of expressions.

        Args:
            `*exprs` (_BaseExpr): Variable amount of expressions to be ANDed together.

        :return: (boolean value)

        Example::

            # (a > 5 || a == 0) && b < 3
            expr = And(
                Or(
                    GT(IntBin("a"), 5),
                    Eq(IntBin("a"), 0)),
                LT(IntBin("b"), 3)).compile()
        """
        self._children = exprs + (_GenericExpr(_ExprOp._AS_EXP_CODE_END_OF_VA_ARGS, 0, {}),)


class Or(_BaseExpr):
    """Create an "or" operator that applies to a variable amount of expressions."""
    _op = _ExprOp.OR

    def __init__(self, *exprs):
        """ Create an "or" operator that applies to a variable amount of expressions.

        Args:
            `*exprs` (_BaseExpr): Variable amount of expressions to be ORed together.

        :return: (boolean value)

        Example::

            # (a == 0 || b == 0)
            expr = Or(
                    Eq(IntBin("a"), 0),
                    Eq(IntBin("b"), 0)).compile()
        """ 
        self._children = exprs + (_GenericExpr(_ExprOp._AS_EXP_CODE_END_OF_VA_ARGS, 0, {}),)


class Exclusive(_BaseExpr):
    """Create an expression that returns True if only one of the expressions are True."""
    _op = _ExprOp.EXCLUSIVE

    def __init__(self, *exprs: _BaseExpr):
        """ Create an expression that returns True if only one of the expressions are True.

        Args:
            `*exprs` (_BaseExpr): Variable amount of expressions to be checked.

        :return: (boolean value)

        Example::

            # exclusive(a == 0, b == 0)
            expr = exclusive(
                            Eq(IntBin("a"), 0),
                            Eq(IntBin("b"), 0)).compile()
        """
        self._children = exprs + (_GenericExpr(_ExprOp._AS_EXP_CODE_END_OF_VA_ARGS, 0, {}),)


#######################################
# Flow Control and Variable Expressions
#######################################


class Cond(_BaseExpr):
    """ Conditionally select an expression from a variable number of expression pairs
        followed by default expression action. Takes a set of test-expression/action-expression pairs. It evaluates
        each test one at a time. If a test returns True, 'cond'
        evaluates and returns the value of the corresponding expression and
        doesn't evaluate any of the other tests or expressions. The final
        expression is the default expression and is evaluated if all tests
        evaluate to False
        All actions-expressions must evaluate to the same type or be the
        Unknown expression.
    """
    _op = _ExprOp.COND

    def __init__(self, *exprs: _BaseExpr):
        """ Conditionally select an expression from a variable number of expression pairs
            followed by default expression action. Takes a set of test-expression/action-expression pairs. It evaluates
            each test one at a time. If a test returns True, 'cond'
            evaluates and returns the value of the corresponding expression and
            doesn't evaluate any of the other tests or expressions. The final
            expression is the default expression and is evaluated if all tests
            evaluate to False
            All actions-expressions must evaluate to the same type or be the
            Unknown expression.
            Requires server version 5.6.0+.

        Args:
            `*exprs` (_BaseExpr): bool exp1, action exp1, bool exp2, action exp2, ..., action-default

        :return: (boolean value)

        Example::

            # Apply operator based on type and test if greater than 100.
            expr = GT(
                    Cond(
                        Eq(IntBin("type"), 0),
                            Add(IntBin("val1"), IntBin("val2")),
                        Eq(IntBin("type"), 1),
                            Sub(IntBin("val1"), IntBin("val2")),
                        Eq(IntBin("type"), 2),
                            Mul(IntBin("val1"), IntBin("val2")))
                    100).compile()
        """
        self._children = exprs + (_GenericExpr(_ExprOp._AS_EXP_CODE_END_OF_VA_ARGS, 0, {}),)


class Let(_BaseExpr):
    """ Defines variables to be used within the <code>let</code> expression's scope. The last
        argument can be any expression and should make use of the defined
        variables. The <code>let</code> expression returns the evaluated result of the last
        argument. This expression is useful if you need to reuse the result of a
        complicated or expensive expression.
    """
    _op = _ExprOp.LET

    def __init__(self, *exprs: _BaseExpr):
        """ Defines variables to be used within the <code>let</code> expression's scope. The last
            argument can be any expression and should make use of the defined
            variables. The <code>let</code> expression returns the evaluated result of the last
            argument. This expression is useful if
            Requires server version 5.6.0+.

        Args:
            `*exprs` (_BaseExpr): Variable number of Def expressions followed by a scoped expression.

        :return: (result of scoped expression)

        Example::

            # for int bin "a", 5 < a < 10
            expr = Let(Def("x", IntBin("a")),
                    And(
                        LT(5, Var("x")),
                        LT(Var("x"), 10))).compile()
        """
        self._children = exprs + (_GenericExpr(_ExprOp._AS_EXP_CODE_END_OF_VA_ARGS, 0, {}),)


class Def(_BaseExpr):
    """ Assign variable to an expression that can be accessed later."""
    _op = _ExprOp.DEF

    def __init__(self, var_name: str, expr: _BaseExpr):
        """ Assign variable to an expression that can be accessed later.
            Requires server version 5.6.0+.

        Args:
            `var_name` (str): Variable name.
            `expr` (_BaseExpr): Variable is set to result of this expression.

        :return: (a variabe name expression pair)

        Example::

            # for int bin "a", 5 < a < 10
            expr = Let(Def("x", IntBin("a")),
                    And(
                        LT(5, Var("x")),
                        LT(Var("x"), 10))).compile()
        """
        self._fixed = {_Keys.VALUE_KEY: var_name}
        self._children = (expr,)


class Var(_BaseExpr):
    """ Retrieve expression value from a variable."""
    _op = _ExprOp.VAR

    def __init__(self, var_name: str):
        """ Retrieve expression value from a variable.
            Requires server version 5.6.0+.

        Args:
            `var_name` (str): Variable name.

        :return: (value stored in variable)

        Example::

            # for int bin "a", 5 < a < 10
            expr = Let(Def("x", IntBin("a")),
                    And(
                        LT(5, Var("x")),
                        LT(Var("x"), 10))).compile()
        """
        self._fixed = {_Keys.VALUE_KEY: var_name}