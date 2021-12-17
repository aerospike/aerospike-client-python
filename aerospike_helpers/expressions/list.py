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
List expressions contain expressions for reading and modifying Lists. Most of
these operations are from the standard :mod:`List API <aerospike_helpers.operations.list_operations>`.

Example::

    import aerospike_helpers.expressions as exp
    #Take the size of list bin "a".
    expr = exp.ListSize(None, exp.ListBin("a")).compile()
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
from aerospike_helpers.expressions.base import ListBin

######################
# List Mod Expressions
######################

TypeBinName = Union[_BaseExpr, str]
TypeListValue = Union[_BaseExpr, List[Any]]
TypeIndex = Union[_BaseExpr, int, aerospike.CDTInfinite]
TypeCTX = Union[None, List[cdt_ctx._cdt_ctx]]
TypeRank = Union[_BaseExpr, int, aerospike.CDTInfinite]
TypeCount = Union[_BaseExpr, int, aerospike.CDTInfinite]
TypeValue = Union[_BaseExpr, Any]
TypePolicy = Union[Dict[str, Any], None]


class ListAppend(_BaseExpr):
    """Create an expression that appends value to end of list."""
    _op = aerospike.OP_LIST_APPEND

    def __init__(self, ctx: 'TypeCTX', policy: 'TypePolicy', value: 'TypeValue', bin: 'TypeBinName'):
        """ Args:
                ctx (TypeCTX): An optional list of nested CDT :mod:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
                policy (TypePolicy): Optional dictionary of :ref:`List policies <aerospike_list_policies>`.
                value (TypeValue): Value or value expression to append to list.
                bin (TypeBinName): bin expression, such as :class:`~aerospike_helpers.expressions.base.MapBin` or :class:`~aerospike_helpers.expressions.base.ListBin`.

            :return: List expression.

            Example::

                # Check if length of list bin "a" is > 5 after appending 1 item.
                expr = exp.GT(
                        exp.ListSize(None, exp.ListAppend(None, None, 3, exp.ListBin("a"))),
                        5).compile()
        """
        self._children = (
            value,
            _GenericExpr(_ExprOp._AS_EXP_CODE_CDT_LIST_CRMOD, 0, {_Keys.LIST_POLICY_KEY: policy} if policy is not None else {}),
            bin if isinstance(bin, _BaseExpr) else ListBin(bin)
        )
        self._fixed = {}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx

        if policy is not None:
            self._fixed[_Keys.LIST_POLICY_KEY] = policy


class ListAppendItems(_BaseExpr):
    """Create an expression that appends a list of items to the end of a list."""
    _op = aerospike.OP_LIST_APPEND_ITEMS

    def __init__(self, ctx: 'TypeCTX', policy: 'TypePolicy', value: 'TypeValue', bin: 'TypeBinName'):
        """ Args:
                ctx (TypeCTX): An optional list of nested CDT :mod:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
                policy (TypePolicy): Optional dictionary of :ref:`List policies <aerospike_list_policies>`.
                value (TypeValue): List or list expression of items to be appended.
                bin (TypeBinName): bin expression, such as :class:`~aerospike_helpers.expressions.base.MapBin` or :class:`~aerospike_helpers.expressions.base.ListBin`.

            :return: List expression.

            Example::

                # Check if length of list bin "a" is > 5 after appending multiple items.
                expr = exp.GT(
                        exp.ListSize(None, exp.ListAppendItems(None, None, [3, 2], exp.ListBin("a"))),
                        5).compile()
        """
        self._children = (
            value,
            _GenericExpr(_ExprOp._AS_EXP_CODE_CDT_LIST_CRMOD, 0, {_Keys.LIST_POLICY_KEY: policy} if policy is not None else {}),
            bin if isinstance(bin, _BaseExpr) else ListBin(bin)
        )
        self._fixed = {}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx

        if policy is not None:
            self._fixed[_Keys.LIST_POLICY_KEY] = policy


class ListInsert(_BaseExpr):
    """Create an expression that inserts value to specified index of list."""
    _op = aerospike.OP_LIST_INSERT

    def __init__(self, ctx: 'TypeCTX', policy: 'TypePolicy', index: 'TypeIndex', value: 'TypeValue', bin: 'TypeBinName'):
        """ Args:
                ctx (TypeCTX): An optional list of nested CDT :mod:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
                policy (TypePolicy): Optional dictionary of :ref:`List policies <aerospike_list_policies>`.
                index (TypeIndex): Target index for insertion, integer or integer expression.
                value (TypeValue): Value or value expression to be inserted.
                bin (TypeBinName): bin expression, such as :class:`~aerospike_helpers.expressions.base.MapBin` or :class:`~aerospike_helpers.expressions.base.ListBin`.

            :return: List expression.

            Example::

                # Check if list bin "a" has length > 5 after insert.
                expr = exp.GT(
                        exp.ListSize(None, exp.ListInsert(None, None, 0, 3, exp.ListBin("a"))),
                        5).compile()
        """
        self._children = (
            index,
            value,
            _GenericExpr(_ExprOp._AS_EXP_CODE_CDT_LIST_MOD, 0, {_Keys.LIST_POLICY_KEY: policy} if policy is not None else {}),
            bin if isinstance(bin, _BaseExpr) else ListBin(bin)
        )
        self._fixed = {}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx

        if policy is not None:
            self._fixed[_Keys.LIST_POLICY_KEY] = policy


class ListInsertItems(_BaseExpr):
    """Create an expression that inserts each input list item starting at specified index of list."""
    _op = aerospike.OP_LIST_INSERT_ITEMS

    def __init__(self, ctx: 'TypeCTX', policy: 'TypePolicy', index: 'TypeIndex', values: 'TypeListValue', bin: 'TypeBinName'):
        """ Args:
                ctx (TypeCTX): An optional list of nested CDT :mod:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
                policy (TypePolicy): Optional dictionary of :ref:`List policies <aerospike_list_policies>`.
                index (TypeIndex): Target index where item insertion will begin, integer or integer expression.
                values (TypeListValue): List or list expression of items to be inserted.
                bin (TypeBinName): bin expression, such as :class:`~aerospike_helpers.expressions.base.MapBin` or :class:`~aerospike_helpers.expressions.base.ListBin`.

            :return: List expression.

            Example::

                # Check if list bin "a" has length > 5 after inserting items.
                expr = exp.GT(
                        exp.ListSize(None, exp.ListInsertItems(None, None, 0, [4, 7], exp.ListBin("a"))),
                        5).compile()
        """
        self._children = (
            index,
            values,
            _GenericExpr(_ExprOp._AS_EXP_CODE_CDT_LIST_MOD, 0, {_Keys.LIST_POLICY_KEY: policy} if policy is not None else {}),
            bin if isinstance(bin, _BaseExpr) else ListBin(bin)
        )
        self._fixed = {}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx

        if policy is not None:
            self._fixed[_Keys.LIST_POLICY_KEY] = policy


class ListIncrement(_BaseExpr):
    """Create an expression that increments list[index] by value."""
    _op = aerospike.OP_LIST_INCREMENT

    def __init__(self, ctx: 'TypeCTX', policy: 'TypePolicy', index: 'TypeIndex', value: 'TypeValue', bin: 'TypeBinName'):
        """ Args:
                ctx (TypeCTX): An optional list of nested CDT :mod:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
                policy (TypePolicy): Optional dictionary of :ref:`List policies <aerospike_list_policies>`.
                index (TypeIndex): Index of value to increment.
                value (TypeValue): Value or value expression.
                bin (TypeBinName): bin expression, such as :class:`~aerospike_helpers.expressions.base.MapBin` or :class:`~aerospike_helpers.expressions.base.ListBin`.

            :return: List expression.

            Example::

                # Check if incremented value in list bin "a" is the largest in the list.
                expr = exp.Eq(
                        exp.ListGetByRank(None, aerospike.LIST_RETURN_VALUE, ResultType.INTEGER, -1, #rank of -1 == largest element.
                            exp.ListIncrement(None, None, 1, 5, exp.ListBin("a"))),
                        exp.ListGetByIndex(None, aerospike.LIST_RETURN_VALUE, ResultType.INTEGER, 1,
                            exp.ListIncrement(None, None, 1, 5, exp.ListBin("a")))
                ).compile()
        """
        self._children = (
            index,
            value,
            _GenericExpr(_ExprOp._AS_EXP_CODE_CDT_LIST_CRMOD, 0, {_Keys.LIST_POLICY_KEY: policy} if policy is not None else {}),
            bin if isinstance(bin, _BaseExpr) else ListBin(bin)
        )
        self._fixed = {}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx

        if policy is not None:
            self._fixed[_Keys.LIST_POLICY_KEY] = policy


class ListSet(_BaseExpr):
    """Create an expression that sets item value at specified index in list."""
    _op = aerospike.OP_LIST_SET

    def __init__(self, ctx: 'TypeCTX', policy: 'TypePolicy', index: 'TypeIndex', value: 'TypeValue', bin: 'TypeBinName'):
        """ Args:
                ctx (TypeCTX): An optional list of nested CDT :mod:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
                policy (TypePolicy): Optional dictionary of :ref:`List policies <aerospike_list_policies>`.
                index (TypeIndex): index of value to set.
                value (TypeValue): value or value expression to set index in list to.
                bin (TypeBinName): bin expression, such as :class:`~aerospike_helpers.expressions.base.MapBin` or :class:`~aerospike_helpers.expressions.base.ListBin`.

            :return: List expression.

            Example::

                # Get smallest element in list bin "a" after setting index 1 to 10.
                expr = exp.ListGetByRank(None, aerospike.LIST_RETURN_VALUE, ResultType.INTEGER, 0,
                                exp.ListSet(None, None, 1, 10, exp.ListBin("a"))).compile()
        """
        self._children = (
            index,
            value,
            _GenericExpr(_ExprOp._AS_EXP_CODE_CDT_LIST_MOD, 0, {_Keys.LIST_POLICY_KEY: policy} if policy is not None else {}),
            bin if isinstance(bin, _BaseExpr) else ListBin(bin)
        )
        self._fixed = {}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx

        if policy is not None:
            self._fixed[_Keys.LIST_POLICY_KEY] = policy


class ListClear(_BaseExpr):
    """Create an expression that removes all items in a list."""
    _op = aerospike.OP_LIST_CLEAR

    def __init__(self, ctx: 'TypeCTX', bin: 'TypeBinName'):
        """ Args:
                ctx (TypeCTX): An optional list of nested CDT :mod:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
                bin (TypeBinName): bin expression, such as :class:`~aerospike_helpers.expressions.base.MapBin` or :class:`~aerospike_helpers.expressions.base.ListBin`.

            :return: List expression.

            Example::

                # Clear list value of list nested in list bin "a" index 1.
                from aerospike_helpers import cdt_ctx
                expr = exp.ListClear([cdt_ctx.cdt_ctx_list_index(1)], "a").compile()
        """
        self._children = (
            bin if isinstance(bin, _BaseExpr) else ListBin(bin),
        )
        self._fixed = {}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class ListSort(_BaseExpr):
    """Create an expression that sorts a list."""
    _op = aerospike.OP_LIST_SORT

    def __init__(self, ctx: 'TypeCTX', order: int, bin: 'TypeBinName'):
        """ Args:
                ctx (TypeCTX): An optional list of nested CDT :mod:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
                order (int): Optional flags modifiying the behavior of list_sort. This should be constructed by bitwise or'ing together values from :ref:`aerospike_list_sort_flag`.
                bin (TypeBinName): bin expression, such as :class:`~aerospike_helpers.expressions.base.MapBin` or :class:`~aerospike_helpers.expressions.base.ListBin`.

            :return: list expression.

            Example::

                # Get value of sorted list bin "a".
                expr = exp.ListSort(None, aerospike.LIST_SORT_DEFAULT, "a").compile()
        """
        self._children = (
            bin if isinstance(bin, _BaseExpr) else ListBin(bin),
        )
        self._fixed = {_Keys.LIST_ORDER_KEY: order}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class ListRemoveByValue(_BaseExpr):
    """Create an expression that removes list items identified by value."""
    _op = aerospike.OP_LIST_REMOVE_BY_VALUE

    def __init__(self, ctx: 'TypeCTX', value: 'TypeValue', bin: 'TypeBinName'):
        """ Args:
                ctx (TypeCTX): An optional list of nested CDT :mod:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
                value (TypeValue): Value or value expression to remove.
                bin (TypeBinName): bin expression, such as :class:`~aerospike_helpers.expressions.base.MapBin` or :class:`~aerospike_helpers.expressions.base.ListBin`.

            :return: list expression.

            Example::

                # See if list bin "a", with `3` removed, is equal to list bin "b".
                expr = exp.Eq(exp.ListRemoveByValue(None, 3, exp.ListBin("a")), ListBin("b")).compile()
        """
        self._children = (
            value,
            bin if isinstance(bin, _BaseExpr) else ListBin(bin)
        )
        self._fixed = {}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class ListRemoveByValueList(_BaseExpr):
    """Create an expression that removes list items identified by values."""
    _op = aerospike.OP_LIST_REMOVE_BY_VALUE_LIST

    def __init__(self, ctx: 'TypeCTX', values: 'TypeListValue', bin: 'TypeBinName'):
        """ Args:
                ctx (TypeCTX): An optional list of nested CDT :mod:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
                values (TypeListValue): List of values or list expression.
                bin (TypeBinName): bin expression, such as :class:`~aerospike_helpers.expressions.base.MapBin` or :class:`~aerospike_helpers.expressions.base.ListBin`.

            :return: list expression.

            Example::

                # Remove elements with values [1, 2, 3] from list bin "a".
                expr = exp.ListRemoveByValueList(None, [1, 2, 3], exp.ListBin("a")).compile()
        """
        self._children = (
            values,
            bin if isinstance(bin, _BaseExpr) else ListBin(bin)
        )
        self._fixed = {}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class ListRemoveByValueRange(_BaseExpr):
    """ Create an expression that removes list items identified by value range
        (begin inclusive, end exclusive). If begin is None, the range is less than end.
        If end is None, the range is greater than or equal to begin.
    """
    _op = aerospike.OP_LIST_REMOVE_BY_VALUE_RANGE

    def __init__(self, ctx: 'TypeCTX', begin: 'TypeValue', end: 'TypeValue', bin: 'TypeBinName'):
        """ Args:
                ctx (TypeCTX): An optional list of nested CDT :mod:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
                begin (TypeValue): Begin value or value expression for range.
                end (TypeValue): End value or value expression for range.
                bin (TypeBinName): bin expression, such as :class:`~aerospike_helpers.expressions.base.MapBin` or :class:`~aerospike_helpers.expressions.base.ListBin`.

            :return: list expression.

            Example::

                # Remove list of items with values >= 3 and < 7 from list bin "a".
                expr = exp.ListRemoveByValueRange(None, 3, 7, exp.ListBin("a")).compile()
        """
        self._children = (
            begin,
            end,
            bin if isinstance(bin, _BaseExpr) else ListBin(bin)
        )
        self._fixed = {}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class ListRemoveByValueRelRankToEnd(_BaseExpr):
    """Create an expression that removes list items nearest to value and greater by relative rank."""
    _op = aerospike.OP_LIST_REMOVE_BY_REL_RANK_RANGE_TO_END

    def __init__(self, ctx: 'TypeCTX', value: 'TypeValue', rank: 'TypeRank', bin: 'TypeBinName'):
        """ Args:
                ctx (TypeCTX): An optional list of nested CDT :mod:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
                value (TypeValue): Start value or value expression.
                rank (TypeRank): Rank integer or integer expression.
                bin (TypeBinName): bin expression, such as :class:`~aerospike_helpers.expressions.base.MapBin` or :class:`~aerospike_helpers.expressions.base.ListBin`.

            :return: list expression.

            Example::

                # Remove elements larger than 4 by relative rank in list bin "a".
                expr = exp.ListRemoveByValueRelRankToEnd(None, 4, 1, exp.ListBin("a")).compile()
        """
        self._children = (
            value,
            rank,
            bin if isinstance(bin, _BaseExpr) else ListBin(bin)
        )
        self._fixed = {}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class ListRemoveByValueRelRankRange(_BaseExpr):
    """ Create an expression that removes list items nearest to value and greater by relative rank with a
        count limit.
    """
    _op = aerospike.OP_LIST_REMOVE_BY_REL_RANK_RANGE

    def __init__(self, ctx: 'TypeCTX', value: 'TypeValue', rank: 'TypeRank', count: 'TypeCount', bin: 'TypeBinName'):
        """ Args:
                ctx (TypeCTX): An optional list of nested CDT :mod:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
                value (TypeValue): Start value or value expression.
                rank (TypeRank): Rank integer or integer expression.
                count (TypeCount): How many elements to remove.
                bin (TypeBinName): bin expression, such as :class:`~aerospike_helpers.expressions.base.MapBin` or :class:`~aerospike_helpers.expressions.base.ListBin`.

            :return: list expression.

            Example::

                # After removing the 3 elements larger than 4 by relative rank, does list bin "a" include 9?.
                expr = exp.GT(
                        exp.ListGetByValue(None, aerospike.LIST_RETURN_COUNT, 9,
                            exp.ListRemoveByValueRelRankRange(None, 4, 1, 0, exp.ListBin("a"))),
                        0).compile()
        """
        self._children = (
            value,
            rank,
            count,
            bin if isinstance(bin, _BaseExpr) else ListBin(bin)
        )
        self._fixed = {}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class ListRemoveByIndex(_BaseExpr):
    """Create an expression that removes "count" list items starting at specified index."""
    _op = aerospike.OP_LIST_REMOVE_BY_INDEX

    def __init__(self, ctx: 'TypeCTX', index: 'TypeIndex', bin: 'TypeBinName'):
        """ Args:
                ctx (TypeCTX): An optional list of nested CDT :mod:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
                index (TypeIndex): Index integer or integer expression of element to remove.
                bin (TypeBinName): bin expression, such as :class:`~aerospike_helpers.expressions.base.MapBin` or :class:`~aerospike_helpers.expressions.base.ListBin`.

            :return: list expression.

            Example::

                # Get size of list bin "a" after index 3 has been removed.
                expr = exp.ListSize(None, exp.ListRemoveByIndex(None, 3, exp.ListBin("a"))).compile()
        """
        self._children = (
            index,
            bin if isinstance(bin, _BaseExpr) else ListBin(bin)
        )
        self._fixed = {}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class ListRemoveByIndexRangeToEnd(_BaseExpr):
    """Create an expression that removes list items starting at specified index to the end of list."""
    _op = aerospike.OP_LIST_REMOVE_BY_INDEX_RANGE_TO_END

    def __init__(self, ctx: 'TypeCTX', index: 'TypeIndex', bin: 'TypeBinName'):
        """ Args:
                ctx (TypeCTX): An optional list of nested CDT :mod:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
                index (TypeIndex): Starting index integer or integer expression of elements to remove.
                bin (TypeBinName): bin expression, such as :class:`~aerospike_helpers.expressions.base.MapBin` or :class:`~aerospike_helpers.expressions.base.ListBin`.

            :return: list expression.

            Example::

                # Remove all elements starting from index 3 in list bin "a".
                expr = exp.ListRemoveByIndexRangeToEnd(None, 3, exp.ListBin("a")).compile()
        """
        self._children = (
            index,
            bin if isinstance(bin, _BaseExpr) else ListBin(bin)
        )
        self._fixed = {}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class ListRemoveByIndexRange(_BaseExpr):
    """Create an expression that removes "count" list items starting at specified index."""
    _op = aerospike.OP_LIST_REMOVE_BY_INDEX_RANGE

    def __init__(self, ctx: 'TypeCTX', index: 'TypeIndex', count: 'TypeCount', bin: 'TypeBinName'):
        """ Args:
                ctx (TypeCTX): An optional list of nested CDT :mod:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
                index (TypeIndex): Starting index integer or integer expression of elements to remove.
                count (TypeCount): Integer or integer expression, how many elements to remove.
                bin (TypeBinName): bin expression, such as :class:`~aerospike_helpers.expressions.base.MapBin` or :class:`~aerospike_helpers.expressions.base.ListBin`.

            :return: list expression.

            Example::

                # Get size of list bin "a" after index 3, 4, and 5 have been removed.
                expr = exp.ListSize(None, exp.ListRemoveByIndexRange(None, 3, 3, exp.ListBin("a"))).compile()
        """
        self._children = (
            index,
            count,
            bin if isinstance(bin, _BaseExpr) else ListBin(bin)
        )
        self._fixed = {}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class ListRemoveByRank(_BaseExpr):
    """Create an expression that removes list item identified by rank."""
    _op = aerospike.OP_LIST_REMOVE_BY_RANK

    def __init__(self, ctx: 'TypeCTX', rank: 'TypeRank', bin: 'TypeBinName'):
        """ Args:
                ctx (TypeCTX): An optional list of nested CDT :mod:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
                rank (TypeRank): Rank integer or integer expression of element to remove.
                bin (TypeBinName): bin expression, such as :class:`~aerospike_helpers.expressions.base.MapBin` or :class:`~aerospike_helpers.expressions.base.ListBin`.

            :return: list expression.

            Example::

                # Remove smallest value in list bin "a".
                expr = exp.ListRemoveByRank(None, 0, exp.ListBin("a")).compile()
        """
        self._children = (
            rank,
            bin if isinstance(bin, _BaseExpr) else ListBin(bin)
        )
        self._fixed = {}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class ListRemoveByRankRangeToEnd(_BaseExpr):
    """Create an expression that removes list items starting at specified rank to the last ranked item."""
    _op = aerospike.OP_LIST_REMOVE_BY_RANK_RANGE_TO_END

    def __init__(self, ctx: 'TypeCTX', rank: 'TypeRank', bin: 'TypeBinName'):
        """ Args:
                ctx (TypeCTX): An optional list of nested CDT :mod:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
                rank (TypeRank): Rank integer or integer expression of element to start removing at.
                bin (TypeBinName): bin expression, such as :class:`~aerospike_helpers.expressions.base.MapBin` or :class:`~aerospike_helpers.expressions.base.ListBin`.

            :return: list expression.

            Example::

                # Remove the 2 largest elements from List bin "a".
                expr = exp.ListRemoveByRankRangeToEnd(None, -2, exp.ListBin("a")).compile()
        """
        self._children = (
            rank,
            bin if isinstance(bin, _BaseExpr) else ListBin(bin)
        )
        self._fixed = {}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class ListRemoveByRankRange(_BaseExpr):
    """Create an expression that removes "count" list items starting at specified rank."""
    _op = aerospike.OP_LIST_REMOVE_BY_RANK_RANGE

    def __init__(self, ctx: 'TypeCTX', rank: 'TypeRank', count: 'TypeCount', bin: 'TypeBinName'):
        """ Args:
                ctx (TypeCTX): An optional list of nested CDT :mod:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
                rank (TypeRank): Rank integer or integer expression of element to start removing at.
                count (TypeCount): Count integer or integer expression of elements to remove.
                bin (TypeBinName): bin expression, such as :class:`~aerospike_helpers.expressions.base.MapBin` or :class:`~aerospike_helpers.expressions.base.ListBin`.

            :return: list expression.

            Example::

                # Remove the 3 smallest items from list bin "a".
                expr = exp.ListRemoveByRankRange(None, 0, 3, exp.ListBin("a")).compile()
        """
        self._children = (
            rank,
            count,
            bin if isinstance(bin, _BaseExpr) else ListBin(bin)
        )
        self._fixed = {}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


#######################
# List Read Expressions
#######################


class ListSize(_BaseExpr):
    """Create an expression that returns list size."""
    _op = aerospike.OP_LIST_SIZE

    def __init__(self, ctx: 'TypeCTX', bin: 'TypeBinName'):
        """ Args:
                ctx (TypeCTX): An optional list of nested CDT :mod:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
                bin (TypeBinName): bin expression, such as :class:`~aerospike_helpers.expressions.base.MapBin` or :class:`~aerospike_helpers.expressions.base.ListBin`.

            :return: Integer expression.

            Example::

                #Take the size of list bin "a".
                expr = exp.ListSize(None, exp.ListBin("a")).compile()
        """
        self._children = (
            bin if isinstance(bin, _BaseExpr) else ListBin(bin),
        )
        self._fixed = {}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class ListGetByValue(_BaseExpr):
    """ Create an expression that selects list items identified by value and returns selected
        data specified by return_type.
    """
    _op = aerospike.OP_LIST_GET_BY_VALUE

    def __init__(self, ctx: 'TypeCTX', return_type: int, value: 'TypeValue', bin: 'TypeBinName'):
        """ Args:
                ctx (TypeCTX): An optional list of nested CDT :mod:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
                return_type (int): Value specifying what should be returned from the operation.
                    This should be one of the :ref:`list_return_types` values.
                value (TypeValue): Value or value expression of element to get.
                bin (TypeBinName): bin expression, such as :class:`~aerospike_helpers.expressions.base.MapBin` or :class:`~aerospike_helpers.expressions.base.ListBin`.

            :return: Expression.

            Example::

                # Get the index of the element with value, 3, in list bin "a".
                expr = exp.ListGetByValue(None, aerospike.LIST_RETURN_INDEX, 3, exp.ListBin("a")).compile()
        """
        self._children = (
            value,
            bin if isinstance(bin, _BaseExpr) else ListBin(bin)
        )
        self._fixed = {_Keys.RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class ListGetByValueRange(_BaseExpr):
    """ Create an expression that selects list items identified by value range and returns selected
        data specified by return_type.
    """
    _op = aerospike.OP_LIST_GET_BY_VALUE_RANGE

    def __init__(
        self,
        ctx: 'TypeCTX',
        return_type: int,
        value_begin: 'TypeValue',
        value_end: 'TypeValue',
        bin: 'TypeBinName'
    ):
        """ Create an expression that selects list items identified by value range and returns selected
            data specified by return_type.

            Args:
                ctx (TypeCTX): An optional list of nested CDT :mod:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
                return_type (int): Value specifying what should be returned from the operation.
                    This should be one of the :ref:`list_return_types` values.
                value_begin (TypeValue): Value or value expression of first element to get.
                value_end (TypeValue): Value or value expression of ending element.
                bin (TypeBinName): bin expression, such as :class:`~aerospike_helpers.expressions.base.MapBin` or :class:`~aerospike_helpers.expressions.base.ListBin`.

            :return: Expression.

            Example::

                # Get rank of values between 3 (inclusive) and 7 (exclusive) in list bin "a".
                expr = exp.ListGetByValueRange(None, aerospike.LIST_RETURN_RANK, 3, 7, exp.ListBin("a")).compile()
        """
        self._children = (
            value_begin,
            value_end,
            bin if isinstance(bin, _BaseExpr) else ListBin(bin)
        )
        self._fixed = {_Keys.RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class ListGetByValueList(_BaseExpr):
    """ Create an expression that selects list items identified by values and returns selected
        data specified by return_type.
    """
    _op = aerospike.OP_LIST_GET_BY_VALUE_LIST

    def __init__(self, ctx: 'TypeCTX', return_type: int, value: 'TypeListValue', bin: 'TypeBinName'):
        """ Args:
                ctx (TypeCTX): An optional list of nested CDT :mod:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
                return_type (int): Value specifying what should be returned from the operation.
                    This should be one of the :ref:`list_return_types` values  Value specifying what should be returned from the operation.
                    This should be one of the :ref:`list_return_types` values.
                value (TypeListValue): List or list expression of values of elements to get.
                bin (TypeBinName): bin expression, such as :class:`~aerospike_helpers.expressions.base.MapBin` or :class:`~aerospike_helpers.expressions.base.ListBin`.

            :return: Expression.

            Example::

                #Get the indexes of the the elements in list bin "a" with values [3, 6, 12].
                expr = exp.ListGetByValueList(None, aerospike.LIST_RETURN_INDEX, [3, 6, 12], exp.ListBin("a")).compile()
        """
        self._children = (
            value,
            bin if isinstance(bin, _BaseExpr) else ListBin(bin)
        )
        self._fixed = {_Keys.RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class ListGetByValueRelRankRangeToEnd(_BaseExpr):
    """Create an expression that selects list items nearest to value and greater by relative rank"""
    _op = aerospike.OP_LIST_GET_BY_VALUE_RANK_RANGE_REL_TO_END

    def __init__(self, ctx: 'TypeCTX', return_type: int, value: 'TypeValue', rank: 'TypeRank', bin: 'TypeBinName'):
        """ Args:
                ctx (TypeCTX): An optional list of nested CDT :mod:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
                return_type (int): Value specifying what should be returned from the operation.
                    This should be one of the :ref:`list_return_types` values.
                value (TypeValue): Value or vaule expression to get items relative to.
                rank (TypeRank): Rank intger expression. rank relative to "value" to start getting elements.
                bin (TypeBinName): bin expression, such as :class:`~aerospike_helpers.expressions.base.MapBin` or :class:`~aerospike_helpers.expressions.base.ListBin`.

            :return: Expression.

            Example::

                # Get the values of all elements in list bin "a" larger than 3.
                expr = exp.ListGetByValueRelRankRangeToEnd(None, aerospike.LIST_RETURN_VALUE, 3, 1, exp.ListBin("a")).compile()
        """
        self._children = (
            value,
            rank,
            bin if isinstance(bin, _BaseExpr) else ListBin(bin)
        )
        self._fixed = {_Keys.RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class ListGetByValueRelRankRange(_BaseExpr):
    """ Create an expression that selects list items nearest to value and greater by relative rank with a
        count limit and returns selected data specified by return_type.
    """
    _op = aerospike.OP_LIST_GET_BY_VALUE_RANK_RANGE_REL

    def __init__(self, ctx: 'TypeCTX', return_type: int, value: 'TypeValue', rank: 'TypeRank', count: 'TypeCount', bin: 'TypeBinName'):
        """ Args:
                ctx (TypeCTX): An optional list of nested CDT :mod:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
                return_type (int): Value specifying what should be returned from the operation.
                    This should be one of the :ref:`list_return_types` values.
                value (TypeValue): Value or vaule expression to get items relative to.
                rank (TypeRank): Rank intger expression. rank relative to "value" to start getting elements.
                count (TypeCount): Integer value or integer value expression, how many elements to get.
                bin (TypeBinName): bin expression, such as :class:`~aerospike_helpers.expressions.base.MapBin` or :class:`~aerospike_helpers.expressions.base.ListBin`.

            :return: Expression.

            Example::

                # Get the next 2 values in list bin "a" larger than 3.
                expr = exp.ListGetByValueRelRankRange(None, aerospike.LIST_RETURN_VALUE, 3, 1, 2, exp.ListBin("a")).compile()
        """
        self._children = (
            value,
            rank,
            count,
            bin if isinstance(bin, _BaseExpr) else ListBin(bin)
        )
        self._fixed = {_Keys.RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class ListGetByIndex(_BaseExpr):
    """ Create an expression that selects list item identified by index
        and returns selected data specified by return_type.
    """
    _op = aerospike.OP_LIST_GET_BY_INDEX

    def __init__(
        self,
        ctx: 'TypeCTX',
        return_type: int,
        value_type: int,
        index: 'TypeIndex',
        bin: 'TypeBinName',
    ):
        """ Args:
                ctx (TypeCTX): An optional list of nested CDT :mod:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
                return_type (int): Value specifying what should be returned from the operation.
                    This should be one of the :ref:`list_return_types` values 
                value_type (int): The value type that will be returned by this expression (ResultType).
                index (TypeIndex): Integer or integer expression of index to get element at.
                bin (TypeBinName): bin expression, such as :class:`~aerospike_helpers.expressions.base.MapBin` or :class:`~aerospike_helpers.expressions.base.ListBin`.

            :return: Expression.

            Example::

                # Get the value at index 0 in list bin "a". (assume this value is an integer)
                expr = exp.ListGetByIndex(None, aerospike.LIST_RETURN_VALUE, ResultType.INTEGER, 0, exp.ListBin("a")).compile()
        """    
        self._children = (
            index,
            bin if isinstance(bin, _BaseExpr) else ListBin(bin)
        )
        self._fixed = {_Keys.VALUE_TYPE_KEY: value_type, _Keys.RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class ListGetByIndexRangeToEnd(_BaseExpr):
    """ Create an expression that selects list items starting at specified index to the end of list
        and returns selected data specified by return_type.
    """
    _op = aerospike.OP_LIST_GET_BY_INDEX_RANGE_TO_END

    def __init__(self, ctx: 'TypeCTX', return_type: int, index: 'TypeIndex', bin: 'TypeBinName'):
        """ Args:
                ctx (TypeCTX): An optional list of nested CDT :mod:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
                return_type (int): Value specifying what should be returned from the operation.
                    This should be one of the :ref:`list_return_types` values.
                index (TypeIndex): Integer or integer expression of index to start getting elements at.
                bin (TypeBinName): bin expression, such as :class:`~aerospike_helpers.expressions.base.MapBin` or :class:`~aerospike_helpers.expressions.base.ListBin`.

            :return: Expression.

            Example::

                # Get element 5 to end from list bin "a".
                expr = exp.ListGetByIndexRangeToEnd(None, aerospike.LIST_RETURN_VALUE, 5, exp.ListBin("a")).compile()
        """
        self._children = (
            index,
            bin if isinstance(bin, _BaseExpr) else ListBin(bin)
        )
        self._fixed = {_Keys.RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class ListGetByIndexRange(_BaseExpr):
    """ Create an expression that selects "count" list items starting at specified index
        and returns selected data specified by return_type.
    """
    _op = aerospike.OP_LIST_GET_BY_INDEX_RANGE

    def __init__(self, ctx: 'TypeCTX', return_type: int, index: 'TypeIndex', count: 'TypeCount', bin: 'TypeBinName'):
        """ Args:
                ctx (TypeCTX): An optional list of nested CDT :mod:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
                return_type (int): Value specifying what should be returned from the operation.
                    This should be one of the :ref:`list_return_types` values.
                index (TypeIndex): Integer or integer expression of index to start getting elements at.
                count (TypeCount): Integer or integer expression for count of elements to get.
                bin (TypeBinName): bin expression, such as :class:`~aerospike_helpers.expressions.base.MapBin` or :class:`~aerospike_helpers.expressions.base.ListBin`.

            :return: Expression.

            Example::

                # Get elements at indexes 3, 4, 5, 6 in list bin "a".
                expr = exp.ListGetByIndexRange(None, aerospike.LIST_RETURN_VALUE, 3, 4, exp.ListBin("a")).compile()
        """
        self._children = (
            index,
            count,
            bin if isinstance(bin, _BaseExpr) else ListBin(bin)
        )
        self._fixed = {_Keys.RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class ListGetByRank(_BaseExpr):
    """ Create an expression that selects list item identified by rank
        and returns selected data specified by return_type.
    """
    _op = aerospike.OP_LIST_GET_BY_RANK

    def __init__(
        self,
        ctx: 'TypeCTX',
        return_type: int,
        value_type: int,
        rank: 'TypeRank',
        bin: 'TypeBinName',
    ):
        """ Args:
                ctx (TypeCTX): An optional list of nested CDT :mod:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
                return_type (int): Value specifying what should be returned from the operation.
                    This should be one of the :ref:`list_return_types` values.
                value_type (int): The value type that will be returned by this expression (ResultType).
                rank (TypeRank): Rank integer or integer expression of element to get.
                bin (TypeBinName): bin expression, such as :class:`~aerospike_helpers.expressions.base.MapBin` or :class:`~aerospike_helpers.expressions.base.ListBin`.

            :return: Expression.

            Example::

                # Get the smallest element in list bin "a".
                expr = exp.ListGetByRank(None, aerospike.LIST_RETURN_VALUE, aerospike.ResultType.INTEGER, 0, exp.ListBin("a")).compile()
        """
        self._children = (
            rank,
            bin if isinstance(bin, _BaseExpr) else ListBin(bin)
        )
        self._fixed = {_Keys.VALUE_TYPE_KEY: value_type, _Keys.RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class ListGetByRankRangeToEnd(_BaseExpr):
    """ Create an expression that selects list items starting at specified rank to the last ranked item
        and returns selected data specified by return_type.
    """
    _op = aerospike.OP_LIST_GET_BY_RANK_RANGE_TO_END

    def __init__(self, ctx: 'TypeCTX', return_type: int, rank: 'TypeRank', bin: 'TypeBinName'):
        """ Args:
                ctx (TypeCTX): An optional list of nested CDT :mod:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
                return_type (int): Value specifying what should be returned from the operation.
                    This should be one of the :ref:`list_return_types` values.
                rank (TypeRank): Rank integer or integer expression of first element to get.
                bin (TypeBinName): bin expression, such as :class:`~aerospike_helpers.expressions.base.MapBin` or :class:`~aerospike_helpers.expressions.base.ListBin`.

            :return: Expression.

            Example::

                # Get the three largest elements in list bin "a".
                expr = exp.ListGetByRankRangeToEnd(None, aerospike.LIST_RETURN_VALUE, -3, exp.ListBin("a")).compile()
        """
        self._children = (
            rank,
            bin if isinstance(bin, _BaseExpr) else ListBin(bin)
        )
        self._fixed = {_Keys.RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class ListGetByRankRange(_BaseExpr):
    """ Create an expression that selects "count" list items starting at specified rank
        and returns selected data specified by return_type.
    """
    _op = aerospike.OP_LIST_GET_BY_RANK_RANGE

    def __init__(self, ctx: 'TypeCTX', return_type: int, rank: 'TypeRank', count: 'TypeCount', bin: 'TypeBinName'):
        """ Args:
                ctx (TypeCTX): An optional list of nested CDT :mod:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
                return_type (int): Value specifying what should be returned from the operation.
                    This should be one of the :ref:`list_return_types` values.
                rank (TypeRank): Rank integer or integer expression of first element to get.
                count (TypeCount): Count integer or integer expression for how many elements to get.
                bin (TypeBinName): bin expression, such as :class:`~aerospike_helpers.expressions.base.MapBin` or :class:`~aerospike_helpers.expressions.base.ListBin`.

            :return: Expression.

            Example::

                # Get the 3 smallest elements in list bin "a".
                expr = exp.ListGetByRankRange(None, aerospike.LIST_RETURN_VALUE, 0, 3, exp.ListBin("a")).compile()
        """
        self._children = (
            rank,
            count,
            bin if isinstance(bin, _BaseExpr) else ListBin(bin)
        )
        self._fixed = {_Keys.RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx
