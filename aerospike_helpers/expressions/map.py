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
Map expressions contain map read and modify expressions.

Example::

    import aerospike_helpers.expressions as exp
    #Take the size of map bin "b".
    expr = exp.MapSize(None, exp.MapBin("b")).compile()
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
from aerospike_helpers.expressions.base import MapBin

########################
# Map Modify Expressions
########################

TypeKey = Union[_BaseExpr, Any]
TypeKeyList = Union[_BaseExpr, List[Any]]
TypeBinName = Union[_BaseExpr, str]
TypeListValue = Union[_BaseExpr, List[Any]]
TypeIndex = Union[_BaseExpr, int, aerospike.CDTInfinite]
TypeCDT = Union[None, List[cdt_ctx._cdt_ctx]]
TypeRank = Union[_BaseExpr, int, aerospike.CDTInfinite]
TypeCount = Union[_BaseExpr, int, aerospike.CDTInfinite]
TypeValue = Union[_BaseExpr, Any]
TypePolicy = Union[Dict[str, Any], None]


class MapPut(_BaseExpr):
    """Create an expression that writes key/val to map bin."""
    _op = aerospike.OP_MAP_PUT

    def __init__(self, ctx: TypeCDT, policy: TypePolicy, key: TypeKey, value: TypeValue, bin: TypeBinName):
        """ Create an expression that writes key/val to map bin.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                policy (TypePolicy): Optional map write policy.
                key (TypeKey): Key value or value expression to put into map.
                value (TypeValue): Value or value expression to put into map.
                bin (TypeBinName): Map bin or map expression.

            :return: Map expression.
        
            Example::

                # Put {27: 'key27'} into map bin "b".
                expr = MapPut(None, None, 27, 'key27', MapBin("b")).compile()
        """        
        self._children = (
            key,
            value,
            _GenericExpr(_ExprOp._AS_EXP_CODE_CDT_MAP_CRMOD, 0, {_Keys.MAP_POLICY_KEY: policy} if policy is not None else {}),
            bin if isinstance(bin, _BaseExpr) else MapBin(bin),
        )
        self._fixed = {}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx

        if policy is not None:
            self._fixed[_Keys.MAP_POLICY_KEY] = policy


class MapPutItems(_BaseExpr):
    """Create an expression that writes each map item to map bin."""
    _op = aerospike.OP_MAP_PUT_ITEMS

    def __init__(self, ctx: TypeCDT, policy: TypePolicy, map: map, bin: TypeBinName):
        """ Create an expression that writes each map item to map bin.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                policy (TypePolicy): Optional map write policy.
                map (map): Map or map expression of items to put into target map.
                bin (TypeBinName): Map bin or map expression.

            :return: Map expression.
        
            Example::

                # Put {27: 'key27', 28: 'key28'} into map bin "b".
                expr = MapPut(None, None, {27: 'key27', 28: 'key28'}, MapBin("b")).compile()
        """        
        self._children = (
            map,
            _GenericExpr(_ExprOp._AS_EXP_CODE_CDT_MAP_CRMOD, 0, {_Keys.MAP_POLICY_KEY: policy} if policy is not None else {}),
            bin if isinstance(bin, _BaseExpr) else MapBin(bin),
        )
        self._fixed = {}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx

        if policy is not None:
            self._fixed[_Keys.MAP_POLICY_KEY] = policy


class MapIncrement(_BaseExpr):
    """ Create an expression that increments a map value, by value, for all items identified by key.
        Valid only for numbers.
    """
    _op = aerospike.OP_MAP_INCREMENT

    def __init__(self, ctx: TypeCDT, policy: TypePolicy, key: TypeKey, value: TypeValue, bin: TypeBinName):
        """ Create an expression that increments a map value, by value, for all items identified by key.
            Valid only for numbers.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                policy (TypePolicy): Optional map write policy.
                key (TypeKey): Key value or value expression element to increment.
                value (TypeValue): Increment element by value expression.
                bin (TypeBinName): Map bin or map expression.

            :return: Map expression.
        
            Example::

                # Increment element at 'vageta' in map bin "b" by 9000.
                expr = MapIncrement(None, None, 'vageta', 9000, MapBin("b")).compile()
        """        
        self._children = (
            key,
            value,
            _GenericExpr(_ExprOp._AS_EXP_CODE_CDT_MAP_CRMOD, 0, {_Keys.MAP_POLICY_KEY: policy} if policy is not None else {}),
            bin if isinstance(bin, _BaseExpr) else MapBin(bin),
        )
        self._fixed = {}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx

        if policy is not None:
            self._fixed[_Keys.MAP_POLICY_KEY] = policy


class MapClear(_BaseExpr):
    """Create an expression that removes all items in map."""
    _op = aerospike.OP_MAP_CLEAR

    def __init__(self, ctx: TypeCDT, bin: TypeBinName):
        """ Create an expression that removes all items in map.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                bin (TypeBinName): Map bin or map expression.

            :return: Map expression.
        
            Example::

                # Clear map bin "b".
                expr = MapClear(None, MapBin("b")).compile()
        """        
        self._children = (
            bin if isinstance(bin, _BaseExpr) else MapBin(bin),
        )
        self._fixed = {}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class MapRemoveByKey(_BaseExpr):
    """Create an expression that removes a map item identified by key."""
    _op = aerospike.OP_MAP_REMOVE_BY_KEY

    def __init__(self, ctx: TypeCDT, key: TypeKey, bin: TypeBinName):
        """ Create an expression that removes a map item identified by key.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                key (TypeKey): Key value or value expression of key to element to remove.
                bin (TypeBinName): Map bin or map expression.

            :return: Map expression.
        
            Example::

                # Remove element at key 1 in map bin "b".
                expr = MapRemoveByKey(None, 1, MapBin("b")).compile()
        """        
        self._children = (
            key,
            bin if isinstance(bin, _BaseExpr) else MapBin(bin),
        )
        self._fixed = {}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class MapRemoveByKeyList(_BaseExpr):
    """Create an expression that removes map items identified by keys."""
    _op = aerospike.OP_MAP_REMOVE_BY_KEY_LIST

    def __init__(self, ctx: TypeCDT, keys: List[TypeKey], bin: TypeBinName):
        """ Create an expression that removes map items identified by keys.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                key (List[TypeKey]): List of key values or a list expression of keys to elements to remove.
                bin (TypeBinName): Map bin or map expression.

            :return: Map expression.
        
            Example::

                # Remove elements at keys [1, 2] in map bin "b".
                expr = MapRemoveByKeyList(None, [1, 2], MapBin("b")).compile()
        """        
        self._children = (
            keys,
            bin if isinstance(bin, _BaseExpr) else MapBin(bin),
        )
        self._fixed = {}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class MapRemoveByKeyRange(_BaseExpr):
    """ Create an expression that removes map items identified by key range 
        (begin inclusive, end exclusive). If begin is None, the range is less than end.
        If end is None, the range is greater than equal to begin.
    """
    _op = aerospike.OP_MAP_REMOVE_BY_KEY_RANGE

    def __init__(self, ctx: TypeCDT, begin: TypeValue, end: TypeValue, bin: TypeBinName):
        """ Create an expression that removes map items identified by key range 
            (begin inclusive, end exclusive). If begin is None, the range is less than end.
            If end is None, the range is greater than equal to begin.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                begin (TypeValue): Begin value expression.
                end (TypeValue): End value expression.
                bin (TypeBinName): Map bin or map expression.

            :return: Map expression.
        
            Example::

                # Remove elements at keys between 1 and 10 in map bin "b".
                expr = MapRemoveByKeyRange(None, 1, 10 MapBin("b")).compile()
        """        
        self._children = (
            begin,
            end,
            bin if isinstance(bin, _BaseExpr) else MapBin(bin),
        )
        self._fixed = {}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class MapRemoveByKeyRelIndexRangeToEnd(_BaseExpr):
    """Create an expression that removes map items nearest to key and greater by index."""
    _op = aerospike.OP_MAP_REMOVE_BY_KEY_REL_INDEX_RANGE_TO_END

    def __init__(self, ctx: TypeCDT, key: TypeKey, index: TypeIndex, bin: TypeBinName):
        """ Create an expression that removes map items nearest to key and greater by index.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                key (TypeKey): Key value or expression for key to start removing from.
                index (TypeIndex): Index integer or integer expression.
                bin (TypeBinName): Map bin or map expression.

            :return: Map expression.
        
            Example::

                # Map bin "b" has {"key1": 1, "key2": 2, "key3": 3, "key4": 4}.
                # Remove each element where the key has greater index than "key1".
                expr = MapRemoveByKeyRelIndexRangeToEnd(None, "key1", 1, MapBin("b")).compile()
        """        
        self._children = (
            key,
            index,
            bin if isinstance(bin, _BaseExpr) else MapBin(bin),
        )
        self._fixed = {}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class MapRemoveByKeyRelIndexRange(_BaseExpr):
    """Create an expression that removes map items nearest to key and greater by index with a count limit."""
    _op = aerospike.OP_MAP_REMOVE_BY_KEY_REL_INDEX_RANGE

    def __init__(self, ctx: TypeCDT, key: TypeKey, index: TypeIndex, count: TypeCount, bin: TypeBinName):
        """ Create an expression that removes map items nearest to key and greater by index with a count limit.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                key (TypeKey): Key value or expression for key to start removing from.
                index (TypeIndex): Index integer or integer expression.
                count (TypeCount): Integer expression for how many elements to remove.
                bin (TypeBinName): Map bin or map expression.

            :return: [Map expression.
        
            Example::

                # Remove 3 elements with keys greater than "key1" from map bin "b".
                expr = MapRemoveByKeyRelIndexRange(None, "key1", 1, 3, MapBin("b")).compile()
        """        
        self._children = (
            key,
            index,
            count,
            bin if isinstance(bin, _BaseExpr) else MapBin(bin),
        )
        self._fixed = {}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class MapRemoveByValue(_BaseExpr):
    """Create an expression that removes map items identified by value."""
    _op = aerospike.OP_MAP_REMOVE_BY_VALUE

    def __init__(self, ctx: TypeCDT, value: TypeValue, bin: TypeBinName):
        """ Create an expression that removes map items identified by value.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                value (TypeValue): Value or value expression to remove.
                bin (TypeBinName): Bin name or map expression.

            :return: Map expression.
        
            Example::

                # Remove {"key1": 1} from map bin "b".
                expr = MapRemoveByValue(None, 1, MapBin("b")).compile()
        """        
        self._children = (
            value,
            bin if isinstance(bin, _BaseExpr) else MapBin(bin),
        )
        self._fixed = {}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class MapRemoveByValueList(_BaseExpr):
    """Create an expression that removes map items identified by values."""
    _op = aerospike.OP_MAP_REMOVE_BY_VALUE_LIST

    def __init__(self, ctx: TypeCDT, values: TypeListValue, bin: TypeBinName):
        """ Create an expression that removes map items identified by values.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                values (TypeListValue): List of values or list expression.
                bin (TypeBinName): Bin name or map expression.

            :return: Map expression.
        
            Example::

                # Remove elements with values 1, 2, 3 from map bin "b".
                expr = MapRemoveByValueList(None, [1, 2, 3], MapBin("b")).compile()
        """        
        self._children = (
            values,
            bin if isinstance(bin, _BaseExpr) else MapBin(bin),
        )
        self._fixed = {}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class MapRemoveByValueRange(_BaseExpr):
    """ Create an expression that removes map items identified by value range
        (begin inclusive, end exclusive). If begin is nil, the range is less than end.
        If end is aerospike.CDTInfinite(), the range is greater than equal to begin.
    """
    _op = aerospike.OP_MAP_REMOVE_BY_VALUE_RANGE

    def __init__(self, ctx: TypeCDT, begin: TypeValue, end: TypeValue, bin: TypeBinName):
        """ Create an expression that removes map items identified by value range
            (begin inclusive, end exclusive). If begin is nil, the range is less than end.
            If end is aerospike.CDTInfinite(), the range is greater than equal to begin.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                begin (TypeValue): Begin value or value expression for range.
                end (TypeValue): End value or value expression for range.
                bin (TypeBinName): Bin name or map expression.

            :return: Map expression.
        
            Example::

                # Remove list of items with values >= 3 and < 7 from map bin "b".
                expr = MapRemoveByValueRange(None, 3, 7, MapBin("b")).compile()
        """        
        self._children = (
            begin,
            end,
            bin if isinstance(bin, _BaseExpr) else MapBin(bin),
        )
        self._fixed = {}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class MapRemoveByValueRelRankRangeToEnd(_BaseExpr):
    """Create an expression that removes map items nearest to value and greater by relative rank."""
    _op = aerospike.OP_MAP_REMOVE_BY_VALUE_REL_RANK_RANGE_TO_END

    def __init__(self, ctx: TypeCDT, value: TypeValue, rank: TypeRank, bin: TypeBinName):
        """ Create an expression that removes map items nearest to value and greater by relative rank.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                value (TypeValue): Value or value expression to start removing from.
                rank (TypeRank): Integer or integer expression of rank.
                bin (TypeBinName): Bin name or map expression.

            :return: Map expression.
        
            Example::

                # Remove all elements with values larger than 3 from map bin "b".
                expr = MapRemoveByValueRelRankRangeToEnd(None, 3, 1, MapBin("b")).compile()
        """        
        self._children = (
            value,
            rank,
            bin if isinstance(bin, _BaseExpr) else MapBin(bin),
        )
        self._fixed = {}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class MapRemoveByValueRelRankRange(_BaseExpr):
    """ Create an expression that removes map items nearest to value and greater by relative rank with a
        count limit.
    """
    _op = aerospike.OP_MAP_REMOVE_BY_VALUE_REL_RANK_RANGE

    def __init__(self, ctx: TypeCDT, value: TypeValue, rank: TypeRank, count: TypeCount, bin: TypeBinName):
        """ Create an expression that removes map items nearest to value and greater by relative rank with a
            count limit.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                value (TypeValue): Value or value expression to start removing from.
                rank (TypeRank): Integer or integer expression of rank.
                count (TypeCount): Integer count or integer expression for how many elements to remove.
                bin (TypeBinName): Bin name or map expression.

            :return: Map expression.
        
            Example::

                # Remove the next 4 elements larger than 3 from map bin "b".
                expr = MapRemoveByValueRelRankRangeToEnd(None, 3, 1, 4, MapBin("b")).compile()
        """        
        self._children = (
            value,
            rank,
            count,
            bin if isinstance(bin, _BaseExpr) else MapBin(bin),
        )
        self._fixed = {}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class MapRemoveByIndex(_BaseExpr):
    """Create an expression that removes map item identified by index."""
    _op = aerospike.OP_MAP_REMOVE_BY_INDEX

    def __init__(self, ctx: TypeCDT, index: TypeIndex, bin: TypeBinName):
        """ Create an expression that removes map item identified by index.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                index (TypeIndex): Index integer or integer expression of element to remove.
                bin (TypeBinName): Bin name or map expression.

            :return: Map expression.
        
            Example::

                # Remove element with smallest key from map bin "b".
                expr = MapRemoveByIndex(None, 0, MapBin("b")).compile()
        """        
        self._children = (
            index,
            bin if isinstance(bin, _BaseExpr) else MapBin(bin),
        )
        self._fixed = {}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class MapRemoveByIndexRangeToEnd(_BaseExpr):
    """Create an expression that removes map items starting at specified index to the end of map."""
    _op = aerospike.OP_MAP_REMOVE_BY_INDEX_RANGE_TO_END

    def __init__(self, ctx: TypeCDT, index: TypeIndex, bin: TypeBinName):
        """ Create an expression that removes map items starting at specified index to the end of map.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                index (TypeIndex): Starting index integer or integer expression of elements to remove.
                bin (TypeBinName): Map bin name or map expression.

            :return: Map expression.
        
            Example::

                # Remove all elements starting from index 3 in map bin "b".
                expr = MapRemoveByIndexRangeToEnd(None, 3, MapBin("b")).compile()
        """        
        self._children = (
            index,
            bin if isinstance(bin, _BaseExpr) else MapBin(bin),
        )
        self._fixed = {}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class MapRemoveByIndexRange(_BaseExpr):
    """Create an expression that removes count map items starting at specified index."""
    _op = aerospike.OP_MAP_REMOVE_BY_INDEX_RANGE

    def __init__(self, ctx: TypeCDT, index: TypeIndex, count: TypeCount, bin: TypeBinName):
        """ Create an expression that removes count map items starting at specified index.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                index (TypeIndex): Starting index integer or integer expression of elements to remove.
                count (TypeCount): Integer or integer expression, how many elements to remove.
                bin (TypeBinName): Map bin name or map expression.

            :return: Map expression.
        
            Example::

                # Get size of map bin "b" after index 3, 4, and 5 have been removed.
                expr = MapSize(None, MapRemoveByIndexRange(None, 3, 3, MapBin("b"))).compile()
        """        
        self._children = (
            index,
            count,
            bin if isinstance(bin, _BaseExpr) else MapBin(bin),
        )
        self._fixed = {}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class MapRemoveByRank(_BaseExpr):
    """Create an expression that removes map item identified by rank."""
    _op = aerospike.OP_MAP_REMOVE_BY_RANK

    def __init__(self, ctx: TypeCDT, rank: TypeRank, bin: TypeBinName):
        """ Create an expression that removes map item identified by rank.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                rank (TypeRank): Rank integer or integer expression of element to remove.
                bin (TypeBinName): Map bin name or map expression.

            :return: Map expression.
        
            Example::

                # Remove smallest value in map bin "b".
                expr = MapRemoveByRank(None, 0, MapBin("b")).compile()
        """        
        self._children = (
            rank,
            bin if isinstance(bin, _BaseExpr) else MapBin(bin),
        )
        self._fixed = {}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class MapRemoveByRankRangeToEnd(_BaseExpr):
    """Create an expression that removes map items starting at specified rank to the last ranked item."""
    _op = aerospike.OP_MAP_REMOVE_BY_RANK_RANGE_TO_END

    def __init__(self, ctx: TypeCDT, rank: TypeRank, bin: TypeBinName):
        """ Create an expression that removes map items starting at specified rank to the last ranked item.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                rank (TypeRank): Rank integer or integer expression of element to start removing at.
                bin (TypeBinName): Map bin name or map expression.

            :return: Map expression.
        
            Example::

                # Remove the 2 largest elements from map bin "b".
                expr = MapRemoveByRankRangeToEnd(None, -2, MapBin("b")).compile()
        """        
        self._children = (
            rank,
            bin if isinstance(bin, _BaseExpr) else MapBin(bin),
        )
        self._fixed = {}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class MapRemoveByRankRange(_BaseExpr):
    """Create an expression that removes "count" map items starting at specified rank."""
    _op = aerospike.OP_MAP_REMOVE_BY_RANK_RANGE

    def __init__(self, ctx: TypeCDT, rank: TypeRank, count: TypeCount, bin: TypeBinName):
        """ Create an expression that removes "count" map items starting at specified rank.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                rank (TypeRank): Rank integer or integer expression of element to start removing at.
                count (TypeCount): Count integer or integer expression of elements to remove.
                bin (TypeBinName): Map bin name or map expression.

            :return: Map expression.
        
            Example::

                # Remove the 3 smallest items from map bin "b".
                expr = MapRemoveByRankRange(None, 0, 3, MapBin("b")).compile()
        """        
        self._children = (
            rank,
            count,
            bin if isinstance(bin, _BaseExpr) else MapBin(bin),
        )
        self._fixed = {}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


######################
# Map Read Expressions
######################


class MapSize(_BaseExpr):
    """Create an expression that returns map size."""
    _op = aerospike.OP_MAP_SIZE

    def __init__(self, ctx: TypeCDT, bin: TypeBinName):
        """ Create an expression that returns map size.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                bin (TypeBinName): Map bin name or map expression.

            :return: Integer expression.
        
            Example::

                #Take the size of map bin "b".
                expr = MapSize(None, MapBin("b")).compile()
        """        
        self._children = (
            bin if isinstance(bin, _BaseExpr) else MapBin(bin),
        )
        self._fixed = {}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class MapGetByKey(_BaseExpr):
    """ Create an expression that selects map item identified by key
        and returns selected data specified by return_type.
    """
    _op = aerospike.OP_MAP_GET_BY_KEY

    def __init__(self, ctx: TypeCDT, return_type: int, value_type: int, key: TypeKey, bin: TypeBinName):
        """ Create an expression that selects map item identified by key
            and returns selected data specified by return_type.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                return_type (int): Value specifying what should be returned from the operation.
                    This should be one of the :ref:`map_return_types` values  One of the aerospike map return types.
                value_type (int): The value type that will be returned by this expression (ResultType).
                key (TypeKey): Key value or value expression of element to get.
                bin (TypeBinName): Map bin name or map expression.

            :return: Expression.
        
            Example::

                # Get the value at key "key0" in map bin "b". (assume the value at key0 is an integer)
                expr = MapGetByKey(None, aerospike.MAP_RETURN_VALUE, ResultType.INTEGER, "key0", MapBin("b")).compile()
        """        
        self._children = (
            key,
            bin if isinstance(bin, _BaseExpr) else MapBin(bin),
        )
        self._fixed = {_Keys.VALUE_TYPE_KEY: value_type, _Keys.RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class MapGetByKeyRange(_BaseExpr):
    """Create an expression that selects map items identified by key range."""
    _op = aerospike.OP_MAP_GET_BY_KEY_RANGE

    def __init__(self, ctx: TypeCDT, return_type: int, begin: TypeKey, end: TypeKey, bin: TypeBinName):
        """ Create an expression that selects map items identified by key range
            (begin inclusive, end exclusive). If begin is nil, the range is less than end.
            If end is aerospike.CDTInfinite(), the range is greater than equal to begin.
            Expression returns selected data specified by return_type.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                return_type (int): Value specifying what should be returned from the operation.
                    This should be one of the :ref:`map_return_types` values  One of the aerospike map return types.
                begin (TypeKey): Key value or expression.
                end (TypeKey): Key value or expression.
                bin (TypeBinName): Map bin name or map expression.

            :return: Expression.
        
            Example::

                # Get elements at keys "key3", "key4", "key5", "key6" in map bin "b".
                expr = MapGetByKeyRange(None, aerospike.MAP_RETURN_VALUE, "key3", "key7", MapBin("b")).compile()
        """        
        self._children = (
            begin,
            end,
            bin if isinstance(bin, _BaseExpr) else MapBin(bin),
        )
        self._fixed = {_Keys.RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class MapGetByKeyList(_BaseExpr):
    """ Create an expression that selects map items identified by keys
        and returns selected data specified by return_type.
    """
    _op = aerospike.OP_MAP_GET_BY_KEY_LIST

    def __init__(self, ctx: TypeCDT, return_type: int, keys: TypeKeyList, bin: TypeBinName):
        """ Create an expression that selects map items identified by keys
            and returns selected data specified by return_type.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                return_type (int): Value specifying what should be returned from the operation.
                    This should be one of the :ref:`map_return_types` values  One of the aerospike map return types.
                keys (TypeKeyList): List of key values or list expression.
                bin (TypeBinName): Map bin name or map expression.

            :return: Expression.
        
            Example::

                # Get elements at keys "key3", "key4", "key5" in map bin "b".
                expr = MapGetByKeyList(None, aerospike.MAP_RETURN_VALUE, ["key3", "key4", "key5"], MapBin("b")).compile()
        """        
        self._children = (
            keys,
            bin if isinstance(bin, _BaseExpr) else MapBin(bin),
        )
        self._fixed = {_Keys.RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class MapGetByKeyRelIndexRangeToEnd(_BaseExpr):
    """Create an expression that selects map items nearest to key and greater by index with a count limit."""
    _op = aerospike.OP_MAP_GET_BY_KEY_REL_INDEX_RANGE_TO_END

    def __init__(self, ctx: TypeCDT, return_type: int, key: TypeKey, index: TypeIndex, bin: TypeBinName):
        """ Create an expression that selects map items nearest to key and greater by index with a count limit.
            Expression returns selected data specified by return_type.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                return_type (int): Value specifying what should be returned from the operation.
                    This should be one of the :ref:`map_return_types` values  One of the aerospike map return types.
                key (TypeKey): Key value or value expression.
                index (TypeIndex): Index integer or integer value expression.
                bin (TypeBinName): Map bin name or map expression.

            :return: Expression.
        
            Example::

                # Get elements with keys larger than "key2" from map bin "b".
                expr = MapGetByKeyRelIndexRangeToEnd(None, aerospike.MAP_RETURN_VALUE, "key2", 1, MapBin("b")).compile()
        """        
        self._children = (
            key,
            index,
            bin if isinstance(bin, _BaseExpr) else MapBin(bin),
        )
        self._fixed = {_Keys.RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class MapGetByKeyRelIndexRange(_BaseExpr):
    """Create an expression that selects map items nearest to key and greater by index with a count limit."""
    _op = aerospike.OP_MAP_GET_BY_KEY_REL_INDEX_RANGE

    def __init__(self, ctx: TypeCDT, return_type: int, key: TypeKey, index: TypeIndex, count: TypeCount, bin: TypeBinName):
        """ Create an expression that selects map items nearest to key and greater by index with a count limit.
            Expression returns selected data specified by return_type.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                return_type (int): Value specifying what should be returned from the operation.
                    This should be one of the :ref:`map_return_types` values  One of the aerospike map return types.
                key (TypeKey): Key value or value expression.
                index (TypeIndex): Index integer or integer value expression.
                count (TypeCount): Integer count or integer value expression.
                bin (TypeBinName): Map bin name or map expression.

            :return: Expression.
        
            Example::

                # Get the next 2 elements with keys larger than "key3" from map bin "b".
                expr = MapGetByKeyRelIndexRange(None, aerospike.MAP_RETURN_VALUE, "key3", 1, 2, MapBin("b")).compile()
        """        
        self._children = (
            key,
            index,
            count,
            bin if isinstance(bin, _BaseExpr) else MapBin(bin),
        )
        self._fixed = {_Keys.RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class MapGetByValue(_BaseExpr):
    """Create an expression that selects map items identified by value."""
    _op = aerospike.OP_MAP_GET_BY_VALUE

    def __init__(self, ctx: TypeCDT, return_type: int, value: TypeValue, bin: TypeBinName):
        """ Create an expression that selects map items identified by value
            and returns selected data specified by return_type.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                return_type (int): Value specifying what should be returned from the operation.
                    This should be one of the :ref:`map_return_types` values  One of the aerospike map return types.
                value (TypeValue): Value or value expression of element to get.
                bin (TypeBinName): Map bin name or map expression.

            :return: Expression.
        
            Example::

                # Get the rank of the element with value, 3, in map bin "b".
                expr = MapGetByValue(None, aerospike.MAP_RETURN_RANK, 3, MapBin("b")).compile()
        """        
        self._children = (
            value,
            bin if isinstance(bin, _BaseExpr) else MapBin(bin)
        )
        self._fixed = {_Keys.RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class MapGetByValueRange(_BaseExpr):
    """ Create an expression that selects map items identified by value range."""
    _op = aerospike.OP_MAP_GET_BY_VALUE_RANGE

    def __init__(
        self,
        ctx: TypeCDT,
        return_type: int,
        value_begin: TypeValue,
        value_end: TypeValue,
        bin: TypeBinName
    ):
        """ Create an expression that selects map items identified by value range
            (begin inclusive, end exclusive). If begin is None, the range is less than end.
            If end is None, the range is greater than equal to begin.
            Expression returns selected data specified by return_type.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                return_type (int): Value specifying what should be returned from the operation.
                    This should be one of the :ref:`map_return_types` values  One of the aerospike map return types.
                value_begin (TypeValue): Value or value expression of first element to get.
                value_end (TypeValue): Value or value expression of ending element.
                bin (TypeBinName): Map bin name or map expression.

            :return: Expression.
        
            Example::

                # Get elements with values between 3 and 7 from map bin "b".
                expr = MapGetByValueRange(None, aerospike.MAP_RETURN_VALUE, 3, 7, MapBin("b")).compile()
        """        
        self._children = (
            value_begin,
            value_end,
            bin if isinstance(bin, _BaseExpr) else MapBin(bin)
        )
        self._fixed = {_Keys.RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class MapGetByValueList(_BaseExpr):
    """Create an expression that selects map items identified by values."""
    _op = aerospike.OP_MAP_GET_BY_VALUE_LIST

    def __init__(self, ctx: TypeCDT, return_type: int, value: TypeListValue, bin: TypeBinName):
        """ Create an expression that selects map items identified by values
            and returns selected data specified by return_type.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                return_type (int): Value specifying what should be returned from the operation.
                    This should be one of the :ref:`map_return_types` values  One of the aerospike map return types.
                value (TypeListValue): List or list expression of values of elements to get.
                bin (TypeBinName): Map bin name or map expression.

            :return: Expression.
        
            Example::

                # Get the indexes of the the elements in map bin "b" with values [3, 6, 12].
                expr = MapGetByValueList(None, aerospike.MAP_RETURN_INDEX, [3, 6, 12], MapBin("b")).compile()
        """        
        self._children = (
            value,
            bin if isinstance(bin, _BaseExpr) else MapBin(bin)
        )
        self._fixed = {_Keys.RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class MapGetByValueRelRankRangeToEnd(_BaseExpr):
    """Create an expression that selects map items nearest to value and greater by relative rank."""
    _op = aerospike.OP_MAP_GET_BY_VALUE_RANK_RANGE_REL_TO_END

    def __init__(self, ctx: TypeCDT, return_type: int, value: TypeValue, rank: TypeRank, bin: TypeBinName):
        """ Create an expression that selects map items nearest to value and greater by relative rank.
            Expression returns selected data specified by return_type.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                return_type (int): Value specifying what should be returned from the operation.
                    This should be one of the :ref:`map_return_types` values  One of the aerospike map return types.
                value (TypeValue): Value or vaule expression to get items relative to.
                rank (TypeRank): Rank intger expression. rank relative to "value" to start getting elements.
                bin (TypeBinName): Map bin name or map expression.

            :return: Expression.
        
            Example::

                # Get the values of all elements in map bin "b" larger than 3.
                expr = MapGetByValueRelRankRangeToEnd(None, aerospike.MAP_RETURN_VALUE, 3, 1, MapBin("b")).compile()
        """        
        self._children = (
            value,
            rank,
            bin if isinstance(bin, _BaseExpr) else MapBin(bin)
        )
        self._fixed = {_Keys.RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class MapGetByValueRelRankRange(_BaseExpr):
    """ Create an expression that selects map items nearest to value and greater by relative rank with a
        count limit. Expression returns selected data specified by return_type.
    """
    _op = aerospike.OP_MAP_GET_BY_VALUE_RANK_RANGE_REL

    def __init__(self, ctx: TypeCDT, return_type: int, value: TypeValue, rank: TypeRank, count: TypeCount, bin: TypeBinName):
        """ Create an expression that selects map items nearest to value and greater by relative rank with a
            count limit. Expression returns selected data specified by return_type.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                return_type (int): Value specifying what should be returned from the operation.
                    This should be one of the :ref:`map_return_types` values  One of the aerospike list return types.
                value (TypeValue): Value or vaule expression to get items relative to.
                rank (TypeRank): Rank intger expression. rank relative to "value" to start getting elements.
                count (TypeCount): Integer value or integer value expression, how many elements to get.
                bin (TypeBinName): List bin name or list expression.

            :return: Expression.
        
            Example::

                # Get the next 2 values in map bin "b" larger than 3.
                expr = MapGetByValueRelRankRange(None, aerospike.MAP_RETURN_VALUE, 3, 1, 2, MapBin("b")).compile()
        """        
        self._children = (
            value,
            rank,
            count,
            bin if isinstance(bin, _BaseExpr) else MapBin(bin)
        )
        self._fixed = {_Keys.RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class MapGetByIndex(_BaseExpr):
    """ Create an expression that selects map item identified by index
        and returns selected data specified by return_type.
    """
    _op = aerospike.OP_MAP_GET_BY_INDEX

    def __init__(
        self,
        ctx: TypeCDT,
        return_type: int,
        value_type: int,
        index: TypeIndex,
        bin: TypeBinName,
    ):
        """ Create an expression that selects map item identified by index
            and returns selected data specified by return_type.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                return_type (int): Value specifying what should be returned from the operation.
                    This should be one of the :ref:`map_return_types` values  One of the aerospike map return types.
                value_type (int): The value type that will be returned by this expression (ResultType).
                index (TypeIndex): Integer or integer expression of index to get element at.
                bin (TypeBinName): Map bin name or map expression.

            :return: Expression.
        
            Example::

                # Get the value at index 0 in map bin "b". (assume this value is an integer)
                expr = MapGetByIndex(None, aerospike.MAP_RETURN_VALUE, ResultType.INTEGER, 0, MapBin("b")).compile()
        """        
        self._children = (
            index,
            bin if isinstance(bin, _BaseExpr) else MapBin(bin)
        )
        self._fixed = {_Keys.VALUE_TYPE_KEY: value_type, _Keys.RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class MapGetByIndexRangeToEnd(_BaseExpr):
    """Create an expression that selects map items starting at specified index to the end of map."""
    _op = aerospike.OP_MAP_GET_BY_INDEX_RANGE_TO_END

    def __init__(self, ctx: TypeCDT, return_type: int, index: TypeIndex, bin: TypeBinName):
        """ Create an expression that selects map items starting at specified index to the end of map
            and returns selected data specified by return_type.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                return_type (int): Value specifying what should be returned from the operation.
                    This should be one of the :ref:`map_return_types` values  One of the aerospike map return types.
                index (TypeIndex): Integer or integer expression of index to start getting elements at.
                bin (TypeBinName): Map bin name or map expression.

            :return: Expression.
        
            Example::

                # Get element at index 5 to end from map bin "b".
                expr = MapGetByIndexRangeToEnd(None, aerospike.MAP_RETURN_VALUE, 5, MapBin("b")).compile()
        """        
        self._children = (
            index,
            bin if isinstance(bin, _BaseExpr) else MapBin(bin)
        )
        self._fixed = {_Keys.RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class MapGetByIndexRange(_BaseExpr):
    """Create an expression that selects "count" map items starting at specified index."""
    _op = aerospike.OP_MAP_GET_BY_INDEX_RANGE

    def __init__(self, ctx: TypeCDT, return_type: int, index: TypeIndex, count: TypeCount, bin: TypeBinName):
        """ Create an expression that selects "count" map items starting at specified index
            and returns selected data specified by return_type.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                return_type (int): Value specifying what should be returned from the operation.
                    This should be one of the :ref:`map_return_types` values  One of the aerospike map return types.
                index (TypeIndex): Integer or integer expression of index to start getting elements at.
                count (TypeCount): Integer or integer expression for count of elements to get.
                bin (TypeBinName): Map bin name or map expression.

            :return: Expression.
        
            Example::

                # Get elements at indexes 3, 4, 5, 6 in map bin "b".
                expr = MapGetByIndexRange(None, aerospike.MAP_RETURN_VALUE, 3, 4, MapBin("b")).compile()
        """        
        self._children = (
            index,
            count,
            bin if isinstance(bin, _BaseExpr) else MapBin(bin)
        )
        self._fixed = {_Keys.RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class MapGetByRank(_BaseExpr):
    """ Create an expression that selects map items identified by rank
        and returns selected data specified by return_type.
    """
    _op = aerospike.OP_MAP_GET_BY_RANK

    def __init__(
        self,
        ctx: TypeCDT,
        return_type: int,
        value_type: int,
        rank: TypeRank,
        bin: TypeBinName,
    ):
        """ Create an expression that selects map items identified by rank
            and returns selected data specified by return_type.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                return_type (int): Value specifying what should be returned from the operation.
                    This should be one of the :ref:`map_return_types` values  One of the aerospike map return types.
                value_type (int): The value type that will be returned by this expression (ResultType).
                rank (TypeRank): Rank integer or integer expression of element to get.
                bin (TypeBinName): Map bin name or map expression.

            :return: Expression.
        
            Example::

                # Get the smallest element in map bin "b".
                expr = MapGetByRank(None, aerospike.MAP_RETURN_VALUE, aerospike.ResultType.INTEGER, 0, MapBin("b")).compile()
        """    
        self._children = (
            rank,
            bin if isinstance(bin, _BaseExpr) else MapBin(bin)
        )
        self._fixed = {_Keys.VALUE_TYPE_KEY: value_type, _Keys.RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class MapGetByRankRangeToEnd(_BaseExpr):
    """Create an expression that selects map items starting at specified rank to the last ranked item."""
    _op = aerospike.OP_MAP_GET_BY_RANK_RANGE_TO_END

    def __init__(self, ctx: TypeCDT, return_type: int, rank: TypeRank, bin: TypeBinName):
        """ Create an expression that selects map items starting at specified rank to the last ranked item
            and returns selected data specified by return_type.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                return_type (int): Value specifying what should be returned from the operation.
                    This should be one of the :ref:`map_return_types` values  One of the aerospike map return types.
                rank (TypeRank): Rank integer or integer expression of first element to get.
                bin (TypeBinName): Map bin name or map expression.

            :return: Expression.
        
            Example::

                # Get the three largest elements in map bin "b".
                expr = MapGetByRankRangeToEnd(None, aerospike.MAP_RETURN_VALUE, -3, MapBin("b")).compile()
        """        
        self._children = (
            rank,
            bin if isinstance(bin, _BaseExpr) else MapBin(bin)
        )
        self._fixed = {_Keys.RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx


class MapGetByRankRange(_BaseExpr):
    """Create an expression that selects "count" map items starting at specified rank."""
    _op = aerospike.OP_MAP_GET_BY_RANK_RANGE

    def __init__(self, ctx: TypeCDT, return_type: int, rank: TypeRank, count: TypeCount, bin: TypeBinName):
        """ Create an expression that selects "count" map items starting at specified rank
            and returns selected data specified by return_type.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                return_type (int): Value specifying what should be returned from the operation.
                    This should be one of the :ref:`map_return_types` values  One of the aerospike map return types.
                rank (TypeRank): Rank integer or integer expression of first element to get.
                count (TypeCount): Count integer or integer expression for how many elements to get.
                bin (TypeBinName): Map bin name or map expression.

            :return: Expression.
        
            Example::

                # Get the 3 smallest elements in map bin "b".
                expr = MapGetByRankRange(None, aerospike.MAP_RETURN_VALUE, 0, 3, MapBin("b")).compile()
        """        
        self._children = (
            rank,
            count,
            bin if isinstance(bin, _BaseExpr) else MapBin(bin)
        )
        self._fixed = {_Keys.RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self._fixed[_Keys.CTX_KEY] = ctx