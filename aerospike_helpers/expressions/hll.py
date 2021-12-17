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
HyperLogLog expressions contain expressions for performing HLL operations.
Most of these operations are equivalent to the :mod:`HyperLogLog API <aerospike_helpers.operations.hll_operations>`.

Example::

    import aerospike_helpers.expressions as exp
    # Get count from HLL bin "d".
    expr = exp.HLLGetCount(exp.HLLBin("d")).compile()
'''

#from __future__ import annotations
from itertools import chain
from typing import List, Optional, Tuple, Union, Dict, Any
import aerospike
from aerospike_helpers import cdt_ctx
from aerospike_helpers.expressions.resources import _BaseExpr
from aerospike_helpers.expressions.resources import ResultType
from aerospike_helpers.expressions.resources import _Keys
from aerospike_helpers.expressions.base import HLLBin

########################
# HLL Modify Expressions
########################

TypeBinName = Union[_BaseExpr, str]
TypeListValue = Union[_BaseExpr, List[Any]]
TypeValue = Union[_BaseExpr, Any]
TypePolicy = Union[Dict[str, Any], None]


class HLLInit(_BaseExpr):
    """
       Creates a new HLL or resets an existing HLL.
       If index_bit_count and mh_bit_count are None, an existing HLL bin will be reset but retain its configuration.
       If 1 of index_bit_count or mh_bit_count are set,
       an existing HLL bin will set that config and retain its current value for the unset config.
       If the HLL bin does not exist, index_bit_count is required to create it, mh_bit_count is optional.
    """
    _op = aerospike.OP_HLL_INIT

    def __init__(self, policy: 'TypePolicy', index_bit_count: Union[int, None], mh_bit_count: Union[int, None], bin: 'TypeBinName'):
        """ Args:
                policy (TypePolicy): An optional dictionary of :ref:`HyperLogLog policies <aerospike_hll_policies>`.
                index_bit_count (int): Number of index bits. Must be between 4 and 16 inclusive.
                mh_bit_count (int): Number of min hash bits. Must be between 4 and 51 inclusive.
                bin (TypeBinName): An :class:`~aerospike_helpers.expressions.base.HLLBin` expression.

            :return: Returns the resulting hll.

            Example::

                # Create an HLL with 12 index bits and 24 min hash bits.
                expr = exp.HLLInit(None, 12, 24, exp.HLLBin("my_hll"))
        """
        self._children = (
            -1 if index_bit_count is None else index_bit_count,
            -1 if mh_bit_count is None else mh_bit_count,
            policy['flags'] if policy is not None and 'flags' in policy else aerospike.HLL_WRITE_DEFAULT,
            bin if isinstance(bin, _BaseExpr) else HLLBin(bin)
        )


class HLLAdd(_BaseExpr):
    """Create an expression that performs an hll_add."""
    _op = aerospike.OP_HLL_ADD

    def __init__(self, policy: 'TypePolicy', list: 'TypeListValue', index_bit_count: Union[int, None], mh_bit_count: Union[int, None], bin: 'TypeBinName'):
        """ Args:
                policy (TypePolicy): An optional dictionary of :ref:`HyperLogLog policies <aerospike_hll_policies>`.
                list (TypeListValue): A list or list expression of elements to add to the HLL.
                index_bit_count (int): Number of index bits. Must be between 4 and 16 inclusive.
                mh_bit_count (int): Number of min hash bits. Must be between 4 and 51 inclusive.
                bin (TypeBinName): An :class:`~aerospike_helpers.expressions.base.HLLBin` expression.

            :return: Returns the resulting hll bin after adding elements from list.

            Example::

                # Let HLL bin "d" have the following elements, ['key1', 'key2', 'key3'], index_bits 8, mh_bits 8.
                # Add ['key4', 'key5', 'key6'] so that the returned value is ['key1', 'key2', 'key3', 'key4', 'key5', 'key6']
                expr = exp.HLLAdd(None, ['key4', 'key5', 'key6'], 8, 8, exp.HLLBin("d")).compile()
        """
        self._children = (
            list,
            -1 if index_bit_count is None else index_bit_count,
            -1 if mh_bit_count is None else mh_bit_count,
            policy['flags'] if policy is not None and 'flags' in policy else aerospike.HLL_WRITE_DEFAULT,
            bin if isinstance(bin, _BaseExpr) else HLLBin(bin)
        )


######################
# HLL Read Expressions
######################


class HLLGetCount(_BaseExpr):
    """Create an expression that performs an as_operations_hll_get_count."""
    _op = aerospike.OP_HLL_GET_COUNT

    def __init__(self, bin: 'TypeBinName'):
        """ Args:
               bin (TypeBinName): An :class:`~aerospike_helpers.expressions.base.HLLBin` expression.

            :return: Integer bin, the estimated number of unique elements in an HLL.

            Example::

                # Get count from HLL bin "d".
                expr = exp.HLLGetCount(exp.HLLBin("d")).compile()
        """
        self._children = (
            bin if isinstance(bin, _BaseExpr) else HLLBin(bin),
        )


class HLLGetUnion(_BaseExpr):
    """Create an expression that performs an hll_get_union."""
    _op = aerospike.OP_HLL_GET_UNION

    def __init__(self, values: 'TypeValue', bin: 'TypeBinName'):
        """ Args:
                values (TypeValue): A single HLL or list of HLLs, values or expressions, to union with bin.
                bin (TypeBinName): An :class:`~aerospike_helpers.expressions.base.HLLBin` expression.

            :return: HLL bin representing the set union.

            Example::

                # Let HLLBin "d" contain keys ['key%s' % str(i) for i in range(10000)].
                # Let values be a list containing HLL objects retrieved from the aerospike database.
                # Find the union of HLL bin "d" and all HLLs in values.
                expr = exp.HLLGetUnion(values, exp.HLLBin("d")).compile()
        """
        self._children = (
            values,
            bin if isinstance(bin, _BaseExpr) else HLLBin(bin),
        )


class HLLGetUnionCount(_BaseExpr):
    """Create an expression that performs an as_operations_hll_get_union_count."""
    _op = aerospike.OP_HLL_GET_UNION_COUNT

    def __init__(self, values: 'TypeValue', bin: 'TypeBinName'):
        """ Args:
                values (TypeValue): A single HLL or list of HLLs, values or expressions, to union with bin.
                bin (TypeBinName): An :class:`~aerospike_helpers.expressions.base.HLLBin` expression.

            :return: Integer bin, estimated number of elements in the set union.

            Example::

                # Let HLLBin "d" contain keys ['key%s' % str(i) for i in range(10000)].
                # Let values be a list containing one HLL object with keys ['key%s' % str(i) for i in range(5000, 15000)].
                # Find the count of keys in the union of HLL bin "d" and all HLLs in values. (Should be around 15000)
                expr = exp.HLLGetUnionCount(values, exp.HLLBin("d")).compile()
        """
        self._children = (
            values,
            bin if isinstance(bin, _BaseExpr) else HLLBin(bin),
        )


class HLLGetIntersectCount(_BaseExpr):
    """Create an expression that performs an as_operations_hll_get_inersect_count."""
    _op = aerospike.OP_HLL_GET_INTERSECT_COUNT

    def __init__(self, values: 'TypeValue', bin: 'TypeBinName'):
        """ Args:
                values (TypeValue): A single HLL or list of HLLs, values or expressions, to intersect with bin.
                bin (TypeBinName): An :class:`~aerospike_helpers.expressions.base.HLLBin` expression.

            :return: Integer bin, estimated number of elements in the set intersection.

            Example::

                # Let HLLBin "d" contain keys ['key%s' % str(i) for i in range(10000)].
                # Let values be a list containing one HLL object with keys ['key%s' % str(i) for i in range(5000, 15000)].
                # Find the count of keys in the intersection of HLL bin "d" and all HLLs in values. (Should be around 5000)
                expr = exp.HLLGetIntersectCount(values, exp.HLLBin("d")).compile()
        """
        self._children = (
            values,
            bin if isinstance(bin, _BaseExpr) else HLLBin(bin),
        )


class HLLGetSimilarity(_BaseExpr):
    """Create an expression that performs an as_operations_hll_get_similarity."""
    _op = aerospike.OP_HLL_GET_SIMILARITY

    def __init__(self, values: 'TypeValue', bin: 'TypeBinName'):
        """ Args:
                values (TypeValue): A single HLL or list of HLLs, values or expressions, to calculate similarity with.
                bin (TypeBinName): An :class:`~aerospike_helpers.expressions.base.HLLBin` expression.

            :return: Float bin, stimated similarity between 0.0 and 1.0.

            Example::

                # Let HLLBin "d" contain keys ['key%s' % str(i) for i in range(10000)].
                # Let values be a list containing one HLL object with keys ['key%s' % str(i) for i in range(5000, 15000)].
                # Find the similarity the HLL in values to HLL bin "d". (Should be around 0.33)
                # Note that similarity is defined as intersect(A, B, ...) / union(A, B, ...).
                expr = exp.HLLGetSimilarity(values, exp.HLLBin("d")).compile()
        """
        self._children = (
            values,
            bin if isinstance(bin, _BaseExpr) else HLLBin(bin),
        )


class HLLDescribe(_BaseExpr):
    """Create an expression that performs an as_operations_hll_describe."""
    _op = aerospike.OP_HLL_DESCRIBE

    def __init__(self, bin: 'TypeBinName'):
        """ Args:
                bin (TypeBinName): An :class:`~aerospike_helpers.expressions.base.HLLBin` expression.

            :return: List bin, a list containing the index_bit_count and minhash_bit_count.

            Example::

                # Get description of HLL bin "d".
                expr = exp.HLLDescribe(exp.HLLBin("d")).compile()
        """
        self._children = (
            bin if isinstance(bin, _BaseExpr) else HLLBin(bin),
        )


class HLLMayContain(_BaseExpr):
    """ Create an expression that checks if the HLL bin contains any keys in
        list.
    """
    _op = aerospike.OP_HLL_MAY_CONTAIN

    def __init__(self, list: 'TypeListValue', bin: 'TypeBinName', ):
        """ Args:
                list (TypeListValue): A list expression of keys to check if the HLL may contain them.
                bin (TypeBinName): An :class:`~aerospike_helpers.expressions.base.HLLBin` expression.

            :return: 1 if bin contains any key in list, 0 otherwise.

            Example::

                # Check if HLL bin "d" contains any of the keys in `list`.
                expr = exp.HLLMayContain(["key1", "key2", "key3"], exp.HLLBin("d")).compile()
        """
        self._children = (
            list,
            bin if isinstance(bin, _BaseExpr) else HLLBin(bin),
        )
