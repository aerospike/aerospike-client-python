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
Bitwise expressions contain bit read and modify expressions.

Example::

    import aerospike_helpers.expressions as exp
    # Let blob bin "c" == bytearray([3] * 5).
    # Count set bits starting at 3rd byte in bin "c" to get count of 6.
    expr = exp.BitCount(16, 8 * 3, exp.BlobBin("c")).compile()
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
from aerospike_helpers.expressions.base import BlobBin

##############################
# Bitwise Operator Expressions
##############################

TypeBitValue = Union[bytes, bytearray]
TypeBinName = Union[_BaseExpr, str]
TypePolicy = Union[Dict[str, Any], None]

class BitNot(_BaseExpr):
    """Create a "not" (not) operator expression."""
    _op = _ExprOp.BIT_NOT

    def __init__(self, expr):
        """ Create a bitwise "not" (~) operator expression.

            Args:
                `expr` (_BaseExpr): An integer value or expression to be logically negated.
        
            :return: (boolean value)

            Example::

                # not (a == 0 or a == 10)
                expr = Not(Or(
                            Eq(IntBin("a"), 0),
                            Eq(IntBin("a"), 10))).compile() TODO update example
        """        
        self._children = expr