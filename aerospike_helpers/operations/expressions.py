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
"""
This module provides helper functions to produce dictionaries to be used with the
:mod:`aerospike.Client.operate` and :mod:`aerospike.Client.operate_ordered` methods of the aerospike module.

Expression operations support reading writing the result of Aerospike expressions.

.. note:: Requires server version >= 5.6.0

"""
import aerospike
from aerospike_helpers.expressions import resources


OP_KEY = "op"
BIN_KEY = "bin"
EXPR_KEY = "expr"
EXPR_FLAGS_KEY = "expr_flags"


def expression_read(expression: resources._BaseExpr, expression_read_flags: int = aerospike.EXP_READ_DEFAULT): #TODO create typehint type for flags
    """Create an expression read operation dictionary.

    Reads and returns the value produced by the evaluated expression.

    Args:
        expression: A compiled Aerospike expression, see expressions at :mod:`aerospike_helpers`.
        expression_read_flags: Optional, one or more Aerospike expression read flags. TODO add a link to flags.
    Returns:
        A dictionary to be passed to operate or operate_ordered.
    """

    op_dict = {
        OP_KEY: aerospike.OP_EXPR_READ,
        EXPR_KEY: expression,
        EXPR_FLAGS_KEY: expression_read_flags
    }

    return op_dict


def expression_write(bin_name: str, expression: resources._BaseExpr, expression_write_flags: int = aerospike.EXP_WRITE_DEFAULT): #TODO create typehint type for flags
    """Create an expression write operation dictionary.

    Writes the value produced by the evaluated expression to the supplied bin.

    Args:
        bin_name: The name of the bin to write to.
        expression: A compiled Aerospike expression, see expressions at :mod:`aerospike_helpers`.
        expression_write_flags: Optional, one or more Aerospike expression write flags. TODO add a link to flags.
    Returns:
        A dictionary to be passed to operate or operate_ordered.
    """

    op_dict = {
        OP_KEY: aerospike.OP_EXPR_WRITE,
        BIN_KEY: bin_name,
        EXPR_KEY: expression,
        EXPR_FLAGS_KEY: expression_write_flags
    }

    return op_dict
