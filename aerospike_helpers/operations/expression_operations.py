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

Expression operations support reading and writing the result of Aerospike expressions.

.. note:: Requires server version >= 5.6.0

"""
import aerospike
from aerospike_helpers.expressions import resources


OP_KEY = "op"
BIN_KEY = "bin"
EXPR_KEY = "expr"
EXPR_FLAGS_KEY = "expr_flags"


def expression_read(bin_name: str, expression: resources._BaseExpr, expression_read_flags: int=0):
    """Create an expression read operation dictionary.

    Reads and returns the value produced by the evaluated expression.

    Args:
        bin_name (str): The name of the bin to read from. Even if no bin is being read from, the value will be returned with this bin name.
        expression: A compiled Aerospike expression, see :ref:`aerospike_operation_helpers.expressions`.
        expression_read_flags (int): :ref:`aerospike_expression_read_flags` (default ``aerospike.EXP_READ_DEFAULT``)
    Returns:
        A dictionary to be passed to operate or operate_ordered.

    Example::

        # Read the value of int bin "balance".
        # Let 'client' be a connected aerospike client.
        # Let int bin 'balance' == 50.

        from aerospike_helpers.operations import expression_operations as expressions
        from aerospike_helpers.expressions import *
        
        expr = IntBin("balance").compile()
        ops = [
            expressions.expression_read("balance", expr)
        ]
        _, _, res = client.operate(self.key, ops)
       print(res)

       # EXPECTED OUTPUT: {"balance": 50}
    """

    op_dict = {
        OP_KEY: aerospike.OP_EXPR_READ,
        BIN_KEY: bin_name,
        EXPR_KEY: expression,
        EXPR_FLAGS_KEY: expression_read_flags
    }

    return op_dict


def expression_write(bin_name: str, expression: resources._BaseExpr, expression_write_flags: int=0):
    """Create an expression write operation dictionary.

    Writes the value produced by the evaluated expression to the supplied bin.

    Args:
        bin_name (str): The name of the bin to write to.
        expression: A compiled Aerospike expression, see :ref:`aerospike_operation_helpers.expressions`.
        expression_write_flags (int): :ref:`aerospike_expression_write_flags` such as ``aerospike.EXP_WRITE_UPDATE_ONLY | aerospike.EXP_WRITE_POLICY_NO_FAIL``   (default ``aerospike.EXP_WRITE_DEFAULT``).
    Returns:
        A dictionary to be passed to operate or operate_ordered.

    Example::

        # Write the value of int bin "balance" + 50 back to "balance".
        # Let 'client' be a connected aerospike client.
        # Let int bin 'balance' == 50.

        from aerospike_helpers.operations import expression_operations as expressions
        from aerospike_helpers.expressions import *
        
        expr = Add(IntBin("balance"), 50).compile()
        ops = [
            expressions.expression_write("balance", expr)
        ]
        client.operate(self.key, ops)
        _, _, res = client.get(self.key)
       print(res)

       # EXPECTED OUTPUT: {"balance": 100}
    """

    op_dict = {
        OP_KEY: aerospike.OP_EXPR_WRITE,
        BIN_KEY: bin_name,
        EXPR_KEY: expression,
        EXPR_FLAGS_KEY: expression_write_flags
    }

    return op_dict
