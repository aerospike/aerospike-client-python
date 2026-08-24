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
Module with helper functions to create dictionaries used by:

* :mod:`aerospike.Client.operate` and :mod:`aerospike.Client.operate_ordered`
* Certain batched commands listed in :mod:`aerospike_helpers.batch.records`
"""
import warnings

import aerospike
from typing import Any, Optional

from aerospike_helpers.cdt_ctx import _cdt_ctx


def read(bin_name: str) -> dict:
    """Create a read operation dictionary.

    The read operation reads and returns the value in `bin_name`.

    Args:
        bin_name: the name of the bin from which to read.
    Returns:
        A dictionary to be passed to operate or operate_ordered.
    """

    return {
        "op": aerospike.OPERATOR_READ,
        "bin": bin_name,
    }


def write(bin_name: str, write_item: Any) -> dict:
    """Create a write operation dictionary.

    The write operation writes `write_item` into the bin specified by bin_name.

    Args:
        bin_name: The name of the bin into which `write_item` will be stored.
        write_item: The value which will be written into the bin.
    Returns:
        A dictionary to be passed to operate or operate_ordered.
    """
    return {"op": aerospike.OPERATOR_WRITE, "bin": bin_name, "val": write_item}


def delete() -> dict:
    """Create a delete operation dictionary.

    The delete operation deletes a record and all associated bins.
    Requires server version >= 4.7.0.8.

    Returns:
        A dictionary to be passed to operate or operate_ordered.
    """

    return {
        "op": aerospike.OPERATOR_DELETE,
    }


def append(bin_name: str, append_item: Any) -> dict:
    """Create an append operation dictionary.

    The append operation appends `append_item` to the value in bin_name.

    .. deprecated:: 19.3.0 Passing a string argument to ``append_item`` is deprecated.
        This legacy operation performs raw byte concatenation, is not Unicode/DBCS-aware, and does not
        support string policy or ctx.

    Args:
        bin_name: The name of the bin to be used.
        append_item: The value which will be appended to the item contained in the specified bin.
    Returns:
        A dictionary to be passed to operate or operate_ordered.
    """
    return {"op": aerospike.OPERATOR_APPEND, "bin": bin_name, "val": append_item}


def prepend(bin_name: str, prepend_item: Any) -> dict:
    """Create a prepend operation dictionary.

    The prepend operation prepends `prepend_item` to the value in bin_name.

    .. deprecated:: 19.3.0 Passing a string argument to ``prepend_item`` is deprecated.
        This legacy operation performs raw byte concatenation, is not Unicode/DBCS-aware, and does not
        support string policy or ctx.

    Args:
        bin_name: The name of the bin to be used.
        prepend_item: The value which will be prepended to the item contained in the specified bin.
    Returns:
        A dictionary to be passed to operate or operate_ordered.
    """
    return {"op": aerospike.OPERATOR_PREPEND, "bin": bin_name, "val": prepend_item}


def increment(bin_name: str, amount: int | float) -> dict:
    """Create an increment operation dictionary.

    The increment operation increases a value in bin_name by the specified amount,
    or creates a bin with the value of amount.

    Args:
        bin_name: The name of the bin to be incremented.
        amount: The amount by which to increment the item in the specified bin.
    Returns:
        A dictionary to be passed to operate or operate_ordered.
    """
    return {"op": aerospike.OPERATOR_INCR, "bin": bin_name, "val": amount}


def touch(ttl: Optional[int] = None) -> dict:
    """Create a touch operation dictionary.

    Using ttl here is deprecated. It should be set in the policy parameter for the operate method.

    Args:
        ttl: Deprecated. The ttl that should be set for the record.
            This should be set in the policy parameter passed to the operate or
            operate_ordered methods.
    Returns:
        A dictionary to be passed to operate or operate_ordered.
    """
    op_dict = {"op": aerospike.OPERATOR_TOUCH}
    if ttl:
        warnings.warn("TTL should be specified in the policy parameter for operate", DeprecationWarning)
        op_dict["val"] = ttl
    return op_dict


def select_by_path(bin_name: str, ctx: list[_cdt_ctx], flags: int) -> dict:
    """
    Create path expression select operation.

    Args:
        bin_name: Name of bin where this select operation is performed against.
        ctx: List of contexts to select nodes. It is an error for ctx to be :py:obj:`None` or an empty list.
            See :ref:`path_expressions_contexts` for possible contexts.
        flags: See :ref:`exp_path_select_flags` for the set of valid flags for this function.

    Returns:
        A dictionary to be passed to operate or operate_ordered.
    """
    op_dict = {"op": aerospike._AS_OPERATOR_CDT_READ, "bin": bin_name, "ctx": ctx, aerospike._CDT_FLAGS_KEY: flags}
    return op_dict


def modify_by_path(bin_name: str, ctx: list[_cdt_ctx], expr: Any, flags: int) -> dict:
    """
    Create path expression modification operation.

    The results of the evaluation of the modifying expression will replace the
    selected element, and the changes are written back to storage.

    Args:
        bin_name: Name of bin that this modify operation is performed against
        ctx: List of contexts to select nodes. It is an error for ctx to be :py:obj:`None` or an empty list.
            See :ref:`path_expressions_contexts` for possible contexts.
        expr: compiled modifying expression.
        flags: See :ref:`exp_path_modify_flags` for the set of valid flags for this function.

    Returns:
        A dictionary to be passed to operate or operate_ordered.
    """
    op_dict = {
        "op": aerospike._AS_OPERATOR_CDT_MODIFY,
        "bin": bin_name,
        "ctx": ctx, aerospike._CDT_APPLY_MOD_EXP_KEY: expr,
        aerospike._CDT_FLAGS_KEY: flags
    }
    return op_dict
