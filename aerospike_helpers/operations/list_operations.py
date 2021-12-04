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
:meth:`aerospike.operate` and :meth:`aerospike.operate_ordered` methods of the aerospike module.

List operations support nested CDTs through an optional ctx context argument.
    The ctx argument is a list of cdt_ctx context operation objects. See :class:`aerospike_helpers.cdt_ctx`.

.. note:: Nested CDT (ctx) requires server version >= 4.6.0

"""
import aerospike


OP_KEY = "op"
BIN_KEY = "bin"
VALUE_KEY = "val"
INDEX_KEY = "index"
LIST_POLICY_KEY = "list_policy"
INVERTED_KEY = "inverted"
RETURN_TYPE_KEY = "return_type"
COUNT_KEY = "count"
RANK_KEY = "rank"
VALUE_BEGIN_KEY = "value_begin"
VALUE_END_KEY = "value_end"
VALUE_LIST_KEY = "value_list"
LIST_ORDER_KEY = "list_order"
SORT_FLAGS_KEY = "sort_flags"
CTX_KEY = "ctx"


def list_append(bin_name: str, value, policy: dict=None, ctx: list=None):
    """Creates a list append operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`

    The list append operation instructs the aerospike server to append an item to the
    end of a list bin.

    Args:
        bin_name (str): The name of the bin to be operated on.
        value: The value to be appended to the end of the list.
        policy (dict): An optional dictionary of :ref:`list write options <aerospike_list_policies>`.
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_LIST_APPEND,
        BIN_KEY: bin_name,
        VALUE_KEY: value
    }

    if policy:
        op_dict[LIST_POLICY_KEY] = policy
    
    if ctx:
        op_dict[CTX_KEY] = ctx

    return op_dict


def list_append_items(bin_name: str, values, policy: dict=None, ctx: list=None):
    """Creates a list append items operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`

    The list append items operation instructs the aerospike server to append multiple items to the
    end of a list bin.

    Args:
        bin_name (str): The name of the bin to be operated on.
        values (list): A sequence of items to be appended to the end of the list.
        policy (dict): An optional dictionary of :ref:`list write options <aerospike_list_policies>`.
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_LIST_APPEND_ITEMS,
        BIN_KEY: bin_name,
        VALUE_KEY: values
    }

    if policy:
        op_dict[LIST_POLICY_KEY] = policy
    
    if ctx:
        op_dict[CTX_KEY] = ctx

    return op_dict

def list_insert(bin_name: str, index, value, policy: dict=None, ctx: list=None):
    """Creates a list insert operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`

    The list insert operation inserts an item at index: `index` into the list contained
    in the specified bin.

    Args:
        bin_name (str): The name of the bin to be operated on.
        index (int): The index at which to insert an item. The value may be positive to use
            zero based indexing or negative to index from the end of the list.
        value: The value to be inserted into the list.
        policy (dict): An optional dictionary of :ref:`list write options <aerospike_list_policies>`.
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_LIST_INSERT,
        BIN_KEY: bin_name,
        INDEX_KEY: index,
        VALUE_KEY: value
    }

    if policy:
        op_dict[LIST_POLICY_KEY] = policy
    
    if ctx:
        op_dict[CTX_KEY] = ctx

    return op_dict

def list_insert_items(bin_name: str, index, values, policy: dict=None, ctx: list=None):
    """Creates a list insert items operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`

    The list insert items operation inserts items at index: `index` into the list contained
    in the specified bin.

    Args:
        bin_name (str): The name of the bin to be operated on.
        index (int): The index at which to insert the items. The value may be positive to use
            zero based indexing or negative to index from the end of the list.
        values (list): The values to be inserted into the list.
        policy (dict): An optional dictionary of :ref:`list write options <aerospike_list_policies>`.
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_LIST_INSERT_ITEMS,
        BIN_KEY: bin_name,
        INDEX_KEY: index,
        VALUE_KEY: values
    }

    if policy:
        op_dict[LIST_POLICY_KEY] = policy
    
    if ctx:
        op_dict[CTX_KEY] = ctx

    return op_dict

def list_increment(bin_name: str, index, value, policy: dict=None, ctx: list=None):
    """Creates a list increment operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`

    The list insert operation inserts an item at index: `index` into the list contained
    in the specified bin.

    Args:
        bin_name (str): The name of the bin to be operated on.
        index (int): The index of the list item to increment.
        value (int, float) : The value to be added to the list item.
        policy (dict): An optional dictionary of :ref:`list write options <aerospike_list_policies>`.
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_LIST_INCREMENT,
        BIN_KEY: bin_name,
        INDEX_KEY: index,
        VALUE_KEY: value
    }

    if policy:
        op_dict[LIST_POLICY_KEY] = policy
    
    if ctx:
        op_dict[CTX_KEY] = ctx

    return op_dict


def list_pop(bin_name: str, index, ctx: list=None):
    """Creates a list pop operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`

    The list pop operation removes and returns an item index: `index` from list contained
    in the specified bin.

    Args:
        bin_name (str): The name of the bin to be operated on.
        index (int): The index of the item to be removed.
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_LIST_POP,
        BIN_KEY: bin_name,
        INDEX_KEY: index
    }

    if ctx:
        op_dict[CTX_KEY] = ctx
    
    return op_dict


def list_pop_range(bin_name: str, index, count, ctx: list=None):
    """Creates a list pop range operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`

    The list insert range operation removes and returns `count` items
    starting from index: `index` from the list contained in the specified bin.

    Args:
        bin_name (str): The name of the bin to be operated on.
        index (int): The index of the first item to be removed.
        count (int): A positive number indicating how many items, including the first, to be removed and returned
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_LIST_POP_RANGE,
        BIN_KEY: bin_name,
        INDEX_KEY: index,
        VALUE_KEY: count
    }

    if ctx:
        op_dict[CTX_KEY] = ctx
    
    return op_dict


def list_remove(bin_name: str, index, ctx: list=None):
    """Create list remove operation.

    The list remove operation removes an item located at `index` in the list specified by `bin_name`

    Args:
        bin_name (str): The name of the bin containing the item to be removed.
        index (int): The index at which to remove the item.
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict =  {
        OP_KEY: aerospike.OP_LIST_REMOVE,
        BIN_KEY: bin_name,
        INDEX_KEY: index
    }

    if ctx:
        op_dict[CTX_KEY] = ctx
    
    return op_dict


def list_remove_range(bin_name: str, index, count, ctx: list=None):
    """Create list remove range operation.

    The list remove range operation removes `count` items starting at `index`
    in the list specified by `bin_name`

    Args:
        bin_name (str): The name of the bin containing the items to be removed.
        index (int): The index of the first item to remove.
        count (int): A positive number representing the number of items to be removed.
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_LIST_REMOVE_RANGE,
        BIN_KEY: bin_name,
        INDEX_KEY: index,
        VALUE_KEY: count
    }

    if ctx:
        op_dict[CTX_KEY] = ctx
    
    return op_dict


def list_clear(bin_name: str, ctx: list=None):
    """Create list clear operation.

    The list clear operation removes all items from the list specified by `bin_name`

    Args:
        bin_name (str): The name of the bin containing the list to be cleared
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.

    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_LIST_CLEAR,
        BIN_KEY: bin_name
    }

    if ctx:
        op_dict[CTX_KEY] = ctx
    
    return op_dict


def list_set(bin_name: str, index, value, policy: dict=None, ctx: list=None):
    """Create a list set operation.

    The list set operations sets the value of the item at `index` to `value`

    Args:
        bin_name (str): The name of the bin containing the list to be operated on.
        index (int): The index of the item to be set.
        value: The value to be assigned to the list item.
        policy (dict): An optional dictionary of :ref:`list write options <aerospike_list_policies>`.
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.

    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_LIST_SET,
        BIN_KEY: bin_name,
        INDEX_KEY: index,
        VALUE_KEY: value
    }
    if policy:
        op_dict[LIST_POLICY_KEY] = policy
    
    if ctx:
        op_dict[CTX_KEY] = ctx

    return op_dict


def list_get(bin_name: str, index, ctx: list=None):
    """Create a list get operation.

    The list get operation gets the value of the item at `index` and returns the value

    Args:
        bin_name (str): The name of the bin containing the list to fetch items from.
        index (int): The index of the item to be returned.
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.

    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_LIST_GET,
        BIN_KEY: bin_name,
        INDEX_KEY: index
    }

    if ctx:
        op_dict[CTX_KEY] = ctx
    
    return op_dict


def list_get_range(bin_name: str, index, count, ctx: list=None):
    """Create a list get range operation.

    The list get range operation gets `count` items starting `index` and returns the values.

    Args:
        bin_name (str): The name of the bin containing the list to fetch items from.
        index (int): The index of the item to be returned.
        count (int): A positive number of items to be returned.
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.

    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_LIST_GET_RANGE,
        BIN_KEY: bin_name,
        INDEX_KEY: index,
        VALUE_KEY: count
    }

    if ctx:
        op_dict[CTX_KEY] = ctx
    
    return op_dict


def list_trim(bin_name: str, index, count, ctx: list=None):
    """Create a list trim operation.

    Server removes items in list bin that do not fall into range specified by index and count range.

    Args:
        bin_name (str): The name of the bin containing the list to be trimmed.
        index (int): The index of the items to be kept.
        count (int): A positive number of items to be kept.
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.

    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_LIST_TRIM,
        BIN_KEY: bin_name,
        INDEX_KEY: index,
        VALUE_KEY: count
    }

    if ctx:
        op_dict[CTX_KEY] = ctx
    
    return op_dict


def list_size(bin_name: str, ctx: list=None):
    """Create a list size operation.

    Server returns the size of the list in the specified bin.

    Args:
        bin_name (str): The name of the bin containing the list.
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.

    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_LIST_SIZE,
        BIN_KEY: bin_name
    }

    if ctx:
        op_dict[CTX_KEY] = ctx
    
    return op_dict


# Post 3.4.0 Operations. Require Server >= 3.16.0.1

def list_get_by_index(bin_name: str, index, return_type, ctx: list=None):
    """Create a list get index operation.

    The list get operation gets the item at `index` and returns a value
    specified by `return_type`

    Args:
        bin_name (str): The name of the bin containing the list to fetch items from.
        index (int): The index of the item to be returned.
        return_type (int): Value specifying what should be returned from the operation.
            This should be one of the :ref:`list_return_types` values
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.

    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_LIST_GET_BY_INDEX,
        BIN_KEY: bin_name,
        RETURN_TYPE_KEY: return_type,
        INDEX_KEY: index
    }

    if ctx:
        op_dict[CTX_KEY] = ctx
    
    return op_dict


def list_get_by_index_range(bin_name: str, index, return_type, count=None, inverted=False, ctx: list=None):
    """Create a list get index range operation.

    The list get by index range operation gets `count` items starting at `index` and returns a value
    specified by `return_type`

    Args:
        bin_name (str): The name of the bin containing the list to fetch items from.
        index (int): The index of the first item to be returned.
        count (int): The number of list items to be selected.
        return_type (int): Value specifying what should be returned from the operation.
            This should be one of the :ref:`list_return_types` values.
        inverted (bool): Optional bool specifying whether to invert the return type.
            If set to `True`, all items outside of the specified range will be returned.
            Default: `False`
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.

    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_LIST_GET_BY_INDEX_RANGE,
        BIN_KEY: bin_name,
        RETURN_TYPE_KEY: return_type,
        INDEX_KEY: index,
        INVERTED_KEY: inverted
    }

    if count is not None:
        op_dict[COUNT_KEY] = count
    
    if ctx:
        op_dict[CTX_KEY] = ctx

    return op_dict


def list_get_by_rank(bin_name: str, rank, return_type, ctx: list=None):
    """Create a list get by rank operation.

    Server selects list item identified by `rank` and returns selected data
    specified by `return_type`.

    Args:
        bin_name (str): The name of the bin containing the list to fetch a value from.
        rank (int): The rank of the item to be fetched.
        return_type (int): Value specifying what should be returned from the operation.
            This should be one of the :ref:`list_return_types` values
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.

    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_LIST_GET_BY_RANK,
        BIN_KEY: bin_name,
        RETURN_TYPE_KEY: return_type,
        RANK_KEY: rank
    }

    if ctx:
        op_dict[CTX_KEY] = ctx
    
    return op_dict


def list_get_by_rank_range(bin_name: str, rank, return_type, count=None, inverted=False, ctx: list=None):
    """Create a list get by rank range operation.

    Server selects `count` items starting at the specified `rank` and returns selected data
    specified by `return_type`.

    Args:
        bin_name (str): The name of the bin containing the list to fetch items from.
        rank (int): The rank of the first items to be returned.
        count (int): A positive number indicating number of items to be returned.
        return_type (int): Value specifying what should be returned from the operation.  This should be one of the :ref:`list_return_types` values
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
        inverted (bool): Optional bool specifying whether to invert the return type.
            If set to `True`, all items outside of the specified rank range will be returned.
            Default: `False`

    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_LIST_GET_BY_RANK_RANGE,
        BIN_KEY: bin_name,
        RETURN_TYPE_KEY: return_type,
        RANK_KEY: rank,
        INVERTED_KEY: inverted
    }

    if count is not None:
        op_dict[COUNT_KEY] = count
    
    if ctx:
        op_dict[CTX_KEY] = ctx

    return op_dict


def list_get_by_value(bin_name: str, value, return_type, inverted=False, ctx: list=None):
    """Create a list get by value operation.

    Server selects list items with a value equal to `value` and returns selected data specified by
    `return_type`.

    Args:
        bin_name (str): The name of the bin containing the list to fetch items from.
        value: The server returns all items matching this value
        return_type (int): Value specifying what should be returned from the operation.  This should be one of the :ref:`list_return_types` values
        inverted (bool): Optional bool specifying whether to invert the return type.
            If set to `True`, all items not equal to `value` will be selected. Default: `False`
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
    Returns:
        A dictionary usable in operate or operate_ordered.The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_LIST_GET_BY_VALUE,
        BIN_KEY: bin_name,
        RETURN_TYPE_KEY: return_type,
        VALUE_KEY: value,
        INVERTED_KEY: inverted
    }

    if ctx:
        op_dict[CTX_KEY] = ctx

    return op_dict


def list_get_by_value_list(bin_name: str, value_list, return_type, inverted=False, ctx: list=None):
    """Create a list get by value list operation.

    Server selects list items with a value contained in `value_list` and returns selected data
    specified by `return_type`.

    Args:
        bin_name (str): The name of the bin containing the list to fetch items from.
        value_list (list): Return items from the list matching an item in this list.
        return_type (int): Value specifying what should be returned from the operation.
            This should be one of the :ref:`list_return_types` values
        inverted (bool): Optional bool specifying whether to invert the return type.
            If set to `True`, all items not matching an entry in `value_list` will be selected.
            Default: `False`
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
    Returns:
        A dictionary usable in operate or operate_ordered.The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_LIST_GET_BY_VALUE_LIST,
        BIN_KEY: bin_name,
        RETURN_TYPE_KEY: return_type,
        VALUE_LIST_KEY: value_list,
        INVERTED_KEY: inverted
    }

    if ctx:
        op_dict[CTX_KEY] = ctx

    return op_dict


def list_get_by_value_range(bin_name: str, return_type, value_begin, value_end, inverted=False, ctx: list=None):
    """Create a list get by value list operation.

    Server selects list items with a value greater than or equal to `value_begin`
    and less than `value_end`. If `value_begin` is `None`, range is greater than or equal
    to the first element of the list. If `value_end` is `None` range extends to the end of the list.
    Server returns selected data specified by `return_type`.

    Args:
        bin_name (str): The name of the bin containing the list to fetch items from.
        value_begin: The start of the value range.
        value_end: The end of the value range.
        return_type (int): Value specifying what should be returned from the operation.
            This should be one of the :ref:`list_return_types` values
        inverted (bool): Optional bool specifying whether to invert the return type.
            If set to `True`, all items not included in the specified range will be returned.
            Default: `False`
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
    Returns:
        A dictionary usable in operate or operate_ordered.The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_LIST_GET_BY_VALUE_RANGE,
        BIN_KEY: bin_name,
        RETURN_TYPE_KEY: return_type,
        INVERTED_KEY: inverted
    }

    if value_begin is not None:
        op_dict[VALUE_BEGIN_KEY] = value_begin

    if value_end is not None:
        op_dict[VALUE_END_KEY] = value_end

    if ctx:
        op_dict[CTX_KEY] = ctx

    return op_dict


def list_remove_by_index(bin_name: str, index, return_type, ctx: list=None):
    """Create a list remove by index operation.

    The list_remove_by_index operation removes the value of the item at `index` and returns a value
    specified by `return_type`

    Args:
        bin_name (str): The name of the bin containing the list to remove an item from.
        index (int): The index of the item to be removed.
        return_type (int): Value specifying what should be returned from the operation.
            This should be one of the :ref:`list_return_types` values
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.

    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_LIST_REMOVE_BY_INDEX,
        BIN_KEY: bin_name,
        RETURN_TYPE_KEY: return_type,
        INDEX_KEY: index
    }

    if ctx:
        op_dict[CTX_KEY] = ctx
    
    return op_dict


def list_remove_by_index_range(bin_name: str, index, return_type, count=None, inverted=False, ctx: list=None):
    """Create a list remove by index range operation.

    The list remove by index range operation removes `count` starting at `index` and returns a value
    specified by `return_type`.

    Args:
        bin_name (str): The name of the bin containing the list to remove items from.
        index (int): The index of the first item to be removed.
        count (int): The number of items to be removed
        return_type (int): Value specifying what should be returned from the operation.
            This should be one of the :ref:`list_return_types` values.
        inverted (bool): Optional bool specifying whether to invert the operation.
            If set to `True`, all items outside of the specified range will be removed.
            Default: `False`
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.

    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_LIST_REMOVE_BY_INDEX_RANGE,
        BIN_KEY: bin_name,
        RETURN_TYPE_KEY: return_type,
        INDEX_KEY: index,
        INVERTED_KEY: inverted
    }

    if count is not None:
        op_dict[COUNT_KEY] = count
    
    if ctx:
        op_dict[CTX_KEY] = ctx

    return op_dict


def list_remove_by_rank(bin_name: str, rank, return_type, ctx: list=None):
    """Create a list remove by rank operation.

    Server removes a list item identified by `rank` and returns selected data
    specified by `return_type`.

    Args:
        bin_name (str): The name of the bin containing the list to fetch a value from.
        rank (int): The rank of the item to be removed.
        return_type (int): Value specifying what should be returned from the operation.
            This should be one of the :ref:`list_return_types` values
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.

    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_LIST_REMOVE_BY_RANK,
        BIN_KEY: bin_name,
        RETURN_TYPE_KEY: return_type,
        RANK_KEY: rank
    }

    if ctx:
        op_dict[CTX_KEY] = ctx

    return op_dict


def list_remove_by_rank_range(bin_name: str, rank, return_type, count=None, inverted=False, ctx: list=None):
    """Create a list remove by rank range operation.

    Server removes `count` items starting at the specified `rank` and returns selected data
    specified by `return_type`.

    Args:
        bin_name (str): The name of the bin containing the list to fetch items from.
        rank (int): The rank of the first item to removed.
        count (int): A positive number indicating number of items to be removed.
        return_type (int): Value specifying what should be returned from the operation.
            This should be one of the :ref:`list_return_types` values
        inverted (bool): Optional bool specifying whether to invert the operation.
            If set to `True`, all items outside of the specified rank range will be removed.
            Default: `False`
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.

    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_LIST_REMOVE_BY_RANK_RANGE,
        BIN_KEY: bin_name,
        RETURN_TYPE_KEY: return_type,
        RANK_KEY: rank,
        INVERTED_KEY: inverted
    }

    if count is not None:
        op_dict[COUNT_KEY] = count
    
    if ctx:
        op_dict[CTX_KEY] = ctx

    return op_dict


def list_remove_by_value(bin_name: str, value, return_type, inverted=False, ctx: list=None):
    """Create a list remove by value operation.

    Server removes list items with a value equal to `value` and returns selected data specified by
    `return_type`.

    Args:
        bin_name (str): The name of the bin containing the list to remove items from.
        value: The server removes all list items matching this value.
        return_type (int): Value specifying what should be returned from the operation.
            This should be one of the :ref:`list_return_types` values
        inverted (bool): Optional bool specifying whether to invert the operation.
            If set to `True`, all items not equal to `value` will be removed.
            Default: `False`
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
    Returns:
        A dictionary usable in operate or operate_ordered.The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_LIST_REMOVE_BY_VALUE,
        BIN_KEY: bin_name,
        RETURN_TYPE_KEY: return_type,
        VALUE_KEY: value,
        INVERTED_KEY: inverted
    }

    if ctx:
        op_dict[CTX_KEY] = ctx

    return op_dict


def list_remove_by_value_list(bin_name: str, value_list, return_type,
        inverted=False, ctx: list=None):
    """Create a list remove by value list operation.

    Server removes list items with a value matching one contained in `value_list`
    and returns selected data specified by `return_type`.

    Args:
        bin_name (str): The name of the bin containing the list to remove items from.
        value_list (list): The server removes all list items matching one of these values.
        return_type (int): Value specifying what should be returned from the operation.
            This should be one of the :ref:`list_return_types` values
        inverted (bool): Optional bool specifying whether to invert the operation.
            If set to `True`, all items not equal to a value contained in `value_list` will be removed.
            Default: `False`
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
    Returns:
        A dictionary usable in operate or operate_ordered.The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_LIST_REMOVE_BY_VALUE_LIST,
        BIN_KEY: bin_name,
        RETURN_TYPE_KEY: return_type,
        VALUE_LIST_KEY: value_list,
        INVERTED_KEY: inverted
    }

    if ctx:
        op_dict[CTX_KEY] = ctx

    return op_dict


def list_remove_by_value_range(bin_name: str, return_type, value_begin=None, value_end=None, inverted=False, ctx: list=None):
    """Create a list remove by value range operation.

    Server removes list items with a value greater than or equal to `value_begin`
    and less than `value_end`.
    If `value_begin` is `None`, range is greater than or equal to the first element of the list.
    If `value_end` is `None` range extends to the end of the list.
    Server returns selected data specified by `return_type`.

    Args:
        bin_name (str): The name of the bin containing the list to fetch items from.
        value_begin: The start of the value range.
        value_end: The end of the value range.
        return_type (int): Value specifying what should be returned from the operation.
            This should be one of the :ref:`list_return_types` values
        inverted (bool): Optional bool specifying whether to invert the operation.
            If set to `True`, all items not included in the specified range will be removed.
            Default: `False`
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
    Returns:
        A dictionary usable in operate or operate_ordered.The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_LIST_REMOVE_BY_VALUE_RANGE,
        BIN_KEY: bin_name,
        RETURN_TYPE_KEY: return_type,
        INVERTED_KEY: inverted
    }

    if value_begin is not None:
        op_dict[VALUE_BEGIN_KEY] = value_begin

    if value_end is not None:
        op_dict[VALUE_END_KEY] = value_end

    if ctx:
        op_dict[CTX_KEY] = ctx

    return op_dict


def list_set_order(bin_name: str, list_order, ctx: list=None):
    """Create a list set order operation.

    The list_set_order operation sets an order on a specified list bin.

    Args:
        bin_name (str): The name of the list bin.
        list_order: The ordering to apply to the list. Should be aerospike.LIST_ORDERED or
            aerospike.LIST_UNORDERED .
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
    Returns:
        A dictionary usable in operate or operate_ordered.The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_LIST_SET_ORDER,
        BIN_KEY: bin_name,
        LIST_ORDER_KEY: list_order
    }

    if ctx:
        op_dict[CTX_KEY] = ctx

    return op_dict


def list_sort(bin_name: str, sort_flags: int=0, ctx: list=None):
    """Create a list sort operation

    The list sort operation will sort the specified list bin.

    Args:
        bin_name (str): The name of the bin to sort.
        sort_flags (int): :ref:`aerospike_list_sort_flag` modifiying the sorting behavior (default ``aerospike.DEFAULT_LIST_SORT``).
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
    Returns:
        A dictionary usable in operate or operate_ordered.The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_LIST_SORT,
        BIN_KEY: bin_name,
        SORT_FLAGS_KEY: sort_flags
    }

    if ctx:
        op_dict[CTX_KEY] = ctx

    return op_dict


def list_get_by_value_rank_range_relative(bin_name: str, value, offset, return_type, count=None,
        inverted=False, ctx: list=None):
    """Create a list get by value rank range relative operation

    Create list get by value relative to rank range operation.
    Server selects list items nearest to value and greater by relative rank.
    Server returns selected data specified by return_type.

    Note:
        This operation requires server version 4.3.0 or greater.

    Examples:
        These examples show what would be returned for specific arguments
        when dealing with an ordered list: ``[0,4,5,9,11,15]``

        ::

            (value, offset, count) = [selected items]
            (5, 0, None) = [5,9,11,15]
            (5, 0, 2) = [5, 9]
            (5, -1, None) = [4, 5, 9, 11, 15]
            (5, -1, 3) = [4, 5, 9]
            (3,3, None) = [11, 15]
            (3,-3, None) = [0, 4,5,9,11,15]
            (3, 0, None) = [4,5,9,11,15]


    Args:
        bin_name (str): The name of the bin containing the list.
        value (str): The value of the item in the list for which to search
        offset (int): Begin returning items with rank == rank(found_item) + offset
        return_type (int): Value specifying what should be returned from the operation.
            This should be one of the :ref:`list_return_types` values 
        count (int): If specified, the number of items to return. If None,
            all items until end of list are returned.
        inverted (bool): If True, the operation is inverted, and items outside
            of the specified range are returned.
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.

    Returns:
        A dictionary usable in operate or operate_ordered.The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_LIST_GET_BY_VALUE_RANK_RANGE_REL,
        BIN_KEY: bin_name,
        VALUE_KEY: value,
        RANK_KEY: offset,
        RETURN_TYPE_KEY: return_type,
        INVERTED_KEY: inverted
    }

    if count is not None:
        op_dict[COUNT_KEY] = count
    
    if ctx:
        op_dict[CTX_KEY] = ctx

    return op_dict


def list_remove_by_value_rank_range_relative(bin_name: str, value, offset, return_type, count=None,
        inverted=False, ctx: list=None):
    """Create a list get by value rank range relative operation

    Create list remove by value relative to rank range operation.
    Server removes and returns list items nearest to value and greater by relative rank.
    Server returns selected data specified by return_type.

    Note:
        This operation requires server version 4.3.0 or greater.

        These examples show what would be removed and returned for specific arguments
        when dealing with an ordered list: ``[0,4,5,9,11,15]``

        ::

        (value, offset, count) = [selected items]
        (5,0,None) = [5,9,11,15]
        (5,0,2) = [5, 9]
        (5,-1, None) = [4,5,9,11,15]
        (5, -1, 3) = [4,5,9]
        (3,3, None) = [11,15]
        (3,-3, None) = [0,4,5,9,11,15]
        (3, 0, None) = [4,5,9,11,15]

    Args:
        bin_name (str): The name of the bin containing the list.
        value (str): The value of the item in the list for which to search
        offset (int): Begin removing and returning items with rank == rank(found_item) + offset
        count (int): If specified, the number of items to remove and return. If None,
            all items until end of list are returned.
        inverted (bool): If True, the operation is inverted, and items outside of the specified
            range are removed and returned.
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.

    Returns:
        A dictionary usable in operate or operate_ordered.The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_LIST_REMOVE_BY_VALUE_RANK_RANGE_REL,
        BIN_KEY: bin_name,
        VALUE_KEY: value,
        RANK_KEY: offset,
        RETURN_TYPE_KEY: return_type,
        INVERTED_KEY: inverted

    }
    if count is not None:
        op_dict[COUNT_KEY] = count
    
    if ctx:
        op_dict[CTX_KEY] = ctx

    return op_dict
