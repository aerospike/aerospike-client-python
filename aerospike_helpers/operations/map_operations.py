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
Helper functions to create map operation dictionaries arguments for.
the :meth:`aerospike.operate` and :meth:`aerospike.operate_ordered` methods of the aerospike client.

Map operations support nested CDTs through an optional ctx context argument.
    The ctx argument is a list of cdt_ctx context operation objects. See :class:`aerospike_helpers.cdt_ctx`.

.. note:: Nested CDT (ctx) requires server version >= 4.6.0

'''
import aerospike

OP_KEY = "op"
BIN_KEY = "bin"
POLICY_KEY = "map_policy"
VALUE_KEY = "val"
KEY_KEY = "key"
INDEX_KEY = "index"
RETURN_TYPE_KEY = "return_type"
INVERTED_KEY = "inverted"
RANGE_KEY = "range"
COUNT_KEY = "count"
RANK_KEY = "rank"
CTX_KEY = "ctx"


def map_set_policy(bin_name: str, policy, ctx: list=None):
    """Creates a map_set_policy_operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`

    The operation allows a user to set the policy for the map.

    Args:
        bin_name (str): The name of the bin containing the map.
        policy (dict): The :ref:`map_policy dictionary <aerospike_map_policies>`.
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_MAP_SET_POLICY,
        BIN_KEY: bin_name,
        POLICY_KEY: policy
    }

    if ctx is not None:
        op_dict[CTX_KEY] = ctx

    return op_dict

def map_put(bin_name: str, key, value, map_policy: dict=None, ctx: list=None):
    """Creates a map_put operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`

    The operation allows a user to set the value of an item in the map stored
    on the server.

    Args:
        bin_name (str): The name of the bin containing the map.
        key: The key for the map.
        value: The item to store in the map with the corresponding key.
        map_policy (dict):  Optional :ref:`map_policy dictionary <aerospike_map_policies>` specifies the mode of writing items to the Map, and dictates the map order if there is no Map at the *bin_name* 
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_MAP_PUT,
        BIN_KEY: bin_name,
        KEY_KEY: key,
        VALUE_KEY: value
    }

    if map_policy is not None:
        op_dict[POLICY_KEY] = map_policy
    
    if ctx is not None:
        op_dict[CTX_KEY] = ctx
    
    return op_dict


def map_put_items(bin_name: str, item_dict, map_policy: dict=None, ctx: list=None):
    """Creates a map_put_items operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`

    The operation allows a user to add or update items in the map stored on the server.

    Args:
        bin_name (str): The name of the bin containing the map.
        item_dict (dict): A dictionary of key value pairs to be added to the map on the server.
        map_policy (dict):  Optional :ref:`map_policy dictionary <aerospike_map_policies>` specifies the mode of writing items to the Map, and dictates the map order if there is no Map at the *bin_name* 
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_MAP_PUT_ITEMS,
        BIN_KEY: bin_name,
        VALUE_KEY: item_dict
    }

    if map_policy is not None:
        op_dict[POLICY_KEY] = map_policy
    
    if ctx is not None:
        op_dict[CTX_KEY] = ctx
    
    return op_dict

def map_increment(bin_name: str, key, amount, map_policy: dict=None, ctx: list=None):
    """Creates a map_increment operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`

    The operation allows a user to increment the value of a value stored in the map on the server.

    Args:
        bin_name (str): The name of the bin containing the map.
        key: The key for the value to be incremented.
        amount: The amount by which to increment the value stored in map[key]
        map_policy (dict):  Optional :ref:`map_policy dictionary <aerospike_map_policies>` specifies the mode of writing items to the Map, and dictates the map order if there is no Map at the *bin_name* 
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict =  {
        OP_KEY: aerospike.OP_MAP_INCREMENT,
        BIN_KEY: bin_name,
        KEY_KEY: key,
        VALUE_KEY: amount
    }

    if map_policy is not None:
        op_dict[POLICY_KEY] = map_policy
    
    if ctx is not None:
        op_dict[CTX_KEY] = ctx

    return op_dict


def map_decrement(bin_name: str, key, amount, map_policy: dict=None, ctx: list=None):
    """Creates a map_decrement operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`

    The operation allows a user to decrement the value of a value stored in the map on the server.

    Args:
        bin_name (str): The name of the bin containing the map.
        key: The key for the value to be decremented.
        amount: The amount by which to decrement the value stored in map[key]
        map_policy (dict):  Optional :ref:`map_policy dictionary <aerospike_map_policies>` specifies the mode of writing items to the Map, and dictates the map order if there is no Map at the *bin_name* 
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_MAP_DECREMENT,
        BIN_KEY: bin_name,
        KEY_KEY: key,
        VALUE_KEY: amount
    }

    if map_policy is not None:
        op_dict[POLICY_KEY] = map_policy
    
    if ctx is not None:
        op_dict[CTX_KEY] = ctx

    return op_dict

def map_size(bin_name: str, ctx: list=None):
    """Creates a map_size operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`

    The operation returns the size of the map stored in the specified bin.

    Args:
        bin_name (str): The name of the bin containing the map.
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_MAP_SIZE,
        BIN_KEY: bin_name
    }

    if ctx is not None:
        op_dict[CTX_KEY] = ctx

    return op_dict


def map_clear(bin_name: str, ctx: list=None):
    """Creates a map_clear operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`

    The operation removes all items from the map stored in the specified bin.

    Args:
        bin_name (str): The name of the bin containing the map.
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_MAP_CLEAR,
        BIN_KEY: bin_name
    }

    if ctx is not None:
        op_dict[CTX_KEY] = ctx

    return op_dict


def map_remove_by_key(bin_name: str, key, return_type, ctx: list=None):
    """Creates a map_remove_by_key operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`

    The operation removes an item, specified by the key from the map stored in the specified bin.

    Args:
        bin_name (str): The name of the bin containing the map.
        key: The key to be removed from the map
        return_type (int): Value specifying what should be returned from the operation.
            This should be one of the :ref:`map_return_types` values.
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_MAP_REMOVE_BY_KEY,
        BIN_KEY: bin_name,
        KEY_KEY: key,
        RETURN_TYPE_KEY: return_type
    }

    if ctx is not None:
        op_dict[CTX_KEY] = ctx

    return op_dict


def map_remove_by_key_list(bin_name: str, key_list, return_type, inverted=False, ctx: list=None):
    """Creates a map_remove_by_key operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`

    The operation removes items, specified by the keys in key_list from the map stored in the specified bin.

    Args:
        bin_name (str): The name of the bin containing the map.
        key_list (list): A list of keys to be removed from the map.
        return_type (int): Value specifying what should be returned from the operation.
            This should be one of the :ref:`map_return_types` values.
        inverted (bool): If true, keys with values not specified in the key_list will be removed,
            and those keys specified in the key_list will be kept. Default: False
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_MAP_REMOVE_BY_KEY_LIST,
        BIN_KEY: bin_name,
        VALUE_KEY: key_list,
        RETURN_TYPE_KEY: return_type,
        INVERTED_KEY: inverted
    }

    if ctx is not None:
        op_dict[CTX_KEY] = ctx

    return op_dict


def map_remove_by_key_range(bin_name: str, key_range_start, key_range_end, return_type, inverted=False, ctx: list=None):
    """Creates a map_remove_by_key_range operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`

    The operation removes items, with keys between key_range_start(inclusive) and
    key_range_end(exclusive) from the map. 

    Args:
        bin_name (str): The name of the bin containing the map.
        key_range_start: The start of the range of keys to be removed. (Inclusive)
        key_range_end: The end of the range of keys to be removed. (Exclusive)
        return_type (int): Value specifying what should be returned from the operation.
            This should be one of the :ref:`map_return_types` values.
        inverted (bool): If True, values outside of the specified range will be removed, and
            values inside of the range will be kept. Default: False
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_MAP_REMOVE_BY_KEY_RANGE,
        BIN_KEY: bin_name,
        KEY_KEY: key_range_start,
        VALUE_KEY: key_range_end,
        RETURN_TYPE_KEY: return_type,
        INVERTED_KEY: inverted
    }

    if ctx is not None:
        op_dict[CTX_KEY] = ctx

    return op_dict


def map_remove_by_value(bin_name: str, value, return_type, inverted=False, ctx: list=None):
    """Creates a map_remove_by_value operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`

    The operation removes key value pairs whose value matches the specified value.

    Args:
        bin_name (str): The name of the bin containing the map.
        value: Entries with a value matching this argument will be removed from the map.
        return_type (int): Value specifying what should be returned from the operation.
            This should be one of the :ref:`map_return_types` values.
        inverted (bool): If True, entries with a value different than the specified value will be removed.
            Default: False
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_MAP_REMOVE_BY_VALUE,
        BIN_KEY: bin_name,
        VALUE_KEY: value,
        RETURN_TYPE_KEY: return_type,
        INVERTED_KEY: inverted
    }

    if ctx is not None:
        op_dict[CTX_KEY] = ctx

    return op_dict


def map_remove_by_value_list(bin_name: str, value_list, return_type, inverted=False, ctx: list=None):
    """Creates a map_remove_by_value_list operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`

    The operation removes key value pairs whose values are specified in the value_list.

    Args:
        bin_name (str): The name of the bin containing the map.
        value_list (list): Entries with a value contained in this list will be removed from the map.
        return_type (int): Value specifying what should be returned from the operation.
            This should be one of the :ref:`map_return_types` values.
        inverted (bool): If True, entries with a value contained in value_list will be kept, and all others
            will be removed and returned.
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_MAP_REMOVE_BY_VALUE_LIST,
        BIN_KEY: bin_name,
        VALUE_KEY: value_list,
        RETURN_TYPE_KEY: return_type,
        INVERTED_KEY: inverted
    }

    if ctx is not None:
        op_dict[CTX_KEY] = ctx

    return op_dict


def map_remove_by_value_range(bin_name: str, value_start, value_end, return_type, inverted=False, ctx: list=None):
    """Creates a map_remove_by_value_range operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`

    The operation removes items, with values between value_start(inclusive) and
    value_end(exclusive) from the map

    Args:
        bin_name (str): The name of the bin containing the map.
        value_start: The start of the range of values to be removed. (Inclusive)
        value_end: The end of the range of values to be removed. (Exclusive)
        return_type (int): Value specifying what should be returned from the operation.
            This should be one of the :ref:`map_return_types` values.
        inverted (bool): If True, values outside of the specified range will be removed, and
            values inside of the range will be kept. Default: False
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_MAP_REMOVE_BY_VALUE_RANGE,
        BIN_KEY: bin_name,
        VALUE_KEY: value_start,
        RANGE_KEY: value_end,
        RETURN_TYPE_KEY: return_type,
        INVERTED_KEY: inverted
    }

    if ctx is not None:
        op_dict[CTX_KEY] = ctx

    return op_dict


def map_remove_by_index(bin_name: str, index, return_type, ctx: list=None):
    """Creates a map_remove_by_index operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`

    The operation removes the entry at index from the map.

    Args:
        bin_name (str): The name of the bin containing the map.
        index (int): The index of the entry to remove.
        return_type (int): Value specifying what should be returned from the operation.
            This should be one of the :ref:`map_return_types` values.
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_MAP_REMOVE_BY_INDEX,
        BIN_KEY: bin_name,
        INDEX_KEY: index,
        RETURN_TYPE_KEY: return_type
    }

    if ctx is not None:
        op_dict[CTX_KEY] = ctx

    return op_dict


def map_remove_by_index_range(bin_name: str, index_start, remove_amt, return_type, inverted=False, ctx: list=None):
    """Creates a map_remove_by_index_range operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`

    The operation removes remove_amt entries starting at index_start from the map.

    Args:
        bin_name (str): The name of the bin containing the map.
        index_start (int): The index of the first entry to remove.
        remove_amt (int): The number of entries to remove from the map.
        return_type (int): Value specifying what should be returned from the operation.
            This should be one of the :ref:`map_return_types` values.
        inverted (bool): If true, entries in the specified index range should be kept, and all other
            entries removed. Default: False
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_MAP_REMOVE_BY_INDEX_RANGE,
        BIN_KEY: bin_name,
        INDEX_KEY: index_start,
        VALUE_KEY: remove_amt,
        RETURN_TYPE_KEY: return_type,
        INVERTED_KEY: inverted
    }

    if ctx is not None:
        op_dict[CTX_KEY] = ctx

    return op_dict


def map_remove_by_rank(bin_name: str, rank, return_type, ctx: list=None):
    """Creates a map_remove_by_rank operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`

    The operation removes the item with the specified rank from the map.

    Args:
        bin_name (str): The name of the bin containing the map.
        rank (int): The rank of the entry to remove.
        return_type (int): Value specifying what should be returned from the operation.
            This should be one of the :ref:`map_return_types` values.
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_MAP_REMOVE_BY_RANK,
        BIN_KEY: bin_name,
        INDEX_KEY: rank,
        RETURN_TYPE_KEY: return_type
    }

    if ctx is not None:
        op_dict[CTX_KEY] = ctx

    return op_dict


def map_remove_by_rank_range(bin_name: str, rank_start, remove_amt, return_type, inverted=False, ctx: list=None):
    """Creates a map_remove_by_rank_range operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`

    The operation removes `remove_amt` items beginning with the item with the specified rank from the map.

    Args:
        bin_name (str): The name of the bin containing the map.
        rank_start (int): The rank of the entry to remove.
        remove_amt (int): The number of entries to remove.
        return_type (int): Value specifying what should be returned from the operation.
            This should be one of the :ref:`map_return_types` values.
        inverted (bool): If True, items with ranks inside the specified range should be kept,
            and all other entries removed. Default: False.
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_MAP_REMOVE_BY_RANK_RANGE,
        BIN_KEY: bin_name,
        INDEX_KEY: rank_start,
        VALUE_KEY: remove_amt,
        RETURN_TYPE_KEY: return_type,
        INVERTED_KEY: inverted
    }

    if ctx is not None:
        op_dict[CTX_KEY] = ctx

    return op_dict


def map_get_by_key(bin_name: str, key, return_type, ctx: list=None):
    """Creates a map_get_by_key operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`

    The operation returns an item, specified by the key from the map stored in the specified bin.

    Args:
        bin_name (str): The name of the bin containing the map.
        key: The key of the item to be returned from the map
        return_type (int): Value specifying what should be returned from the operation.
            This should be one of the :ref:`map_return_types` values.
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_MAP_GET_BY_KEY,
        BIN_KEY: bin_name,
        KEY_KEY: key,
        RETURN_TYPE_KEY: return_type
    }

    if ctx:
        op_dict[CTX_KEY] = ctx

    return op_dict


def map_get_by_key_range(bin_name: str, key_range_start, key_range_end, return_type, inverted=False, ctx: list=None):
    """Creates a map_get_by_key_range operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`

    The operation returns items with keys between key_range_start(inclusive) and
    key_range_end(exclusive) from the map

    Args:
        bin_name (str): The name of the bin containing the map.
        key_range_start: The start of the range of keys to be returned. (Inclusive)
        key_range_end: The end of the range of keys to be returned. (Exclusive)
        return_type (int): Value specifying what should be returned from the operation.
            This should be one of the :ref:`map_return_types` values.
        inverted (bool): If True, values outside of the specified range will be returned, and
            values inside of the range will be ignored. Default: False
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_MAP_GET_BY_KEY_RANGE,
        BIN_KEY: bin_name,
        KEY_KEY: key_range_start,
        RANGE_KEY: key_range_end,
        RETURN_TYPE_KEY: return_type,
        INVERTED_KEY: inverted
    }

    if ctx:
        op_dict[CTX_KEY] = ctx

    return op_dict


def map_get_by_key_list(bin_name: str, key_list, return_type, inverted=False, ctx: list=None):
    """Creates a map_get_by_key_list operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`

    The operation returns items, specified by the keys in key_list from the map stored in the specified bin.

    Args:
        bin_name (str): The name of the bin containing the map.
        key_list (list): A list of keys to be returned from the map.
        return_type (int): Value specifying what should be returned from the operation.
            This should be one of the :ref:`map_return_types` values.
        inverted (bool): If true, keys with values not specified in the key_list will be returned,
            and those keys specified in the key_list will be ignored. Default: False
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_MAP_GET_BY_KEY_LIST,
        BIN_KEY: bin_name,
        VALUE_KEY: key_list,
        RETURN_TYPE_KEY: return_type,
        INVERTED_KEY: inverted
    }

    if ctx:
        op_dict[CTX_KEY] = ctx

    return op_dict

def map_get_by_value(bin_name: str, value, return_type, inverted=False, ctx: list=None):
    """Creates a map_get_by_value operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`

    The operation returns entries whose value matches the specified value.

    Args:
        bin_name (str): The name of the bin containing the map.
        value: Entries with a value matching this argument will be returned from the map.
        return_type (int): Value specifying what should be returned from the operation.
            This should be one of the :ref:`map_return_types` values.
        inverted (bool): If True, entries with a value different than the specified value will be returned.
            Default: False
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_MAP_GET_BY_VALUE,
        BIN_KEY: bin_name,
        VALUE_KEY: value,
        RETURN_TYPE_KEY: return_type,
        INVERTED_KEY: inverted
    }

    if ctx:
        op_dict[CTX_KEY] = ctx

    return op_dict


def map_get_by_value_range(bin_name: str, value_start, value_end, return_type, inverted=False, ctx: list=None):
    """Creates a map_get_by_value_range operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`

    The operation returns items, with values between value_start(inclusive) and
    value_end(exclusive) from the map

    Args:
        bin_name (str): The name of the bin containing the map.
        value_start: The start of the range of values to be returned. (Inclusive)
        value_end: The end of the range of values to be returned. (Exclusive)
        return_type (int): Value specifying what should be returned from the operation.
            This should be one of the :ref:`map_return_types` values.
        inverted (bool): If True, values outside of the specified range will be returned, and
            values inside of the range will be ignored. Default: False
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_MAP_GET_BY_VALUE_RANGE,
        BIN_KEY: bin_name,
        VALUE_KEY: value_start,
        RANGE_KEY: value_end,
        RETURN_TYPE_KEY: return_type,
        INVERTED_KEY: inverted
    }

    if ctx:
        op_dict[CTX_KEY] = ctx

    return op_dict


def map_get_by_value_list(bin_name: str, key_list, return_type, inverted=False, ctx: list=None):
    """Creates a map_get_by_value_list operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`

    The operation returns entries whose values are specified in the value_list.

    Args:
        bin_name (str): The name of the bin containing the map.
        value_list (list): Entries with a value contained in this list will be returned from the map.
        return_type (int): Value specifying what should be returned from the operation.
            This should be one of the :ref:`map_return_types` values.
        inverted (bool): If True, entries with a value contained in value_list will be ignored, and all others
            will be returned.
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_MAP_GET_BY_VALUE_LIST,
        BIN_KEY: bin_name,
        VALUE_KEY: key_list,
        RETURN_TYPE_KEY: return_type,
        INVERTED_KEY: inverted
    }

    if ctx:
        op_dict[CTX_KEY] = ctx

    return op_dict


def map_get_by_index(bin_name: str, index, return_type, ctx: list=None):
    """Creates a map_get_by_index operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`

    The operation returns the entry at index from the map.

    Args:
        bin_name (str): The name of the bin containing the map.
        index (int): The index of the entry to return.
        return_type (int): Value specifying what should be returned from the operation.
            This should be one of the :ref:`map_return_types` values.
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_MAP_GET_BY_INDEX,
        BIN_KEY: bin_name,
        INDEX_KEY: index,
        RETURN_TYPE_KEY: return_type
    }

    if ctx:
        op_dict[CTX_KEY] = ctx

    return op_dict


def map_get_by_index_range(bin_name: str, index_start, get_amt, return_type, inverted=False, ctx: list=None):
    """Creates a map_get_by_index_range operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`

    The operation returns get_amt entries starting at index_start from the map.

    Args:
        bin_name (str): The name of the bin containing the map.
        index_start (int): The index of the first entry to return.
        get_amt (int): The number of entries to return from the map.
        return_type (int): Value specifying what should be returned from the operation.
            This should be one of the :ref:`map_return_types` values.
        inverted (bool): If true, entries in the specified index range should be ignored, and all other
            entries returned. Default: False
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_MAP_GET_BY_INDEX_RANGE,
        BIN_KEY: bin_name,
        INDEX_KEY: index_start,
        VALUE_KEY: get_amt,
        RETURN_TYPE_KEY: return_type,
        INVERTED_KEY: inverted
    }

    if ctx:
        op_dict[CTX_KEY] = ctx

    return op_dict


def map_get_by_rank(bin_name: str, rank, return_type, ctx: list=None):
    """Creates a map_get_by_rank operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`

    The operation returns the item with the specified rank from the map.

    Args:
        bin_name (str): The name of the bin containing the map.
        rank (int): The rank of the entry to return.
        return_type (int): Value specifying what should be returned from the operation.
            This should be one of the :ref:`map_return_types` values.
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_MAP_GET_BY_RANK,
        BIN_KEY: bin_name,
        INDEX_KEY: rank,
        RETURN_TYPE_KEY: return_type       
    }

    if ctx:
        op_dict[CTX_KEY] = ctx

    return op_dict


def map_get_by_rank_range(bin_name: str, rank_start, get_amt, return_type, inverted=False, ctx: list=None):
    """Creates a map_get_by_rank_range operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`

    The operation returns item within the specified rank range from the map.

    Args:
        bin_name (str): The name of the bin containing the map.
        rank_start (int): The start of the rank of the entries to return.
        get_amt (int): The number of entries to return.
        return_type (int): Value specifying what should be returned from the operation.
            This should be one of the :ref:`map_return_types` values.
        inverted (bool): If True, items with ranks inside the specified range should be ignored,
            and all other entries returned. Default: False.
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.
    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_MAP_GET_BY_RANK_RANGE,
        BIN_KEY: bin_name,
        INDEX_KEY: rank_start,
        VALUE_KEY: get_amt,
        RETURN_TYPE_KEY: return_type,
        INVERTED_KEY: inverted
    }

    if ctx:
        op_dict[CTX_KEY] = ctx

    return op_dict

def map_remove_by_value_rank_range_relative(
        bin_name: str, value, offset, return_type, count=None, inverted=False, ctx: list=None):
    """Create a map remove by value rank range relative operation

    Create map remove by value relative to rank range operation.
    Server removes and returns map items nearest to value and greater by relative rank.
    Server returns selected data specified by return_type.

    Note:
        This operation requires server version 4.3.0 or greater.

    Examples:

        Examples for map ``{0: 6, 10: 18, 6: 12, 15: 24}``
        and return type of ``aerospike.MAP_RETURN_KEY``

        ::

            (value, offset, count) = [removed keys]
            (6, 0, None) = [0, 6, 10, 15]
            (5, 0, 2) = [0, 6]
            (7, -1, 1) = [0]
            (7, -1, 3) = [0, 6, 10]

    Args:
        bin_name (str): The name of the bin containing the map.
        value: The value of the entry in the map for which to search
        offset (int): Begin removing and returning items with rank == rank(found_item) + offset
        count (int): If specified, the number of items to remove and return. If None,
            all items with rank greater than found_item are returned.
        return_type: Specifies what to return from the operation.
        inverted (bool): If True, the operation is inverted
            and items outside of the specified range are returned.
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.

    Returns:
        A dictionary usable in operate or operate_ordered.The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_MAP_REMOVE_BY_VALUE_RANK_RANGE_REL,
        BIN_KEY: bin_name,
        VALUE_KEY: value,
        RANK_KEY: offset,
        RETURN_TYPE_KEY: return_type

    }
    if count is not None:
        op_dict[COUNT_KEY] = count

    if inverted:
        op_dict[INVERTED_KEY] = True
    
    if ctx:
        op_dict[CTX_KEY] = ctx

    return op_dict


def map_get_by_value_rank_range_relative(
        bin_name: str, value, offset, return_type, count=None, inverted=False, ctx: list=None):
    """Create a map remove by value rank range relative operation

    Create list map get by value relative to rank range operation.
    Server returns map items with value nearest to value and greater
    by relative rank. Server returns selected data specified by return_type.

    Note:
        This operation requires server version 4.3.0 or greater.

    Examples:

        Examples for map ``{0: 6, 10: 18, 6: 12, 15: 24}`` and return type of ``aerospike.MAP_RETURN_KEY``

        ::

        (value, offset, count) = [returned keys]
        (6, 0, None) = [0, 6, 10, 15]
        (5, 0, 2) = [0, 6]
        (7, -1, 1) = [0]
        (7, -1, 3) = [0, 6, 10]

    Args:
        bin_name (str): The name of the bin containing the map.
        value (str): The value of the item in the list for which to search
        offset (int): Begin removing and returning items with rank == rank(fount_item) + offset
        count (int): If specified, the number of items to remove and return. If None,
            all items until end of list are returned.
        inverted (bool): If True, the operation is inverted
            and items outside of the specified range are returned.
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.

    Returns:
        A dictionary usable in operate or operate_ordered.The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_MAP_GET_BY_VALUE_RANK_RANGE_REL,
        BIN_KEY: bin_name,
        VALUE_KEY: value,
        RANK_KEY: offset,
        RETURN_TYPE_KEY: return_type

    }
    if count is not None:
        op_dict[COUNT_KEY] = count

    if inverted:
        op_dict[INVERTED_KEY] = True
    
    if ctx:
        op_dict[CTX_KEY] = ctx

    return op_dict


def map_remove_by_key_index_range_relative(
        bin_name: str, key, offset, return_type, count=None, inverted=False, ctx: list=None):
    """Create a map get by value rank range relative operation

    Create map remove by key relative to index range operation.
    Server removes and returns map items with key nearest to value and greater by relative index.
    Server returns selected data specified by return_type.

    Note:
        This operation requires server version 4.3.0 or greater.

    Examples:

        Examples for a key ordered map ``{0: 6, 6: 12, 10: 18, 15: 24}``
        and return type of ``aerospike.MAP_RETURN_KEY``

    ::

    (value, offset, count) = [removed keys]
    (5, 0, None) = [6, 10, 15]
    (5, 0, 2) = [6, 10]
    (5,-1, None) = [0, 6, 10, 15]
    (5, -1, 3) = [0, 6, 10]
    (3, 2, None) = [15]
    (3, 5, None) = []

    Args:
        bin_name (str): The name of the bin containing the list.
        key (str): The key of the item in the list for which to search
        offset (int): Begin removing and returning items with rank == rank(fount_item) + offset
        count (int): If specified, the number of items to remove and return. If None,
            all items until end of list are returned.
        inverted (bool): If True, the operation is inverted
            and items outside of the specified range are returned.
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.

    Returns:
        A dictionary usable in operate or operate_ordered.The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_MAP_REMOVE_BY_KEY_INDEX_RANGE_REL,
        BIN_KEY: bin_name,
        KEY_KEY: key,
        INDEX_KEY: offset,
        RETURN_TYPE_KEY: return_type

    }
    if count is not None:
        op_dict[COUNT_KEY] = count

    if inverted:
        op_dict[INVERTED_KEY] = True
    
    if ctx:
        op_dict[CTX_KEY] = ctx

    return op_dict


def map_get_by_key_index_range_relative(
        bin_name: str, value, offset, return_type, count=None, inverted=False, ctx: list=None):
    """Create a map get by value rank range relative operation

    Create map get by key relative to index range operation.
    Server removes and returns map items with key nearest to value and greater by relative index.
    Server returns selected data specified by return_type.

    Note:
        This operation requires server version 4.3.0 or greater.

    Examples:

        Examples for a key ordered map ``{0: 6, 6: 12, 10: 18, 15: 24}``
        and return type of ``aerospike.MAP_RETURN_KEY``

        ::

        (value, offset, count) = [returned keys]
        (5, 0, None) = [6, 10, 15]
        (5, 0, 2) = [6, 10]
        (5,-1, None) = [0, 6, 10, 15]
        (5, -1, 3) = [0, 6, 10]
        (3, 2, None) = [15]
        (3, 5, None) = []

    Args:
        bin_name (str): The name of the bin containing the list.
        value (str): The value of the item in the list for which to search
        offset (int): Begin removing and returning items with rank == rank(fount_item) + offset
        count (int): If specified, the number of items to remove and return. If None,
            all items until end of list are returned.
        inverted (bool): If True, the operation is inverted
            and items outside of the specified range are returned.
        ctx (list): An optional list of nested CDT :class:`cdt_ctx <aerospike_helpers.cdt_ctx>` context operation objects.

    Returns:
        A dictionary usable in operate or operate_ordered.The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_MAP_GET_BY_KEY_INDEX_RANGE_REL,
        BIN_KEY: bin_name,
        KEY_KEY: value,
        INDEX_KEY: offset,
        RETURN_TYPE_KEY: return_type

    }
    if count is not None:
        op_dict[COUNT_KEY] = count

    if inverted:
        op_dict[INVERTED_KEY] = True
    
    if ctx:
        op_dict[CTX_KEY] = ctx

    return op_dict
