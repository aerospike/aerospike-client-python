"""
This module provides helper functions to produce dictionaries to be used with the
`client.operate` and `client.operate_ordered` methods of the aerospike module.
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


def list_append(bin_name, value, policy=None):
    """Creates a list append operation to be used with operate, or operate_ordered

    The list append operation instructs the aerospike server to append an item to the
    end of a list bin.

    Args:
        bin_name (str): The name of the bin to be operated on.
        value: The value to be appended to the end of the list.
        policy (dict): An optional dictionary of list write options.
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

    return op_dict


def list_append_items(bin_name, values, policy=None):
    """Creates a list append items operation to be used with operate, or operate_ordered

    The list append items operation instructs the aerospike server to append multiple items to the
    end of a list bin.

    Args:
        bin_name (str): The name of the bin to be operated on.
        values: (list): A sequence of items to be appended to the end of the list.
        policy (dict): An optional dictionary of list write options.
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

    return op_dict

def list_insert(bin_name, index, value, policy=None):
    """Creates a list insert operation to be used with operate, or operate_ordered

    The list insert operation inserts an item at index: `index` into the list contained
    in the specified bin.

    Args:
        bin_name (str): The name of the bin to be operated on.
        index (int): The index at which to insert an item. The value may be positive to use
            zero based indexing or negative to index from the end of the list.
        value: The value to be inserted into the list.
        policy (dict): An optional dictionary of list write options.
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

    return op_dict

def list_insert_items(bin_name, index, values, policy=None):
    """Creates a list insert items operation to be used with operate, or operate_ordered

    The list insert items operation inserts items at index: `index` into the list contained
    in the specified bin.

    Args:
        bin_name (str): The name of the bin to be operated on.
        index (int): The index at which to insert the items. The value may be positive to use
            zero based indexing or negative to index from the end of the list.
        values (list): The values to be inserted into the list.
        policy (dict): An optional dictionary of list write options.
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

    return op_dict

def list_increment(bin_name, index, value, policy=None):
    """Creates a list increment operation to be used with operate, or operate_ordered

    The list insert operation inserts an item at index: `index` into the list contained
    in the specified bin.

    Args:
        bin_name (str): The name of the bin to be operated on.
        index (int): The index of the list item to increment.
        value (int, float) : The value to be added to the list item.
        policy (dict): An optional dictionary of list write options.
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

    return op_dict


def list_pop(bin_name, index):
    """Creates a list pop operation to be used with operate, or operate_ordered

    The list insert operation removes and returns an item index: `index` from list contained
    in the specified bin.

    Args:
        bin_name (str): The name of the bin to be operated on.
        index (int): The index of the item to be removed.
    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    return {
        OP_KEY: aerospike.OP_LIST_POP,
        BIN_KEY: bin_name,
        INDEX_KEY: index
    }


def list_pop_range(bin_name, index, count):
    """Creates a list pop range operation to be used with operate, or operate_ordered

    The list insert range operation removes and returns `count` items
    starting from index: `index` from the list contained in the specified bin.

    Args:
        bin_name (str): The name of the bin to be operated on.
        index (int): The index of the first item to be removed.
        count (int): A positive number indicating how many items, including the first,
        to be removed and returned
    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    return {
        OP_KEY: aerospike.OP_LIST_POP_RANGE,
        BIN_KEY: bin_name,
        INDEX_KEY: index,
        VALUE_KEY: count
    }


def list_remove(bin_name, index):
    """Create list remove operation.

    The list remove operation removes an item located at `index` in the list specified by `bin_name`

    Args:
        bin_name (str): The name of the bin containing the item to be removed.
        index (int): The index at which to remove the item.
    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    return {
        OP_KEY: aerospike.OP_LIST_REMOVE,
        BIN_KEY: bin_name,
        INDEX_KEY: index
    }


def list_remove_range(bin_name, index, count):
    """Create list remove range operation.

    The list remove range operation removes `count` items starting at `index`
    in the list specified by `bin_name`

    Args:
        bin_name (str): The name of the bin containing the items to be removed.
        index (int): The index of the first item to remove.
        count (int): A positive number representing the number of items to be removed.
    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    return {
        OP_KEY: aerospike.OP_LIST_REMOVE_RANGE,
        BIN_KEY: bin_name,
        INDEX_KEY: index,
        VALUE_KEY: count
    }


def list_clear(bin_name):
    """Create list clear operation.

    The list clear operation removes all items from the list specified by `bin_name`

    Args:
        bin_name (str): The name of the bin containing the list to be cleared

    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    return {
        OP_KEY: aerospike.OP_LIST_CLEAR,
        BIN_KEY: bin_name
    }


def list_set(bin_name, index, value, policy=None):
    """Create a list set operation.

    The list set operations sets the value of the item at `index` to `value`

    Args:
        bin_name (str): The name of the bin containing the list to be operated on.
        index (int): The index of the item to be set.
        value: The value to be assigned to the list item.
        policy (dict): An optional dictionary of list write options.

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

    return op_dict


def list_get(bin_name, index):
    """Create a list get operation.

    The list get operation gets the value of the item at `index` and returns the value

    Args:
        bin_name (str): The name of the bin containing the list to fetch items from.
        index (int): The index of the item to be returned.

    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    return {
        OP_KEY: aerospike.OP_LIST_GET,
        BIN_KEY: bin_name,
        INDEX_KEY: index,
    }


def list_get_range(bin_name, index, count):
    """Create a list get range operation.

    The list get range operation gets `count` items starting `index` and returns the values.

    Args:
        bin_name (str): The name of the bin containing the list to fetch items from.
        index (int): The index of the item to be returned.
        count (int): A positive number of items to be returned.

    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    return {
        OP_KEY: aerospike.OP_LIST_GET_RANGE,
        BIN_KEY: bin_name,
        INDEX_KEY: index,
        VALUE_KEY: count
    }


def list_trim(bin_name, index, count):
    """Create a list trim operation.

    Server removes items in list bin that do not fall into range specified by index and count range.

    Args:
        bin_name (str): The name of the bin containing the list to be trimmed.
        index (int): The index of the items to be kept.
        count (int): A positive number of items to be kept.

    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    return {
        OP_KEY: aerospike.OP_LIST_TRIM,
        BIN_KEY: bin_name,
        INDEX_KEY: index,
        VALUE_KEY: count
    }


def list_size(bin_name):
    """Create a list size operation.

    Server returns the size of the list in the specified bin.

    Args:
        bin_name (str): The name of the bin containing the list.

    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    return {
        OP_KEY: aerospike.OP_LIST_SIZE,
        BIN_KEY: bin_name
    }


# Post 3.4.0 Operations. Require Server >= 3.16.0.1

def list_get_by_index(bin_name, index, return_type):
    """Create a list get index operation.

    The list get operation gets the item at `index` and returns a value
    specified by `return_type`

    Args:
        bin_name (str): The name of the bin containing the list to fetch items from.
        index (int): The index of the item to be returned.
        return_type (int): Value specifying what should be returned from the operation.
            This should be one of the aerospike.LIST_RETURN_* values

    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    return {
        OP_KEY: aerospike.OP_LIST_GET_BY_INDEX,
        BIN_KEY: bin_name,
        RETURN_TYPE_KEY: return_type,
        INDEX_KEY: index
    }


def list_get_by_index_range(bin_name, index, return_type, count=None, inverted=False):
    """Create a list get index range operation.

    The list get by index range operation gets `count` items starting at `index` and returns a value
    specified by `return_type`

    Args:
        bin_name (str): The name of the bin containing the list to fetch items from.
        index (int): The index of the first item to be returned.
        count (int): The number of list items to be selected.
        return_type (int): Value specifying what should be returned from the operation.
            This should be one of the aerospike.LIST_RETURN_* values.
        inverted (bool): Optional bool specifying whether to invert the return type.
            If set to true, all items outside of the specified range will be returned.
            Default: `False`

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

    return op_dict


def list_get_by_rank(bin_name, rank, return_type):
    """Create a list get by rank operation.

    Server selects list item identified by `rank` and returns selected data
    specified by `return_type`.

    Args:
        bin_name (str): The name of the bin containing the list to fetch a value from.
        rank (int): The rank of the item to be fetched.
        return_type (int): Value specifying what should be returned from the operation.
            This should be one of the aerospike.LIST_RETURN_* values

    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    return {
        OP_KEY: aerospike.OP_LIST_GET_BY_RANK,
        BIN_KEY: bin_name,
        RETURN_TYPE_KEY: return_type,
        RANK_KEY: rank
    }


def list_get_by_rank_range(bin_name, rank, return_type, count=None, inverted=False):
    """Create a list get by rank range operation.

    Server selects `count` items starting at the specified `rank` and returns selected data
    specified by `return_type`.

    Args:
        bin_name (str): The name of the bin containing the list to fetch items from.
        rank (int): The rank of the first items to be returned.
        count (int): A positive number indicating number of items to be returned.
        return_type (int): Value specifying what should be returned from the operation.
            This should be one of the aerospike.LIST_RETURN_* values
        inverted (bool): Optional bool specifying whether to invert the return type.
            If set to true, all items outside of the specified rank range will be returned.
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

    return op_dict


def list_get_by_value(bin_name, value, return_type, inverted=False):
    """Create a list get by value operation.

    Server selects list items with a value equal to `value` and returns selected data specified by
    `return_type`.

    Args:
        bin_name (str): The name of the bin containing the list to fetch items from.
        value: The server returns all items matching this value
        return_type (int): Value specifying what should be returned from the operation.
            This should be one of the aerospike.LIST_RETURN_* values
        inverted (bool): Optional bool specifying whether to invert the return type.
            If set to true, all items not equal to `value` will be selected. Default: `False`
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

    return op_dict


def list_get_by_value_list(bin_name, value_list, return_type, inverted=False):
    """Create a list get by value list operation.

    Server selects list items with a value contained in `value_list` and returns selected data
    specified by `return_type`.

    Args:
        bin_name (str): The name of the bin containing the list to fetch items from.
        value_list (list): Return items from the list matching an item in this list.
        return_type (int): Value specifying what should be returned from the operation.
            This should be one of the aerospike.LIST_RETURN_* values
        inverted (bool): Optional bool specifying whether to invert the return type.
            If set to `True`, all items not matching an entry in `value_list` will be selected.
            Default: `False`
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

    return op_dict


def list_get_by_value_range(bin_name, return_type, value_begin, value_end, inverted=False):
    """Create a list get by value list operation.

    Server selects list items with a value greater than or equal to `value_begin`
    and less than `value_end`. Server returns selected data specified by `return_type`.

    Args:
        bin_name (str): The name of the bin containing the list to fetch items from.
        value_begin: The start of the value range.
        value_end: The end of the value range.
        return_type (int): Value specifying what should be returned from the operation.
            This should be one of the aerospike.LIST_RETURN_* values
        inverted (bool): Optional bool specifying whether to invert the return type.
            If set to `True`, all items not included in the specified range will be returned.
            Default: `False`
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

    return op_dict


def list_remove_by_index(bin_name, index, return_type):
    """Create a list remove by index operation.

    The list get operation removes the value of the item at `index` and returns a value
    specified by `return_type`

    Args:
        bin_name (str): The name of the bin containing the list to remove an item from.
        index (int): The index of the item to be removed.
        return_type (int): Value specifying what should be returned from the operation.
            This should be one of the aerospike.LIST_RETURN_* values

    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    return {
        OP_KEY: aerospike.OP_LIST_REMOVE_BY_INDEX,
        BIN_KEY: bin_name,
        RETURN_TYPE_KEY: return_type,
        INDEX_KEY: index
    }


def list_remove_by_index_range(bin_name, index, return_type, count=None, inverted=False):
    """Create a list remove by index range operation.

    The list remove by index range operation removes `count` starting at `index` and returns a value
    specified by `return_type`

    Args:
        bin_name (str): The name of the bin containing the list to remove items from.
        index (int): The index of the first item to be removed.
        count (int): The number of items to be removed
        return_type (int): Value specifying what should be returned from the operation.
            This should be one of the aerospike.LIST_RETURN_* values.
        inverted (bool): Optional bool specifying whether to invert the operation.
            If set to true, all items outside of the specified range will be removed.
            Default: `False`

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

    return op_dict


def list_remove_by_rank(bin_name, rank, return_type):
    """Create a list remove by rank operation.

    Server removes a list item identified by `rank` and returns selected data
    specified by `return_type`.

    Args:
        bin_name (str): The name of the bin containing the list to fetch a value from.
        rank (int): The rank of the item to be removed.
        return_type (int): Value specifying what should be returned from the operation.
            This should be one of the aerospike.LIST_RETURN_* values

    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    return {
        OP_KEY: aerospike.OP_LIST_REMOVE_BY_RANK,
        BIN_KEY: bin_name,
        RETURN_TYPE_KEY: return_type,
        RANK_KEY: rank
    }


def list_remove_by_rank_range(bin_name, rank, return_type, count=None, inverted=False):
    """Create a list remove by rank range operation.

    Server removes `count` items starting at the specified `rank` and returns selected data
    specified by `return_type`.

    Args:
        bin_name (str): The name of the bin containing the list to fetch items from.
        rank (int): The rank of the first item to removed.
        count (int): A positive number indicating number of items to be removed.
        return_type (int): Value specifying what should be returned from the operation.
            This should be one of the aerospike.LIST_RETURN_* values
        inverted (bool): Optional bool specifying whether to invert the operation.
            If set to true, all items outside of the specified rank range will be removed.
            Default: `False`

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

    return op_dict


def list_remove_by_value(bin_name, value, return_type, inverted=False):
    """Create a list remove by value operation.

    Server removes list items with a value equal to `value` and returns selected data specified by
    `return_type`.

    Args:
        bin_name (str): The name of the bin containing the list to remove items from.
        value: The server removes all list items matching this value.
        return_type (int): Value specifying what should be returned from the operation.
            This should be one of the aerospike.LIST_RETURN_* values
        inverted (bool): Optional bool specifying whether to invert the operation.
            If set to true, all items not equal to `value` will be removed.
            Default: `False`
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

    return op_dict


def list_remove_by_value_list(bin_name, value_list, return_type, inverted=False):
    """Create a list remove by value list operation.

    Server removes list items with a value matching one contained in `value_list`
    and returns selected data specified by `return_type`.

    Args:
        bin_name (str): The name of the bin containing the list to remove items from.
        value_list (list): The server removes all list items matching one of these values.
        return_type (int): Value specifying what should be returned from the operation.
            This should be one of the aerospike.LIST_RETURN_* values
        inverted (bool): Optional bool specifying whether to invert the operation.
            If set to true, all items not equal to a value contained in
            `value_list` will be removed.
            Default: `False`
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

    return op_dict


def list_remove_by_value_range(bin_name, return_type, value_begin=None,
                               value_end=None, inverted=False):
    """Create a list remove by value range operation.

    Server removes list items with a value greater than or equal to `value_begin`
    and less than `value_end`. Server returns selected data specified by `return_type`.

    Args:
        bin_name (str): The name of the bin containing the list to fetch items from.
        value_begin: The start of the value range.
        value_end: The end of the value range.
        return_type (int): Value specifying what should be returned from the operation.
            This should be one of the aerospike.LIST_RETURN_* values
        inverted (bool): Optional bool specifying whether to invert the operation.
            If set to `True`, all items not included in the specified range will be removed.
            Default: `False`
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

    return op_dict


def list_set_order(bin_name, list_order):
    """Create a list set order operation.

    The list_set_order operation sets an order on a specified list bin.

    Args:
        bin_name (str): The name of the list bin.
        list_order: The ordering to apply to the list. Should be aerospike.LIST_ORDERED or
            aerospike.LIST_UNORDERED .
    Returns:
        A dictionary usable in operate or operate_ordered.The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    return {
        OP_KEY: aerospike.OP_LIST_SET_ORDER,
        BIN_KEY: bin_name,
        LIST_ORDER_KEY: list_order
    }


def list_sort(bin_name, sort_flags=aerospike.LIST_SORT_DEFAULT):
    """Create a list sort operation

    The list sort operation will sort the specified list bin.

    Args:
        bin_name (str): The name of the bin to sort.
        sort_flags: Optional. A list of flags bitwise or'd together.
            Available flags are currently `aerospike.LIST_SORT_DROP_DUPLICATES`
    Returns:
        A dictionary usable in operate or operate_ordered.The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    op_dict = {
        OP_KEY: aerospike.OP_LIST_SORT,
        BIN_KEY: bin_name,
        SORT_FLAGS_KEY: sort_flags
    }

    return op_dict
