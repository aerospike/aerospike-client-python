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


def list_append(bin_name, val, policy=None):
    op_dict = {
        OP_KEY: aerospike.OP_LIST_APPEND,
        BIN_KEY: bin_name,
        VALUE_KEY: val
    }

    if policy:
        op_dict[LIST_POLICY_KEY] = policy
    
    return op_dict


def list_append_items(bin_name, values, policy=None):
    op_dict = {
        OP_KEY: aerospike.OP_LIST_APPEND_ITEMS,
        BIN_KEY: bin_name,
        VALUE_KEY: list(values)
    }

    if policy:
        op_dict[LIST_POLICY_KEY] = policy
    
    return op_dict

def list_insert(bin_name, index, value, policy=None):
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

    op_dict = {
        OP_KEY: aerospike.OP_LIST_INSERT_ITEMS,
        BIN_KEY: bin_name,
        INDEX_KEY: index,
        VALUE_KEY: list(values)
    }

    if policy:
        op_dict[LIST_POLICY_KEY] = policy
    
    return op_dict

def list_pop(bin_name, index):
    return {
        OP_KEY: aerospike.OP_LIST_POP,
        BIN_KEY: bin_name,
        INDEX_KEY: index
    }


def list_pop_range(bin_name, range_start, range_end):
    return {
        OP_KEY: aerospike.OP_LIST_POP_RANGE,
        BIN_KEY: bin_name,
        INDEX_KEY: range_start,
        VALUE_KEY: range_end
    }


def list_remove(bin_name, index):
    return {
        OP_KEY: aerospike.OP_LIST_REMOVE,
        BIN_KEY: bin_name,
        INDEX_KEY: index
    }


def list_remove_range(bin_name, range_start, range_end):
    return {
        OP_KEY: aerospike.OP_LIST_REMOVE_RANGE,
        BIN_KEY: bin_name,
        INDEX_KEY: range_start,
        VALUE_KEY: range_end
    }


def list_clear(bin_name):
    return {
        OP_KEY: aerospike.OP_LIST_CLEAR,
        BIN_KEY: bin_name
    }


def list_set(bin_name, index, value, policy=None):
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
    return {
        OP_KEY: aerospike.OP_LIST_GET,
        BIN_KEY: bin_name,
        INDEX_KEY: index,
    }


def list_get_range(bin_name, range_start, range_end):
    return {
        OP_KEY: aerospike.OP_LIST_GET_RANGE,
        BIN_KEY: bin_name,
        INDEX_KEY: range_start,
        VALUE_KEY: range_end
    }


def list_trim(bin_name, range_start, range_end):
    return {
        OP_KEY: aerospike.OP_LIST_TRIM,
        BIN_KEY: bin_name,
        INDEX_KEY: range_start,
        VALUE_KEY: range_end
    }


def list_size(bin_name):
    return {
        OP_KEY: aerospike.OP_LIST_SIZE,
        BIN_KEY: bin_name
    }


# Post 3.4.0 Operations. Require Server >= 3.16.0.1 

def list_get_by_index(bin_name, index, return_type):
    return {
        OP_KEY: aerospike.OP_LIST_GET_BY_INDEX,
        BIN_KEY: bin_name,
        RETURN_TYPE_KEY: return_type,
        INDEX_KEY: index
    }


def list_get_by_index_range(bin_name, index, return_type, count=None, inverted=False):

    op_dict = {
        OP_KEY: aerospike.OP_LIST_GET_BY_INDEX_RANGE,
        BIN_KEY: bin_name,
        RETURN_TYPE_KEY: return_type,
        INDEX_KEY: index,
    }

    if count:
        op_dict[COUNT_KEY] = count

    if inverted:
        op_dict[INVERTED_KEY] = True
    
    return op_dict


def list_get_by_rank(bin_name, rank, return_type):
    return {
        OP_KEY: aerospike.OP_LIST_GET_BY_RANK,
        BIN_KEY: bin_name,
        RETURN_TYPE_KEY: return_type,
        RANK_KEY: rank
    }


def list_get_by_rank_range(bin_name, rank, return_type, count=None, inverted=False):
    op_dict = {
        OP_KEY: aerospike.OP_LIST_GET_BY_RANK_RANGE,
        BIN_KEY: bin_name,
        RETURN_TYPE_KEY: return_type,
        RANK_KEY: rank
    }
    
    if count:
        op_dict[COUNT_KEY] = count

    if inverted:
        op_dict[INVERTED_KEY] = True
    
    return op_dict


def list_get_by_value(bin_name, value, return_type, inverted=False):
    op_dict = {
        OP_KEY: aerospike.OP_LIST_GET_BY_VALUE,
        BIN_KEY: bin_name,
        RETURN_TYPE_KEY: return_type,
        VALUE_KEY: value
    }

    if inverted:
        op_dict[INVERTED_KEY] = True
    
    return op_dict


def list_get_by_value_list(bin_name, value_list, return_type, inverted=False):
    op_dict = {
        OP_KEY: aerospike.OP_LIST_GET_BY_VALUE_LIST,
        BIN_KEY: bin_name,
        RETURN_TYPE_KEY: return_type,
        VALUE_LIST_KEY: value_list
    }

    if inverted:
        op_dict[INVERTED_KEY] = True
    
    return op_dict


def list_get_by_value_range(bin_name, return_type, value_begin, value_end, inverted=False):
    op_dict = {
        OP_KEY: aerospike.OP_LIST_GET_BY_VALUE_RANGE,
        BIN_KEY: bin_name,
        RETURN_TYPE_KEY: return_type,
    }

    if value_begin:
        op_dict[VALUE_BEGIN_KEY] = value_begin

    if value_end:
        op_dict[VALUE_END_KEY] = value_end

    if inverted:
        op_dict[INVERTED_KEY] = True
    
    return op_dict


def list_remove_by_index(bin_name, index, return_type):
    return {
        OP_KEY: aerospike.OP_LIST_REMOVE_BY_INDEX,
        BIN_KEY: bin_name,
        RETURN_TYPE_KEY: return_type,
        INDEX_KEY: index
    }


def list_remove_by_index_range(bin_name, index, return_type, count=None, inverted=False):

    op_dict = {
        OP_KEY: aerospike.OP_LIST_REMOVE_BY_INDEX_RANGE,
        BIN_KEY: bin_name,
        RETURN_TYPE_KEY: return_type,
        INDEX_KEY: index,
    }

    if count:
        op_dict[COUNT_KEY] = count

    if inverted:
        op_dict[INVERTED_KEY] = True
    
    return op_dict


def list_remove_by_rank(bin_name, rank, return_type):
    return {
        OP_KEY: aerospike.OP_LIST_REMOVE_BY_RANK,
        BIN_KEY: bin_name,
        RETURN_TYPE_KEY: return_type,
        RANK_KEY: rank
    }


def list_remove_by_rank_range(bin_name, rank, return_type, count=None, inverted=False):
    op_dict = {
        OP_KEY: aerospike.OP_LIST_REMOVE_BY_RANK_RANGE,
        BIN_KEY: bin_name,
        RETURN_TYPE_KEY: return_type,
        RANK_KEY: rank
    }
    
    if count:
        op_dict[COUNT_KEY] = count

    if inverted:
        op_dict[INVERTED_KEY] = True
    
    return op_dict


def list_remove_by_value(bin_name, value, return_type, inverted=False):
    op_dict = {
        OP_KEY: aerospike.OP_LIST_REMOVE_BY_VALUE,
        BIN_KEY: bin_name,
        RETURN_TYPE_KEY: return_type,
        VALUE_KEY: value
    }

    if inverted:
        op_dict[INVERTED_KEY] = True
    
    return op_dict


def list_remove_by_value_list(bin_name, value_list, return_type, inverted=False):
    op_dict = {
        OP_KEY: aerospike.OP_LIST_REMOVE_BY_VALUE_LIST,
        BIN_KEY: bin_name,
        RETURN_TYPE_KEY: return_type,
        VALUE_LIST_KEY: value_list
    }

    if inverted:
        op_dict[INVERTED_KEY] = True
    
    return op_dict


def list_remove_by_value_range(bin_name, return_type, value_begin=None, value_end=None, inverted=False):
    op_dict = {
        OP_KEY: aerospike.OP_LIST_REMOVE_BY_VALUE_RANGE,
        BIN_KEY: bin_name,
        RETURN_TYPE_KEY: return_type,
    }

    if value_begin:
        op_dict[VALUE_BEGIN_KEY] = value_begin

    if value_end:
        op_dict[VALUE_END_KEY] = value_end

    if inverted:
        op_dict[INVERTED_KEY] = True
    
    return op_dict


def list_set_order(bin_name, list_order):
    return {
        OP_KEY: aerospike.OP_LIST_SET_ORDER,
        BIN_KEY: bin_name,
        LIST_ORDER_KEY: list_order
    }


def list_sort(bin_name, sort_flags=None):
    op_dict = {
        OP_KEY: aerospike.OP_LIST_SORT,
        BIN_KEY: bin_name
    }

    if sort_flags:
        op_dict[SORT_FLAGS_KEY] = sort_flags
    
    return op_dict
