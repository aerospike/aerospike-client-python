'''
Helper functions to create arguments to the operate and operate_ordered
methods of the aerospike client.
'''
import aerospike

OP_KEY = "op"
BIN_KEY = "bin"
POLICY_KEY = "policy"
VALUE_KEY = "val"
KEY_KEY = "key"
INDEX_KEY = "index"
RETURN_TYPE_KEY = "return_type"
INVERTED_KEY = "return_type"
RANGE_KEY = "range"


def map_set_policy(bin_name, policy):
    return {
        OP_KEY: aerospike.OP_MAP_SET_POLICY,
        BIN_KEY: bin_name,
        POLICY_KEY: policy
    }


def map_put(bin_name, key, value):
    return {
        OP_KEY: aerospike.OP_MAP_PUT,
        BIN_KEY: bin_name,
        KEY_KEY: key,
        VALUE_KEY: value
    }


def map_put_items(bin_name, item_dict):
    return {
        OP_KEY: aerospike.OP_MAP_PUT_ITEMS,
        BIN_KEY: bin_name,
        VALUE_KEY: item_dict
    }


def map_increment(bin_name, key, amount):
    return {
        OP_KEY: aerospike.OP_MAP_INCREMENT,
        BIN_KEY: bin_name,
        KEY_KEY: key,
        VALUE_KEY: amount
    }


def map_decrement(bin_name, key, amount):
    return {
        OP_KEY: aerospike.OP_MAP_DECREMENT,
        BIN_KEY: bin_name,
        KEY_KEY: key,
        VALUE_KEY: amount
    }


def map_size(bin_name):
    return {
        OP_KEY: aerospike.OP_MAP_SIZE,
        BIN_KEY: bin_name
    }


def map_clear(bin_name):
    return {
        OP_KEY: aerospike.OP_MAP_CLEAR,
        BIN_KEY: bin_name
    }


def map_remove_by_key(bin_name, key, return_type=None, inverted=False):
    op_dict = {
        OP_KEY: aerospike.OP_MAP_REMOVE_BY_KEY,
        BIN_KEY: bin_name,
        KEY_KEY: key
    }
    if return_type:
        op_dict[RETURN_TYPE_KEY] = return_type

    if inverted:
        op_dict[INVERTED_KEY] = True

    return op_dict


def map_remove_by_key_list(bin_name, key_list, return_type=None, inverted=False):
    op_dict = {
        OP_KEY: aerospike.OP_MAP_REMOVE_BY_KEY_LIST,
        BIN_KEY: bin_name,
        KEY_KEY: key_list
    }
    if return_type:
        op_dict[RETURN_TYPE_KEY] = return_type

    if inverted:
        op_dict[INVERTED_KEY] = True

    return op_dict


def map_remove_by_key_range(bin_name, key_range_start,
                            key_range_end, return_type=None, inverted=False):
    op_dict = {
        OP_KEY: aerospike.OP_MAP_REMOVE_BY_KEY_RANGE,
        BIN_KEY: bin_name,
        KEY_KEY: key_range_start,
        VALUE_KEY: key_range_end
    }

    if return_type:
        op_dict[RETURN_TYPE_KEY] = return_type

    if inverted:
        op_dict[INVERTED_KEY] = True

    return op_dict


def map_remove_by_value(bin_name, value, return_type=None, inverted=False):
    op_dict = {
        OP_KEY: aerospike.OP_MAP_REMOVE_BY_VALUE,
        BIN_KEY: bin_name,
        VALUE_KEY: value
    }

    if return_type:
        op_dict[RETURN_TYPE_KEY] = return_type

    if inverted:
        op_dict[INVERTED_KEY] = True

    return op_dict


def map_remove_by_value_list(bin_name, value_list, return_type=None, inverted=False):
    op_dict = {
        OP_KEY: aerospike.OP_MAP_REMOVE_BY_VALUE_LIST,
        BIN_KEY: bin_name,
        VALUE_KEY: value_list
    }
    if return_type:
        op_dict[RETURN_TYPE_KEY] = return_type

    if inverted:
        op_dict[INVERTED_KEY] = True

    return op_dict


def map_remove_by_value_range(bin_name, value_start, value_end, return_type=None, inverted=False):
    op_dict = {
        OP_KEY: aerospike.OP_MAP_REMOVE_BY_VALUE_RANGE,
        BIN_KEY: bin_name,
        VALUE_KEY: value_start,
        RANGE_KEY: value_end
    }

    if return_type:
        op_dict[RETURN_TYPE_KEY] = return_type

    if inverted:
        op_dict[INVERTED_KEY] = True

    return op_dict


def map_remove_by_index(bin_name, index, return_type=None, inverted=False):
    op_dict = {
        OP_KEY: aerospike.OP_MAP_REMOVE_BY_INDEX,
        BIN_KEY: bin_name,
        INDEX_KEY: index
    }

    if return_type:
        op_dict[RETURN_TYPE_KEY] = return_type

    if inverted:
        op_dict[INVERTED_KEY] = True

    return op_dict


def map_remove_by_index_range(bin_name, index_start, remove_amt, return_type=None, inverted=False):
    op_dict = {
        OP_KEY: aerospike.OP_MAP_REMOVE_BY_INDEX_RANGE,
        BIN_KEY: bin_name,
        INDEX_KEY: index_start,
        VALUE_KEY: remove_amt
    }
    if return_type:
        op_dict[RETURN_TYPE_KEY] = return_type

    if inverted:
        op_dict[INVERTED_KEY] = True

    return op_dict


def map_remove_by_rank(bin_name, rank, return_type=None, inverted=False):
    op_dict = {
        OP_KEY: aerospike.OP_MAP_REMOVE_BY_RANK,
        BIN_KEY: bin_name,
        INDEX_KEY: rank
    }

    if return_type:
        op_dict[RETURN_TYPE_KEY] = return_type

    if inverted:
        op_dict[INVERTED_KEY] = True

    return op_dict


def map_remove_by_rank_range(bin_name, rank_start, remove_amt, return_type=None, inverted=False):
    op_dict = {
        OP_KEY: aerospike.OP_MAP_REMOVE_BY_RANK_RANGE,
        BIN_KEY: bin_name,
        INDEX_KEY: rank_start,
        VALUE_KEY: remove_amt
    }

    if return_type:
        op_dict[RETURN_TYPE_KEY] = return_type

    if inverted:
        op_dict[INVERTED_KEY] = True

    return op_dict


def map_get_by_key(bin_name, key, return_type=None, inverted=False):
    op_dict = {
        OP_KEY: aerospike.OP_MAP_GET_BY_KEY,
        BIN_KEY: bin_name,
        KEY_KEY: key
    }

    if return_type:
        op_dict[RETURN_TYPE_KEY] = return_type

    if inverted:
        op_dict[INVERTED_KEY] = True

    return op_dict


def map_get_by_key_range(bin_name, key_range_start,
                         key_range_end, return_type=None, inverted=False):
    op_dict = {
        OP_KEY: aerospike.OP_MAP_GET_BY_KEY_RANGE,
        BIN_KEY: bin_name,
        KEY_KEY: key_range_start,
        RANGE_KEY: key_range_end
    }

    if return_type:
        op_dict[RETURN_TYPE_KEY] = return_type

    if inverted:
        op_dict[INVERTED_KEY] = True

    return op_dict


def map_get_by_key_list(bin_name, key_list, return_type=None, inverted=False):
    op_dict = {
        OP_KEY: aerospike.OP_MAP_GET_BY_KEY_LIST,
        BIN_KEY: bin_name,
        VALUE_KEY: key_list
    }
    if return_type:
        op_dict[RETURN_TYPE_KEY] = return_type

    if inverted:
        op_dict[INVERTED_KEY] = True

    return op_dict

def map_get_by_value(bin_name, value, return_type=None, inverted=False):
    op_dict = {
        OP_KEY: aerospike.OP_MAP_GET_BY_VALUE,
        BIN_KEY: bin_name,
        VALUE_KEY: value
    }

    if return_type:
        op_dict[RETURN_TYPE_KEY] = return_type

    if inverted:
        op_dict[INVERTED_KEY] = True

    return op_dict


def map_get_by_value_range(bin_name, value_start, value_end, return_type=None, inverted=False):
    op_dict = {
        OP_KEY: aerospike.OP_MAP_GET_BY_VALUE_RANGE,
        BIN_KEY: bin_name,
        VALUE_KEY: value_start,
        RANGE_KEY: value_end
    }

    if return_type:
        op_dict[RETURN_TYPE_KEY] = return_type

    if inverted:
        op_dict[INVERTED_KEY] = True

    return op_dict


def map_get_by_value_list(bin_name, key_list, return_type=None, inverted=False):
    op_dict = {
        OP_KEY: aerospike.OP_MAP_GET_BY_VALUE_LIST,
        BIN_KEY: bin_name,
        VALUE_KEY: key_list
    }
    if return_type:
        op_dict[RETURN_TYPE_KEY] = return_type

    if inverted:
        op_dict[INVERTED_KEY] = True

    return op_dict


def map_get_by_index(bin_name, index, return_type=None, inverted=False):
    op_dict = {
        OP_KEY: aerospike.OP_MAP_GET_BY_INDEX,
        BIN_KEY: bin_name,
        INDEX_KEY: index
    }

    if return_type:
        op_dict[RETURN_TYPE_KEY] = return_type

    if inverted:
        op_dict[INVERTED_KEY] = True

    return op_dict


def map_get_by_index_range(bin_name, index_start, get_amt, return_type=None, inverted=False):
    op_dict = {
        OP_KEY: aerospike.OP_MAP_GET_BY_INDEX_RANGE,
        BIN_KEY: bin_name,
        INDEX_KEY: index_start,
        VALUE_KEY: get_amt
    }

    if return_type:
        op_dict[RETURN_TYPE_KEY] = return_type

    if inverted:
        op_dict[INVERTED_KEY] = True

    return op_dict


def map_get_by_rank(bin_name, rank, return_type=None, inverted=False):
    op_dict = {
        OP_KEY: aerospike.OP_MAP_GET_BY_RANK,
        BIN_KEY: bin_name,
        INDEX_KEY: rank
    }

    if return_type:
        op_dict[RETURN_TYPE_KEY] = return_type

    if inverted:
        op_dict[INVERTED_KEY] = True

    return op_dict


def map_get_by_rank_range(bin_name, rank_start, get_amt, return_type=None, inverted=False):
    op_dict = {
        OP_KEY: aerospike.OP_MAP_GET_BY_RANK_RANGE,
        BIN_KEY: bin_name,
        INDEX_KEY: rank_start,
        VALUE_KEY: get_amt
    }

    if return_type:
        op_dict[RETURN_TYPE_KEY] = return_type

    if inverted:
        op_dict[INVERTED_KEY] = True

    return op_dict
