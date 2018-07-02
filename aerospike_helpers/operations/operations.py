import warnings

import aerospike
from aerospike_helpers.operations import list_operations as list_ops
from aerospike_helpers.operations import map_operations as map_ops


def read(bin_name):
    """Create a read operation dictionary
    Args:
        bin: String the name of the bin from which to read

    Returns: A dictionary to be passed to operate or operate_ordered
    """

    return {
        "op": aerospike.OPERATOR_READ,
        "bin": bin_name,
    }


def write(bin_name, write_item):
    """Create a read operation dictionary
    Args:
        bin (string): The name of the bin into which `write_item` will be stored.
        write_item: The value which will be written into the bin
    Returns: A dictionary to be passed to operate or operate_ordered
    """
    return {
        "op": aerospike.OPERATOR_WRITE,
        "bin": bin_name,
        "val": write_item
    }


def append(bin_name, append_item):
    """Create an append operation dictionary
    Args:
        bin (string): The name of the bin to be used.
        append_item: The value which will be appended to the item contained in the specified bin.
    Returns: A dictionary to be passed to operate or operate_ordered
    """
    return {
        "op": aerospike.OPERATOR_APPEND,
        "bin": bin_name,
        "val": append_item
    }


def prepend(bin_name, prepend_item):
    """Create a prepend operation dictionary
    Args:
        bin (string): The name of the bin to be used.
        prepend_item: The value which will be prepended to the item contained in the specified bin.
    Returns: A dictionary to be passed to operate or operate_ordered
    """
    return {
        "op": aerospike.OPERATOR_PREPEND,
        "bin": bin_name,
        "val": prepend_item
    }


def increment(bin_name, amount):
    """Create a prepend operation dictionary
    Args:
        bin (string): The name of the bin to be incremented.
        amount: The amount by which to increment the item in the specified bin.
    Returns: A dictionary to be passed to operate or operate_ordered
    """
    return {
        "op": aerospike.OPERATOR_INCR,
        "bin": bin_name,
        "val": amount
    }


def touch(ttl=None):
    """Create a touch operation dictionary
        Using ttl here is deprecated. It should be set in the record metadata for the operate method
    Args:
        ttl (int): Deprecated. The ttl that should be set for the record. This should be set in the metadata passed
            to the operate or operate_ordered methods.
        amount: The amount by which to increment the item in the specified bin.
    Returns: A dictionary to be passed to operate or operate_ordered
    """
    op_dict = {"op": aerospike.OPERATOR_TOUCH}
    if ttl:
        warnings.warn(
            "TTL should be specified in the meta dictionary for operate", DeprecationWarning)
        op_dict["val"] = ttl
    return op_dict


class ASOperationsBuilder(object):
    '''
    Helper class for building a list of operations for use in #operate
    and #operate ordered
    '''
    def __init__(self):
        self.operations = []

    def __iter__(self):
        '''
        Iterator, this lets somebody use list(operations)
        '''
        for op in self.operations:
            yield op

    def read(self, bin_name):
        self.operations.append(read(bin_name))
        return self

    def write(self, bin_name, write_item):
        self.operations.append(write(bin_name, write_item))
        return self

    def append(self, bin_name, append_item):
        self.operations.append(bin_name, append_item)
        return self

    def prepend(self, bin_name, prepend_item):
        self.operations.append(prepend(bin_name, prepend_item))
        return self

    def increment(self, bin_name, amount):
        self.operations.append(increment(bin_name, amount))
        return self

    def touch(self, ttl):
        self.operations.append(touch(ttl))
        return self

    def list_append(self, bin_name, value):
        self.operations.append(list_ops.list_append(bin_name, value))
        return self

    def list_append_items(self, bin_name, values):
        self.operations.append(list_ops.list_append_items(bin_name, values))
        return self

    def list_insert(self, bin_name, index, value):
        self.operations.append(list_ops.list_insert(bin_name, index, value))
        return self

    def list_insert_items(self, bin_name, index, values):
        self.operations.append(list_ops.list_insert_items(bin_name, index, values))
        return self

    def list_pop(self, bin_name, index):
        self.operations.append(list_ops.list_pop(bin_name, index))
        return self

    def list_pop_range(self, bin_name, range_start, range_end):
        self.operations.append(
            list_ops.list_pop_range(bin_name, range_start, range_end)
        )
        return self

    def list_remove(self, bin_name, index):
        self.operations.append(
            list_ops.list_remove(bin_name, index)
        )
        return self

    def list_remove_range(self, bin_name, range_start, range_end):
        self.operations.append(
            list_ops.list_remove_range(bin_name, range_start, range_end)
        )

    def list_clear(self, bin_name):
        self.operations.append(list_ops.list_clear(bin_name))
        return self

    def list_set(self, bin_name, index, value):
        self.operations.append(list_ops.list_set(bin_name, index, value))
        return self

    def list_get(self, bin_name, index):
        self.operations.append(list_ops.list_get(bin_name, index))
        return self

    def list_get_range(self, bin_name, range_start, range_end):
        self.operations.append(
            list_ops.list_get_range(bin_name, range_start, range_end)
        )
        return self

    def list_trim(self, bin_name, range_start, range_end):
        self.operations.append(
            list_ops.list_trim(bin_name, range_start, range_end)
        )
        return self

    def list_size(self, bin_name):
        self.operations.append(list_ops.list_size(bin_name))
        return self

    def map_set_policy(self, bin_name, policy):
        self.operations.append(map_ops.map_set_policy(bin_name, policy))
        return self

    def map_put(self, bin_name, key, value):
        self.operations.append(map_ops.map_put(bin_name, key, value))
        return self

    def map_put_items(self, bin_name, item_dict):
        self.operations.append(map_ops.map_put_items(bin_name, item_dict))
        return self

    def map_increment(self, bin_name, key, amount):
        self.operations.append(map_ops.map_increment(bin_name, key, amount))
        return self

    def map_decrement(self, bin_name, key, amount):
        self.operations.append(map_ops.map_decrement(bin_name, key, amount))
        return self

    def map_size(self, bin_name):
        self.operations.append(map_ops.map_size(bin_name))
        return self

    def map_clear(self, bin_name):
        self.operations.append(map_ops.map_clear(bin_name))
        return self

    def map_remove_by_key(self, bin_name, key, return_type=None):
        self.operations.append(
            map_ops.map_remove_by_key(bin_name, key, return_type=return_type)
        )
        return self

    def map_remove_key_list(self, bin_name, key_list, return_type=None):
        self.operations.append(
            map_ops.map_remove_by_key_list(bin_name, key_list, return_type=return_type)
        )
        return self

    def map_remove_by_key_range(self, bin_name, key_range_start, key_range_end, return_type=None):
        self.operations.append(
            map_ops.map_remove_by_key_range(bin_name, key_range_start, key_range_end, return_type=return_type))
        return self

    def map_remove_by_value(self, bin_name, value, return_type=None):
        self.operations.append(
            map_ops.map_remove_by_value(bin_name, value, return_type=return_type))
        return self

    def map_remove_by_value_list(self, bin_name, value_list, return_type=None):
        self.operations.append(
            map_ops.map_remove_by_value_list(bin_name, value_list, return_type=return_type))
        return self

    def map_remove_by_value_range(self, bin_name, value_start, value_end, return_type=None):
        self.operations.append(
            map_ops.map_remove_by_value_range(bin_name, value_start, value_end, return_type=return_type))
        return self

    def map_remove_by_index(self, bin_name, index, return_type=None):
        self.operations.append(
            map_ops.map_remove_by_index(bin_name, index, return_type=return_type))
        return self

    def map_remove_by_index_range(self, bin_name, index_start, amount, return_type=None):
        self.operations.append(
            map_ops.map_remove_by_index_range(
                bin_name, index_start, amount, return_type=return_type))
        return self

    def map_remove_by_rank(self, bin_name, rank, return_type=None):
        self.operations.append(
            map_ops.map_remove_by_rank(
                bin_name, rank, return_type=return_type))
        return self

    def map_remove_by_rank_range(self, bin_name, rank, amount, return_type=None):
        self.operations.append(
            map_ops.map_remove_by_rank_range(
                bin_name, rank, amount, return_type=return_type))
        return self

    def map_get_by_key(self, bin_name, key, return_type=None):
        self.operations.append(
            map_ops.map_get_by_key(bin_name, key, return_type=return_type))
        return self

    def map_get_key_range(self, bin_name, key_range_start, key_range_end, return_type=None):
        self.operations.append(
            map_ops.map_get_by_key_range(bin_name, key_range_start, key_range_end, return_type=return_type))
        return self

    def map_get_by_value(self, bin_name, value, return_type=None):
        self.operations.append(
            map_ops.map_get_by_value(bin_name, value, return_type=return_type))
        return self

    def map_get_by_value_range(self, bin_name, value_start, value_end, return_type=None):
        self.operations.append(
            map_ops.map_get_by_value_range(bin_name, value_start, value_end, return_type=return_type))
        return self

    def map_get_by_index(self, bin_name, index, return_type=None):
        self.operations.append(
            map_ops.map_get_by_index(bin_name, index, return_type=return_type))
        return self

    def map_get_by_index_range(self, bin_name, index_start, get_amt, return_type=None):
        self.operations.append(
            map_ops.map_get_by_index_range(bin_name, index_start, get_amt, return_type=return_type))
        return self

    def map_get_by_rank(self, bin_name, rank, return_type=None):
        self.operations.append(
            map_ops.map_get_by_rank(bin_name, rank, return_type=return_type))
        return self

    def map_get_by_rank_range(self, bin_name, rank, get_amt, return_type=None):
        self.operations.append(
            map_ops.map_get_by_rank_range(bin_name, rank, get_amt, return_type=return_type))
        return self

    def build(self):
        '''
        return a copy of the internal list
        '''
        return self.operations[:]
