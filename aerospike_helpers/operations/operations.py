'''
Module with helper functions to create dictionaries consumed by
the operate and operate_ordered methods for the aerospike.client class.
'''
import warnings

import aerospike



def read(bin_name):
    """Create a read operation dictionary

    The read operation reads and returns the value in `bin_name`

    Args:
        bin: String the name of the bin from which to read
    Returns:
        A dictionary to be passed to operate or operate_ordered
    """

    return {
        "op": aerospike.OPERATOR_READ,
        "bin": bin_name,
    }


def write(bin_name, write_item):
    """Create a read operation dictionary

    The write operation writes `write_item` into the bin specified by bin_name

    Args:
        bin (string): The name of the bin into which `write_item` will be stored.
        write_item: The value which will be written into the bin
    Returns:
        A dictionary to be passed to operate or operate_ordered
    """
    return {
        "op": aerospike.OPERATOR_WRITE,
        "bin": bin_name,
        "val": write_item
    }


def append(bin_name, append_item):
    """Create an append operation dictionary

    The append operation appends `append_item` to the value in bin_name

    Args:
        bin (string): The name of the bin to be used.
        append_item: The value which will be appended to the item contained in the specified bin.
    Returns:
        A dictionary to be passed to operate or operate_ordered
    """
    return {
        "op": aerospike.OPERATOR_APPEND,
        "bin": bin_name,
        "val": append_item
    }


def prepend(bin_name, prepend_item):
    """Create a prepend operation dictionary

    The prepend operation prepends `prepend_item` to the value in bin_name

    Args:
        bin (string): The name of the bin to be used.
        prepend_item: The value which will be prepended to the item contained in the specified bin.
    Returns:
        A dictionary to be passed to operate or operate_ordered
    """
    return {
        "op": aerospike.OPERATOR_PREPEND,
        "bin": bin_name,
        "val": prepend_item
    }


def increment(bin_name, amount):
    """Create a prepend operation dictionary

    The increment operation increases a value in bin_name by the specified amount,
    or creates a bin with the value of amount

    Args:
        bin (string): The name of the bin to be incremented.
        amount: The amount by which to increment the item in the specified bin.
    Returns:
        A dictionary to be passed to operate or operate_ordered
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
        ttl (int): Deprecated. The ttl that should be set for the record.
            This should be set in the metadata passed to the operate or
            operate_ordered methods.
        amount: The amount by which to increment the item in the specified bin.
    Returns:
        A dictionary to be passed to operate or operate_ordered
    """
    op_dict = {"op": aerospike.OPERATOR_TOUCH}
    if ttl:
        warnings.warn(
            "TTL should be specified in the meta dictionary for operate", DeprecationWarning)
        op_dict["val"] = ttl
    return op_dict
