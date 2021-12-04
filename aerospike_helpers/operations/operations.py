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
Module with helper functions to create dictionaries consumed by
the :mod:`aerospike.Client.operate` and :mod:`aerospike.Client.operate_ordered` methods for the aerospike.client class.
'''
import warnings

import aerospike



def read(bin_name):
    """Create a read operation dictionary.

    The read operation reads and returns the value in `bin_name`.

    Args:
        bin (str): the name of the bin from which to read.
    Returns:
        A dictionary to be passed to operate or operate_ordered.
    """

    return {
        "op": aerospike.OPERATOR_READ,
        "bin": bin_name,
    }


def write(bin_name, write_item):
    """Create a write operation dictionary.

    The write operation writes `write_item` into the bin specified by bin_name.

    Args:
        bin (str): The name of the bin into which `write_item` will be stored.
        write_item: The value which will be written into the bin.
    Returns:
        A dictionary to be passed to operate or operate_ordered.
    """
    return {
        "op": aerospike.OPERATOR_WRITE,
        "bin": bin_name,
        "val": write_item
    }


def delete():
    """Create a delete operation dictionary.

    The delete operation deletes a record and all associated bins.
    Requires server version >= 4.7.0.8.

    Returns:
        A dictionary to be passed to operate or operate_ordered.
    """

    return {
        "op": aerospike.OPERATOR_DELETE,
    }


def append(bin_name, append_item):
    """Create an append operation dictionary.

    The append operation appends `append_item` to the value in bin_name.

    Args:
        bin (str): The name of the bin to be used.
        append_item: The value which will be appended to the item contained in the specified bin.
    Returns:
        A dictionary to be passed to operate or operate_ordered.
    """
    return {
        "op": aerospike.OPERATOR_APPEND,
        "bin": bin_name,
        "val": append_item
    }


def prepend(bin_name, prepend_item):
    """Create a prepend operation dictionary.

    The prepend operation prepends `prepend_item` to the value in bin_name.

    Args:
        bin (str): The name of the bin to be used.
        prepend_item: The value which will be prepended to the item contained in the specified bin.
    Returns:
        A dictionary to be passed to operate or operate_ordered.
    """
    return {
        "op": aerospike.OPERATOR_PREPEND,
        "bin": bin_name,
        "val": prepend_item
    }


def increment(bin_name, amount):
    """Create an increment operation dictionary.

    The increment operation increases a value in bin_name by the specified amount,
    or creates a bin with the value of amount.

    Args:
        bin (str): The name of the bin to be incremented.
        amount: The amount by which to increment the item in the specified bin.
    Returns:
        A dictionary to be passed to operate or operate_ordered.
    """
    return {
        "op": aerospike.OPERATOR_INCR,
        "bin": bin_name,
        "val": amount
    }


def touch(ttl: int=None):
    """Create a touch operation dictionary.

    Using ttl here is deprecated. It should be set in the record metadata for the operate method.

    Args:
        ttl (int): Deprecated. The ttl that should be set for the record.
            This should be set in the metadata passed to the operate or
            operate_ordered methods.
    Returns:
        A dictionary to be passed to operate or operate_ordered.
    """
    op_dict = {"op": aerospike.OPERATOR_TOUCH}
    if ttl:
        warnings.warn(
            "TTL should be specified in the meta dictionary for operate", DeprecationWarning)
        op_dict["val"] = ttl
    return op_dict
