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
Helper functions to create HyperLogLog operation dictionary arguments for:

* :mod:`aerospike.Client.operate` and :mod:`aerospike.Client.operate_ordered`
* Certain batch operations listed in :mod:`aerospike_helpers.batch.records`

HyperLogLog bins and operations allow for your application to form fast, reasonable approximations
of members in the union or intersection between multiple HyperLogLog bins.
HyperLogLog’s estimates are a balance between complete accuracy and efficient savings
in space and speed in dealing with extremely large datasets.

    .. note:: HyperLogLog operations require server version >= 4.9.0

    .. seealso:: `HyperLogLog (Data Type) more info. \
        <https://docs.aerospike.com/server/guide/data-types/hll#operations>`_.

Example::

    import aerospike
    from aerospike_helpers.operations import hll_operations as hll_ops
    from aerospike_helpers.operations import operations

    NUM_INDEX_BITS = 12
    NUM_MH_BITS = 24

    # Configure the client.
    config = {"hosts": [("127.0.0.1", 3000)]}
    # Create a client and connect it to the cluster.
    client = aerospike.client(config)

    # Create customer keys
    TEST_NS = "test"
    TEST_SET = "demo"
    customerNames = ["Amy", "Farnsworth", "Scruffy"]
    keys = []
    for customer in customerNames:
        keys.append((TEST_NS, TEST_SET, customer))

    itemsViewedPerCustomer = [
        # [item1, item2, ... item500]
        list("item%s" % str(i) for i in range(0, 500)), # Amy
        list("item%s" % str(i) for i in range(0, 750)), # Farnsworth
        list("item%s" % str(i) for i in range(250, 1000)), # Scruffy
    ]

    for key, itemsViewed in zip(keys, itemsViewedPerCustomer):
        customerName = key[2]
        ops = [
            operations.write("name", customerName),
            hll_ops.hll_add("viewed", itemsViewed, NUM_INDEX_BITS, NUM_MH_BITS),
        ]
        client.operate(key, ops)

    # Find out how many items viewed Amy, Farnsworth, and Scruffy have in common.
    farnsworthRecord = client.get(keys[1])
    scruffyRecord = client.get(keys[2])
    farnsworthViewedItems = farnsworthRecord[2]["viewed"]
    scruffyViewedItems = scruffyRecord[2]["viewed"]
    viewed = [farnsworthViewedItems, scruffyViewedItems]
    ops = [
        hll_ops.hll_get_intersect_count("viewed", viewed)
    ]
    # Pass in Amy's key
    _, _, res = client.operate(keys[0], ops)
    print("Estimated items viewed intersection:", res["viewed"])
    # Estimated items viewed intersection: 251
    # Actual intersection: 250

    # Find out how many unique products Amy, Farnsworth, and Scruffy have viewed.
    ops = [hll_ops.hll_get_union_count("viewed", viewed)]
    _, _, res = client.operate(keys[0], ops)

    print("Estimated items viewed union:", res["viewed"])
    # Estimated items viewed union: 1010
    # Actual union: 1000

    # Find the similarity of Amy, Farnsworth, and Scruffy's product views.
    ops = [hll_ops.hll_get_similarity("viewed", viewed)]
    _, _, res = client.operate(keys[0], ops)

    print("Estimated items viewed similarity: %f%%" % (res["viewed"] * 100))
    # Estimated items viewed similarity: 24.888393%
    # Actual similarity: 25%

"""

import aerospike


OP_KEY = "op"
BIN_KEY = "bin"
HLL_POLICY_KEY = "hll_policy"
INDEX_BIT_COUNT_KEY = "index_bit_count"
MH_BIT_COUNT_KEY = "mh_bit_count"
VALUE_LIST_KEY = "value_list"


def hll_add(bin_name: str, values, index_bit_count=None, mh_bit_count=None, policy=None):
    """Creates a hll_add operation.

    Server will add the values to the hll bin.
    If the HLL bin does not exist, it will be created with index_bit_count and/or mh_bit_count if they have been
    supplied.

    Returns a dictionary to be used with :meth:`aerospike.Client.operate` and :meth:`aerospike.Client.operate_ordered`.

    Args:
        bin_name (str): The name of the bin to be operated on.
        values: The values to be added to the HLL set.
        index_bit_count: An optional number of index bits. Must be between 4 and 16 inclusive.
        mh_bit_count: An optional number of min hash bits. Must be between 4 and 58 inclusive.
        policy (dict): An optional dictionary of :ref:`HyperLogLog policies <aerospike_hll_policies>`.
    """
    op_dict = {
        OP_KEY: aerospike.OP_HLL_ADD,
        BIN_KEY: bin_name,
        VALUE_LIST_KEY: values,
        INDEX_BIT_COUNT_KEY: -1 if index_bit_count is None else index_bit_count,
        MH_BIT_COUNT_KEY: -1 if mh_bit_count is None else mh_bit_count,
    }

    if policy:
        op_dict[HLL_POLICY_KEY] = policy

    return op_dict


def hll_describe(bin_name):
    """Creates a hll_describe operation.

    Server returns index and minhash bit counts used to create HLL bin in a list of integers.
    The list size is 2.

    Returns a dictionary to be used with :meth:`aerospike.Client.operate` and :meth:`aerospike.Client.operate_ordered`.

    Args:
        bin_name (str): The name of the bin to be operated on.
    """
    op_dict = {
        OP_KEY: aerospike.OP_HLL_DESCRIBE,
        BIN_KEY: bin_name,
    }

    return op_dict


def hll_fold(bin_name: str, index_bit_count):
    """Creates a hll_fold operation.

    Servers folds index_bit_count to the specified value.
    This can only be applied when minhash bit count on the HLL bin is 0.
    Server does not return a value.

    Returns a dictionary to be used with :meth:`aerospike.Client.operate` and :meth:`aerospike.Client.operate_ordered`.

    Args:
        bin_name (str): The name of the bin to be operated on.
        index_bit_count: number of index bits. Must be between 4 and 16 inclusive.
    """
    op_dict = {OP_KEY: aerospike.OP_HLL_FOLD, BIN_KEY: bin_name, INDEX_BIT_COUNT_KEY: index_bit_count}

    return op_dict


def hll_get_count(bin_name):
    """Creates a hll_get_count operation.

    Server returns estimated count of elements in the HLL bin.

    Returns a dictionary to be used with :meth:`aerospike.Client.operate` and :meth:`aerospike.Client.operate_ordered`.

    Args:
        bin_name (str): The name of the bin to be operated on.
    """
    op_dict = {
        OP_KEY: aerospike.OP_HLL_GET_COUNT,
        BIN_KEY: bin_name,
    }

    return op_dict


def hll_get_intersect_count(bin_name: str, hll_list):
    """Creates a hll_get_intersect_count operation.

    Server returns estimate of elements that would be contained by the intersection of these HLL objects.

    Returns a dictionary to be used with :meth:`aerospike.Client.operate` and :meth:`aerospike.Client.operate_ordered`.

    Args:
        bin_name (str): The name of the bin to be operated on.
        hll_list (list): The HLLs to be intersected.
    """
    op_dict = {OP_KEY: aerospike.OP_HLL_GET_INTERSECT_COUNT, BIN_KEY: bin_name, VALUE_LIST_KEY: hll_list}

    return op_dict


def hll_get_similarity(bin_name: str, hll_list):
    """Creates a hll_get_similarity operation.

    Server returns estimated similarity of the HLL objects.
    Server returns a float.

    Returns a dictionary to be used with :meth:`aerospike.Client.operate` and :meth:`aerospike.Client.operate_ordered`.

    Args:
        bin_name (str): The name of the bin to be operated on.
        hll_list (list): The HLLs used for similarity estimation.
    """
    op_dict = {OP_KEY: aerospike.OP_HLL_GET_SIMILARITY, BIN_KEY: bin_name, VALUE_LIST_KEY: hll_list}

    return op_dict


def hll_get_union(bin_name: str, hll_list):
    """Creates a hll_get_union operation.

    Server returns an HLL object that is the union of all specified HLL objects
    in hll_list with the HLL bin.

    Returns a dictionary to be used with :meth:`aerospike.Client.operate` and :meth:`aerospike.Client.operate_ordered`.

    Args:
        bin_name (str): The name of the bin to be operated on.
        hll_list (list): The HLLs to be unioned.
    """
    op_dict = {OP_KEY: aerospike.OP_HLL_GET_UNION, BIN_KEY: bin_name, VALUE_LIST_KEY: hll_list}

    return op_dict


def hll_get_union_count(bin_name: str, hll_list):
    """Creates a hll_get_union_count operation.

    Server returns the estimated count of elements that would be contained by the union of all specified HLL objects
    in the list with the HLL bin.

    Returns a dictionary to be used with :meth:`aerospike.Client.operate` and :meth:`aerospike.Client.operate_ordered`.

    Args:
        bin_name (str): The name of the bin to be operated on.
        hll_list (list): The HLLs to be unioned.
    """
    op_dict = {OP_KEY: aerospike.OP_HLL_GET_UNION_COUNT, BIN_KEY: bin_name, VALUE_LIST_KEY: hll_list}

    return op_dict


def hll_init(bin_name: str, index_bit_count=None, mh_bit_count=None, policy=None):
    """Creates a hll_init operation.

    Server creates a new HLL or resets an existing HLL.
    If index_bit_count and mh_bit_count are None, an existing HLL bin will be reset but retain its configuration.
    If 1 of index_bit_count or mh_bit_count are set,
    an existing HLL bin will set that config and retain its current value for the unset config.
    If the HLL bin does not exist, index_bit_count is required to create it, mh_bit_count is optional.
    Server does not return a value.

    Returns a dictionary to be used with :meth:`aerospike.Client.operate` and :meth:`aerospike.Client.operate_ordered`.

    Args:
        bin_name (str): The name of the bin to be operated on.
        index_bit_count: An optional number of index bits. Must be between 4 and 16 inclusive.
        mh_bit_count: An optional number of min hash bits. Must be between 4 and 58 inclusive.
        policy (dict): An optional dictionary of :ref:`HyperLogLog policies <aerospike_hll_policies>`.
    """
    op_dict = {
        OP_KEY: aerospike.OP_HLL_INIT,
        BIN_KEY: bin_name,
        INDEX_BIT_COUNT_KEY: -1 if index_bit_count is None else index_bit_count,
        MH_BIT_COUNT_KEY: -1 if mh_bit_count is None else mh_bit_count,
    }

    if policy:
        op_dict[HLL_POLICY_KEY] = policy

    return op_dict


def hll_refresh_count(bin_name: str):
    """Creates a hll_refresh_count operation.

    Server updates the cached count if it is stale.
    Server returns the count.

    Returns a dictionary to be used with :meth:`aerospike.Client.operate` and :meth:`aerospike.Client.operate_ordered`.
    Args:
        bin_name (str): The name of the bin to be operated on.
    """
    op_dict = {
        OP_KEY: aerospike.OP_HLL_REFRESH_COUNT,
        BIN_KEY: bin_name,
    }

    return op_dict


def hll_set_union(bin_name: str, hll_list, policy=None):
    """Creates a hll_set_union operation.

    Server sets the union of all specified HLL objects with the HLL bin.
    Server returns nothing.

    Returns a dictionary to be used with :meth:`aerospike.Client.operate` and :meth:`aerospike.Client.operate_ordered`.

    Args:
        bin_name (str): The name of the bin to be operated on.
        hll_list (list): The HLLs who's union will be set.
        policy (dict): An optional dictionary of :ref:`HyperLogLog policies <aerospike_hll_policies>`.
    """
    op_dict = {OP_KEY: aerospike.OP_HLL_SET_UNION, BIN_KEY: bin_name, VALUE_LIST_KEY: hll_list}

    if policy:
        op_dict[HLL_POLICY_KEY] = policy

    return op_dict
