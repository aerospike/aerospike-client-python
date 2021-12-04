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
Helper functions to create HyperLogLog operation dictionary arguments for
the :meth:`aerospike.operate` and :meth:`aerospike.operate_ordered` methods of the aerospike client.
HyperLogLog bins and operations allow for your application to form fast, reasonable approximations
of members in the union or intersection between multiple HyperLogLog bins.
HyperLogLog’s estimates are a balance between complete accuracy and efficient savings
in space and speed in dealing with extremely large datasets.

    .. note:: HyperLogLog operations require server version >= 4.9.0

    .. seealso:: `HyperLogLog (Data Type) more info. <https://www.aerospike.com/docs/guide/hyperloglog.html>`_.

Example::

    import sys

    import aerospike
    from aerospike import exception as ex
    from aerospike_helpers.operations import hll_operations as hll_ops
    from aerospike_helpers.operations import operations


    TEST_NS = "test"
    TEST_SET = "demo"
    NUM_INDEX_BITS = 12
    NUM_MH_BITS = 24

    # Configure the client.
    config = {"hosts": [("127.0.0.1", 3000)]}

    # Create a client and connect it to the cluster.
    try:
        client = aerospike.client(config).connect()
    except ex.ClientError as e:
        print("Error: {0} [{1}]".format(e.msg, e.code))
        sys.exit(1)

    # Add HLL bins.
    customers = ["Amy", "Farnsworth", "Scruffy"]
    customer_record_keys = [
        (TEST_NS, TEST_SET, "Amy"),
        (TEST_NS, TEST_SET, "Farnsworth"),
        (TEST_NS, TEST_SET, "Scruffy"),
    ]
    items_viewed = [
        ("item%s" % str(i) for i in range(0, 500)),
        ("item%s" % str(i) for i in range(0, 750)),
        ("item%s" % str(i) for i in range(250, 1000)),
    ]

    for customer, key, items in zip(customers, customer_record_keys, items_viewed):
        ops = [
            operations.write("name", customer),
            hll_ops.hll_add("viewed", list(items), NUM_INDEX_BITS, NUM_MH_BITS),
        ]

        try:
            client.operate(key, ops)
        except ex.ClientError as e:
            print("Error: {0} [{1}]".format(e.msg, e.code))
            sys.exit(1)

    # Find out how many items viewed Amy, Farnsworth, and Scruffy have in common.
    Farnsworth_viewed = client.get(customer_record_keys[1])[2]["viewed"]
    Scruffy_viewed = client.get(customer_record_keys[2])[2]["viewed"]
    viewed = [Farnsworth_viewed, Scruffy_viewed]
    ops = [hll_ops.hll_get_intersect_count("viewed", viewed)]

    try:
        _, _, res = client.operate(customer_record_keys[0], ops)
    except ex.ClientError as e:
        print("Error: {0} [{1}]".format(e.msg, e.code))
        sys.exit(1)

    print(
        "Estimated items viewed intersection: %d."
        % res["viewed"]
    )
    print("Actual intersection: 250.\\n")

    # Find out how many unique products Amy, Farnsworth, and Scruffy have viewed.
    Farnsworth_viewed = client.get(customer_record_keys[1])[2]["viewed"]
    Scruffy_viewed = client.get(customer_record_keys[2])[2]["viewed"]
    viewed = [Farnsworth_viewed, Scruffy_viewed]
    ops = [hll_ops.hll_get_union_count("viewed", viewed)]

    try:
        _, _, res = client.operate(customer_record_keys[0], ops)
    except ex.ClientError as e:
        print("Error: {0} [{1}]".format(e.msg, e.code))
        sys.exit(1)

    print(
        "Estimated items viewed union: %d."
        % res["viewed"]
    )
    print("Actual union: 1000.\\n")

    # Find the similarity of Amy, Farnsworth, and Scruffy's product views.
    Farnsworth_viewed = client.get(customer_record_keys[1])[2]["viewed"]
    Scruffy_viewed = client.get(customer_record_keys[2])[2]["viewed"]
    viewed = [Farnsworth_viewed, Scruffy_viewed]
    ops = [hll_ops.hll_get_similarity("viewed", viewed)]

    try:
        _, _, res = client.operate(customer_record_keys[0], ops)
    except ex.ClientError as e:
        print("Error: {0} [{1}]".format(e.msg, e.code))
        sys.exit(1)

    print(
        "Estimated items viewed similarity: %f%%."
        % (res["viewed"] * 100)
    )
    print("Actual similarity: 25%.")

    """
    Expected output:
    Estimated items viewed intersection: 235.
    Actual intersection: 250.

    Estimated items viewed union: 922.
    Actual union: 1000.

    Estimated items viewed similarity: 25.488069%.
    Actual similarity: 25%.
    """

'''

import aerospike


OP_KEY = "op"
BIN_KEY = "bin"
HLL_POLICY_KEY = "hll_policy"
INDEX_BIT_COUNT_KEY = "index_bit_count"
MH_BIT_COUNT_KEY = "mh_bit_count"
VALUE_LIST_KEY = "value_list"


def hll_add(bin_name: str, values, index_bit_count=None, mh_bit_count=None, policy=None):
    """Creates a hll_add operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`.

    Server will add the values to the hll bin.
    If the HLL bin does not exist, it will be created with index_bit_count and/or mh_bit_count if they have been supplied.

    Args:
        bin_name (str): The name of the bin to be operated on.
        values: The values to be added to the HLL set.
        index_bit_count: An optional number of index bits. Must be bewtween 4 and 16 inclusive.
        mh_bit_count: An optional number of min hash bits. Must be bewtween 4 and 58 inclusive.
        policy (dict): An optional dictionary of :ref:`HyperLogLog policies <aerospike_hll_policies>`.
    """
    op_dict = {
        OP_KEY: aerospike.OP_HLL_ADD,
        BIN_KEY: bin_name,
        VALUE_LIST_KEY: values,
        INDEX_BIT_COUNT_KEY: -1 if index_bit_count is None else index_bit_count,
        MH_BIT_COUNT_KEY: -1 if mh_bit_count is None else mh_bit_count
    }

    if policy:
        op_dict[HLL_POLICY_KEY] = policy

    return op_dict


def hll_describe(bin_name):
    """Creates a hll_describe operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`.

    Server returns index and minhash bit counts used to create HLL bin in a list of integers. 
    The list size is 2.

    Args:
        bin_name (str): The name of the bin to be operated on.
    """
    op_dict = {
        OP_KEY: aerospike.OP_HLL_DESCRIBE,
        BIN_KEY: bin_name,
    }

    return op_dict


def hll_fold(bin_name: str, index_bit_count):
    """Creates a hll_fold operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`.

    Servers folds index_bit_count to the specified value.
    This can only be applied when minhash bit count on the HLL bin is 0.
    Server does not return a value.

    Args:
        bin_name (str): The name of the bin to be operated on.
        index_bit_count: number of index bits. Must be bewtween 4 and 16 inclusive.
    """
    op_dict = {
        OP_KEY: aerospike.OP_HLL_FOLD,
        BIN_KEY: bin_name,
        INDEX_BIT_COUNT_KEY: index_bit_count
    }

    return op_dict


def hll_get_count(bin_name):
    """Creates a hll_get_count operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`.

    Server returns estimated count of elements in the HLL bin. 

    Args:
        bin_name (str): The name of the bin to be operated on.
    """
    op_dict = {
        OP_KEY: aerospike.OP_HLL_GET_COUNT,
        BIN_KEY: bin_name,
    }

    return op_dict


def hll_get_intersect_count(bin_name: str, hll_list):
    """Creates a hll_get_intersect_count operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`.

    Server returns estimate of elements that would be contained by the intersection of these HLL objects.

    Args:
        bin_name (str): The name of the bin to be operated on.
        hll_list (list): The HLLs to be intersected.
    """
    op_dict = {
        OP_KEY: aerospike.OP_HLL_GET_INTERSECT_COUNT,
        BIN_KEY: bin_name,
        VALUE_LIST_KEY: hll_list
    }

    return op_dict


def hll_get_similarity(bin_name: str, hll_list):
    """Creates a hll_get_similarity operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`.

    Server returns estimated similarity of the HLL objects.
    Server returns a float.

    Args:
        bin_name (str): The name of the bin to be operated on.
        hll_list (list): The HLLs used for similarity estimation.
    """
    op_dict = {
        OP_KEY: aerospike.OP_HLL_GET_SIMILARITY,
        BIN_KEY: bin_name,
        VALUE_LIST_KEY: hll_list
    }

    return op_dict

def hll_get_union(bin_name: str, hll_list):
    """Creates a hll_get_union operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`.

    Server returns an HLL object that is the union of all specified HLL objects
    in hll_list with the HLL bin.

    Args:
        bin_name (str): The name of the bin to be operated on.
        hll_list (list): The HLLs to be unioned.
    """
    op_dict = {
        OP_KEY: aerospike.OP_HLL_GET_UNION,
        BIN_KEY: bin_name,
        VALUE_LIST_KEY: hll_list
    }

    return op_dict

def hll_get_union_count(bin_name: str, hll_list):
    """Creates a hll_get_union_count operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`.

    Server returns the estimated count of elements that would be contained by the union of all specified HLL objects
    in the list with the HLL bin.

    Args:
        bin_name (str): The name of the bin to be operated on.
        hll_list (list): The HLLs to be unioned.
    """
    op_dict = {
        OP_KEY: aerospike.OP_HLL_GET_UNION_COUNT,
        BIN_KEY: bin_name,
        VALUE_LIST_KEY: hll_list
    }

    return op_dict


def hll_init(bin_name: str, index_bit_count=None, mh_bit_count=None, policy=None):
    """Creates a hll_init operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`.

    Server creates a new HLL or resets an existing HLL.
    If index_bit_count and mh_bit_count are None, an existing HLL bin will be reset but retain its configuration.
    If 1 of index_bit_count or mh_bit_count are set,
    an existing HLL bin will set that config and retain its current value for the unset config.
    If the HLL bin does not exist, index_bit_count is required to create it, mh_bit_count is optional.
    Server does not return a value.

    Args:
        bin_name (str): The name of the bin to be operated on.
        index_bit_count: An optional number of index bits. Must be bewtween 4 and 16 inclusive.
        mh_bit_count: An optional number of min hash bits. Must be bewtween 4 and 58 inclusive.
        policy (dict): An optional dictionary of :ref:`HyperLogLog policies <aerospike_hll_policies>`.
    """
    op_dict = {
        OP_KEY: aerospike.OP_HLL_INIT,
        BIN_KEY: bin_name,
        INDEX_BIT_COUNT_KEY: -1 if index_bit_count is None else index_bit_count,
        MH_BIT_COUNT_KEY: -1 if mh_bit_count is None else mh_bit_count
    }

    if policy:
        op_dict[HLL_POLICY_KEY] = policy

    return op_dict


def hll_refresh_count(bin_name: str):
    """Creates a hll_refresh_count operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`.

    Server updates the cached count if it is stale.
    Server returns the count. 

    Args:
        bin_name (str): The name of the bin to be operated on.
    """
    op_dict = {
        OP_KEY: aerospike.OP_HLL_REFRESH_COUNT,
        BIN_KEY: bin_name,
    }

    return op_dict

def hll_set_union(bin_name: str, hll_list, policy=None):
    """Creates a hll_set_union operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`.

    Server sets the union of all specified HLL objects with the HLL bin.
    Server returns nothing.

    Args:
        bin_name (str): The name of the bin to be operated on.
        hll_list (list): The HLLs who's union will be set.
        policy (dict): An optional dictionary of :ref:`HyperLogLog policies <aerospike_hll_policies>`.
    """
    op_dict = {
        OP_KEY: aerospike.OP_HLL_SET_UNION,
        BIN_KEY: bin_name,
        VALUE_LIST_KEY: hll_list
    }

    if policy:
        op_dict[HLL_POLICY_KEY] = policy

    return op_dict
