# HyperLogLog operations
import aerospike


OP_KEY = "op"
BIN_KEY = "bin"
CTX_KEY = "ctx"
HLL_POLICY_KEY = "hll_policy"
INDEX_BIT_COUNT_KEY = "index_bit_count"
VALUE_LIST_KEY = "value_list"


def hll_add(bin_name, values, index_bit_count, policy=None, ctx=None):
    """Creates a hll_add operation to be used with operate, or operate_ordered.

    hll_add instructs the server to add values to the HLL set. 
    If the HLL bin does not exist, it will be created with index_bit_count.
    Server

    Args:
        bin_name (str): The name of the bin to be operated on.
        values: The values to be added to the HLL set.
        index_bit_count: number of index bits. Must be bewtween 4 and 16 inclusive.
        policy (dict): An optional dictionary of :ref:`hll policy options <aerospike_hll_policies>`.
        ctx (list): An optional list of nested CDT context operations (:mod:`cdt_cdx <aerospike_helpers.cdt_ctx>` object) for use on nested CDTs.
    """
    op_dict = {
        OP_KEY: aerospike.OP_HLL_ADD,
        BIN_KEY: bin_name,
        VALUE_LIST_KEY: values,
        INDEX_BIT_COUNT_KEY: index_bit_count,
    }

    if policy:
        op_dict[HLL_POLICY_KEY] = policy

    if ctx:
        op_dict[CTX_KEY] = ctx

    return op_dict

def hll_init(bin_name, index_bit_count, policy=None, ctx=None):
    """Creates a hll_init operation to be used with operate, or operate_ordered.

    hll_init, server creates a new HLL or resets an existing HLL. 
    Server does not return a value.

    Args:
        bin_name (str): The name of the bin to be operated on.
        index_bit_count: number of index bits. Must be bewtween 4 and 16 inclusive.
        policy (dict): An optional dictionary of :ref:`hll policy options <aerospike_hll_policies>`.
        ctx (list): An optional list of nested CDT context operations (:mod:`cdt_cdx <aerospike_helpers.cdt_ctx>` object) for use on nested CDTs.
    """
    op_dict = {
        OP_KEY: aerospike.OP_HLL_INIT,
        BIN_KEY: bin_name,
        INDEX_BIT_COUNT_KEY: index_bit_count,
    }

    if policy:
        op_dict[HLL_POLICY_KEY] = policy

    if ctx:
        op_dict[CTX_KEY] = ctx

    return op_dict