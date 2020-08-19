# HyperLogLog operations
# HyperLogLog operations on HLL items nested in lists/maps are not currently supported by the server. The ctx argument in HLL operations is always set to None.
import aerospike


OP_KEY = "op"
BIN_KEY = "bin"
CTX_KEY = "ctx"
HLL_POLICY_KEY = "hll_policy"
INDEX_BIT_COUNT_KEY = "index_bit_count"
MH_BIT_COUNT_KEY = "mh_bit_count"
VALUE_LIST_KEY = "value_list"


def hll_add(bin_name, values, index_bit_count, policy=None, ctx=None):
    """Creates a hll_add operation to be used with operate, or operate_ordered.

    Server will add values to the HLL set. 
    If the HLL bin does not exist, it will be created with index_bit_count.
    Server

    Args:
        bin_name (str): The name of the bin to be operated on.
        values: The values to be added to the HLL set.
        index_bit_count: number of index bits. Must be bewtween 4 and 16 inclusive.
        policy (dict): An optional dictionary of :ref:`hll policy options <aerospike_hll_policies>`.
        ctx (list): (CURRENTLY UNSUPPORTED) An optional list of nested CDT context operations (:mod:`cdt_cdx <aerospike_helpers.cdt_ctx>` object) for use on nested CDTs.
    """
    op_dict = {
        OP_KEY: aerospike.OP_HLL_ADD,
        BIN_KEY: bin_name,
        VALUE_LIST_KEY: values,
        INDEX_BIT_COUNT_KEY: index_bit_count
    }

    if policy:
        op_dict[HLL_POLICY_KEY] = policy

    if ctx:
        op_dict[CTX_KEY] = ctx

    return op_dict


def hll_add_mh(bin_name, values, index_bit_count, mh_bit_count, policy=None, ctx=None):
    """Creates a hll_add_mh operation to be used with operate, or operate_ordered.

    Server will add values to the HLL set. 
    If the HLL bin does not exist, it will be created with index_bit_count.
    Server

    Args:
        bin_name (str): The name of the bin to be operated on.
        values: The values to be added to the HLL set.
        index_bit_count: number of index bits. Must be bewtween 4 and 16 inclusive.
        mh_bit_count: number of min hash bits. Must be bewtween 4 and 58 inclusive.
        policy (dict): An optional dictionary of :ref:`hll policy options <aerospike_hll_policies>`.
        ctx (list): (CURRENTLY UNSUPPORTED) An optional list of nested CDT context operations (:mod:`cdt_cdx <aerospike_helpers.cdt_ctx>` object) for use on nested CDTs.
    """
    op_dict = {
        OP_KEY: aerospike.OP_HLL_ADD_MH,
        BIN_KEY: bin_name,
        VALUE_LIST_KEY: values,
        INDEX_BIT_COUNT_KEY: index_bit_count,
        MH_BIT_COUNT_KEY: mh_bit_count
    }

    if policy:
        op_dict[HLL_POLICY_KEY] = policy

    if ctx:
        op_dict[CTX_KEY] = ctx

    return op_dict


def hll_init(bin_name, index_bit_count, policy=None, ctx=None):
    """Creates a hll_init operation to be used with operate, or operate_ordered.

    Server creates a new HLL or resets an existing HLL. 
    Server does not return a value.

    Args:
        bin_name (str): The name of the bin to be operated on.
        index_bit_count: number of index bits. Must be bewtween 4 and 16 inclusive.
        policy (dict): An optional dictionary of :ref:`hll policy options <aerospike_hll_policies>`.
        ctx (list): (CURRENTLY UNSUPPORTED) An optional list of nested CDT context operations (:mod:`cdt_cdx <aerospike_helpers.cdt_ctx>` object) for use on nested CDTs.
    """
    op_dict = {
        OP_KEY: aerospike.OP_HLL_INIT,
        BIN_KEY: bin_name,
        INDEX_BIT_COUNT_KEY: index_bit_count
    }

    if policy:
        op_dict[HLL_POLICY_KEY] = policy

    if ctx:
        op_dict[CTX_KEY] = ctx

    return op_dict


def hll_get_count(bin_name, ctx=None):
    """Creates a hll_get_count operation to be used with operate, or operate_ordered.

    Server returns estimated count of elements in the HLL bin. 

    Args:
        bin_name (str): The name of the bin to be operated on.
        ctx (list): (CURRENTLY UNSUPPORTED) An optional list of nested CDT context operations (:mod:`cdt_cdx <aerospike_helpers.cdt_ctx>` object) for use on nested CDTs.
    """
    op_dict = {
        OP_KEY: aerospike.OP_HLL_GET_COUNT,
        BIN_KEY: bin_name,
    }

    if ctx:
        op_dict[CTX_KEY] = ctx

    return op_dict


def hll_describe(bin_name, ctx=None):
    """Creates a hll_describe operation to be used with operate, or operate_ordered.

    Server returns index and minhash bit counts used to create HLL bin in a list of integers. 
    The list size is 2.

    Args:
        bin_name (str): The name of the bin to be operated on.
        ctx (list): (CURRENTLY UNSUPPORTED) An optional list of nested CDT context operations (:mod:`cdt_cdx <aerospike_helpers.cdt_ctx>` object) for use on nested CDTs.
    """
    op_dict = {
        OP_KEY: aerospike.OP_HLL_DESCRIBE,
        BIN_KEY: bin_name,
    }

    if ctx:
        op_dict[CTX_KEY] = ctx

    return op_dict


def hll_fold(bin_name, index_bit_count, ctx=None):
    """Creates a hll_fold operation to be used with operate, or operate_ordered.

    Servers folds index_bit_count to the specified value.
    This can only be applied when minhash bit count on the HLL bin is 0.
    Server does not return a value.

    Args:
        bin_name (str): The name of the bin to be operated on.
        index_bit_count: number of index bits. Must be bewtween 4 and 16 inclusive.
        ctx (list): (CURRENTLY UNSUPPORTED) An optional list of nested CDT context operations (:mod:`cdt_cdx <aerospike_helpers.cdt_ctx>` object) for use on nested CDTs.
    """
    op_dict = {
        OP_KEY: aerospike.OP_HLL_FOLD,
        BIN_KEY: bin_name,
        INDEX_BIT_COUNT_KEY: index_bit_count
    }

    if ctx:
        op_dict[CTX_KEY] = ctx

    return op_dict

def hll_get_intersect_count(bin_name, hll_list, ctx=None):
    """Creates a hll_get_intersect_count operation to be used with operate, or operate_ordered.

    Server returns estimate of elements that would be contained by the intersection of these HLL objects.

    Args:
        bin_name (str): The name of the bin to be operated on.
        hll_list (list): The HLLs to be intersected.
        ctx (list): (CURRENTLY UNSUPPORTED) An optional list of nested CDT context operations (:mod:`cdt_cdx <aerospike_helpers.cdt_ctx>` object) for use on nested CDTs.
    """
    op_dict = {
        OP_KEY: aerospike.OP_HLL_GET_INTERSECT_COUNT,
        BIN_KEY: bin_name,
        VALUE_LIST_KEY: hll_list
    }

    if ctx:
        op_dict[CTX_KEY] = ctx

    return op_dict

def hll_get_similarity(bin_name, hll_list, ctx=None):
    """Creates a hll_get_similarity operation to be used with operate, or operate_ordered.

    Server returns estimated similarity of the HLL objects.
    Server returns a float.

    Args:
        bin_name (str): The name of the bin to be operated on.
        hll_list (list): The HLLs used for similarity estimation.
        ctx (list): (CURRENTLY UNSUPPORTED) An optional list of nested CDT context operations (:mod:`cdt_cdx <aerospike_helpers.cdt_ctx>` object) for use on nested CDTs.
    """
    op_dict = {
        OP_KEY: aerospike.OP_HLL_GET_SIMILARITY,
        BIN_KEY: bin_name,
        VALUE_LIST_KEY: hll_list
    }

    if ctx:
        op_dict[CTX_KEY] = ctx

    return op_dict

def hll_get_union(bin_name, hll_list, ctx=None):
    """Creates a hll_get_union operation to be used with operate, or operate_ordered.

    Server returns an HLL object that is the union of all specified HLL objects
    in hll_list with the HLL bin.

    Args:
        bin_name (str): The name of the bin to be operated on.
        hll_list (list): The HLLs to be unioned.
        ctx (list): (CURRENTLY UNSUPPORTED) An optional list of nested CDT context operations (:mod:`cdt_cdx <aerospike_helpers.cdt_ctx>` object) for use on nested CDTs.
    """
    op_dict = {
        OP_KEY: aerospike.OP_HLL_GET_UNION,
        BIN_KEY: bin_name,
        VALUE_LIST_KEY: hll_list
    }

    if ctx:
        op_dict[CTX_KEY] = ctx

    return op_dict

def hll_get_union_count(bin_name, hll_list, ctx=None):
    """Creates a hll_get_union_count operation to be used with operate, or operate_ordered.

    Server returns the estimated count of elements that would be contained by the union of all specified HLL objects
    in the list with the HLL bin.

    Args:
        bin_name (str): The name of the bin to be operated on.
        hll_list (list): The HLLs to be unioned.
        ctx (list): (CURRENTLY UNSUPPORTED) An optional list of nested CDT context operations (:mod:`cdt_cdx <aerospike_helpers.cdt_ctx>` object) for use on nested CDTs.
    """
    op_dict = {
        OP_KEY: aerospike.OP_HLL_GET_UNION_COUNT,
        BIN_KEY: bin_name,
        VALUE_LIST_KEY: hll_list
    }

    if ctx:
        op_dict[CTX_KEY] = ctx

    return op_dict

def hll_init_mh(bin_name, index_bit_count, mh_bit_count, policy=None, ctx=None):
    """Creates a hll_init_mh operation to be used with operate, or operate_ordered.

    Server creates a new HLL or resets an existing HLL. 
    Server does not return a value.

    Args:
        bin_name (str): The name of the bin to be operated on.
        index_bit_count: number of index bits. Must be bewtween 4 and 16 inclusive.
        mh_bit_count: number of min hash bits. Must be bewtween 4 and 58 inclusive.
        policy (dict): An optional dictionary of :ref:`hll policy options <aerospike_hll_policies>`.
        ctx (list): (CURRENTLY UNSUPPORTED) An optional list of nested CDT context operations (:mod:`cdt_cdx <aerospike_helpers.cdt_ctx>` object) for use on nested CDTs.
    """
    op_dict = {
        OP_KEY: aerospike.OP_HLL_INIT_MH,
        BIN_KEY: bin_name,
        INDEX_BIT_COUNT_KEY: index_bit_count,
        MH_BIT_COUNT_KEY: mh_bit_count
    }

    if policy:
        op_dict[HLL_POLICY_KEY] = policy

    if ctx:
        op_dict[CTX_KEY] = ctx

    return op_dict

def hll_refresh_count(bin_name, ctx=None):
    """Creates a hll_refresh_count operation to be used with operate, or operate_ordered.

    Server updates the cached count if it is stale.
    Server returns the count. 

    Args:
        bin_name (str): The name of the bin to be operated on.
        ctx (list): (CURRENTLY UNSUPPORTED) An optional list of nested CDT context operations (:mod:`cdt_cdx <aerospike_helpers.cdt_ctx>` object) for use on nested CDTs.
    """
    op_dict = {
        OP_KEY: aerospike.OP_HLL_REFRESH_COUNT,
        BIN_KEY: bin_name,
    }

    if ctx:
        op_dict[CTX_KEY] = ctx

    return op_dict

def hll_set_union(bin_name, hll_list, policy=None, ctx=None):
    """Creates a hll_set_union operation to be used with operate, or operate_ordered.

    Server sets the union of all specified HLL objects with the HLL bin.
    Server returns nothing.

    Args:
        bin_name (str): The name of the bin to be operated on.
        hll_list (list): The HLLs who's union will be set.
        policy (dict): An optional dictionary of :ref:`hll policy options <aerospike_hll_policies>`.
        ctx (list): (CURRENTLY UNSUPPORTED) An optional list of nested CDT context operations (:mod:`cdt_cdx <aerospike_helpers.cdt_ctx>` object) for use on nested CDTs.
    """
    op_dict = {
        OP_KEY: aerospike.OP_HLL_SET_UNION,
        BIN_KEY: bin_name,
        VALUE_LIST_KEY: hll_list
    }

    if policy:
        op_dict[HLL_POLICY_KEY] = policy

    if ctx:
        op_dict[CTX_KEY] = ctx

    return op_dict

def hll_update(bin_name, values, policy=None, ctx=None):
    """Creates a hll_update operation to be used with operate, or operate_ordered.

    This operation assumes the HLL bin already exists.
    Server adds values to HLL set.
    Server returns number of entries that caused HLL to update a register.

    Args:
        bin_name (str): The name of the bin to be operated on.
        values (list): The values to be added.
        policy (dict): An optional dictionary of :ref:`hll policy options <aerospike_hll_policies>`.
        ctx (list): (CURRENTLY UNSUPPORTED) An optional list of nested CDT context operations (:mod:`cdt_cdx <aerospike_helpers.cdt_ctx>` object) for use on nested CDTs.
    """
    op_dict = {
        OP_KEY: aerospike.OP_HLL_UPDATE,
        BIN_KEY: bin_name,
        VALUE_LIST_KEY: values
    }

    if policy:
        op_dict[HLL_POLICY_KEY] = policy

    if ctx:
        op_dict[CTX_KEY] = ctx

    return op_dict
