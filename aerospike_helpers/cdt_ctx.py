'''
Helper functions to generate complex data type context (cdt_ctx) objects for use with operations on nested CDTs (list, map, etc).
for example:
Note We assume 'client' is a connected aerospike client.

Example::

    from aerospike_helpers import cdt_ctx
    from aerospike_helpers.operations import list_operations

    list_example = [['first', ['test', 'example']], 'test']

    ctx = [
        cdt_ctx.cdt_ctx_list_index(0),
        cdt_ctx.cdt_ctx_list_value(['test', 'example'])
    ]

    ops = [
        list_operations.list_append(nested_list_example_bin, value_to_append, list_write_policy, ctx)
    ]

    client.operate(example_key, ops)

List_example is now [['first', ['test', 'example', value_to_append]], 'test'].
List and map cdt_ctx objects can be mixed in a list to navigate large CDTs.
'''
import aerospike


class _cdt_ctx:
    """
    Class used to represent a single ctx_operation.
    """
    def __init__(self, id=None, value=None):
        self.id = id
        self.value = value


def cdt_ctx_list_index(index):
    """Creates a nested cdt_ctx object for use with list or map operations.
    
    The cdt_ctx object is initialized to lookup an object in a list by index.
    If the index is negative, the lookup starts backwards from the end of the list.
    If it is out of bounds, a parameter error will be returned.

    Args:
        index (int): The index to look for in the list.
    
    Returns:
        A cdt_ctx object, a list of these is usable with list and map operations.
    """
    return _cdt_ctx(aerospike.CDT_CTX_LIST_INDEX, index)


def cdt_ctx_list_rank(rank):
    """Creates a nested cdt_ctx object for use with list or map operations.
    
    The cdt_ctx object is initialized to lookup an object in a list by rank.
    If the rank is negative, the lookup starts backwards from the largest rank value.

    Args:
        rank (int): The rank to look for in the list.
    
    Returns:
        A cdt_ctx object, a list of these is usable with list and map operations.
    """
    return _cdt_ctx(aerospike.CDT_CTX_LIST_RANK, rank)


def cdt_ctx_list_value(value):
    """Creates a nested cdt_ctx object for use with list or map operations.
    
    The cdt_ctx object is initialized to lookup an object in a list by value.

    Args:
        value (object): The value to look for in the list.
    
    Returns:
        A cdt_ctx object, a list of these is usable with list and map operations.
    """
    return _cdt_ctx(aerospike.CDT_CTX_LIST_VALUE, value)


def cdt_ctx_map_index(index):
    """Creates a nested cdt_ctx object for use with list or map operations.
    
    The cdt_ctx object is initialized to lookup an object in a map by index.
    If the index is negative, the lookup starts backwards from the end of the map.
    If it is out of bounds, a parameter error will be returned.

    Args:
        index (int): The index to look for in the map.
    
    Returns:
        A cdt_ctx object, a list of these is usable with list and map operations.
    """
    return _cdt_ctx(aerospike.CDT_CTX_MAP_INDEX, index)


def cdt_ctx_map_rank(rank):
    """Creates a nested cdt_ctx object for use with list or map operations.
    
    The cdt_ctx object is initialized to lookup an object in a map by index.
    If the rank is negative, the lookup starts backwards from the largest rank value.

    Args:
        rank (int): The rank to look for in the map.
    
    Returns:
        A cdt_ctx object, a list of these is usable with list and map operations.
    """
    return _cdt_ctx(aerospike.CDT_CTX_MAP_RANK, rank)


def cdt_ctx_map_key(key):
    """Creates a nested cdt_ctx object for use with list or map operations.
    
    The cdt_ctx object is initialized to lookup an object in a map by key.

    Args:
        key (object): The key to look for in the map.
    
    Returns:
        A cdt_ctx object, a list of these is usable with list and map operations.
    """
    return _cdt_ctx(aerospike.CDT_CTX_MAP_KEY, key)


def cdt_ctx_map_value(value):
    """Creates a nested cdt_ctx object for use with list or map operations.
    
    The cdt_ctx object is initialized to lookup an object in a map by value.

    Args:
        value (object): The value to look for in the map.
    
    Returns:
        A cdt_ctx object, a list of these is usable with list and map operations.
    """
    return _cdt_ctx(aerospike.CDT_CTX_MAP_VALUE, value)
