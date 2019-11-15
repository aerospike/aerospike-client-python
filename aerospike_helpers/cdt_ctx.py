'''
Helper functions to generate complex data type context (cdt_ctx) objects for use with operations on nested CDTs (list, map, etc).

Example::

    from __future__ import print_function
    import aerospike
    from aerospike import exception as ex
    from aerospike_helpers import cdt_ctx
    from aerospike_helpers.operations import map_operations
    from aerospike_helpers.operations import list_operations
    import sys

    # Configure the client.
    config = {"hosts": [("127.0.0.1", 3000)]}

    # Create a client and connect it to the cluster.
    try:
        client = aerospike.client(config).connect()
    except ex.ClientError as e:
        print("Error: {0} [{1}]".format(e.msg, e.code))
        sys.exit(1)

    key = ("test", "demo", "foo")
    nested_list = [{"name": "John", "id": 100}, {"name": "Bill", "id": 200}]
    nested_list_bin_name = "nested_list"

    # Write the record.
    try:
        client.put(key, {nested_list_bin_name: nested_list})
    except ex.RecordError as e:
        print("Error: {0} [{1}]".format(e.msg, e.code))

    # EXAMPLE 1: read a value from the map nested at list index 1.
    try:
        ctx = [cdt_ctx.cdt_ctx_list_index(1)]

        ops = [
            map_operations.map_get_by_key(
                nested_list_bin_name, "id", aerospike.MAP_RETURN_VALUE, ctx
            )
        ]

        _, _, result = client.operate(key, ops)
        print("EXAMPLE 1, id is: ", result)
    except ex.ClientError as e:
        print("Error: {0} [{1}]".format(e.msg, e.code))
        sys.exit(1)

    # EXAMPLE 2: write a new nested map at list index 2 and get the value at its 'name' key.
    # NOTE: The map is appened to the list, then the value is read using the ctx.
    try:
        new_map = {"name": "Cindy", "id": 300}

        ctx = [cdt_ctx.cdt_ctx_list_index(2)]

        ops = [
            list_operations.list_append(nested_list_bin_name, new_map),
            map_operations.map_get_by_key(
                nested_list_bin_name, "name", aerospike.MAP_RETURN_VALUE, ctx
            ),
        ]

        _, _, result = client.operate(key, ops)
        print("EXAMPLE 2, name is: ", result)
    except ex.ClientError as e:
        print("Error: {0} [{1}]".format(e.msg, e.code))
        sys.exit(1)

    # Cleanup and close the connection to the Aerospike cluster.
    client.remove(key)
    client.close()

    """
    EXPECTED OUTPUT:
    EXAMPLE 1, id is:  {'nested_list': 200}
    EXAMPLE 2, name is:  {'nested_list': 'Cindy'}
    """
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
