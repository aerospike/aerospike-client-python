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
Helper functions to generate complex data type context (cdt_ctx) objects for use with operations on nested CDTs (list,
map, etc).

Example::

    import aerospike
    from aerospike import exception as ex
    from aerospike_helpers import cdt_ctx
    from aerospike_helpers.operations import map_operations
    from aerospike_helpers.operations import list_operations
    import sys

    # Configure the client.
    config = {"hosts": [("127.0.0.1", 3000)]}
    client = aerospike.client(config)

    key = ("test", "demo", "foo")
    listWithMaps = [
        {"name": "John", "id": 100},
        {"name": "Bill", "id": 200}
    ]
    binName = "users"

    # Write the record
    client.put(key, {binName: listWithMaps})

    # Example 1: read the id of the second person on the list
    # Get context of the second person
    ctx = [cdt_ctx.cdt_ctx_list_index(1)]
    ops = [
        map_operations.map_get_by_key(
            binName, "id", aerospike.MAP_RETURN_VALUE, ctx
        )
    ]

    _, _, result = client.operate(key, ops)
    print(result)
    # {'users': 200}

    # Example 2: add a new person and get their rating of Facebook
    cindy = {
        "name": "Cindy",
        "id": 300,
        "ratings": {
            "Facebook": 4,
            "Snapchat": 5
        }
    }

    # Context list used for read operation after adding Cindy
    # Cindy will be the third person (index 2)
    # Then go to their ratings
    ctx = [cdt_ctx.cdt_ctx_list_index(2), cdt_ctx.cdt_ctx_map_key("ratings")]
    ops = [
        list_operations.list_append(binName, cindy),
        map_operations.map_get_by_key(
            binName, "Facebook", aerospike.MAP_RETURN_VALUE, ctx
        )
    ]

    _, _, result = client.operate(key, ops)
    print(result)
    # {'users': 4}

    # Example 3: create a CDT secondary index from a base64 encoded _cdt_ctx with info command
    policy = {}

    bs_b4_cdt = client.get_cdtctx_base64(ctx_list_index)

    r = []
    r.append("sindex-create:ns=test;set=demo;indexname=test_string_list_cdt_index")
    # use index_type_string to convert enum value to string
    r.append(";indextype=%s" % (cdt_ctx.index_type_string(aerospike.INDEX_TYPE_LIST)))
    # use index_datatype_string to convert enum value to string
    r.append(";indexdata=string_list,%s" % (cdt_ctx.index_datatype_string(aerospike.INDEX_STRING)))
    r.append(";context=%s" % (bs_b4_cdt))
    req = ''.join(r)

    # print("req is ==========={}", req)
    retobj = client.info_all(req, policy=None)
    # print("res is ==========={}", res)
    client.index_remove('test', 'test_string_list_cdt_index', policy)

    # Cleanup
    client.remove(key)
    client.close()
"""
import aerospike


def index_type_string(index_type):
    """
    Converts index_type enum value to string.

    Args:
        index_type (int): The index_type to convert into equivalent string value.

    Returns:
        (string) - must be one of 'default', 'list', 'mapkeys', 'mapvalues'

    """
    if index_type == aerospike.INDEX_TYPE_DEFAULT:
        return "default"
    if index_type == aerospike.INDEX_TYPE_LIST:
        return "list"
    if index_type == aerospike.INDEX_TYPE_MAPKEYS:
        return "mapkeys"
    if index_type == aerospike.INDEX_TYPE_MAPVALUES:
        return "mapvalues"
    return "invalid"


def index_datatype_string(index_datatype):
    """
    Converts index_datatype enum value to string.

    Args:
        index_datatype (int): The index_datatype to convert into equivalent string value.

    Returns:
        (string) - must be one of must be one of 'numeric', 'string', 'geo2dsphere'
    """
    if index_datatype == aerospike.INDEX_NUMERIC:
        return "numeric"
    if index_datatype == aerospike.INDEX_STRING:
        return "string"
    if index_datatype == aerospike.INDEX_GEO2DSPHERE:
        return "geo2dsphere"
    return "invalid"


CDT_CTX_ORDER_KEY = "order_key"
CDT_CTX_PAD_KEY = "pad_key"


class _cdt_ctx:
    """
    Class used to represent a single ctx_operation.
    """

    def __init__(self, *, id=None, value=None, extra_args=None):
        self.id = id
        self.value = value
        self.extra_args = extra_args


def cdt_ctx_list_index(index):
    """
    Creates a nested cdt_ctx object to lookup an object in a list by index.

    If the index is negative, the lookup starts backwards from the end of the list.
    If it is out of bounds, a parameter error will be returned.

    Args:
        index (int): The index to look for in the list.

    Returns:
        :class:`~aerospike_helpers.cdt_ctx._cdt_ctx`
    """
    return _cdt_ctx(id=aerospike.CDT_CTX_LIST_INDEX, value=index)


def cdt_ctx_list_rank(rank):
    """
    Creates a nested cdt_ctx object to lookup an object in a list by rank.

    If the rank is negative, the lookup starts backwards from the largest rank value.

    Args:
        rank (int): The rank to look for in the list.

    Returns:
        :class:`~aerospike_helpers.cdt_ctx._cdt_ctx`
    """
    return _cdt_ctx(id=aerospike.CDT_CTX_LIST_RANK, value=rank)


def cdt_ctx_list_value(value):
    """
    Creates a nested cdt_ctx object to lookup an object in a list by value.

    Args:
        value (object): The value to look for in the list.

    Returns:
        :class:`~aerospike_helpers.cdt_ctx._cdt_ctx`
    """
    return _cdt_ctx(id=aerospike.CDT_CTX_LIST_VALUE, value=value)


def cdt_ctx_list_index_create(index: int, order: int = 0, pad: bool = False) -> _cdt_ctx:
    """
    Creates a nested cdt_ctx object to create an list and insert at a given index.

    If a list already exists at the index, a new list will not be created.
    Any operations using this cdt_ctx object will be applied to the existing list.

    If a non-list element exists at the index, an :py:exc:`~aerospike.exception.InvalidRequest` will be thrown.

    Args:
        index (int): The index to create the list at.
        order (int): The :ref:`sort order <aerospike_list_order>` to create the List with.
            (default: ``aerospike.LIST_UNORDERED``)
        pad (bool): If index is out of bounds and ``pad`` is :py:obj:`True`,
            then the list will be created at the index with :py:obj:`None` elements inserted behind it.
            ``pad`` is only compatible with unordered lists.

    Returns:
        :class:`~aerospike_helpers.cdt_ctx._cdt_ctx`
    """
    return _cdt_ctx(
        id=aerospike.CDT_CTX_LIST_INDEX_CREATE, value=index, extra_args={CDT_CTX_ORDER_KEY: order, CDT_CTX_PAD_KEY: pad}
    )


def cdt_ctx_map_index(index):
    """
    The cdt_ctx object is initialized to lookup an object in a map by index.

    If the index is negative, the lookup starts backwards from the end of the map.

    If it is out of bounds, a parameter error will be returned.

    Args:
        index (int): The index to look for in the map.

    Returns:
        :class:`~aerospike_helpers.cdt_ctx._cdt_ctx`
    """
    return _cdt_ctx(id=aerospike.CDT_CTX_MAP_INDEX, value=index)


def cdt_ctx_map_rank(rank):
    """
    The cdt_ctx object is initialized to lookup an object in a map by index.

    If the rank is negative, the lookup starts backwards from the largest rank value.

    Args:
        rank (int): The rank to look for in the map.

    Returns:
        :class:`~aerospike_helpers.cdt_ctx._cdt_ctx`
    """
    return _cdt_ctx(id=aerospike.CDT_CTX_MAP_RANK, value=rank)


def cdt_ctx_map_key(key):
    """
    The cdt_ctx object is initialized to lookup an object in a map by key.

    Args:
        key (object): The key to look for in the map.

    Returns:
        :class:`~aerospike_helpers.cdt_ctx._cdt_ctx`
    """
    return _cdt_ctx(id=aerospike.CDT_CTX_MAP_KEY, value=key)


def cdt_ctx_map_value(value):
    """
    The cdt_ctx object is initialized to lookup an object in a map by value.

    Args:
        value (object): The value to look for in the map.

    Returns:
        :class:`~aerospike_helpers.cdt_ctx._cdt_ctx`
    """
    return _cdt_ctx(id=aerospike.CDT_CTX_MAP_VALUE, value=value)


def cdt_ctx_map_key_create(key: any, order: int = 0) -> _cdt_ctx:
    """
    Create a map with the given sort order at the given key.

    Args:
        key (object): The key to create the map at.
        order (int): The :ref:`sort order <aerospike_map_order>` to create the List with.
            (default: ``aerospike.MAP_UNORDERED``)

    Returns:
        :class:`~aerospike_helpers.cdt_ctx._cdt_ctx`
    """
    return _cdt_ctx(id=aerospike.CDT_CTX_MAP_KEY_CREATE, value=key, extra_args={CDT_CTX_ORDER_KEY: order})
