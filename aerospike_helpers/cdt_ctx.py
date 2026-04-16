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

.. _path_expressions_contexts:

Path Expressions Contexts
-------------------------

These :py:class:`_cdt_ctx` methods are meant to be used with path expressions:

- :py:meth:`cdt_ctx_all_children`
- :py:meth:`cdt_ctx_all_children_with_filter`
- :py:meth:`cdt_ctx_map_keys_in`
- :py:meth:`cdt_ctx_and_filter`
"""
import aerospike

# Somehow sphinx-autodoc-typehints isn't setting TYPE_CHECKING to true, so there's a
# NameError when using Any
from typing import Any

def index_type_string(index_type: int) -> str:
    """
    Converts index_type enum value to string.

    Args:
        index_type: The index_type to convert into equivalent string value.

    Returns:
        must be one of 'default', 'list', 'mapkeys', 'mapvalues'

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


def index_datatype_string(index_datatype: int) -> str:
    """
    Converts index_datatype enum value to string.

    Args:
        index_datatype: The index_datatype to convert into equivalent string value.

    Returns:
        must be one of 'numeric', 'string', 'geo2dsphere'
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


def cdt_ctx_list_index(index: int) -> _cdt_ctx:
    """
    Creates a nested cdt_ctx object to lookup an object in a list by index.

    If the index is negative, the lookup starts backwards from the end of the list.
    If it is out of bounds, a parameter error will be returned.

    Args:
        index: The index to look for in the list.
    """
    return _cdt_ctx(id=aerospike.CDT_CTX_LIST_INDEX, value=index)


def cdt_ctx_list_rank(rank: int) -> _cdt_ctx:
    """
    Creates a nested cdt_ctx object to lookup an object in a list by rank.

    If the rank is negative, the lookup starts backwards from the largest rank value.

    Args:
        rank: The rank to look for in the list.
    """
    return _cdt_ctx(id=aerospike.CDT_CTX_LIST_RANK, value=rank)


def cdt_ctx_list_value(value: Any) -> _cdt_ctx:
    """
    Creates a nested cdt_ctx object to lookup an object in a list by value.

    Args:
        value: The value to look for in the list.

    """
    return _cdt_ctx(id=aerospike.CDT_CTX_LIST_VALUE, value=value)


def cdt_ctx_list_index_create(index: int, order: int = 0, pad: bool = False) -> _cdt_ctx:
    """
    Creates a nested cdt_ctx object to create an list and insert at a given index.

    If a list already exists at the index, a new list will not be created.
    Any operations using this cdt_ctx object will be applied to the existing list.

    If a non-list element exists at the index, an :py:exc:`~aerospike.exception.InvalidRequest` will be thrown.

    Args:
        index: The index to create the list at.
        order: The :ref:`sort order <aerospike_list_order>` to create the List with.
            (default: ``aerospike.LIST_UNORDERED``)
        pad: If index is out of bounds and ``pad`` is :py:obj:`True`,
            then the list will be created at the index with :py:obj:`None` elements inserted behind it.
            ``pad`` is only compatible with unordered lists.

    """
    return _cdt_ctx(
        id=aerospike.CDT_CTX_LIST_INDEX_CREATE, value=index, extra_args={CDT_CTX_ORDER_KEY: order, CDT_CTX_PAD_KEY: pad}
    )


def cdt_ctx_map_index(index: int) -> _cdt_ctx:
    """
    The cdt_ctx object is initialized to lookup an object in a map by index.

    If the index is negative, the lookup starts backwards from the end of the map.

    If it is out of bounds, a parameter error will be returned.

    Args:
        index: The index to look for in the map.

    """
    return _cdt_ctx(id=aerospike.CDT_CTX_MAP_INDEX, value=index)


def cdt_ctx_map_rank(rank: int) -> _cdt_ctx:
    """
    The cdt_ctx object is initialized to lookup an object in a map by index.

    If the rank is negative, the lookup starts backwards from the largest rank value.

    Args:
        rank: The rank to look for in the map.

    """
    return _cdt_ctx(id=aerospike.CDT_CTX_MAP_RANK, value=rank)


def cdt_ctx_map_key(key: Any) -> _cdt_ctx:
    """
    The cdt_ctx object is initialized to lookup an object in a map by key.

    Args:
        key: The key to look for in the map.

    """
    return _cdt_ctx(id=aerospike.CDT_CTX_MAP_KEY, value=key)


def cdt_ctx_map_value(value: Any) -> _cdt_ctx:
    """
    The cdt_ctx object is initialized to lookup an object in a map by value.

    Args:
        value: The value to look for in the map.

    """
    return _cdt_ctx(id=aerospike.CDT_CTX_MAP_VALUE, value=value)


def cdt_ctx_map_key_create(key: Any, order: int = 0) -> _cdt_ctx:
    """
    Create a map with the given sort order at the given key.

    Args:
        key: The key to create the map at.
        order: The :ref:`sort order <aerospike_map_order>` to create the List with.
            (default: ``aerospike.MAP_UNORDERED``)

    """
    return _cdt_ctx(id=aerospike.CDT_CTX_MAP_KEY_CREATE, value=key, extra_args={CDT_CTX_ORDER_KEY: order})

# Path expressions

def cdt_ctx_all_children() -> _cdt_ctx:
    """
    At the current context, causes a query to return a list of all the children
    of the current item. For a map, this will recurse into the map elements.
    For a list, this will include all the children in the list.

    """
    return _cdt_ctx(id=aerospike._AS_CDT_CTX_EXP)

def cdt_ctx_all_children_with_filter(expression: "TypeExpression") -> _cdt_ctx:
    """
    All children of the current level will be selected, and then the filter expression
    is applied to each item in turn.  Items that cause the expression to evaluate to true will be added to the
    list of items returned in a query for this level.  Items that cause the expression to evaluate to false
    will be filtered out.

    Args:
        expression: Compiled expression. This expression must return a boolean.

    """
    return _cdt_ctx(id=aerospike._AS_CDT_CTX_EXP, extra_args={aerospike._CDT_CTX_FILTER_EXPR_KEY: expression})

def cdt_ctx_and_filter(expression: "TypeExpression") -> _cdt_ctx:
    """
    Add a boolean expression filter AND-combined with a previous :meth:`cdt_ctx_map_keys_in`.

    This applies the expression at the same level as the previous path context.

    Restrictions:

    Only one :meth:`cdt_ctx_and_filter` is allowed per context level. Multiple :meth:`cdt_ctx_and_filter`
    calls cannot be chained. To combine multiple conditions, use :class:`~aerospike_helpers.expressions.base.And` within
    a single :meth:`cdt_ctx_and_filter`.

    The preceding context entry must not be an expression type (i.e. :meth:`cdt_ctx_and_filter`
    cannot follow :meth:`cdt_ctx_all_children_with_filter` or :meth:`cdt_ctx_all_children`).

    :meth:`cdt_ctx_and_filter` cannot be the first entry in the context chain.

    Args:
        expression: Compiled expression. This expression must return a boolean.

    """
    return _cdt_ctx(id=aerospike._AS_CDT_CTX_AND | aerospike._AS_CDT_CTX_EXP,
                    extra_args={aerospike._CDT_CTX_FILTER_EXPR_KEY: expression})

def cdt_ctx_map_keys_in(keys: list) -> _cdt_ctx:
    """
    Restrict map context to the given list of keys, provided they exist.

    For example, if a map ``{"A": 1, "B": 2, "C": 3}`` exists, and you pass
    keys ``["A", "C", "D"]`` in as the list of keys, the result will only
    include ``{"A": 1, "C": 3}``, since element "D" does not exist in the map.

    This can be followed by :meth:`cdt_ctx_and_filter` to filter out the remaining map entries.

    This can only be used by path expressions.

    Args:
        keys: The keys to look for in the map.

    """
    return _cdt_ctx(id=aerospike._AS_CDT_CTX_MAP_KEYS_IN, value=keys)
