.. _aerospike.KeyOrderedDict:

.. currentmodule:: aerospike

================================================
KeyOrderedDict Class --- :class:`KeyOrderedDict`
================================================

:class:`KeyOrderedDict`
=======================
    The KeyOrderedDict class is a dictionary that directly maps to a key ordered map on the Aerospike server.
    This assists in matching key ordered maps through various read operations. See the example snippet below.

    .. code-block:: python

        import aerospike
        from aerospike_helpers.operations import map_operations as mop
        from aerospike_helpers.operations import list_operations as lop
        import aerospike_helpers.cdt_ctx as ctx
        from aerospike import KeyOrderedDict

        config = { 'hosts': [ ("localhost", 3000), ] }
        client = aerospike.client(config).connect()
        map_policy={'map_order': aerospike.MAP_KEY_VALUE_ORDERED}

        key = ("test", "demo", 100)
        client.put(key, {'map_list': []})

        map_ctx1 = ctx.cdt_ctx_list_index(0)
        map_ctx2 = ctx.cdt_ctx_list_index(1)
        map_ctx3 = ctx.cdt_ctx_list_index(2)

        my_dict1 = {'a': 1, 'b': 2, 'c': 3}
        my_dict2 = {'d': 4, 'e': 5, 'f': 6}
        my_dict3 = {'g': 7, 'h': 8, 'i': 9}

        ops = [ 
                lop.list_append_items('map_list', [my_dict1, my_dict2, my_dict3]),
                mop.map_set_policy('map_list', map_policy, [map_ctx1]),
                mop.map_set_policy('map_list', map_policy, [map_ctx2]),
                mop.map_set_policy('map_list', map_policy, [map_ctx3])
            ]
        client.operate(key, ops)

        _, _, res = client.get(key)
        print(res)

        element = KeyOrderedDict({'f': 6, 'e': 5, 'd': 4}) # this will match my_dict2 because it will be converted to key ordered.

        ops = [ 
                lop.list_get_by_value('map_list', element, aerospike.LIST_RETURN_COUNT)
            ]
        _, _, res = client.operate(key, ops)
        print(res)

        client.remove(key)
        client.close()

        # EXPECTED OUTPUT:
        # {'map_list': [{'a': 1, 'b': 2, 'c': 3}, {'d': 4, 'e': 5, 'f': 6}, {'g': 7, 'h': 8, 'i': 9}]}
        # {'map_list': 1}

    KeyOrderedDict inherits from :class:`dict` and has no extra functionality.
    The only difference is its mapping to a key ordered map.
