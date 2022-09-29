# -*- coding: utf-8 -*-
import pytest
import sys
from aerospike import exception as e
from aerospike_helpers.operations import map_operations as mop
from aerospike_helpers.operations import list_operations as lop
from aerospike import KeyOrderedDict
import aerospike_helpers.cdt_ctx as ctx

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)


class TestOrderedDictGetByValue(object):

    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        """
        Setup Method
        """
        map_policy={'map_order': aerospike.MAP_KEY_VALUE_ORDERED}

        self.list_key = ("test", "demo", 100)
        self.as_connection.put(self.list_key, {'map_list': []})

        self.map_key = ("test", "demo", 200)
        self.as_connection.put(self.map_key, {'map_of_maps': {}})

        lmap_ctx1 = ctx.cdt_ctx_list_index(0)
        lmap_ctx2 = ctx.cdt_ctx_list_index(1)
        lmap_ctx3 = ctx.cdt_ctx_list_index(2)

        mmap_ctx1 = ctx.cdt_ctx_map_key('key1')
        mmap_ctx2 = ctx.cdt_ctx_map_key('key2')
        mmap_ctx3 = ctx.cdt_ctx_map_key('key3')

        my_dict1 = {'a': 1, 'b': 2, 'c': 3}
        my_dict2 = {'d': 4, 'e': 5, 'f': 6}
        my_dict3 = {'g': 7, 'h': 8, 'i': 9}

        ops = [ 
                lop.list_append_items("map_list", [my_dict1, my_dict2, my_dict3]),
                mop.map_set_policy('map_list', map_policy, [lmap_ctx1]),
                mop.map_set_policy('map_list', map_policy, [lmap_ctx2]),
                mop.map_set_policy('map_list', map_policy, [lmap_ctx3])
            ]
        self.as_connection.operate(self.list_key, ops)

        ops = [ 
                mop.map_put_items('map_of_maps', {'key1': my_dict1, 'key2': my_dict2, 'key3': my_dict2}, map_policy=map_policy),
                mop.map_set_policy('map_of_maps', map_policy, [mmap_ctx1]),
                mop.map_set_policy('map_of_maps', map_policy, [mmap_ctx2]),
                mop.map_set_policy('map_of_maps', map_policy, [mmap_ctx3])
            ]
        self.as_connection.operate(self.map_key, ops)

        yield

        try:
            self.as_connection.remove(self.list_key)
            self.as_connection.remove(self.map_key)
        except e.AerospikeError:
            pass

    def test_list_get_by_value_ordered_dict(self):
        '''
        OrderedDicts get converted to key sorted as_maps, test if they match key ordered map bins on the server.
        '''

        element = KeyOrderedDict({'f': 6, 'e': 5, 'd': 4})

        ops = [
                lop.list_get_by_value('map_list', element, aerospike.LIST_RETURN_COUNT)
            ]
        _, _, res = self.as_connection.operate(self.list_key, ops)

        assert res == {'map_list': 1}

    def test_list_remove_by_value_ordered_dict(self):
        '''
        OrderedDicts get converted to key sorted as_maps, test if they match key ordered map bins on the server.
        '''

        element = KeyOrderedDict({'f': 6, 'e': 5, 'd': 4})

        ops = [
                lop.list_remove_by_value('map_list', element, aerospike.LIST_RETURN_COUNT)
            ]
        _, _, res = self.as_connection.operate(self.list_key, ops)

        assert res == {'map_list': 1}

    def test_map_get_by_value_ordered_dict(self):
        '''
        OrderedDicts get converted to key sorted as_maps, test if they match key ordered map bins on the server.
        '''

        element = KeyOrderedDict({'f': 6, 'e': 5, 'd': 4})

        ops = [
                mop.map_get_by_value('map_of_maps', element, aerospike.MAP_RETURN_COUNT)
            ]
        _, _, res = self.as_connection.operate(self.map_key, ops)

        assert res == {'map_of_maps': 2}