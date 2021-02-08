# -*- coding: utf-8 -*-
import sys
import random
import unittest
from datetime import datetime

import pytest

from aerospike import exception as e
from aerospike_helpers.operations import bitwise_operations
from aerospike_helpers import cdt_ctx
from aerospike_helpers.operations import list_operations
from aerospike_helpers.operations import map_operations

list_index = "list_index"
list_rank = "list_rank"
list_value = "list_value"
map_index = "map_index"
map_key = "map_key"
map_rank = "map_rank"
map_value = "map_value"

ctx_ops = {
    list_index: cdt_ctx.cdt_ctx_list_index,
    list_rank: cdt_ctx.cdt_ctx_list_rank,
    list_value: cdt_ctx.cdt_ctx_list_value,
    map_index: cdt_ctx.cdt_ctx_map_index,
    map_key: cdt_ctx.cdt_ctx_map_key,
    map_rank: cdt_ctx.cdt_ctx_map_rank,
    map_value: cdt_ctx.cdt_ctx_map_value,
}

def add_ctx_op(ctx_type, value):
    ctx_func = ctx_ops[ctx_type]
    return ctx_func(value)

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)

class TestCTXOperations(object):

    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        """
        Setup Method
        """
        self.keys = []
        self.test_key = 'test', 'demo', 'nested_cdt_ops'
        self.nested_list = [['hi', 'friend', ['how', 'are', 
        ['you']]], ['hey', ['numbers', [1, 2, 3]], 'home boy']]
        random.seed(datetime.now())
        self.nested_list_order = [[4, 2, 5],[1, 4, 2, 3],[[2,2,2]]]
        self.nested_map = { 'first': {'greet': 'hi', 'numbers': [3, 1, 2], 3: 'hello'},
        'second': {'nested': {4,5,6,}, 'hundred': 100}, 'third': {'one': {'cat': 'dog', 'barn': {'horse': 'shoe', 'fish': 'pond'}, 'cage': ['bird']}, 'two': []}}
        self.layered_map = {'first': {'one': {1: {'g': 'layer', 'l': 'done'} } }, 'second': {'two': {2: {'g': 'layer', 'l': 'bye'} } } }
        self.num_map = {1: {1: 'v1', 2: 'v2', 3: 'v3'}, 2: {4: 'v4', 5: 'v5', 6: 'v6'}, 3: {7: 'v7', 8: 'v8', 9: 'v9', 10: {11: 'v11'}}}

        self.layered_map_bin = 'layered_map'
        self.nested_list_bin = 'nested_list'
        self.nested_list_order_bin = 'nested_order'
        self.nested_map_bin = 'nested_map'
        self.num_map_bin = 'num_map'

        ctx_sort_nested_map1 = [
            cdt_ctx.cdt_ctx_map_key('first')
        ]

        sort_map_ops = [
            map_operations.map_set_policy('nested_map', {'map_order': aerospike.MAP_KEY_ORDERED}, ctx_sort_nested_map1),
        ]

        self.as_connection.put(
            self.test_key,
            {
                self.nested_list_bin: self.nested_list,
                self.nested_list_order_bin: self.nested_list_order,
                self.nested_map_bin: self.nested_map,
                self.layered_map_bin: self.layered_map,
                self.num_map_bin: self.num_map
            }
        )
        self.keys.append(self.test_key)

        #apply map order policy
        _, _, _ = self.as_connection.operate(self.test_key, sort_map_ops)

        yield

        for key in self.keys:
            try:
                self.as_connection.remove(key)
            except e.AerospikeError:
                pass

    @pytest.mark.parametrize("ctx_types, value, list_indexes, expected", [
        ([list_index], 'toast', [1], [['hi', 'friend', ['how', 'are', 
        ['you']]], ['hey', ['numbers', [1, 2, 3]], 'home boy', 'toast']]),
        ([list_index], 'jam', [0], [['hi', 'friend', ['how', 'are', 
        ['you']], 'jam'], ['hey', ['numbers', [1, 2, 3]], 'home boy']]),
        ([list_index, list_index, list_index], 4, [1,1,1], [['hi', 'friend', ['how', 'are', 
        ['you']]], ['hey', ['numbers', [1, 2, 3, 4]], 'home boy']]),
        ([list_index, list_index], '?', [0,2], [['hi', 'friend', ['how', 'are', 
        ['you'], '?']], ['hey', ['numbers', [1, 2, 3]], 'home boy']]),
        ([list_index, list_index, list_rank], '?', [1,1,-1], [['hi', 'friend', ['how', 'are', 
        ['you']]], ['hey', ['numbers', [1, 2, 3, '?']], 'home boy']]),
        ([list_index, list_value], '?', [1, ['numbers', [1, 2, 3]]], [['hi', 'friend', ['how', 'are', 
        ['you']]], ['hey', ['numbers', [1, 2, 3], '?'], 'home boy']]),
    ])
    def test_ctx_list_append(self, ctx_types, value, list_indexes, expected):
        """
        Invoke list_append() append value to a list.
        """
        ctx = []
        for x in range(0, len(list_indexes)) :
            ctx.append(add_ctx_op(ctx_types[x], list_indexes[x]))

        ops = [
            list_operations.list_append(self.nested_list_bin, value, None, ctx)
        ]

        self.as_connection.operate(self.test_key, ops)
        _, _, bins = self.as_connection.get(self.test_key)
        assert bins[self.nested_list_bin] == expected

    @pytest.mark.parametrize("value, list_indexes, expected", [
        ('toast', [2], e.OpNotApplicable),
        ('?', 'cat', e.ParamError)
    ])
    def test_ctx_list_append_negative(self, value, list_indexes, expected):
        """
        Invoke list_append() append value to a list with expected failures.
        """
        ctx = []
        for index in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(index))

        ops = [
            list_operations.list_append(self.nested_list_bin, value, None, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("ctx_types, values, list_indexes, expected", [
        ([list_rank], ['toast'], [0], [['hi', 'friend', ['how', 'are', 
        ['you']]], ['hey', ['numbers', [1, 2, 3]], 'home boy', 'toast']]),
        ([list_index], ['jam', 'butter', 2], [0], [['hi', 'friend', ['how', 'are', 
        ['you']], 'jam', 'butter', 2], ['hey', ['numbers', [1, 2, 3]], 'home boy']]),
        ([list_rank, list_index, list_value], [4, 5, 6], [0,1,[1 ,2 ,3]], [['hi', 'friend', ['how', 'are', 
        ['you']]], ['hey', ['numbers', [1, 2, 3, 4, 5, 6]], 'home boy']]),
        ([list_rank, list_index], ['?', '!'], [1,2], [['hi', 'friend', ['how', 'are', 
        ['you'], '?', '!']], ['hey', ['numbers', [1, 2, 3]], 'home boy']])
    ])
    def test_ctx_list_append_items(self, ctx_types, values, list_indexes, expected):
        """
        Invoke list_append_items() to append values to a list.
        """
        ctx = []
        for x in range(0, len(list_indexes)) :
            ctx.append(add_ctx_op(ctx_types[x], list_indexes[x]))

        ops = [
            list_operations.list_append_items(self.nested_list_bin, values, None, ctx)
        ]

        self.as_connection.operate(self.test_key, ops)
        _, _, bins = self.as_connection.get(self.test_key)
        assert bins[self.nested_list_bin] == expected

    @pytest.mark.parametrize("values, list_indexes, expected", [
        (['toast'], [2], e.OpNotApplicable),
        (['?'], 'cat', e.ParamError),
        ('toast', [2], e.ParamError)
    ])
    def test_ctx_list_append_items_negative(self, values, list_indexes, expected):
        """
        Invoke list_append_items() to append values to a list with expected failures.
        """
        ctx = []
        for index in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(index))

        ops = [
            list_operations.list_append_items(self.nested_list_bin, values, None, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("index, value, list_indexes, expected", [
        (0, 'toast', [1], [['hi', 'friend', ['how', 'are', 
        ['you']]], ['toast', 'hey', ['numbers', [1, 2, 3]], 'home boy']]),
        (2, 'jam', [0], [['hi', 'friend', 'jam', ['how', 'are', 
        ['you']]], ['hey', ['numbers', [1, 2, 3]], 'home boy']]),
        (1, 4, [1,1,1], [['hi', 'friend', ['how', 'are', 
        ['you']]], ['hey', ['numbers', [1, 4, 2, 3]], 'home boy']]),
        (2, '?', [0,2], [['hi', 'friend', ['how', 'are', 
        '?', ['you']]], ['hey', ['numbers', [1, 2, 3]], 'home boy']])
    ])
    def test_ctx_list_insert(self, index, value, list_indexes, expected):
        """
        Invoke list_insert() to insert a value into a list.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_insert(self.nested_list_bin, index, value, None, ctx)
        ]

        self.as_connection.operate(self.test_key, ops)
        _, _, bins = self.as_connection.get(self.test_key)
        assert bins[self.nested_list_bin] == expected

    @pytest.mark.parametrize("index, value, list_indexes, expected", [
        (1, 'toast', [2], e.OpNotApplicable),
        (0, '?', 'cat', e.ParamError),
    ])
    def test_ctx_list_insert_negative(self, index, value, list_indexes, expected):
        """
        Invoke list_insert() to insert a value into a list with expected failures.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_insert(self.nested_list_bin, index, value, None, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("index, value, list_indexes, expected", [
        (0, 2, [1,1,1], [['hi', 'friend', ['how', 'are', 
        ['you']]], ['hey', ['numbers', [3, 2, 3]], 'home boy']]),
        (0, 0, [1,1,1], [['hi', 'friend', ['how', 'are', 
        ['you']]], ['hey', ['numbers', [1, 2, 3]], 'home boy']]),
        (2, 300, [1,1,1], [['hi', 'friend', ['how', 'are', 
        ['you']]], ['hey', ['numbers', [1, 2, 303]], 'home boy']]),
    ])
    def test_ctx_list_increment(self, index, value, list_indexes, expected):
        """
        Invoke list_increment() to increment a value in a list.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_increment(self.nested_list_bin, index, value, None, ctx)
        ]

        self.as_connection.operate(self.test_key, ops)
        _, _, bins = self.as_connection.get(self.test_key)
        assert bins[self.nested_list_bin] == expected

    @pytest.mark.parametrize("index, value, policy, list_indexes, expected", [
        (0, 1, None, [1], e.InvalidRequest),
        (0, 'cat', None, [1], e.InvalidRequest),
        (0, 1, None, [1,1,1,1], e.InvalidRequest),
    ])
    def test_ctx_list_increment_negative(self, index, value, policy, list_indexes, expected):
        """
        Invoke list_increment() to increment a value in a list with expected failures.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_increment(self.nested_list_bin, index, value, policy, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("index, list_indexes, expected", [
        (0, [1,1,1], [['hi', 'friend', ['how', 'are', 
        ['you']]], ['hey', ['numbers', [2, 3]], 'home boy']]),
        (2, [1], [['hi', 'friend', ['how', 'are', 
        ['you']]], ['hey', ['numbers', [1, 2, 3]]]]),
        (2, [0], [['hi', 'friend'], ['hey', ['numbers', [1, 2, 3]], 'home boy']]),
        (1, [0,2], [['hi', 'friend', ['how', 
        ['you']]], ['hey', ['numbers', [1, 2, 3]], 'home boy']]),
    ])
    def test_ctx_list_pop(self, index, list_indexes, expected):
        """
        Invoke list_pop() to pop a value off a list.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_pop(self.nested_list_bin, index, ctx)
        ]

        self.as_connection.operate(self.test_key, ops)
        _, _, bins = self.as_connection.get(self.test_key)
        assert bins[self.nested_list_bin] == expected

    @pytest.mark.parametrize("index, list_indexes, expected", [
        (3, [1,1,1], e.OpNotApplicable),
        (2, [1,1,1,1], e.InvalidRequest),
        ('cat', [0], e.ParamError),
    ])
    def test_ctx_list_pop_negative(self, index, list_indexes, expected):
        """
        Invoke list_pop() to pop a value off a list with expected failures.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_pop(self.nested_list_bin, index, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("index, list_indexes, count, expected", [
        (0, [1,1,1], 3, [['hi', 'friend', ['how', 'are', 
        ['you']]], ['hey', ['numbers', []], 'home boy']]),
        (2, [1], 1, [['hi', 'friend', ['how', 'are', 
        ['you']]], ['hey', ['numbers', [1, 2, 3]]]]),
        (1, [0], 2, [['hi'], ['hey', ['numbers', [1, 2, 3]], 'home boy']]),
        (0, [0,2], 3, [['hi', 'friend', []], ['hey', ['numbers', [1, 2, 3]], 'home boy']]),
    ])
    def test_ctx_list_pop_range(self, index, list_indexes, count, expected):
        """
        Invoke list_pop_range() to pop values off a list.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_pop_range(self.nested_list_bin, index, count, ctx)
        ]

        self.as_connection.operate(self.test_key, ops)
        _, _, bins = self.as_connection.get(self.test_key)
        assert bins[self.nested_list_bin] == expected

    @pytest.mark.parametrize("index, list_indexes, count, expected", [
        #(4, [1,1,1], 1, e.OpNotApplicable),
        (2, [1,1,1,1], 1, e.InvalidRequest),
        ('cat', [0], 1, e.ParamError),
        #(0, [1,1,1], 20, e.OpNotApplicable),

    ])
    def test_ctx_list_pop_range_negative(self, index, list_indexes, count, expected):
        """
        Invoke list_pop_range() to pop values off a list with expected failures.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_pop_range(self.nested_list_bin, index, count, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("index, list_indexes, expected", [
        (2, [1,1,1], [['hi', 'friend', ['how', 'are', 
        ['you']]], ['hey', ['numbers', [1, 2]], 'home boy']]),
        (1, [0,2], [['hi', 'friend', ['how', 
        ['you']]], ['hey', ['numbers', [1, 2, 3]], 'home boy']]),
        (0, [1], [['hi', 'friend', ['how', 'are', 
        ['you']]], [['numbers', [1, 2, 3]], 'home boy']]),
        (1, [1,1], [['hi', 'friend', ['how', 'are', 
        ['you']]], ['hey', ['numbers'], 'home boy']]),
    ])
    def test_ctx_list_remove(self, index, list_indexes, expected):
        """
        Invoke list_remove() to remove a value from a list.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_remove(self.nested_list_bin, index, ctx)
        ]

        self.as_connection.operate(self.test_key, ops)
        _, _, bins = self.as_connection.get(self.test_key)
        assert bins[self.nested_list_bin] == expected

    @pytest.mark.parametrize("index, list_indexes, expected", [
        (0, 'cat', e.ParamError),
        (40, [1], e.OpNotApplicable),
        (0, [1,1,1,1], e.InvalidRequest),
    ])
    def test_ctx_list_remove_negative(self, index, list_indexes, expected):
        """
        Invoke list_remove() to remove a value from a list with expected failures.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_remove(self.nested_list_bin, index, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("count, index, list_indexes, expected", [
        (3, 0, [1,1,1], [['hi', 'friend', ['how', 'are', 
        ['you']]], ['hey', ['numbers', []], 'home boy']]),
        (1, 1, [0,2], [['hi', 'friend', ['how', 
        ['you']]], ['hey', ['numbers', [1, 2, 3]], 'home boy']]),
        (2, 1, [1], [['hi', 'friend', ['how', 'are', 
        ['you']]], ['hey']]),
        (1, 1, [1,1], [['hi', 'friend', ['how', 'are', 
        ['you']]], ['hey', ['numbers'], 'home boy']]),
    ])
    def test_ctx_list_remove_range(self, count, index, list_indexes, expected):
        """
        Invoke list_remove_range() to remove values from a list.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_remove_range(self.nested_list_bin, index, count, ctx)
        ]

        self.as_connection.operate(self.test_key, ops)
        _, _, bins = self.as_connection.get(self.test_key)
        assert bins[self.nested_list_bin] == expected

    @pytest.mark.parametrize("count, index, list_indexes, expected", [
        (1, 0, 'cat', e.ParamError),
        (1, 0, [1,1,1,1], e.InvalidRequest),
    ])
    def test_ctx_list_remove_range_negative(self, count, index, list_indexes, expected):
        """
        Invoke list_remove_range() to remove values from a list with expected failures.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_remove_range(self.nested_list_bin, index, count, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("list_indexes, expected", [
        ([1,1,1], [['hi', 'friend', ['how', 'are', 
        ['you']]], ['hey', ['numbers', []], 'home boy']]),
        ([0,2], [['hi', 'friend', []], ['hey', ['numbers', [1, 2, 3]], 'home boy']]),
        ([1], [['hi', 'friend', ['how', 'are', 
        ['you']]], []]),
        ([1,1], [['hi', 'friend', ['how', 'are', 
        ['you']]], ['hey', [], 'home boy']]),
    ])
    def test_ctx_list_clear(self, list_indexes, expected):
        """
        Invoke list_clear() to empty a list.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_clear(self.nested_list_bin, ctx)
        ]

        self.as_connection.operate(self.test_key, ops)
        _, _, bins = self.as_connection.get(self.test_key)
        assert bins[self.nested_list_bin] == expected

    @pytest.mark.parametrize("list_indexes, expected", [
        ('cat', e.ParamError),
        ([1,1,1,1], e.InvalidRequest),
    ])
    def test_ctx_list_clear_negative(self, list_indexes, expected):
        """
        Invoke list_clear() to empty a list with expected failures.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_clear(self.nested_list_bin, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("index, value, list_indexes, expected", [
        (0, 'toast', [1], [['hi', 'friend', ['how', 'are', 
        ['you']]], ['toast', ['numbers', [1, 2, 3]], 'home boy']]),
        (2, 'jam', [0], [['hi', 'friend', 'jam'], ['hey', ['numbers', [1, 2, 3]], 'home boy']]),
        (1, 'honey', [1,1,1], [['hi', 'friend', ['how', 'are', 
        ['you']]], ['hey', ['numbers', [1, 'honey', 3]], 'home boy']]),
        (2, 6, [0,2], [['hi', 'friend', ['how', 'are', 
        6]], ['hey', ['numbers', [1, 2, 3]], 'home boy']]),
        (5, 'toast', [1], [['hi', 'friend', ['how', 'are', 
        ['you']]], ['hey', ['numbers', [1, 2, 3]], 'home boy', None, None, 'toast']])
    ])
    def test_ctx_list_set(self, index, value, list_indexes, expected):
        """
        Invoke list_set() to set a value in a list.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_set(self.nested_list_bin, index, value, None, ctx)
        ]

        self.as_connection.operate(self.test_key, ops)
        _, _, bins = self.as_connection.get(self.test_key)
        assert bins[self.nested_list_bin] == expected

    @pytest.mark.parametrize("index, value, list_indexes, expected", [
        (1, 'toast', [2], e.OpNotApplicable),
        (0, '?', 'cat', e.ParamError),
    ])
    def test_ctx_list_set_negative(self, index, value, list_indexes, expected):
        """
        Invoke list_set() to set a value in a list with expected failures.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_set(self.nested_list_bin, index, value, None, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("index, list_indexes, expected", [
        (0, [1,1,1], 1),
        (2, [1,1,1], 3),
        (1, [1,1], [1, 2, 3]),
        (2, [0,2], ['you']),
    ])
    def test_ctx_list_get(self, index, list_indexes, expected):
        """
        Invoke list_get() to retrieve a value from a list.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_get(self.nested_list_bin, index, ctx)
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        assert res[self.nested_list_bin] == expected

    @pytest.mark.parametrize("index, list_indexes, expected", [
        (1, [2], e.OpNotApplicable),
        (4, [1,1,1], e.OpNotApplicable),
        ('cat', [1], e.ParamError),
    ])
    def test_ctx_list_get_negative(self, index, list_indexes, expected):
        """
        Invoke list_get() to retrieve a value from a list with expected failures.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_get(self.nested_list_bin, index, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("index, count, list_indexes, expected", [
        (0, 3, [1,1,1], [1,2,3]),
        (2, 1, [1,1,1], [3]),
        (1, 5, [1], [['numbers', [1, 2, 3]], 'home boy']),
        (1, 2, [0,2], ['are', ['you']]),
        (4, 1, [1,1,1], []),
    ])
    def test_ctx_list_get_range(self, index, count, list_indexes, expected):
        """
        Invoke list_get_range() to retrieve values from a list.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_get_range(self.nested_list_bin, index, count, ctx)
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        assert res[self.nested_list_bin] == expected

    @pytest.mark.parametrize("index, count, list_indexes, expected", [
        (1, 1, [2], e.OpNotApplicable),
        ('cat', 1, [1], e.ParamError),
    ])
    def test_ctx_list_get_range_negative(self, index, count, list_indexes, expected):
        """
        Invoke list_get_range() to retrieve values from a list with expected failures.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_get_range(self.nested_list_bin, index, count, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("index, count, list_indexes, expected", [
        (0, 1, [0], [['hi'], ['hey', ['numbers', [1, 2, 3]], 'home boy']]),
        (0, 0, [1], [['hi', 'friend', ['how', 'are', ['you']]], []]),
        (0, 2, [1,1,1], [['hi', 'friend', ['how', 'are', 
        ['you']]], ['hey', ['numbers', [1, 2]], 'home boy']]),
        (1, 3, [0,2], [['hi', 'friend', ['are', 
        ['you']]], ['hey', ['numbers', [1, 2, 3]], 'home boy']])
    ])
    def test_ctx_list_trim(self, index, count, list_indexes, expected):
        """
        Invoke list_trim() to remove list elements outside the given range.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_trim(self.nested_list_bin, index, count, ctx)
        ]

        self.as_connection.operate(self.test_key, ops)
        _, _, bins = self.as_connection.get(self.test_key)
        assert bins[self.nested_list_bin] == expected

    @pytest.mark.parametrize("index, count, list_indexes, expected", [
        (3, 1, [2], e.OpNotApplicable),
        (0, 2, 'cat', e.ParamError),
        (1, 'dog', [2], e.ParamError),
        ('lizard', 1, [2], e.OpNotApplicable),
    ])
    def test_ctx_list_trim_negative(self, index, count, list_indexes, expected):
        """
        Invoke list_trim() to remove list elements outside the given range with expected failures.
        """
        ctx = []
        for index in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(index))

        ops = [
            list_operations.list_trim(self.nested_list_bin, index, count, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("list_indexes, expected", [
        ([1,1,1], 3),
        ([1], 3),
        ([1,1], 3),
        ([0,2], 3),
        ([0,2,2], 1),
    ])
    def test_ctx_list_size(self, list_indexes, expected):
        """
        Invoke list_size() to get the size of a list.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_size(self.nested_list_bin, ctx)
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        assert res[self.nested_list_bin] == expected

    @pytest.mark.parametrize("list_indexes, expected", [
        ([4], e.OpNotApplicable),
        ([1,1,1,1], e.BinIncompatibleType),
        (['cat'], e.ParamError),
    ])
    def test_ctx_list_size(self, list_indexes, expected):
        """
        Invoke list_size() to get the size of a list with expected failures.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_size(self.nested_list_bin, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("value, offset, return_type, count, inverted, list_indexes, expected_bin, expected_val", [
        (2, 0, aerospike.LIST_RETURN_VALUE, 2, False, [1,1,1], [['hi', 'friend', ['how', 'are', 
        ['you']]], ['hey', ['numbers', [1]], 'home boy']], [2,3]),
        ('hi', 1, aerospike.LIST_RETURN_INDEX, 2, False, [0], [['hi', 'friend'], ['hey', ['numbers', [1,2,3]], 'home boy']], [2]),
        ('numbers', 0, aerospike.LIST_RETURN_VALUE, 1, True, [1,1], [['hi', 'friend', ['how', 'are', 
        ['you']]], ['hey', ['numbers'], 'home boy']], [[1,2,3]]),
    ])
    def test_ctx_list_remove_by_value_rank_range(self, value, offset, return_type, count,
     inverted, list_indexes, expected_bin, expected_val):
        """
        Invoke list_remove_by_value_rank_range() to remove elements in a range by rank relative
        to the element specified by the given value.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_remove_by_value_rank_range_relative(self.nested_list_bin, value, offset,
             return_type, count, inverted, ctx)
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        _, _, bins = self.as_connection.get(self.test_key)
        assert bins[self.nested_list_bin] == expected_bin
        assert res[self.nested_list_bin] == expected_val

    @pytest.mark.parametrize("value, offset, return_type, count, inverted, list_indexes, expected", [
        (2, 0, aerospike.LIST_RETURN_VALUE, 2, False, [1,1,1,1], e.InvalidRequest),
    ])
    def test_ctx_list_remove_by_value_rank_range_negative(self, value, offset, return_type, count,
     inverted, list_indexes, expected):
        """
        Invoke list_remove_by_value_rank_range() to remove elements in a range by rank relative
        to the element specified by the given value with expected failures.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_remove_by_value_rank_range_relative(self.nested_list_bin, value, offset,
             return_type, count, inverted, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("value, offset, return_type, count, inverted, list_indexes, expected", [
        (2, 0, aerospike.LIST_RETURN_VALUE, 2, False, [1,1,1], [2,3]),
        ('hi', 0, aerospike.LIST_RETURN_INDEX, None, False, [0], [0,2]),
        ('numbers', 0, aerospike.LIST_RETURN_VALUE, 1, True, [1,1], [[1,2,3]]),
    ])
    def test_ctx_list_get_by_value_rank_range_relative(self, value, offset, return_type, count,
     inverted, list_indexes, expected):
        """
        Invoke list_get_by_value_rank_range() to get elements in a range by rank relative
        to the element specified by the given value.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_get_by_value_rank_range_relative(self.nested_list_bin, value, offset,
             return_type, count, inverted, ctx)
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        assert res[self.nested_list_bin] == expected


    @pytest.mark.parametrize("value, offset, return_type, count, inverted, list_indexes, expected", [
        (2, 0, aerospike.LIST_RETURN_VALUE, 2, False, [1,1,1,6], e.OpNotApplicable),
        (2, 0, aerospike.LIST_RETURN_VALUE, 2, False, [1,1,1,1], e.BinIncompatibleType),
    ])
    def test_ctx_list_get_by_value_rank_range_relative_negative(self, value, offset, return_type, count,
     inverted, list_indexes, expected):
        """
        Invoke list_get_by_value_rank_range() to get elements in a range by rank relative
        to the element specified by the given value with expected failures.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_get_by_value_rank_range_relative(self.nested_list_bin, value, offset,
             return_type, count, inverted, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("index, return_type, list_indexes, expected", [
        (0, aerospike.LIST_RETURN_COUNT, [1,1,1], 1),
        (2, aerospike.LIST_RETURN_VALUE, [1,1,1], 3),
        (1, aerospike.LIST_RETURN_VALUE, [1,1], [1, 2, 3]),
        (2, aerospike.LIST_RETURN_RANK, [0,2], 2),
    ])
    def test_ctx_list_get_by_index(self, index, return_type, list_indexes, expected):
        """
        Invoke list_get_by_index() to get the value at index from a list.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_get_by_index(self.nested_list_bin, index, return_type, ctx)
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        assert res[self.nested_list_bin] == expected

    @pytest.mark.parametrize("index, return_type, list_indexes, expected", [
        (1, aerospike.LIST_RETURN_VALUE, [2], e.OpNotApplicable),
        (4, aerospike.LIST_RETURN_VALUE, [1,1,1], e.OpNotApplicable),
        ('cat', aerospike.LIST_RETURN_VALUE, [1], e.ParamError),
    ])
    def test_ctx_list_get_by_index_negative(self, index, return_type, list_indexes, expected):
        """
        Invoke list_get_by_index() to get the value at index from a list with expected failures.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_get_by_index(self.nested_list_bin, index, return_type, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("index, return_type, count, inverted, list_indexes, expected", [
        (0, aerospike.LIST_RETURN_COUNT, 3, False, [1], 3),
        (2, aerospike.LIST_RETURN_VALUE, 1, True, [1,1,1], [1,2]),
        (4, aerospike.LIST_RETURN_VALUE, 1, False, [1,1,1], []),
        (0, aerospike.LIST_RETURN_VALUE, 2, False, [1,1], ['numbers', [1, 2, 3]]),
        (1, aerospike.LIST_RETURN_RANK, 3, False, [0,2], [0, 2]),
    ])
    def test_ctx_list_get_by_index_range(self, index, return_type, count, inverted, list_indexes, expected):
        """
        Invoke list_get_by_index() to get the values at index for count from a list.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_get_by_index_range(self.nested_list_bin, index, return_type, count, inverted, ctx)
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        assert res[self.nested_list_bin] == expected

    @pytest.mark.parametrize("index, return_type, count, inverted, list_indexes, expected", [
        (1, aerospike.LIST_RETURN_VALUE, 3, False, [2], e.OpNotApplicable),
        (1, aerospike.LIST_RETURN_VALUE, 3, False, ['dog'], e.ParamError),
        (1, 42, 1, False, [1], e.OpNotApplicable),
        #(1, aerospike.LIST_RETURN_VALUE, 1, 'dog', [1], e.ParamError), why does this pass with bad bool?
    ])
    def test_ctx_list_get_by_index_range_negative(self, index, return_type, count, inverted, list_indexes, expected):
        """
        Invoke list_get_by_index() to get the values at index for count from a list with expected failures.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_get_by_index_range(self.nested_list_bin, index, return_type, count, inverted, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("rank, return_type, list_indexes, expected", [
        (0, aerospike.LIST_RETURN_COUNT, [1,1,1], 1),
        (2, aerospike.LIST_RETURN_VALUE, [1,1,1], 3),
        (1, aerospike.LIST_RETURN_VALUE, [1,1], [1, 2, 3]),
        (2, aerospike.LIST_RETURN_VALUE, [0,2], ['you']),
    ])
    def test_ctx_list_get_by_rank(self, rank, return_type, list_indexes, expected):
        """
        Invoke list_get_by_rank() to get an entry of the given rank from a list.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_get_by_rank(self.nested_list_bin, rank, return_type, ctx)
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        assert res[self.nested_list_bin] == expected

    @pytest.mark.parametrize("rank, return_type, list_indexes, expected", [
        (1, aerospike.LIST_RETURN_VALUE, [2], e.OpNotApplicable),
        (3, aerospike.LIST_RETURN_VALUE, [1,1,1], e.OpNotApplicable),
        ('cat', aerospike.LIST_RETURN_VALUE, [1], e.ParamError),
    ])
    def test_ctx_list_get_by_rank_negative(self, rank, return_type, list_indexes, expected):
        """
        Invoke list_get_by_rank() to get an entry of the given rank from a list with expected failures.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_get_by_rank(self.nested_list_bin, rank, return_type, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("rank, return_type, count, inverted, list_indexes, expected", [
        (0, aerospike.LIST_RETURN_COUNT, 3, False, [1], 3),
        (2, aerospike.LIST_RETURN_VALUE, 1, True, [1,1,1], [1,2]),
        (4, aerospike.LIST_RETURN_VALUE, 1, False, [1,1,1], []),
        (0, aerospike.LIST_RETURN_VALUE, 2, False, [1,1], ['numbers', [1, 2, 3]]),
        (1, aerospike.LIST_RETURN_RANK, 3, False, [0,2], [0, 2]),
        (20, aerospike.LIST_RETURN_VALUE, 3, False, [0], []),
    ])
    def test_ctx_list_get_by_rank_range(self, rank, return_type, count, inverted, list_indexes, expected):
        """
        Invoke list_get_by_rank_range() to start getting elements at value for count from a list.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_get_by_index_range(self.nested_list_bin, rank, return_type, count, inverted, ctx)
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        assert res[self.nested_list_bin] == expected

    @pytest.mark.parametrize("rank, return_type, count, inverted, list_indexes, expected", [
        (1, aerospike.LIST_RETURN_VALUE, 3, False, [2], e.OpNotApplicable),
        (1, aerospike.LIST_RETURN_VALUE, 3, False, ['dog'], e.ParamError),
        (1, 42, 1, False, [1], e.OpNotApplicable),
        #(1, aerospike.LIST_RETURN_VALUE, 1, 'dog', [1], e.ParamError), why does this pass with bad bool?
    ])
    def test_ctx_list_get_by_rank_range_negative(self, rank, return_type, count, inverted, list_indexes, expected):
        """
        Invoke list_get_by_rank_range() to start getting elements at value for count from a list with expected failures.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_get_by_index_range(self.nested_list_bin, rank, return_type, count, inverted, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("value, return_type, inverted, list_indexes, expected", [
        (2, aerospike.LIST_RETURN_COUNT, False, [1,1,1], 1),
        ([1,2,3], aerospike.LIST_RETURN_RANK, False, [1,1], [1]),
        ('home boy', aerospike.LIST_RETURN_INDEX, False, [1], [2]),
        ('how', aerospike.LIST_RETURN_VALUE, True, [0,2], ['are', ['you']]),
    ])
    def test_ctx_list_get_by_value(self, value, return_type, inverted, list_indexes, expected):
        """
        Invoke list_get_by_value() to get the given value from a list.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_get_by_value(self.nested_list_bin, value, return_type, inverted, ctx)
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        assert res[self.nested_list_bin] == expected

    @pytest.mark.parametrize("value, return_type, inverted, list_indexes, expected", [
        (1, aerospike.LIST_RETURN_VALUE, False, [2], e.OpNotApplicable),
        (2, aerospike.LIST_RETURN_VALUE, False, [1,1,1,1], e.BinIncompatibleType),
        (1, 'bad_return_type', False, [1], e.ParamError),
    ])
    def test_ctx_list_get_by_value_negative(self, value, return_type, inverted, list_indexes, expected):
        """
        Invoke list_get_by_value() to get the given value from a list with expected failures.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_get_by_value(self.nested_list_bin, value, return_type, inverted, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("values, return_type, inverted, list_indexes, expected", [
        ([2,3], aerospike.LIST_RETURN_COUNT, False, [1,1,1], 2),
        ([[1,2,3], 'numbers'], aerospike.LIST_RETURN_RANK, False, [1,1], [1,0]),
        (['hi', ['how', 'are', ['you']]], aerospike.LIST_RETURN_INDEX, False, [0], [0,2]),
        (['how'], aerospike.LIST_RETURN_VALUE, True, [0,2], ['are', ['you']]),
    ])
    def test_ctx_list_get_by_value_list(self, values, return_type, inverted, list_indexes, expected):
        """
        Invoke list_get_by_value_list() to get the given values from a list.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_get_by_value_list(self.nested_list_bin, values, return_type, inverted, ctx)
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        assert res[self.nested_list_bin] == expected

    @pytest.mark.parametrize("values, return_type, inverted, list_indexes, expected", [
        ([1], aerospike.LIST_RETURN_VALUE, False, [2], e.OpNotApplicable),
        ([2], aerospike.LIST_RETURN_VALUE, False, [1,1,1,1], e.BinIncompatibleType),
        ([1], 'bad_return_type', False, [1], e.ParamError),
    ])
    def test_ctx_list_get_by_value_list_negative(self, values, return_type, inverted, list_indexes, expected):
        """
        Invoke list_get_by_value_list() to get the given values from a list with expected failures.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_get_by_value_list(self.nested_list_bin, values, return_type, inverted, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("return_type, value_begin, value_end, inverted, list_indexes, expected", [
        (aerospike.LIST_RETURN_COUNT, 0, 2, False, [1,1,1], 1),
        (aerospike.LIST_RETURN_RANK, None, None, False, [1,1], [0,1]),
        (aerospike.LIST_RETURN_INDEX, 2, 3,  False, [1,1,1], [1]),
        (aerospike.LIST_RETURN_VALUE, 'a', 'c', True, [0,2], ['how', ['you']]),
    ])
    def test_ctx_list_get_by_value_range(self, return_type, value_begin, value_end, inverted, list_indexes, expected):
        """
        Invoke list_get_by_value_range() get elements with values between value_begin and value_end
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_get_by_value_range(self.nested_list_bin, return_type, value_begin, value_end, inverted, ctx)
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        assert res[self.nested_list_bin] == expected

    @pytest.mark.parametrize("return_type, value_begin, value_end, inverted, list_indexes, expected", [
        (aerospike.LIST_RETURN_VALUE, 0, 1, False, [2], e.OpNotApplicable),
        (aerospike.LIST_RETURN_VALUE, 0, 1, False, [1,1,1,1], e.BinIncompatibleType),
        ('bad_return_type', 0, 1, False, [1], e.ParamError),
    ])
    def test_ctx_list_get_by_value_range_negative(self, return_type, value_begin, value_end, inverted, list_indexes, expected):
        """
        Invoke list_get_by_value_range() get elements with values between value_begin and value_end with expected failures.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_get_by_value_range(self.nested_list_bin, return_type, value_begin, value_end, inverted, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("index, return_type, list_indexes, expected_res, expected_bin", [
        (0, aerospike.LIST_RETURN_COUNT, [1,1,1], 1, [['hi', 'friend', ['how', 'are', ['you']]],
         ['hey', ['numbers', [2, 3]], 'home boy']]),
        (2, aerospike.LIST_RETURN_VALUE, [1,1,1], 3, [['hi', 'friend', ['how', 'are', ['you']]],
         ['hey', ['numbers', [1, 2]], 'home boy']]),
        (1, aerospike.LIST_RETURN_VALUE, [1,1], [1, 2, 3], [['hi', 'friend', ['how', 'are', ['you']]],
         ['hey', ['numbers'], 'home boy']]),
        (2, aerospike.LIST_RETURN_RANK, [0,2], 2, [['hi', 'friend', ['how', 'are']],
         ['hey', ['numbers', [1, 2, 3]], 'home boy']]),
    ])
    def test_ctx_list_remove_by_index(self, index, return_type, list_indexes, expected_res, expected_bin):
        """
        Invoke list_remove_by_index() to remove the element at index in a list.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_remove_by_index(self.nested_list_bin, index, return_type, ctx)
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        assert res[self.nested_list_bin] == expected_res
        _, _, bins = self.as_connection.get(self.test_key)
        assert bins[self.nested_list_bin] == expected_bin

    @pytest.mark.parametrize("index, return_type, list_indexes, expected", [
        (1, aerospike.LIST_RETURN_VALUE, [2], e.OpNotApplicable),
        (4, aerospike.LIST_RETURN_VALUE, [1,1,1], e.OpNotApplicable),
        ('cat', aerospike.LIST_RETURN_VALUE, [1], e.ParamError),
        (0, aerospike.LIST_RETURN_VALUE, [1,1,1,1], e.InvalidRequest),
    ])
    def test_ctx_list_remove_by_index_negative(self, index, return_type, list_indexes, expected):
        """
        Invoke list_remove_by_index() to remove the element at index in a list with expected failures.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_remove_by_index(self.nested_list_bin, index, return_type, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("index, return_type, count, inverted, list_indexes, expected_res, expected_bin", [
        (0, aerospike.LIST_RETURN_COUNT, 1, False, [1,1,1], 1, [['hi', 'friend', ['how', 'are', ['you']]],
         ['hey', ['numbers', [2, 3]], 'home boy']]),
        (1, aerospike.LIST_RETURN_VALUE, 3, False, [1,1,1], [2,3], [['hi', 'friend', ['how', 'are', ['you']]],
         ['hey', ['numbers', [1]], 'home boy']]),
        (0, aerospike.LIST_RETURN_VALUE, 2, False, [1,1], ['numbers',[1, 2, 3]], [['hi', 'friend', ['how', 'are', ['you']]],
         ['hey', [], 'home boy']]),
        (2, aerospike.LIST_RETURN_RANK, 1, True, [0,2], [1,0], [['hi', 'friend', [['you']]],
         ['hey', ['numbers', [1, 2, 3]], 'home boy']]),
    ])
    def test_ctx_list_remove_by_index_range(self, index, return_type, count, inverted, list_indexes, expected_res, expected_bin):
        """
        Invoke Invoke list_remove_by_index_range() to remove elements starting at index for count.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_remove_by_index_range(self.nested_list_bin, index, return_type, count, inverted, ctx)
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        assert res[self.nested_list_bin] == expected_res
        _, _, bins = self.as_connection.get(self.test_key)
        assert bins[self.nested_list_bin] == expected_bin

    @pytest.mark.parametrize("index, return_type, count, inverted, list_indexes, expected", [
        (1, aerospike.LIST_RETURN_VALUE, 3, False, [2], e.OpNotApplicable),
        (1, aerospike.LIST_RETURN_VALUE, 1, False, ['dog'], e.ParamError),
        (1, 42, 1, False, [1], e.OpNotApplicable),
        (0, aerospike.LIST_RETURN_INDEX, 'dog', False, [1,1,1], e.ParamError),
        (0, aerospike.LIST_RETURN_VALUE, 3, False, [1,1,1,1], e.InvalidRequest),
        #(4, aerospike.LIST_RETURN_VALUE, 3, False, [1], e.OpNotApplicable), why does this silently fail?
    ])
    def test_ctx_list_remove_by_index_range_negative(self, index, return_type, count, inverted, list_indexes, expected):
        """
        Invoke Invoke list_remove_by_index_range() to remove elements starting at index for count with expected failures.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_remove_by_index_range(self.nested_list_bin, index, return_type, count, inverted, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("rank, return_type, list_indexes, expected_res, expected_bin", [
        (0, aerospike.LIST_RETURN_COUNT, [1,1,1], 1, [['hi', 'friend', ['how', 'are', ['you']]],
         ['hey', ['numbers', [2, 3]], 'home boy']]),
        (2, aerospike.LIST_RETURN_VALUE, [1,1,1], 3, [['hi', 'friend', ['how', 'are', ['you']]],
         ['hey', ['numbers', [1, 2]], 'home boy']]),
        (1, aerospike.LIST_RETURN_VALUE, [1,1], [1, 2, 3], [['hi', 'friend', ['how', 'are', ['you']]],
         ['hey', ['numbers'], 'home boy']]),
        (2, aerospike.LIST_RETURN_VALUE, [0,2], ['you'], [['hi', 'friend', ['how', 'are']],
         ['hey', ['numbers', [1, 2, 3]], 'home boy']]),
    ])
    def test_ctx_list_remove_by_rank(self, rank, return_type, list_indexes, expected_res, expected_bin):
        """
        Invoke Invoke list_remove_by_rank() to remove the element with the given rank.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_remove_by_rank(self.nested_list_bin, rank, return_type, ctx)
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        assert res[self.nested_list_bin] == expected_res
        _, _, bins = self.as_connection.get(self.test_key)
        assert bins[self.nested_list_bin] == expected_bin

    @pytest.mark.parametrize("rank, return_type, list_indexes, expected", [
        (1, aerospike.LIST_RETURN_VALUE, [2], e.OpNotApplicable),
        (3, aerospike.LIST_RETURN_VALUE, [1,1,1], e.OpNotApplicable),
        (1, aerospike.LIST_RETURN_VALUE, [1,1,1,1], e.InvalidRequest),
        ('cat', aerospike.LIST_RETURN_VALUE, [1], e.ParamError),
    ])
    def test_ctx_list_remove_by_rank_negative(self, rank, return_type, list_indexes, expected):
        """
        Invoke Invoke list_remove_by_rank() to remove the element with the given rank with expected failures.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_remove_by_rank(self.nested_list_bin, rank, return_type, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("rank, return_type, count, inverted, list_indexes, expected_res, expected_bin", [
        (0, aerospike.LIST_RETURN_COUNT, 1, False, [1,1,1], 1, [['hi', 'friend', ['how', 'are', ['you']]],
         ['hey', ['numbers', [2, 3]], 'home boy']]),
        (1, aerospike.LIST_RETURN_VALUE, 3, False, [1,1,1], [2,3], [['hi', 'friend', ['how', 'are', ['you']]],
         ['hey', ['numbers', [1]], 'home boy']]),
        (0, aerospike.LIST_RETURN_VALUE, 1, False, [1,1], ['numbers'], [['hi', 'friend', ['how', 'are', ['you']]],
         ['hey', [[1,2,3]], 'home boy']]),
        (2, aerospike.LIST_RETURN_RANK, 2, True, [0,2], [0,1], [['hi', 'friend', [['you']]],
         ['hey', ['numbers', [1, 2, 3]], 'home boy']])
    ])
    def test_ctx_list_remove_by_rank_range(self, rank, return_type, count, inverted, list_indexes, expected_res, expected_bin):
        """
        Invoke list_remove_by_rank_range() to remove the element with the given rank for count.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_remove_by_rank_range(self.nested_list_bin, rank, return_type, count, inverted, ctx)
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        assert res[self.nested_list_bin] == expected_res
        _, _, bins = self.as_connection.get(self.test_key)
        assert bins[self.nested_list_bin] == expected_bin

    @pytest.mark.parametrize("rank, return_type, count, inverted, list_indexes, expected", [
        (1, aerospike.LIST_RETURN_VALUE, 3, False, [2], e.OpNotApplicable),
        (1, aerospike.LIST_RETURN_VALUE, 1, False, ['dog'], e.ParamError),
        (1, 42, 1, False, [1], e.OpNotApplicable),
        (0, aerospike.LIST_RETURN_INDEX, 'dog', False, [1,1,1], e.ParamError),
        (0, aerospike.LIST_RETURN_VALUE, 3, False, [1,1,1,1], e.InvalidRequest),
        ('dog', aerospike.LIST_RETURN_VALUE, 3, False, [1,1,1], e.ParamError),
    ])
    def test_ctx_list_remove_by_rank_range_negative(self, rank, return_type, count, inverted, list_indexes, expected):
        """
        Invoke list_remove_by_rank_range() to remove elements starting with rank for count with expected failures.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_remove_by_rank_range(self.nested_list_bin, rank, return_type, count, inverted, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("value, return_type, inverted, list_indexes, expected_res, expected_bin", [
        (1, aerospike.LIST_RETURN_COUNT, False, [1,1,1], 1, [['hi', 'friend', ['how', 'are', ['you']]],
         ['hey', ['numbers', [2, 3]], 'home boy']]),
        (3, aerospike.LIST_RETURN_VALUE, False, [1,1,1], [3], [['hi', 'friend', ['how', 'are', ['you']]],
         ['hey', ['numbers', [1, 2]], 'home boy']]),
        ([1,2,3], aerospike.LIST_RETURN_RANK, False, [1,1], [1], [['hi', 'friend', ['how', 'are', ['you']]],
         ['hey', ['numbers'], 'home boy']]),
        (['you'], aerospike.LIST_RETURN_INDEX, True, [0,2], [0,1], [['hi', 'friend', [['you']]],
         ['hey', ['numbers', [1, 2, 3]], 'home boy']]),
    ])
    def test_ctx_list_remove_by_value(self, value, return_type, inverted, list_indexes, expected_res, expected_bin):
        """
        Invoke list_remove_by_value() to remove the element with the given value.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_remove_by_value(self.nested_list_bin, value, return_type, inverted, ctx)
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        assert res[self.nested_list_bin] == expected_res
        _, _, bins = self.as_connection.get(self.test_key)
        assert bins[self.nested_list_bin] == expected_bin

    @pytest.mark.parametrize("value, return_type, inverted, list_indexes, expected", [
        (1, aerospike.LIST_RETURN_VALUE, False, [2], e.OpNotApplicable),
        (1, aerospike.LIST_RETURN_VALUE, False, [1,1,1,1], e.InvalidRequest),
    ])
    def test_ctx_list_remove_by_value_negative(self, value, return_type, inverted, list_indexes, expected):
        """
        Invoke list_remove_by_value() to remove the element with the given value with expected failures.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_remove_by_value(self.nested_list_bin, value, return_type, inverted, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("values, return_type, inverted, list_indexes, expected_res, expected_bin", [
        ([2,3], aerospike.LIST_RETURN_COUNT, False, [1,1,1], 2,
         [['hi', 'friend', ['how', 'are', ['you']]], ['hey', ['numbers', [1]], 'home boy']]),
        ([[1,2,3], 'numbers'], aerospike.LIST_RETURN_RANK, False, [1,1], [1,0],
         [['hi', 'friend', ['how', 'are', ['you']]], ['hey', [], 'home boy']]),
        (['hi', ['how', 'are', ['you']]], aerospike.LIST_RETURN_INDEX, False, [0], [0,2],
         [['friend'], ['hey', ['numbers', [1, 2, 3]], 'home boy']]),
        (['how'], aerospike.LIST_RETURN_VALUE, True, [0,2], ['are', ['you']],
         [['hi', 'friend', ['how']], ['hey', ['numbers', [1, 2, 3]], 'home boy']]),
    ])
    def test_ctx_list_remove_by_value_list(self, values, return_type, inverted, list_indexes, expected_res, expected_bin):
        """
        Invoke list_remove_by_value_list() to remove elements with the given values.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_remove_by_value_list(self.nested_list_bin, values, return_type, inverted, ctx)
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        assert res[self.nested_list_bin] == expected_res
        _, _, bins = self.as_connection.get(self.test_key)
        assert bins[self.nested_list_bin] == expected_bin

    @pytest.mark.parametrize("values, return_type, inverted, list_indexes, expected", [
        ([1], aerospike.LIST_RETURN_VALUE, False, [2], e.OpNotApplicable),
        ([2], aerospike.LIST_RETURN_VALUE, False, [1,1,1,1], e.InvalidRequest),
        ([1], 'bad_return_type', False, [1], e.ParamError),
    ])
    def test_ctx_list_remove_by_value_list_negative(self, values, return_type, inverted, list_indexes, expected):
        """
        Invoke list_remove_by_value_list() to remove elements with the given values with expected failures.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_remove_by_value_list(self.nested_list_bin, values, return_type, inverted, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("return_type, value_begin, value_end, inverted, list_indexes, expected_res, expected_bin", [
        (aerospike.LIST_RETURN_COUNT, 0, 2, False, [1,1,1], 1,
         [['hi', 'friend', ['how', 'are', ['you']]], ['hey', ['numbers', [2, 3]], 'home boy']]),
        (aerospike.LIST_RETURN_RANK, None, None, False, [1,1], [0,1],
         [['hi', 'friend', ['how', 'are', ['you']]], ['hey', [], 'home boy']]),
        (aerospike.LIST_RETURN_INDEX, 2, 3,  False, [1,1,1], [1],
         [['hi', 'friend', ['how', 'are', ['you']]], ['hey', ['numbers', [1,3]], 'home boy']]),
        (aerospike.LIST_RETURN_VALUE, 'a', 'c', True, [0,2], ['how', ['you']],
         [['hi', 'friend', ['are']], ['hey', ['numbers', [1, 2, 3]], 'home boy']]),
    ])
    def test_ctx_list_remove_by_value_range(self, return_type, value_begin, value_end, inverted, list_indexes, expected_res, expected_bin):
        """
        Invoke list_remove_by_value_range() to remove elements between value_begin and value_end.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_remove_by_value_range(self.nested_list_bin, return_type, value_begin, value_end, inverted, ctx)
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        assert res[self.nested_list_bin] == expected_res
        _, _, bins = self.as_connection.get(self.test_key)
        assert bins[self.nested_list_bin] == expected_bin

    @pytest.mark.parametrize("return_type, value_begin, value_end, inverted, list_indexes, expected", [
        (aerospike.LIST_RETURN_VALUE, 0, 1, False, [2], e.OpNotApplicable),
        (aerospike.LIST_RETURN_VALUE, 0, 1, False, [1,1,1,1], e.InvalidRequest),
        ('bad_return_type', 0, 1, False, [1], e.ParamError),
    ])
    def test_ctx_list_remove_by_value_range_negative(self, return_type, value_begin, value_end, inverted, list_indexes, expected):
        """
        Invoke list_remove_by_value_range() to remove elements between value_begin and value_end with expected failures.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_remove_by_value_range(self.nested_list_bin, return_type, value_begin, value_end, inverted, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("list_order, list_indexes, expected", [
        (aerospike.LIST_ORDERED, [0], [[2,4,5],[1,4,2,3],[[2,2,2]]]),
        (aerospike.LIST_ORDERED, [1], [[4,2,5],[1,2,3,4],[[2,2,2]]]),
        (aerospike.LIST_UNORDERED, [0], [[4,2,5],[1,4,2,3],[[2,2,2]]]),
    ])
    def test_ctx_list_set_order(self, list_order, list_indexes, expected):
        """
        Invoke list_set_order() to set the order of the list.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_set_order(self.nested_list_order_bin, list_order, ctx)
        ]

        self.as_connection.operate(self.test_key, ops)
        _, _, bins = self.as_connection.get(self.test_key)
        assert bins[self.nested_list_order_bin] == expected

    @pytest.mark.parametrize("list_order, list_indexes, expected", [
        (aerospike.LIST_ORDERED, [0,1], e.InvalidRequest),
        ('bad_list_order_type', [1], e.ParamError),
    ])
    def test_ctx_list_set_order_negative(self, list_order, list_indexes, expected):
        """
        Invoke list_set_order() to set the order of the list with expected failures.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_set_order(self.nested_list_order_bin, list_order, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("sort_flags, list_indexes, expected", [
        (aerospike.LIST_SORT_DEFAULT, [0], [[2,4,5],[1,4,2,3],[[2,2,2]]]),
        (aerospike.LIST_SORT_DROP_DUPLICATES, [2,0], [[4,2,5],[1,4,2,3],[[2]]]),
        (aerospike.LIST_SORT_DEFAULT, [1], [[4,2,5],[1,2,3,4],[[2,2,2]]]),
    ])
    def test_ctx_list_sort(self, sort_flags, list_indexes, expected):
        """
        Invoke list_sort() to set how the list will be sorted.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_sort(self.nested_list_order_bin, sort_flags, ctx)
        ]

        self.as_connection.operate(self.test_key, ops)
        _, _, bins = self.as_connection.get(self.test_key)
        assert bins[self.nested_list_order_bin] == expected

    @pytest.mark.parametrize("sort_flags, list_indexes, expected", [
        (aerospike.LIST_SORT_DEFAULT, [0,1], e.InvalidRequest),
        (aerospike.LIST_SORT_DROP_DUPLICATES, [0,1], e.InvalidRequest),
        ('bad_sort_flags_type', [1], e.ParamError),
    ])
    def test_ctx_list_sort_negative(self, sort_flags, list_indexes, expected):
        """
        Invoke list_sort() to set how the list will be sorted with expected failures.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_list_index(place))

        ops = [
            list_operations.list_sort(self.nested_list_order_bin, sort_flags, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("ctx_types, key, return_type, list_indexes, expected", [
        ([map_index], 'greet', aerospike.MAP_RETURN_VALUE, [0], 'hi'),
        ([map_index], 3, aerospike.MAP_RETURN_VALUE, [0], 'hello'),
        ([map_index], 'nested', aerospike.MAP_RETURN_VALUE, [1], {4,5,6}),
        ([map_index], 'dog', aerospike.MAP_RETURN_VALUE, [1], None),
        ([map_index, map_index, map_index], 'fish', aerospike.MAP_RETURN_VALUE, [2,0,0], 'pond'), # why does this fail?
        ([map_key], 'nested', aerospike.MAP_RETURN_INDEX, ['second'], 1)
    ])
    def test_ctx_map_get_by_key(self, ctx_types, key, return_type, list_indexes, expected):
        """
        Invoke map_get_by_key() to get the value at key in the map.
        """
        ctx = []
        for x in range(0, len(list_indexes)) :
            ctx.append(add_ctx_op(ctx_types[x], list_indexes[x]))

        ops = [
            map_operations.map_get_by_key(self.nested_map_bin, key, return_type, ctx)
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        assert res[self.nested_map_bin] == expected

    @pytest.mark.parametrize("key, return_type, list_indexes, expected", [
        ('greet', aerospike.MAP_RETURN_VALUE, [3], e.OpNotApplicable),
        ('nested', aerospike.MAP_RETURN_VALUE, [1,0,0], e.OpNotApplicable),
        ('greet', aerospike.MAP_RETURN_VALUE, 'teddy', e.ParamError),
    ])
    def test_ctx_map_get_by_key_negative(self, key, return_type, list_indexes, expected):
        """
        Invoke map_get_by_key() to get the value at key in the map with expected failures.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_map_index(place))

        ops = [
            map_operations.map_get_by_key(self.nested_map_bin, key, return_type, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("ctx_types, key_start, key_stop, return_type, inverted, list_indexes, expected", [
        ([map_index], 0, 4, aerospike.MAP_RETURN_VALUE, False, [0], ['v3', 'v1', 'v2']),
        ([map_index], 7, 9, aerospike.MAP_RETURN_VALUE, False, [2], ['v8', 'v7']),
        ([map_key, map_key], 11, 12, aerospike.MAP_RETURN_VALUE, False, [3, 10], ['v11']),
        ([map_index], 7, 9, aerospike.MAP_RETURN_VALUE, True, [2], ['v9', {11: 'v11'}]),
    ])
    def test_ctx_map_get_by_key_range(self, ctx_types, key_start, key_stop, return_type, inverted, list_indexes, expected):
        """
        Invoke map_get_by_key_range() to get the values starting at key_start up to key_stop.
        """
        ctx = []
        for x in range(0, len(list_indexes)) :
            ctx.append(add_ctx_op(ctx_types[x], list_indexes[x]))

        ops = [
            map_operations.map_get_by_key_range(self.num_map_bin, key_start, key_stop, return_type, inverted, ctx)
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        assert res[self.num_map_bin] == expected

    @pytest.mark.parametrize("ctx_types, key_start, key_stop, return_type, inverted, list_indexes, expected", [
        ([map_index], 0, 4, aerospike.MAP_RETURN_VALUE, False, [3], e.OpNotApplicable),
        ([map_key, map_key, map_index], 11, 12, aerospike.MAP_RETURN_VALUE, False, [3, 10, 0], e.OpNotApplicable),
    ])
    def test_ctx_map_get_by_key_range_negative(self, ctx_types, key_start, key_stop, return_type, inverted, list_indexes, expected):
        """
        Invoke map_get_by_key_range() to get the values starting at key_start up to key_stop with expected failures.
        """
        ctx = []
        for x in range(0, len(list_indexes)) :
            ctx.append(add_ctx_op(ctx_types[x], list_indexes[x]))

        ops = [
            map_operations.map_get_by_key_range(self.nested_map_bin, key_start, key_stop, return_type, inverted, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("ctx_types, key, return_type, inverted, list_indexes, expected", [
        ([map_index], ['greet'], aerospike.MAP_RETURN_VALUE, False, [0], ['hi']),
        ([map_index], ['numbers', 3], aerospike.MAP_RETURN_VALUE, False, [0], ['hello', [3,1,2]]),
        ([map_index], ['nested', 'hundred'], aerospike.MAP_RETURN_VALUE, False, [1], [100, {4,5,6}]),
        ([map_index], ['dog'], aerospike.MAP_RETURN_VALUE, False, [1], []),
        ([map_index, map_index, map_index], ['horse', 'fish'], aerospike.MAP_RETURN_VALUE, False, [2,0,0], ['pond', 'shoe']),
        ([map_key], ['nested'], aerospike.MAP_RETURN_INDEX, True, ['second'], [0])
    ])
    def test_ctx_map_get_by_key_list(self, ctx_types, key, return_type, inverted, list_indexes, expected):
        """
        Invoke map_get_by_key_list() to get the values at the supplied keys.
        """
        ctx = []
        for x in range(0, len(list_indexes)) :
            ctx.append(add_ctx_op(ctx_types[x], list_indexes[x]))

        ops = [
            map_operations.map_get_by_key_list(self.nested_map_bin, key, return_type, inverted, ctx)
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        res = res[self.nested_map_bin]

        assert res == expected

    @pytest.mark.parametrize("key, return_type, inverted, list_indexes, expected", [
        (['greet'], aerospike.MAP_RETURN_VALUE, False, [3], e.OpNotApplicable),
        (['nested'], aerospike.MAP_RETURN_VALUE, False, [1,0,0], e.OpNotApplicable),
        (['greet'], aerospike.MAP_RETURN_VALUE, False, 'teddy', e.ParamError),
    ])
    def test_ctx_map_get_by_key_list_negative(self, key, return_type, inverted, list_indexes, expected):
        """
        Invoke map_get_by_key_list() to get the values at the supplied keys with expected failures.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_map_index(place))

        ops = [
            map_operations.map_get_by_key_list(self.nested_map_bin, key, return_type, inverted, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("ctx_types, index, return_type, list_indexes, expected", [
        ([map_index], 1, aerospike.MAP_RETURN_VALUE, [2], []),
        ([map_key], 0, aerospike.MAP_RETURN_VALUE, ['first'], 'hello'),
        ([map_index, map_key, map_index], 1, aerospike.MAP_RETURN_VALUE, [2,'one',0], 'shoe'),
    ])
    def test_ctx_map_get_by_index(self, ctx_types, index, return_type, list_indexes, expected):
        """
        Invoke map_get_by_index() to get the value at index.
        """
        ctx = []
        for x in range(0, len(list_indexes)) :
            ctx.append(add_ctx_op(ctx_types[x], list_indexes[x]))

        ops = [
            map_operations.map_get_by_index(self.nested_map_bin, index, return_type, ctx)
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        assert res[self.nested_map_bin] == expected

    @pytest.mark.parametrize("index, return_type, list_indexes, expected", [
        (0, aerospike.MAP_RETURN_VALUE, [3], e.OpNotApplicable),
        (0, aerospike.MAP_RETURN_VALUE, [1,0,0], e.OpNotApplicable),
        (0, aerospike.MAP_RETURN_VALUE, 'teddy', e.ParamError),
        (200, aerospike.MAP_RETURN_VALUE, [0], e.OpNotApplicable),
    ])
    def test_ctx_map_get_by_index_negative(self, index, return_type, list_indexes, expected):
        """
        Invoke map_get_by_index() to get the value at index with expected failures.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_map_index(place))

        ops = [
            map_operations.map_get_by_index(self.nested_map_bin, index, return_type, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("ctx_types, index, rmv_count, return_type, inverted, list_indexes, expected", [
        ([map_index], 1, 1, aerospike.MAP_RETURN_VALUE, False, [0], ['hi']),
        ([map_index], 0, 3, aerospike.MAP_RETURN_VALUE, False, [0], ['hello', 'hi', [3,1,2]]),
        ([map_index], 0, 2, aerospike.MAP_RETURN_VALUE, False, [1], [100, {4,5,6}]),
        ([map_index, map_index, map_index], 0, 2, aerospike.MAP_RETURN_VALUE, False, [2,0,0], ['pond', 'shoe']),
        ([map_key], 1, 2, aerospike.MAP_RETURN_INDEX, True, ['second'], [0]),
        ([map_rank, map_value], 0, 3, aerospike.MAP_RETURN_INDEX, False, [1, {'cat': 'dog',
         'barn': {'horse': 'shoe', 'fish': 'pond'}, 'cage': ['bird']}], [0,1,2])
    ])
    def test_ctx_map_get_by_index_range(self, ctx_types, index, rmv_count, return_type, inverted, list_indexes, expected):
        """
        Invoke map_get_by_index_range() to get the value starting at index for rmv_count.
        """
        ctx = []
        for x in range(0, len(list_indexes)) :
            ctx.append(add_ctx_op(ctx_types[x], list_indexes[x]))

        ops = [
            map_operations.map_get_by_index_range(self.nested_map_bin, index, rmv_count, return_type, inverted, ctx)
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        res = res[self.nested_map_bin]

        assert res == expected

    @pytest.mark.parametrize("index, rmv_count, return_type, inverted, list_indexes, expected", [
        (1, 1, aerospike.MAP_RETURN_VALUE, False, [3], e.OpNotApplicable),
        (1, 1, aerospike.MAP_RETURN_VALUE, False, [1,0,0], e.OpNotApplicable),
        (1, 1, aerospike.MAP_RETURN_VALUE, False, 'teddy', e.ParamError),
        (1, 'bad_rmv_count', aerospike.MAP_RETURN_VALUE, False, [1], e.ParamError),
    ])
    def test_ctx_map_get_by_index_range_negative(self, index, rmv_count, return_type, inverted, list_indexes, expected):
        """
        Invoke map_get_by_index_range() to get the value starting at index for rmv_count with expected failures.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_map_index(place))

        ops = [
            map_operations.map_get_by_index_range(self.nested_map_bin, index, rmv_count, return_type, inverted, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("ctx_types, value, return_type, inverted, list_indexes, expected", [
        ([map_index, map_rank, map_key], 'done', aerospike.MAP_RETURN_VALUE, False, [0,0,1], ['done']),
        ([map_index, map_rank, map_index], 'bye', aerospike.MAP_RETURN_VALUE, False, [1,0,0], ['bye']),
        ([map_index, map_rank, map_index], {'g': 'layer', 'l': 'done'}, aerospike.MAP_RETURN_VALUE, False, [0,0], [{'g': 'layer', 'l': 'done'}]),
    ])
    def test_ctx_map_get_by_value(self, ctx_types, value, return_type, inverted, list_indexes, expected):
        """
        Invoke map_get_by_value() to get the value in the map.
        """
        ctx = []
        for x in range(0, len(list_indexes)) :
            ctx.append(add_ctx_op(ctx_types[x], list_indexes[x]))

        ops = [
            map_operations.map_get_by_value(self.layered_map_bin, value, return_type, inverted, ctx)
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        assert res[self.layered_map_bin] == expected

    @pytest.mark.parametrize("value, return_type, inverted, list_indexes, expected", [
        (0, aerospike.MAP_RETURN_VALUE, False, [3], e.OpNotApplicable),
        (0, aerospike.MAP_RETURN_VALUE, False, [1,0,0], e.OpNotApplicable),
        (0, aerospike.MAP_RETURN_VALUE, False, 'teddy', e.ParamError),
    ])
    def test_ctx_map_get_by_value_negative(self, value, return_type, inverted, list_indexes, expected):
        """
        Invoke map_get_by_value() to get the value in the map with expected failures.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_map_index(place))

        ops = [
            map_operations.map_get_by_value(self.nested_map_bin, value, return_type, inverted, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("ctx_types, value_start, value_end, return_type, inverted, list_indexes, expected", [
        ([map_index], 'v1', 'v4', aerospike.MAP_RETURN_VALUE, False, [0], ['v3', 'v1', 'v2']),
        ([map_index], 'v5', 'v9', aerospike.MAP_RETURN_VALUE, False, [2], ['v8', 'v7']),
        ([map_key, map_key], 'v11', 'v12', aerospike.MAP_RETURN_VALUE, False, [3, 10], ['v11']),
        ([map_index], 'v5', 'v9', aerospike.MAP_RETURN_VALUE, True, [2], ['v9', {11: 'v11'}]),
    ])
    def test_ctx_map_get_by_value_range(self, ctx_types, value_start, value_end, return_type, inverted, list_indexes, expected):
        """
        Invoke map_get_by_value_range to get the elements between value_start and value_end.
        """
        ctx = []
        for x in range(0, len(list_indexes)):
            ctx.append(add_ctx_op(ctx_types[x], list_indexes[x]))

        ops = [
            map_operations.map_get_by_value_range(self.num_map_bin, value_start, value_end, return_type, inverted, ctx)
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        assert res[self.num_map_bin] == expected

    @pytest.mark.parametrize("value_start, value_end, return_type, inverted, list_indexes, expected", [
        ('v0', 'v4', aerospike.MAP_RETURN_VALUE, False, [3], e.OpNotApplicable),
        ('v0', 'v4', aerospike.MAP_RETURN_VALUE, False, [1,0,0], e.OpNotApplicable),
        ('v0', 'v4', aerospike.MAP_RETURN_VALUE, False, 'bad_cdt_types', e.ParamError),
    ])
    def test_ctx_map_get_by_value_range_negative(self, value_start, value_end, return_type, inverted, list_indexes, expected):
        """
        Invoke map_get_by_value_range to get the elements between value_start and value_end with expected failures.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_map_index(place))

        ops = [
            map_operations.map_get_by_value_range(self.nested_map_bin, value_start, value_end, return_type, inverted, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("ctx_types, values, return_type, inverted, list_indexes, expected", [
        ([map_index], ['hi', 'hello'], aerospike.MAP_RETURN_VALUE, False, [0], ['hello', 'hi']),
        ([map_index], ['hello'], aerospike.MAP_RETURN_VALUE, False, [0], ['hello']),
        ([map_value], [{4,5,6}, 100], aerospike.MAP_RETURN_VALUE, False, [{'nested': {4,5,6,}, 'hundred': 100}], [100, {4,5,6}]),
        ([map_index], ['dog'], aerospike.MAP_RETURN_VALUE, False, [1], []),
        ([map_index, map_key], ['dog', ['bird']], aerospike.MAP_RETURN_VALUE, True, [2,'one'], [{'horse': 'shoe', 'fish': 'pond'}]),
    ])
    def test_ctx_map_get_by_value_list(self, ctx_types, values, return_type, inverted, list_indexes, expected):
        """
        Invoke map_get_by_value_list to get the provided values from a map.
        """
        ctx = []
        for x in range(0, len(list_indexes)) :
            ctx.append(add_ctx_op(ctx_types[x], list_indexes[x]))

        ops = [
            map_operations.map_get_by_value_list(self.nested_map_bin, values, return_type, inverted, ctx)
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        res = res[self.nested_map_bin]

        assert res == expected

    @pytest.mark.parametrize("values, return_type, inverted, list_indexes, expected", [
        ('greet', aerospike.MAP_RETURN_VALUE, False, [3], e.OpNotApplicable),
        ('nested', aerospike.MAP_RETURN_VALUE, False, [1,0,0], e.OpNotApplicable),
        ('greet', aerospike.MAP_RETURN_VALUE, False, 'teddy', e.ParamError),
    ])
    def test_ctx_map_get_by_value_list_negative(self, values, return_type, inverted, list_indexes, expected):
        """
        Invoke map_get_by_value_list to get the provided values from a map with expected failures.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_map_index(place))

        ops = [
            map_operations.map_get_by_value_list(self.nested_map_bin, values, return_type, inverted, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("ctx_types, rank, return_type, list_indexes, expected", [
        ([map_index], 1, aerospike.MAP_RETURN_VALUE, [0], 'hi'),
        ([map_index], 0, aerospike.MAP_RETURN_VALUE, [0], 'hello'),
        ([map_index], 1, aerospike.MAP_RETURN_VALUE, [1], {4,5,6}),
        ([map_index, map_index, map_index], 0, aerospike.MAP_RETURN_VALUE, [2,0,0], 'pond'),
        ([map_key], 1, aerospike.MAP_RETURN_INDEX, ['second'], 1)
    ])
    def test_ctx_map_get_by_rank(self, ctx_types, rank, return_type, list_indexes, expected):
        """
        Invoke map_get_by_rank to get the entry with the given rank.
        """
        ctx = []
        for x in range(0, len(list_indexes)) :
            ctx.append(add_ctx_op(ctx_types[x], list_indexes[x]))

        ops = [
            map_operations.map_get_by_rank(self.nested_map_bin, rank, return_type, ctx)
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        assert res[self.nested_map_bin] == expected

    @pytest.mark.parametrize("rank, return_type, list_indexes, expected", [
        (1, aerospike.MAP_RETURN_VALUE, [3], e.OpNotApplicable),
        (1, aerospike.MAP_RETURN_VALUE, [1,0,0], e.OpNotApplicable),
        (1, aerospike.MAP_RETURN_VALUE, 'teddy', e.ParamError),
        (6, aerospike.MAP_RETURN_VALUE, [1], e.OpNotApplicable),
    ])
    def test_ctx_map_get_by_rank_negative(self, rank, return_type, list_indexes, expected):
        """
        Invoke map_get_by_rank to get the entry with the given rank with expected failures.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_map_rank(place))

        ops = [
            map_operations.map_get_by_rank(self.nested_map_bin, rank, return_type, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("ctx_types, rank, rmv_count, return_type, inverted, list_indexes, expected", [
        ([map_index], 0, 4, aerospike.MAP_RETURN_VALUE, False, [0], ['v1', 'v2', 'v3']),
        ([map_index], 0, 2, aerospike.MAP_RETURN_VALUE, False, [2], ['v7', 'v8']),
        ([map_key, map_key], 0, 1, aerospike.MAP_RETURN_VALUE, False, [3, 10], ['v11']),
        ([map_index], 0, 2, aerospike.MAP_RETURN_VALUE, True, [2], ['v9', {11: 'v11'}]),
    ])
    def test_ctx_map_get_by_rank_range(self, ctx_types, rank, rmv_count, return_type, inverted, list_indexes, expected):
        """
        Invoke map_get_by_rank_range to get values starting at rank for rmv_count.
        """
        ctx = []
        for x in range(0, len(list_indexes)) :
            ctx.append(add_ctx_op(ctx_types[x], list_indexes[x]))

        ops = [
            map_operations.map_get_by_rank_range(self.num_map_bin, rank, rmv_count, return_type, inverted, ctx)
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        assert res[self.num_map_bin] == expected

    @pytest.mark.parametrize("rank, rmv_count, return_type, inverted, list_indexes, expected", [
        (1, 1, aerospike.MAP_RETURN_VALUE, False, [3], e.OpNotApplicable),
        (1, 1, aerospike.MAP_RETURN_VALUE, False, [1,0,0], e.OpNotApplicable),
        (1, 1, aerospike.MAP_RETURN_VALUE, False, 'teddy', e.ParamError),
        (1, 'bad_rmv_count', aerospike.MAP_RETURN_VALUE, False, [1], e.ParamError),
        (['bad_rank'], 1, aerospike.MAP_RETURN_VALUE, False, [1], e.ParamError),
    ])
    def test_ctx_map_get_by_rank_range_negative(self, rank, rmv_count, return_type, inverted, list_indexes, expected):
        """
        Invoke map_get_by_rank_range to get values starting at rank for rmv_count with expected failures.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_map_rank(place))

        ops = [
            map_operations.map_get_by_rank_range(self.nested_map_bin, rank, rmv_count, return_type, inverted, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("ctx_types, policy, map_indexes, expected", [
        ([map_key], {'map_order': aerospike.MAP_KEY_VALUE_ORDERED}, ['first'],
        {3: 'hello', 'greet': 'hi', 'numbers': [3,1,2]}),
    ])
    def test_ctx_map_set_policy(self, ctx_types, policy, map_indexes, expected):
        """
        Invoke map_set_policy() to apply a map policy to a nested map.
        """
        ctx = []
        for x in range(0, len(map_indexes)) :
            ctx.append(add_ctx_op(ctx_types[x], map_indexes[x]))

        ops = [
            map_operations.map_set_policy(self.nested_map_bin, policy, ctx),
            map_operations.map_get_by_key(self.nested_map_bin, 'first', aerospike.MAP_RETURN_VALUE)
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        _, _, bins = self.as_connection.get(self.test_key)

        assert res[self.nested_map_bin] == expected

    @pytest.mark.parametrize("ctx_types, policy, map_indexes, expected", [
        (0, {'map_order': aerospike.MAP_UNORDERED}, [3], e.OpNotApplicable),
        (0, {'map_order': aerospike.MAP_UNORDERED}, [1,0,0], e.OpNotApplicable),
        (0, {'map_order': aerospike.MAP_UNORDERED}, 'teddy', e.ParamError),
        (0, 'bad_policy', [0], e.ParamError),
    ])
    def test_ctx_map_set_policy_negative(self, ctx_types, policy, map_indexes, expected):
        """
        Invoke map_set_policy() to apply a map policy to a nested map with expected failures.
        """
        ctx = []
        for place in map_indexes:
            ctx.append(cdt_ctx.cdt_ctx_map_index(place))

        ops = [
            map_operations.map_set_policy(self.nested_map_bin, policy, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("ctx_types, key, value, map_policy, map_indexes, expected_val, expected_bin", [
        ([map_index, map_rank, map_key], 3, 4, None, [0,0,1], 3, {'second': {'two': {2: {'l': 'bye', 'g': 'layer'}}},
         'first': {'one': {1: {3: 4, 'l': 'done', 'g': 'layer'}}}}),
        ([map_index, map_rank, map_key], 'here', 'place', None, [1,0,2], 3, {'second': {'two': {2: {'here': 'place', 'l': 'bye', 'g': 'layer'}}},
         'first': {'one': {1: {'l': 'done', 'g': 'layer'}}}}),
        ([map_index, map_rank], 'here', 'place', None, [1,0], 2, {'second': {'two': {2: {'l': 'bye', 'g': 'layer'}, 'here': 'place'}},
         'first': {'one': {1: {'l': 'done', 'g': 'layer'}}}}),
    ])
    def test_ctx_map_put(self, ctx_types, key, value, map_policy, map_indexes, expected_val, expected_bin):
        """
        Invoke map_put() to place a value at key in a nested map.
        """
        ctx = []
        for x in range(0, len(map_indexes)) :
            ctx.append(add_ctx_op(ctx_types[x], map_indexes[x]))

        ops = [
            map_operations.map_put(self.layered_map_bin, key, value, map_policy, ctx)
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        assert res[self.layered_map_bin] == expected_val
        _, _, bins = self.as_connection.get(self.test_key)
        assert bins[self.layered_map_bin] == expected_bin

    @pytest.mark.parametrize("ctx_types, key, value, map_policy, map_indexes, expected", [
        ([map_index, map_rank, map_key], 3, 4, None, [0,0,3], e.OpNotApplicable),
        ([map_index], 3, 4, {'map_write_flags': aerospike.MAP_WRITE_FLAGS_UPDATE_ONLY}, [0], e.ElementNotFoundError),
    ])
    def test_ctx_map_put_negative(self, ctx_types, key, value, map_policy, map_indexes, expected):
        """
        Invoke map_put() to place a value at key in a nested map with expected failures.
        """
        ctx = []
        for x in range(0, len(map_indexes)) :
            ctx.append(add_ctx_op(ctx_types[x], map_indexes[x]))

        ops = [
            map_operations.map_put(self.layered_map_bin, key, value, map_policy, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("ctx_types, values, map_policy, map_indexes, expected_val, expected_bin", [
        ([map_index, map_rank, map_key], {3: 4}, None, [0,0,1], 3, {'second': {'two': {2: {'l': 'bye', 'g': 'layer'}}},
         'first': {'one': {1: {3: 4, 'l': 'done', 'g': 'layer'}}}}),
        ([map_index, map_rank, map_key], {3: 4, 'here': 'place'}, None, [1,0,2], 4, {'second': {'two': {2: {3: 4, 'here': 'place', 'l': 'bye', 'g': 'layer'}}},
         'first': {'one': {1: {'l': 'done', 'g': 'layer'}}}}),
        ([map_index, map_rank], {'here': 'place', 1: 2}, None, [1,0], 3, {'second': {'two': {2: {'l': 'bye', 'g': 'layer'}, 'here': 'place', 1: 2}},
         'first': {'one': {1: {'l': 'done', 'g': 'layer'}}}}),
    ])
    def test_ctx_map_put_items(self, ctx_types, values, map_policy, map_indexes, expected_val, expected_bin):
        """
        Invoke map_put_items on nested maps
        """
        ctx = []
        for x in range(0, len(map_indexes)) :
            ctx.append(add_ctx_op(ctx_types[x], map_indexes[x]))

        ops = [
            map_operations.map_put_items(self.layered_map_bin, values, map_policy, ctx)
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        assert res[self.layered_map_bin] == expected_val
        _, _, bins = self.as_connection.get(self.test_key)
        assert bins[self.layered_map_bin] == expected_bin

    @pytest.mark.parametrize("ctx_types, values, map_policy, map_indexes, expected", [
        ([map_index, map_rank, map_key], {3: 4}, None, [0,0,3], e.OpNotApplicable),
        ([map_index], {3: 4}, {'map_write_flags': aerospike.MAP_WRITE_FLAGS_UPDATE_ONLY}, [0], e.ElementNotFoundError),
    ])
    def test_ctx_map_put_items_negative(self, ctx_types, values, map_policy, map_indexes, expected):
        """
        Invoke map_put on nested maps with expected failure
        """
        ctx = []
        for x in range(0, len(map_indexes)) :
            ctx.append(add_ctx_op(ctx_types[x], map_indexes[x]))

        ops = [
            map_operations.map_put_items(self.layered_map_bin, values, map_policy, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("ctx_types, key, amount, map_policy, map_indexes, expected_bin", [
        ([map_index, map_rank, map_key], 1, 27, None, [0,0,1], {'second': {'two': {2: {'l': 'bye', 'g': 'layer'}}},
         'first': {'one': {1: {1: 27, 'l': 'done', 'g': 'layer'}}}}),
        ([map_index, map_rank], 56, 211, None, [0,0], {'second': {'two': {2: {'l': 'bye', 'g': 'layer'}}},
         'first': {'one': {1: {'l': 'done', 'g': 'layer'}, 56: 211}}}),
        ([map_index], 40, 2, None, [1], {'second': {40: 2, 'two': {2: {'l': 'bye', 'g': 'layer'}}},
         'first': {'one': {1: {'l': 'done', 'g': 'layer'}}}}),
    ])
    def test_ctx_map_increment(self, ctx_types, key, amount, map_policy, map_indexes, expected_bin):
        """
        Invoke map_increment to increment an element in a nested map.
        """
        ctx = []
        for x in range(0, len(map_indexes)) :
            ctx.append(add_ctx_op(ctx_types[x], map_indexes[x]))

        ops = [
            map_operations.map_increment(self.layered_map_bin, key, amount, map_policy, ctx)
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        _, _, bins = self.as_connection.get(self.test_key)
        assert bins[self.layered_map_bin] == expected_bin

    @pytest.mark.parametrize("ctx_types, key, amount, map_policy, map_indexes, expected", [
        ([map_index, map_rank, map_key], 'l', 27, None, [0,0,1], e.InvalidRequest),
        ([map_key], 'one', 27, None, ['first'], e.InvalidRequest),
        ([map_key], 20, 27, {'map_write_flags': aerospike.MAP_WRITE_FLAGS_UPDATE_ONLY}, ['first'], e.ElementNotFoundError), #why does this fail?
    ])
    def test_ctx_map_increment_negative(self, ctx_types, key, amount, map_policy, map_indexes, expected):
        """
        Invoke map_increment on nested maps with expected failure.
        """
        if map_policy is not None:
            pytest.xfail("map_increment does not support map_write_flags see: PROD-806")
        
        ctx = []
        for x in range(0, len(map_indexes)) :
            ctx.append(add_ctx_op(ctx_types[x], map_indexes[x]))

        ops = [
            map_operations.map_increment(self.layered_map_bin, key, amount, map_policy, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("ctx_types, key, amount, map_policy, map_indexes, expected_bin", [
        ([map_index], 'hundred', 27, None, [1], { 'first': {'greet': 'hi', 'numbers': [3, 1, 2], 3: 'hello'},
         'second': {'nested': {4,5,6,}, 'hundred': 73},
         'third': {'one': {'cat': 'dog', 'barn': {'horse': 'shoe', 'fish': 'pond'}, 'cage': ['bird']}, 'two': []}}),
        ([map_index, map_rank, map_key], 'new', 10, None, [2,1,'barn'], { 'first': {'greet': 'hi', 'numbers': [3, 1, 2], 3: 'hello'},
         'second': {'nested': {4,5,6,}, 'hundred': 100},
         'third': {'one': {'cat': 'dog', 'barn': {'horse': 'shoe', 'fish': 'pond', 'new': -10}, 'cage': ['bird']}, 'two': []}}),
        ([map_index, map_key], 2, 50, None, [2,'one'], { 'first': {'greet': 'hi', 'numbers': [3, 1, 2], 3: 'hello'},
         'second': {'nested': {4,5,6,}, 'hundred': 100},
         'third': {'one': {'cat': 'dog', 'barn': {'horse': 'shoe', 'fish': 'pond'}, 'cage': ['bird'], 2: -50}, 'two': []}}),
    ])
    def test_ctx_map_decrement(self, ctx_types, key, amount, map_policy, map_indexes, expected_bin):
        """
        Invoke map_decrement to decrement an element in a nested map.
        """
        ctx = []
        for x in range(0, len(map_indexes)) :
            ctx.append(add_ctx_op(ctx_types[x], map_indexes[x]))

        ops = [
            map_operations.map_decrement(self.nested_map_bin, key, amount, map_policy, ctx)
        ]

        _, _, _ = self.as_connection.operate(self.test_key, ops)
        _, _, bins = self.as_connection.get(self.test_key)
        assert bins[self.nested_map_bin] == expected_bin

    @pytest.mark.parametrize("ctx_types, key, amount, map_policy, map_indexes, expected", [
        ([map_index, map_rank, map_key], 'l', 27, None, [0,0,1], e.InvalidRequest),
        ([map_key], 'one', 27, None, ['first'], e.InvalidRequest),
        ([map_key], 20, 27, {'map_write_flags': aerospike.MAP_WRITE_FLAGS_UPDATE_ONLY}, ['first'], e.ElementNotFoundError), #why does this fail?
    ])
    def test_ctx_map_decrement_negative(self, ctx_types, key, amount, map_policy, map_indexes, expected):
        """
        Invoke map_decrement on nested maps with expected failure.
        """
        if map_policy is not None:
            pytest.xfail("map_decrement does not support map_write_flags see: PROD-806")

        ctx = []
        for x in range(0, len(map_indexes)) :
            ctx.append(add_ctx_op(ctx_types[x], map_indexes[x]))

        ops = [
            map_operations.map_decrement(self.layered_map_bin, key, amount, map_policy, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("ctx_types, map_indexes, expected", [
        ([map_index], [0], 1),
        ([map_index, map_rank, map_value], [0,0, {'g': 'layer', 'l': 'done'}], 2),
        ([map_index, map_index], [1,0], 1),
    ])
    def test_ctx_map_size(self, ctx_types, map_indexes, expected):
        """
        Invoke map_size() to get the size of a nested map.
        """
        ctx = []
        for x in range(0, len(map_indexes)) :
            ctx.append(add_ctx_op(ctx_types[x], map_indexes[x]))

        ops = [
            map_operations.map_size(self.layered_map_bin, ctx)
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        assert res[self.layered_map_bin] == expected

    @pytest.mark.parametrize("ctx_types, map_indexes, expected", [
        ([map_index], [3], e.OpNotApplicable),
        ([map_index, map_rank, map_value], [0,0, {'dog': 'cat'}], e.OpNotApplicable),
        ([map_index, map_index, map_index, map_index], [1,0,0,0], e.InvalidRequest),
    ])
    def test_ctx_map_size_negative(self, ctx_types, map_indexes, expected):
        """
        Invoke map_size() on a nested map with expected failure.
        """
        ctx = []
        for x in range(0, len(map_indexes)) :
            ctx.append(add_ctx_op(ctx_types[x], map_indexes[x]))

        ops = [
            map_operations.map_size(self.layered_map_bin, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("ctx_types, map_indexes, expected", [
        ([map_index],  [1], { 'first': {'greet': 'hi', 'numbers': [3, 1, 2], 3: 'hello'},
         'second': {},
         'third': {'one': {'cat': 'dog', 'barn': {'horse': 'shoe', 'fish': 'pond'}, 'cage': ['bird']}, 'two': []}}),
        ([map_index, map_key],  [2,'one'], { 'first': {'greet': 'hi', 'numbers': [3, 1, 2], 3: 'hello'},
         'second': {'nested': {4,5,6,}, 'hundred': 100},
         'third': {'one': {}, 'two': []}}),
        ([map_index, map_key, map_value],  [2,'one', {'horse': 'shoe', 'fish': 'pond'}],
         { 'first': {'greet': 'hi', 'numbers': [3, 1, 2], 3: 'hello'},
         'second': {'nested': {4,5,6,}, 'hundred': 100},
         'third': {'one': {'cat': 'dog', 'barn': {}, 'cage': ['bird']}, 'two': []}}),
    ])
    def test_ctx_map_clear(self, ctx_types, map_indexes, expected):
        """
        Invoke map_clear to empty a nested map.
        """
        ctx = []
        for x in range(0, len(map_indexes)) :
            ctx.append(add_ctx_op(ctx_types[x], map_indexes[x]))

        ops = [
            map_operations.map_clear(self.nested_map_bin, ctx)
        ]

        _, _, _ = self.as_connection.operate(self.test_key, ops)
        _, _, bins = self.as_connection.get(self.test_key)
        assert bins[self.nested_map_bin] == expected

    @pytest.mark.parametrize("ctx_types, map_indexes, expected", [
        ([map_index],  [3], e.OpNotApplicable),
        ([map_index, map_key],  [2, 'bad_val'], e.OpNotApplicable),
        ([map_index, map_key, map_value],  [2,'one', {'horse': 'shoe', 'fish': 'john'}], e.OpNotApplicable)
    ])
    def test_ctx_map_clear_negative(self, ctx_types, map_indexes, expected):
        """
        Invoke map_clear on nested maps with expected failure.
        """
        ctx = []
        for x in range(0, len(map_indexes)) :
            ctx.append(add_ctx_op(ctx_types[x], map_indexes[x]))

        ops = [
            map_operations.map_clear(self.nested_map_bin, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("ctx_types, key, return_type, list_indexes, expected_val, expected_bin", [
        ([map_index], 'greet', aerospike.MAP_RETURN_VALUE, [0], 'hi', { 'first': {'numbers': [3, 1, 2], 3: 'hello'},
         'second': {'nested': {4,5,6,}, 'hundred': 100}, 'third': {'one': {'cat': 'dog', 'barn': {'horse': 'shoe', 'fish': 'pond'},
          'cage': ['bird']}, 'two': []}}),
        ([map_index], 3, aerospike.MAP_RETURN_VALUE, [0], 'hello', { 'first': {'greet': 'hi', 'numbers': [3, 1, 2]},
         'second': {'nested': {4,5,6,}, 'hundred': 100}, 'third': {'one': {'cat': 'dog', 'barn': {'horse': 'shoe', 'fish': 'pond'},
         'cage': ['bird']}, 'two': []}}),
        ([map_index], 'nested', aerospike.MAP_RETURN_VALUE, [1], {4,5,6}, { 'first': {'greet': 'hi', 'numbers': [3, 1, 2], 3: 'hello'},
         'second': {'hundred': 100}, 'third': {'one': {'cat': 'dog', 'barn': {'horse': 'shoe', 'fish': 'pond'},
         'cage': ['bird']}, 'two': []}}),
        ([map_index], 'dog', aerospike.MAP_RETURN_VALUE, [1], None, { 'first': {'greet': 'hi', 'numbers': [3, 1, 2], 3: 'hello'},
         'second': {'nested': {4,5,6,}, 'hundred': 100}, 'third': {'one': {'cat': 'dog', 'barn': {'horse': 'shoe', 'fish': 'pond'},
         'cage': ['bird']}, 'two': []}}),
        ([map_index, map_index, map_index], 'fish', aerospike.MAP_RETURN_VALUE, [2,0,0], 'pond',
         { 'first': {'greet': 'hi', 'numbers': [3, 1, 2], 3: 'hello'}, 'second': {'nested': {4,5,6,}, 'hundred': 100},
         'third': {'one': {'cat': 'dog', 'barn': {'horse': 'shoe'}, 'cage': ['bird']}, 'two': []}}),
        ([map_key], 'nested', aerospike.MAP_RETURN_INDEX, ['second'], 1, { 'first': {'greet': 'hi', 'numbers': [3, 1, 2], 3: 'hello'},
         'second': { 'hundred': 100}, 'third': {'one': {'cat': 'dog', 'barn': {'horse': 'shoe', 'fish': 'pond'},
         'cage': ['bird']}, 'two': []}})
    ])
    def test_ctx_map_remove_by_key(self, ctx_types, key, return_type, list_indexes, expected_val, expected_bin):
        """
        Invoke map_remove_by_key() to remove an element at key.
        """
        ctx = []
        for x in range(0, len(list_indexes)) :
            ctx.append(add_ctx_op(ctx_types[x], list_indexes[x]))

        ops = [
            map_operations.map_remove_by_key(self.nested_map_bin, key, return_type, ctx)
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        assert res[self.nested_map_bin] == expected_val
        _, _, bins = self.as_connection.get(self.test_key)
        assert bins[self.nested_map_bin] == expected_bin

    @pytest.mark.parametrize("key, return_type, list_indexes, expected", [
        ('greet', aerospike.MAP_RETURN_VALUE, [3], e.OpNotApplicable),
        ('nested', aerospike.MAP_RETURN_VALUE, [1,0,0], e.OpNotApplicable),
        ('greet', aerospike.MAP_RETURN_VALUE, 'teddy', e.ParamError),
    ])
    def test_ctx_map_remove_by_key_negative(self, key, return_type, list_indexes, expected):
        """
        Invoke map_remove_by_key() with expected failures.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_map_index(place))

        ops = [
            map_operations.map_remove_by_key(self.nested_map_bin, key, return_type, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("ctx_types, key, return_type, inverted, list_indexes, expected_val, expected_bin", [
        ([map_index], ['greet'], aerospike.MAP_RETURN_VALUE, False, [0], ['hi'], { 'first': {'numbers': [3, 1, 2], 3: 'hello'},
         'second': {'nested': {4,5,6,}, 'hundred': 100}, 'third': {'one': {'cat': 'dog', 'barn': {'horse': 'shoe', 'fish': 'pond'},
          'cage': ['bird']}, 'two': []}}),
        ([map_index], ['numbers', 3], aerospike.MAP_RETURN_VALUE, False, [0], ['hello', [3,1,2]], { 'first': {'greet': 'hi',},
         'second': {'nested': {4,5,6,}, 'hundred': 100}, 'third': {'one': {'cat': 'dog', 'barn': {'horse': 'shoe', 'fish': 'pond'},
         'cage': ['bird']}, 'two': []}}),
        ([map_index], ['nested', 'hundred'], aerospike.MAP_RETURN_VALUE, False, [1], [100, {4,5,6}], { 'first': {'greet': 'hi', 'numbers': [3, 1, 2], 3: 'hello'},
         'second': {}, 'third': {'one': {'cat': 'dog', 'barn': {'horse': 'shoe', 'fish': 'pond'},
         'cage': ['bird']}, 'two': []}}),
        ([map_index], ['dog'], aerospike.MAP_RETURN_VALUE, False, [1], [], { 'first': {'greet': 'hi', 'numbers': [3, 1, 2], 3: 'hello'},
         'second': {'nested': {4,5,6,}, 'hundred': 100}, 'third': {'one': {'cat': 'dog', 'barn': {'horse': 'shoe', 'fish': 'pond'},
         'cage': ['bird']}, 'two': []}}),
        ([map_index, map_index, map_index], ['horse', 'fish'], aerospike.MAP_RETURN_VALUE, False, [2,0,0], ['pond', 'shoe'],
         { 'first': {'greet': 'hi', 'numbers': [3, 1, 2], 3: 'hello'}, 'second': {'nested': {4,5,6,}, 'hundred': 100},
         'third': {'one': {'cat': 'dog', 'barn': {}, 'cage': ['bird']}, 'two': []}}),
        ([map_key], ['nested'], aerospike.MAP_RETURN_INDEX, True, ['second'], [0], { 'first': {'greet': 'hi', 'numbers': [3, 1, 2], 3: 'hello'},
         'second': { 'nested': {4,5,6,}}, 'third': {'one': {'cat': 'dog', 'barn': {'horse': 'shoe', 'fish': 'pond'},
         'cage': ['bird']}, 'two': []}})
    ])
    def test_ctx_map_remove_by_key_list(self, ctx_types, key, return_type, inverted, list_indexes, expected_val, expected_bin):
        """
        Invoke map_remove_by_key_list() to remove the elements at the provided keys.
        """
        ctx = []
        for x in range(0, len(list_indexes)) :
            ctx.append(add_ctx_op(ctx_types[x], list_indexes[x]))

        ops = [
            map_operations.map_remove_by_key_list(self.nested_map_bin, key, return_type, inverted, ctx)
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        res = res[self.nested_map_bin]

        assert res == expected_val
        _, _, bins = self.as_connection.get(self.test_key)
        assert bins[self.nested_map_bin] == expected_bin

    @pytest.mark.parametrize("key, return_type, inverted, list_indexes, expected", [
        (['greet'], aerospike.MAP_RETURN_VALUE, False, [3], e.OpNotApplicable),
        (['nested'], aerospike.MAP_RETURN_VALUE, False, [1,0,0], e.OpNotApplicable),
        (['greet'], aerospike.MAP_RETURN_VALUE, False, 'teddy', e.ParamError),
    ])
    def test_ctx_map_remove_key_list_negative(self, key, return_type, inverted, list_indexes, expected):
        """
        Invoke map_remove_key_list_negative() with expected failures.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_map_index(place))

        ops = [
            map_operations.map_remove_by_key_list(self.nested_map_bin, key, return_type, inverted, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("ctx_types, key_start, key_end, return_type, inverted, list_indexes, expected_val, expected_bin", [
        ([map_index], 0, 4, aerospike.MAP_RETURN_VALUE, False, [0], ['v3', 'v1', 'v2'], {1: {},
         2: {4: 'v4', 5: 'v5', 6: 'v6'}, 3: {7: 'v7', 8: 'v8', 9: 'v9', 10: {11: 'v11'}}}),
        ([map_index], 5, 9, aerospike.MAP_RETURN_VALUE, False, [2], ['v8', 'v7'], {1: {1: 'v1', 2: 'v2', 3: 'v3'},
         2: {4: 'v4', 5: 'v5', 6: 'v6'}, 3: {9: 'v9',  10: {11: 'v11'}}}),
        ([map_key, map_key], 11, 12, aerospike.MAP_RETURN_VALUE, False, [3, 10], ['v11'], {1: {1: 'v1', 2: 'v2', 3: 'v3'},
         2: {4: 'v4', 5: 'v5', 6: 'v6'}, 3: {7: 'v7', 8: 'v8', 9: 'v9', 10: {}}}),
        ([map_index], 5, 9, aerospike.MAP_RETURN_VALUE, True, [2], ['v9', {11: 'v11'}], {1: {1: 'v1', 2: 'v2', 3: 'v3'},
         2: {4: 'v4', 5: 'v5', 6: 'v6'}, 3: {7: 'v7', 8: 'v8'}}),
    ])
    def test_ctx_map_remove_by_key_range(self, ctx_types, key_start, key_end, return_type, inverted, list_indexes, expected_val, expected_bin):
        """
        Invoke map_remove_by_key_range() to remove elements between key_start and key_end.
        """
        ctx = []
        for x in range(0, len(list_indexes)):
            ctx.append(add_ctx_op(ctx_types[x], list_indexes[x]))

        ops = [
            map_operations.map_remove_by_key_range(self.num_map_bin, key_start, key_end, return_type, inverted, ctx)
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        _, _, bins = self.as_connection.get(self.test_key)
        assert res[self.num_map_bin] == expected_val
        assert bins[self.num_map_bin] == expected_bin

    @pytest.mark.parametrize("key_start, key_end, return_type, inverted, list_indexes, expected", [
        (0, 4, aerospike.MAP_RETURN_VALUE, False, [3], e.OpNotApplicable),
        (0, 4, aerospike.MAP_RETURN_VALUE, False, [1,0,0], e.OpNotApplicable),
        (0, 4, aerospike.MAP_RETURN_VALUE, False, 'bad_cdt_types', e.ParamError),
    ])
    def test_ctx_map_remove_by_key_range_negative(self, key_start, key_end, return_type, inverted, list_indexes, expected):
        """
        Invoke map_remove_by_key_range() with expected failures.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_map_index(place))

        ops = [
            map_operations.map_remove_by_key_range(self.nested_map_bin, key_start, key_end, return_type, inverted, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("ctx_types, value, return_type, inverted, list_indexes, expected_val, expected_bin", [
        ([map_index], 'hi', aerospike.MAP_RETURN_VALUE, False, [0], ['hi'], { 'first': {'numbers': [3, 1, 2], 3: 'hello'},
         'second': {'nested': {4,5,6,}, 'hundred': 100}, 'third': {'one': {'cat': 'dog', 'barn': {'horse': 'shoe', 'fish': 'pond'},
          'cage': ['bird']}, 'two': []}}),
        ([map_index], 'hello', aerospike.MAP_RETURN_VALUE, False, [0], ['hello'], { 'first': {'greet': 'hi', 'numbers': [3, 1, 2]},
         'second': {'nested': {4,5,6,}, 'hundred': 100}, 'third': {'one': {'cat': 'dog', 'barn': {'horse': 'shoe', 'fish': 'pond'},
         'cage': ['bird']}, 'two': []}}),
        ([map_index], {4,5,6}, aerospike.MAP_RETURN_VALUE, False, [1], [{4,5,6}], { 'first': {'greet': 'hi', 'numbers': [3, 1, 2], 3: 'hello'},
         'second': {'hundred': 100}, 'third': {'one': {'cat': 'dog', 'barn': {'horse': 'shoe', 'fish': 'pond'},
         'cage': ['bird']}, 'two': []}}),
        ([map_index], 'dog', aerospike.MAP_RETURN_VALUE, False, [1], [], { 'first': {'greet': 'hi', 'numbers': [3, 1, 2], 3: 'hello'},
         'second': {'nested': {4,5,6,}, 'hundred': 100}, 'third': {'one': {'cat': 'dog', 'barn': {'horse': 'shoe', 'fish': 'pond'},
         'cage': ['bird']}, 'two': []}}),
        ([map_index, map_index, map_index], 'pond', aerospike.MAP_RETURN_VALUE, True, [2,0,0], ['shoe'],
         { 'first': {'greet': 'hi', 'numbers': [3, 1, 2], 3: 'hello'}, 'second': {'nested': {4,5,6,}, 'hundred': 100},
         'third': {'one': {'cat': 'dog', 'barn': {'fish': 'pond'}, 'cage': ['bird']}, 'two': []}}),
        ([map_key], 100, aerospike.MAP_RETURN_INDEX, False, ['second'], [0], { 'first': {'greet': 'hi', 'numbers': [3, 1, 2], 3: 'hello'},
         'second': {'nested': {4,5,6}}, 'third': {'one': {'cat': 'dog', 'barn': {'horse': 'shoe', 'fish': 'pond'},
         'cage': ['bird']}, 'two': []}})
    ])
    def test_ctx_map_remove_by_value(self, ctx_types, value, return_type, inverted, list_indexes, expected_val, expected_bin):
        """
        Invoke map_remove_by_value to remove the element with the given value.
        """
        ctx = []
        for x in range(0, len(list_indexes)) :
            ctx.append(add_ctx_op(ctx_types[x], list_indexes[x]))

        ops = [
            map_operations.map_remove_by_value(self.nested_map_bin, value, return_type, inverted, ctx)
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        assert res[self.nested_map_bin] == expected_val
        _, _, bins = self.as_connection.get(self.test_key)
        assert bins[self.nested_map_bin] == expected_bin

    @pytest.mark.parametrize("value, return_type, inverted, list_indexes, expected", [
        ('greet', aerospike.MAP_RETURN_VALUE, False, [3], e.OpNotApplicable),
        ('nested', aerospike.MAP_RETURN_VALUE, False, [1,0,0], e.OpNotApplicable),
        ('greet', aerospike.MAP_RETURN_VALUE, False, 'teddy', e.ParamError),
    ])
    def test_ctx_map_remove_by_value_negative(self, value, return_type, inverted, list_indexes, expected):
        """
        Invoke map_remove_by_value() with expected failures.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_map_index(place))

        ops = [
            map_operations.map_remove_by_value(self.nested_map_bin, value, return_type, inverted, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("ctx_types, values, return_type, inverted, list_indexes, expected_val, expected_bin", [
        ([map_index], ['hi', 'hello'], aerospike.MAP_RETURN_VALUE, False, [0], ['hello', 'hi'], { 'first': {'numbers': [3, 1, 2]},
         'second': {'nested': {4,5,6,}, 'hundred': 100}, 'third': {'one': {'cat': 'dog', 'barn': {'horse': 'shoe', 'fish': 'pond'},
          'cage': ['bird']}, 'two': []}}),
        ([map_index], ['hello'], aerospike.MAP_RETURN_VALUE, False, [0], ['hello'], { 'first': {'greet': 'hi', 'numbers': [3, 1, 2]},
         'second': {'nested': {4,5,6,}, 'hundred': 100}, 'third': {'one': {'cat': 'dog', 'barn': {'horse': 'shoe', 'fish': 'pond'},
         'cage': ['bird']}, 'two': []}}),
        ([map_value], [{4,5,6}, 100], aerospike.MAP_RETURN_VALUE, False, [{'nested': {4,5,6,}, 'hundred': 100}], [100, {4,5,6}], { 'first': {'greet': 'hi', 'numbers': [3, 1, 2], 3: 'hello'},
         'second': {}, 'third': {'one': {'cat': 'dog', 'barn': {'horse': 'shoe', 'fish': 'pond'},
         'cage': ['bird']}, 'two': []}}),
        ([map_index], ['dog'], aerospike.MAP_RETURN_VALUE, False, [1], [], { 'first': {'greet': 'hi', 'numbers': [3, 1, 2], 3: 'hello'},
         'second': {'nested': {4,5,6,}, 'hundred': 100}, 'third': {'one': {'cat': 'dog', 'barn': {'horse': 'shoe', 'fish': 'pond'},
         'cage': ['bird']}, 'two': []}}),
        ([map_index, map_key], ['dog', ['bird']], aerospike.MAP_RETURN_VALUE, True, [2,'one'], [{'horse': 'shoe', 'fish': 'pond'}],
         { 'first': {'greet': 'hi', 'numbers': [3, 1, 2], 3: 'hello'}, 'second': {'nested': {4,5,6,}, 'hundred': 100},
         'third': {'one': {'cat': 'dog', 'cage': ['bird']}, 'two': []}}),
    ])
    def test_ctx_map_remove_by_value_list(self, ctx_types, values, return_type, inverted, list_indexes, expected_val, expected_bin):
        """
        Invoke map_remove_by_value_list() to remove elements with the given values.
        """
        ctx = []
        for x in range(0, len(list_indexes)) :
            ctx.append(add_ctx_op(ctx_types[x], list_indexes[x]))

        ops = [
            map_operations.map_remove_by_value_list(self.nested_map_bin, values, return_type, inverted, ctx)
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        res = res[self.nested_map_bin]
        assert res == expected_val
        _, _, bins = self.as_connection.get(self.test_key)
        assert bins[self.nested_map_bin] == expected_bin

    @pytest.mark.parametrize("values, return_type, inverted, list_indexes, expected", [
        ('greet', aerospike.MAP_RETURN_VALUE, False, [3], e.OpNotApplicable),
        ('nested', aerospike.MAP_RETURN_VALUE, False, [1,0,0], e.OpNotApplicable),
        ('greet', aerospike.MAP_RETURN_VALUE, False, 'teddy', e.ParamError),
    ])
    def test_ctx_map_remove_by_value_list_negative(self, values, return_type, inverted, list_indexes, expected):
        """
        Invoke map_remove_by_value_list() with expected failures.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_map_index(place))

        ops = [
            map_operations.map_remove_by_value_list(self.nested_map_bin, values, return_type, inverted, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("ctx_types, value_start, value_end, return_type, inverted, list_indexes, expected_val, expected_bin", [
        ([map_index], 'v1', 'v4', aerospike.MAP_RETURN_VALUE, False, [0], ['v3', 'v1', 'v2'], {1: {},
         2: {4: 'v4', 5: 'v5', 6: 'v6'}, 3: {7: 'v7', 8: 'v8', 9: 'v9', 10: {11: 'v11'}}}),
        ([map_index], 'v5', 'v9', aerospike.MAP_RETURN_VALUE, False, [2], ['v8', 'v7'], {1: {1: 'v1', 2: 'v2', 3: 'v3'},
         2: {4: 'v4', 5: 'v5', 6: 'v6'}, 3: {9: 'v9',  10: {11: 'v11'}}}),
        ([map_key, map_key], 'v11', 'v12', aerospike.MAP_RETURN_VALUE, False, [3, 10], ['v11'], {1: {1: 'v1', 2: 'v2', 3: 'v3'},
         2: {4: 'v4', 5: 'v5', 6: 'v6'}, 3: {7: 'v7', 8: 'v8', 9: 'v9', 10: {}}}),
        ([map_index], 'v5', 'v9', aerospike.MAP_RETURN_VALUE, True, [2], ['v9', {11: 'v11'}], {1: {1: 'v1', 2: 'v2', 3: 'v3'},
         2: {4: 'v4', 5: 'v5', 6: 'v6'}, 3: {7: 'v7', 8: 'v8'}}),
    ])
    def test_ctx_map_remove_by_value_range(self, ctx_types, value_start, value_end, return_type, inverted, list_indexes, expected_val, expected_bin):
        """
        Invoke map_remove_by_value_range to remove elements with values between value_start and value_end.
        """
        ctx = []
        for x in range(0, len(list_indexes)):
            ctx.append(add_ctx_op(ctx_types[x], list_indexes[x]))

        ops = [
            map_operations.map_remove_by_value_range(self.num_map_bin, value_start, value_end, return_type, inverted, ctx)
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        _, _, bins = self.as_connection.get(self.test_key)
        assert res[self.num_map_bin] == expected_val
        assert bins[self.num_map_bin] == expected_bin

    @pytest.mark.parametrize("value_start, value_end, return_type, inverted, list_indexes, expected", [
        ('v0', 'v4', aerospike.MAP_RETURN_VALUE, False, [3], e.OpNotApplicable),
        ('v0', 'v4', aerospike.MAP_RETURN_VALUE, False, [1,0,0], e.OpNotApplicable),
        ('v0', 'v4', aerospike.MAP_RETURN_VALUE, False, 'bad_cdt_types', e.ParamError),
    ])
    def test_ctx_map_remove_by_value_range_negative(self, value_start, value_end, return_type, inverted, list_indexes, expected):
        """
        Invoke map_remove_by_value_range on a nested map with expected failures.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_map_index(place))

        ops = [
            map_operations.map_remove_by_value_range(self.nested_map_bin, value_start, value_end, return_type, inverted, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("ctx_types, index, return_type, list_indexes, expected_val, expected_bin", [
        ([map_index], 1, aerospike.MAP_RETURN_VALUE, [0], 'hi', { 'first': {'numbers': [3, 1, 2], 3: 'hello'},
         'second': {'nested': {4,5,6,}, 'hundred': 100}, 'third': {'one': {'cat': 'dog', 'barn': {'horse': 'shoe', 'fish': 'pond'},
          'cage': ['bird']}, 'two': []}}),
        ([map_index], 0, aerospike.MAP_RETURN_VALUE, [0], 'hello', { 'first': {'greet': 'hi', 'numbers': [3, 1, 2]},
         'second': {'nested': {4,5,6,}, 'hundred': 100}, 'third': {'one': {'cat': 'dog', 'barn': {'horse': 'shoe', 'fish': 'pond'},
         'cage': ['bird']}, 'two': []}}),
        ([map_index], 1, aerospike.MAP_RETURN_VALUE, [1], {4,5,6}, { 'first': {'greet': 'hi', 'numbers': [3, 1, 2], 3: 'hello'},
         'second': {'hundred': 100}, 'third': {'one': {'cat': 'dog', 'barn': {'horse': 'shoe', 'fish': 'pond'},
         'cage': ['bird']}, 'two': []}}),
        ([map_index, map_index, map_index], 0, aerospike.MAP_RETURN_VALUE, [2,0,0], 'pond',
         { 'first': {'greet': 'hi', 'numbers': [3, 1, 2], 3: 'hello'}, 'second': {'nested': {4,5,6,}, 'hundred': 100},
         'third': {'one': {'cat': 'dog', 'barn': {'horse': 'shoe'}, 'cage': ['bird']}, 'two': []}}),
        ([map_key], 1, aerospike.MAP_RETURN_INDEX, ['second'], 1, { 'first': {'greet': 'hi', 'numbers': [3, 1, 2], 3: 'hello'},
         'second': { 'hundred': 100}, 'third': {'one': {'cat': 'dog', 'barn': {'horse': 'shoe', 'fish': 'pond'},
         'cage': ['bird']}, 'two': []}})
    ])
    def test_ctx_map_remove_by_index(self, ctx_types, index, return_type, list_indexes, expected_val, expected_bin):
        """
        Invoke map_remove_by_index() to remove the element at index.
        """
        ctx = []
        for x in range(0, len(list_indexes)) :
            ctx.append(add_ctx_op(ctx_types[x], list_indexes[x]))

        ops = [
            map_operations.map_remove_by_index(self.nested_map_bin, index, return_type, ctx)
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        assert res[self.nested_map_bin] == expected_val
        _, _, bins = self.as_connection.get(self.test_key)
        assert bins[self.nested_map_bin] == expected_bin

    @pytest.mark.parametrize("index, return_type, list_indexes, expected", [
        (1, aerospike.MAP_RETURN_VALUE, [3], e.OpNotApplicable),
        (1, aerospike.MAP_RETURN_VALUE, [1,0,0], e.OpNotApplicable),
        (1, aerospike.MAP_RETURN_VALUE, 'teddy', e.ParamError),
        (6, aerospike.MAP_RETURN_VALUE, [1], e.OpNotApplicable),
    ])
    def test_ctx_map_remove_by_index_negative(self, index, return_type, list_indexes, expected):
        """
        Invoke map_remove_by_index() with expected failures.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_map_index(place))

        ops = [
            map_operations.map_remove_by_index(self.nested_map_bin, index, return_type, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("ctx_types, index, rmv_count, return_type, inverted, list_indexes, expected_val, expected_bin", [
        ([map_index], 1, 1, aerospike.MAP_RETURN_VALUE, False, [0], ['hi'], { 'first': {'numbers': [3, 1, 2], 3: 'hello'},
         'second': {'nested': {4,5,6,}, 'hundred': 100}, 'third': {'one': {'cat': 'dog', 'barn': {'horse': 'shoe', 'fish': 'pond'},
          'cage': ['bird']}, 'two': []}}),
        ([map_index], 0, 3, aerospike.MAP_RETURN_VALUE, False, [0], ['hello', 'hi', [3,1,2]], { 'first': {},
         'second': {'nested': {4,5,6,}, 'hundred': 100}, 'third': {'one': {'cat': 'dog', 'barn': {'horse': 'shoe', 'fish': 'pond'},
         'cage': ['bird']}, 'two': []}}),
        ([map_index], 0, 2, aerospike.MAP_RETURN_VALUE, False, [1], [100, {4,5,6}], { 'first': {'greet': 'hi', 'numbers': [3, 1, 2], 3: 'hello'},
         'second': {}, 'third': {'one': {'cat': 'dog', 'barn': {'horse': 'shoe', 'fish': 'pond'},
         'cage': ['bird']}, 'two': []}}),
        ([map_index, map_index, map_index], 0, 2, aerospike.MAP_RETURN_VALUE, False, [2,0,0], ['pond', 'shoe'],
         { 'first': {'greet': 'hi', 'numbers': [3, 1, 2], 3: 'hello'}, 'second': {'nested': {4,5,6,}, 'hundred': 100},
         'third': {'one': {'cat': 'dog', 'barn': {}, 'cage': ['bird']}, 'two': []}}),
        ([map_key], 1, 2, aerospike.MAP_RETURN_INDEX, True, ['second'], [0], { 'first': {'greet': 'hi', 'numbers': [3, 1, 2], 3: 'hello'},
         'second': {'nested': {4,5,6}}, 'third': {'one': {'cat': 'dog', 'barn': {'horse': 'shoe', 'fish': 'pond'},
         'cage': ['bird']}, 'two': []}}),
        ([map_key, map_value], 0, 3, aerospike.MAP_RETURN_INDEX, False, ['third', {'cat': 'dog', 'barn': {'fish': 'pond', 'horse': 'shoe'}, 'cage': ['bird']}],
         [0,1,2], { 'first': {'greet': 'hi', 'numbers': [3, 1, 2], 3: 'hello'}, 'second': {'nested': {4,5,6,}, 'hundred': 100},
         'third': {'one': {}, 'two': []}})
    ])
    def test_ctx_map_remove_by_index_range(self, ctx_types, index, rmv_count, return_type, inverted, list_indexes, expected_val, expected_bin):
        """
        Invoke map_remove_by_index_range() to remove elements starting at index for rmv_count.
        """
        ctx = []
        for x in range(0, len(list_indexes)) :
            ctx.append(add_ctx_op(ctx_types[x], list_indexes[x]))
        
        ops = [
            map_operations.map_remove_by_index_range(self.nested_map_bin, index, rmv_count, return_type, inverted, ctx)       
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        res = res[self.nested_map_bin]
        assert res == expected_val
        _, _, bins = self.as_connection.get(self.test_key)
        assert bins[self.nested_map_bin] == expected_bin

    @pytest.mark.parametrize("index, rmv_count, return_type, inverted, list_indexes, expected", [
        (1, 1, aerospike.MAP_RETURN_VALUE, False, [3], e.OpNotApplicable),
        (1, 1, aerospike.MAP_RETURN_VALUE, False, [1,0,0], e.OpNotApplicable),
        (1, 1, aerospike.MAP_RETURN_VALUE, False, 'teddy', e.ParamError),
        (1, 'bad_rmv_count', aerospike.MAP_RETURN_VALUE, False, [1], e.ParamError),
    ])
    def test_ctx_map_remove_by_index_range_negative(self, index, rmv_count, return_type, inverted, list_indexes, expected):
        """
        Invoke map_remove_by_index_range() with expected failures.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_map_index(place))

        ops = [
            map_operations.map_remove_by_index_range(self.nested_map_bin, index, rmv_count, return_type, inverted, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("ctx_types, rank, return_type, list_indexes, expected_val, expected_bin", [
        ([map_index], 1, aerospike.MAP_RETURN_VALUE, [0], 'hi', { 'first': {'numbers': [3, 1, 2], 3: 'hello'},
         'second': {'nested': {4,5,6,}, 'hundred': 100}, 'third': {'one': {'cat': 'dog', 'barn': {'horse': 'shoe', 'fish': 'pond'},
          'cage': ['bird']}, 'two': []}}),
        ([map_index], 0, aerospike.MAP_RETURN_VALUE, [0], 'hello', { 'first': {'greet': 'hi', 'numbers': [3, 1, 2]},
         'second': {'nested': {4,5,6,}, 'hundred': 100}, 'third': {'one': {'cat': 'dog', 'barn': {'horse': 'shoe', 'fish': 'pond'},
         'cage': ['bird']}, 'two': []}}),
        ([map_index], 1, aerospike.MAP_RETURN_VALUE, [1], {4,5,6}, { 'first': {'greet': 'hi', 'numbers': [3, 1, 2], 3: 'hello'},
         'second': {'hundred': 100}, 'third': {'one': {'cat': 'dog', 'barn': {'horse': 'shoe', 'fish': 'pond'},
         'cage': ['bird']}, 'two': []}}),
        ([map_index, map_index, map_index], 0, aerospike.MAP_RETURN_VALUE, [2,0,0], 'pond',
         { 'first': {'greet': 'hi', 'numbers': [3, 1, 2], 3: 'hello'}, 'second': {'nested': {4,5,6,}, 'hundred': 100},
         'third': {'one': {'cat': 'dog', 'barn': {'horse': 'shoe'}, 'cage': ['bird']}, 'two': []}}),
        ([map_key], 1, aerospike.MAP_RETURN_INDEX, ['second'], 1, { 'first': {'greet': 'hi', 'numbers': [3, 1, 2], 3: 'hello'},
         'second': { 'hundred': 100}, 'third': {'one': {'cat': 'dog', 'barn': {'horse': 'shoe', 'fish': 'pond'},
         'cage': ['bird']}, 'two': []}})
    ])
    def test_ctx_map_remove_by_rank(self, ctx_types, rank, return_type, list_indexes, expected_val, expected_bin):
        """
        Invoke map_remove_by_rank() to remove the element with the given rank.
        """
        ctx = []
        for x in range(0, len(list_indexes)) :
            ctx.append(add_ctx_op(ctx_types[x], list_indexes[x]))

        ops = [
            map_operations.map_remove_by_rank(self.nested_map_bin, rank, return_type, ctx)
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        assert res[self.nested_map_bin] == expected_val
        _, _, bins = self.as_connection.get(self.test_key)
        assert bins[self.nested_map_bin] == expected_bin

    @pytest.mark.parametrize("rank, return_type, list_indexes, expected", [
        (1, aerospike.MAP_RETURN_VALUE, [3], e.OpNotApplicable),
        (1, aerospike.MAP_RETURN_VALUE, [1,0,0], e.OpNotApplicable),
        (1, aerospike.MAP_RETURN_VALUE, 'teddy', e.ParamError),
        (6, aerospike.MAP_RETURN_VALUE, [1], e.OpNotApplicable),
    ])
    def test_ctx_map_remove_by_rank_negative(self, rank, return_type, list_indexes, expected):
        """
        Invoke map_remove_by_rank() with expected failures.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_map_rank(place))

        ops = [
            map_operations.map_remove_by_rank(self.nested_map_bin, rank, return_type, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("ctx_types, rank, rmv_count, return_type, inverted, list_indexes, expected_val, expected_bin", [
        ([map_index], 0, 4, aerospike.MAP_RETURN_VALUE, False, [0], ['v1', 'v2', 'v3'], {1: {},
         2: {4: 'v4', 5: 'v5', 6: 'v6'}, 3: {7: 'v7', 8: 'v8', 9: 'v9', 10: {11: 'v11'}}}),
        ([map_index], 0, 2, aerospike.MAP_RETURN_VALUE, False, [2], ['v7', 'v8'], {1: {1: 'v1', 2: 'v2', 3: 'v3'},
         2: {4: 'v4', 5: 'v5', 6: 'v6'}, 3: {9: 'v9',  10: {11: 'v11'}}}),
        ([map_key, map_key], 0, 1, aerospike.MAP_RETURN_VALUE, False, [3, 10], ['v11'], {1: {1: 'v1', 2: 'v2', 3: 'v3'},
         2: {4: 'v4', 5: 'v5', 6: 'v6'}, 3: {7: 'v7', 8: 'v8', 9: 'v9', 10: {}}}),
        ([map_index], 0, 2, aerospike.MAP_RETURN_VALUE, True, [2], ['v9', {11: 'v11'}], {1: {1: 'v1', 2: 'v2', 3: 'v3'},
         2: {4: 'v4', 5: 'v5', 6: 'v6'}, 3: {7: 'v7', 8: 'v8'}}),
    ])
    def test_ctx_map_remove_by_rank_range(self, ctx_types, rank, rmv_count, return_type, inverted, list_indexes, expected_val, expected_bin):
        """
        Invoke map_remove_by_rank_range() to remove the elements starting with the given rank for rmv_count.
        """
        ctx = []
        for x in range(0, len(list_indexes)) :
            ctx.append(add_ctx_op(ctx_types[x], list_indexes[x]))

        ops = [
            map_operations.map_remove_by_rank_range(self.num_map_bin, rank, rmv_count, return_type, inverted, ctx)
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        assert res[self.num_map_bin] == expected_val
        _, _, bins = self.as_connection.get(self.test_key)
        assert bins[self.num_map_bin] == expected_bin

    @pytest.mark.parametrize("rank, rmv_count, return_type, inverted, list_indexes, expected", [
        (1, 1, aerospike.MAP_RETURN_VALUE, False, [3], e.OpNotApplicable),
        (1, 1, aerospike.MAP_RETURN_VALUE, False, [1,0,0], e.OpNotApplicable),
        (1, 1, aerospike.MAP_RETURN_VALUE, False, 'teddy', e.ParamError),
        (1, 'bad_rmv_count', aerospike.MAP_RETURN_VALUE, False, [1], e.ParamError),
        (['bad_rank'], 1, aerospike.MAP_RETURN_VALUE, False, [1], e.ParamError),
    ])
    def test_ctx_map_remove_by_rank_range_negative(self, rank, rmv_count, return_type, inverted, list_indexes, expected):
        """
        Invoke map_remove_by_rank_range() with expected failures.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_map_rank(place))

        ops = [
            map_operations.map_remove_by_rank_range(self.nested_map_bin, rank, rmv_count, return_type, inverted, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("ctx_types, value, offset, return_type, count, inverted, list_indexes, expected_val, expected_bin", [
        ([map_index], 'hi', 0, aerospike.MAP_RETURN_VALUE, 1, False, [0], ['hi'], { 'first': {'numbers': [3, 1, 2], 3: 'hello'},
         'second': {'nested': {4,5,6,}, 'hundred': 100}, 'third': {'one': {'cat': 'dog', 'barn': {'horse': 'shoe', 'fish': 'pond'},
         'cage': ['bird']}, 'two': []}}),
        ([map_index], 'hi', 1, aerospike.MAP_RETURN_VALUE, 3, True, [0], ['hello', 'hi'], { 'first': {'numbers': [3, 1, 2]},
         'second': {'nested': {4,5,6,}, 'hundred': 100}, 'third': {'one': {'cat': 'dog', 'barn': {'horse': 'shoe', 'fish': 'pond'},
         'cage': ['bird']}, 'two': []}}),
        ([map_key, map_index, map_value], 'pond', 0, aerospike.MAP_RETURN_VALUE, 2, False, ['third',0,{'horse': 'shoe', 'fish': 'pond'}], ['pond', 'shoe'],
         { 'first': {'greet': 'hi', 'numbers': [3, 1, 2], 3: 'hello'}, 'second': {'nested': {4,5,6}, 'hundred': 100},
         'third': {'one': {'cat': 'dog', 'barn': {}, 'cage': ['bird']}, 'two': []}}),
        ([map_key, map_rank], {'horse': 'shoe', 'fish': 'pond'}, 0, aerospike.MAP_RETURN_VALUE, 2, True, ['third',1], ['dog',['bird']],
         { 'first': {'greet': 'hi', 'numbers': [3, 1, 2], 3: 'hello'}, 'second': {'nested': {4,5,6}, 'hundred': 100},
         'third': {'one': {'barn': {'horse': 'shoe', 'fish': 'pond'}}, 'two': []}}),
    ])
    def test_ctx_map_remove_by_value_rank_range_relative(self, ctx_types, value, offset, return_type, count, inverted, list_indexes, expected_val, expected_bin):
        """
        Invoke map_remove_by_value_rank_range_relative() to remove elements starting with value for count by relative rank.
        """
        ctx = []
        for x in range(0, len(list_indexes)) :
            ctx.append(add_ctx_op(ctx_types[x], list_indexes[x]))

        ops = [
            map_operations.map_remove_by_value_rank_range_relative(self.nested_map_bin, value, offset, return_type, count, inverted, ctx)
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        assert res[self.nested_map_bin] == expected_val
        _, _, bins = self.as_connection.get(self.test_key)
        assert bins[self.nested_map_bin] == expected_bin

    @pytest.mark.parametrize("value, offset, return_type, count, inverted, list_indexes, expected", [
        ('greet', 'bad_offset', aerospike.MAP_RETURN_VALUE, 1, False, [3], e.ParamError),
        ('greet', 0, aerospike.MAP_RETURN_VALUE, 1, False, [3], e.OpNotApplicable),
        ('greet', 0, aerospike.MAP_RETURN_VALUE, 'bad_count', False, [3], e.ParamError),
        ('greet', 0, aerospike.MAP_RETURN_VALUE, 1, False, [0,0,0,0], e.OpNotApplicable),
    ])
    def test_ctx_map_remove_by_value_rank_range_relative_negative(self, value, offset, return_type, count, inverted, list_indexes, expected):
        """
        Invoke value_rank_range_relative() on a nested map with expected failures.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_map_index(place))

        ops = [
            map_operations.map_remove_by_value_rank_range_relative(self.nested_map_bin, value, offset, return_type, count, inverted, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("ctx_types, value, offset, return_type, count, inverted, list_indexes, expected", [
        ([map_index], 'hi', 0, aerospike.MAP_RETURN_VALUE, 1, False, [0], ['hi']),
        ([map_index], 'hi', 1, aerospike.MAP_RETURN_VALUE, 3, True, [0], ['hello', 'hi']),
        ([map_key, map_index, map_value], 'horse', 0, aerospike.MAP_RETURN_VALUE, 2, False,
         ['third',0,{'horse': 'shoe', 'fish': 'pond'}], ['pond', 'shoe'],),
        ([map_key, map_rank], {'horse': 'shoe', 'fish': 'pond'}, 0, aerospike.MAP_RETURN_VALUE, 2, True,
         ['third',1], ['dog',['bird']]),
    ])
    def test_ctx_map_get_by_value_rank_range_relative(self, ctx_types, value, offset, return_type, count, inverted, list_indexes, expected):
        """
        Invoke map_get_by_value_rank_range_relative() to get elements starting with value for count by relative rank.
        """
        ctx = []
        for x in range(0, len(list_indexes)) :
            ctx.append(add_ctx_op(ctx_types[x], list_indexes[x]))

        ops = [
            map_operations.map_get_by_value_rank_range_relative(self.nested_map_bin, value, offset, return_type, count, inverted, ctx)
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        assert res[self.nested_map_bin] == expected

    @pytest.mark.parametrize("value, offset, return_type, count, inverted, list_indexes, expected", [
        ('greet', 'bad_offset', aerospike.MAP_RETURN_VALUE, 1, False, [3], e.ParamError),
        ('greet', 0, aerospike.MAP_RETURN_VALUE, 1, False, [3], e.OpNotApplicable),
        ('greet', 0, aerospike.MAP_RETURN_VALUE, 'bad_count', False, [3], e.ParamError),
        ('greet', 0, aerospike.MAP_RETURN_VALUE, 1, False, [0,0,0,0], e.OpNotApplicable),
    ])
    def test_ctx_map_get_by_value_rank_range_relative_negative(self, value, offset, return_type, count, inverted, list_indexes, expected):
        """
        Invoke map_get_by_value_rank_range_relative() on a nested map with expected failures
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_map_index(place))

        ops = [
            map_operations.map_get_by_value_rank_range_relative(self.nested_map_bin, value, offset, return_type, count, inverted, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("ctx_types, key, offset, return_type, count, inverted, list_indexes, expected_val, expected_bin", [
        ([map_index], 'greet', 0, aerospike.MAP_RETURN_VALUE, 1, False, [0], ['hi'], { 'first': {'numbers': [3, 1, 2], 3: 'hello'},
         'second': {'nested': {4,5,6,}, 'hundred': 100}, 'third': {'one': {'cat': 'dog', 'barn': {'horse': 'shoe', 'fish': 'pond'},
         'cage': ['bird']}, 'two': []}}),
        ([map_index], 'greet', 1, aerospike.MAP_RETURN_VALUE, 3, True, [0], ['hello', 'hi'], { 'first': {'numbers': [3, 1, 2]},
         'second': {'nested': {4,5,6,}, 'hundred': 100}, 'third': {'one': {'cat': 'dog', 'barn': {'horse': 'shoe', 'fish': 'pond'},
         'cage': ['bird']}, 'two': []}}),
        ([map_key, map_index, map_value], 'fish', 0, aerospike.MAP_RETURN_VALUE, 2, False, ['third',0,{'horse': 'shoe', 'fish': 'pond'}], ['pond', 'shoe'],
         { 'first': {'greet': 'hi', 'numbers': [3, 1, 2], 3: 'hello'}, 'second': {'nested': {4,5,6}, 'hundred': 100},
         'third': {'one': {'cat': 'dog', 'barn': {}, 'cage': ['bird']}, 'two': []}}),
        ([map_key, map_rank], 'barn', 0, aerospike.MAP_RETURN_VALUE, 2, True, ['third',1], ['dog'],
         { 'first': {'greet': 'hi', 'numbers': [3, 1, 2], 3: 'hello'}, 'second': {'nested': {4,5,6}, 'hundred': 100},
         'third': {'one': {'barn': {'horse': 'shoe', 'fish': 'pond'}, 'cage': ['bird']}, 'two': []}}),
    ])
    def test_ctx_map_remove_by_key_index_range_relative(self, ctx_types, key, offset, return_type, count, inverted, list_indexes, expected_val, expected_bin):
        """
        Invoke map_remove_by_key_index_range_relative() to remove elements starting with value for count by relative rank.
        """
        ctx = []
        for x in range(0, len(list_indexes)) :
            ctx.append(add_ctx_op(ctx_types[x], list_indexes[x]))

        ops = [
            map_operations.map_remove_by_key_index_range_relative(self.nested_map_bin, key, offset, return_type, count, inverted, ctx)
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        assert res[self.nested_map_bin] == expected_val
        _, _, bins = self.as_connection.get(self.test_key)
        assert bins[self.nested_map_bin] == expected_bin

    @pytest.mark.parametrize("key, offset, return_type, count, inverted, list_indexes, expected", [
        ('greet', 'bad_offset', aerospike.MAP_RETURN_VALUE, 1, False, [3], e.ParamError),
        ('greet', 0, aerospike.MAP_RETURN_VALUE, 1, False, [3], e.OpNotApplicable),
        ('greet', 0, aerospike.MAP_RETURN_VALUE, 'bad_count', False, [3], e.ParamError),
        ('greet', 0, aerospike.MAP_RETURN_VALUE, 1, False, [0,0,0,0], e.OpNotApplicable),
    ])
    def test_ctx_map_remove_by_key_index_range_relative_negative(self, key, offset, return_type, count, inverted, list_indexes, expected):
        """
        Invoke map_remove_by_key_index_range_relative_negative on a nested map with expected failures
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_map_index(place))

        ops = [
            map_operations.map_remove_by_key_index_range_relative(self.nested_map_bin, key, offset, return_type, count, inverted, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("ctx_types, key, offset, return_type, count, inverted, list_indexes, expected_val, expected_bin", [
        ([map_index], 'greet', 0, aerospike.MAP_RETURN_VALUE, 1, False, [0], ['hi'], { 'first': {'numbers': [3, 1, 2], 3: 'hello'},
         'second': {'nested': {4,5,6,}, 'hundred': 100}, 'third': {'one': {'cat': 'dog', 'barn': {'horse': 'shoe', 'fish': 'pond'},
         'cage': ['bird']}, 'two': []}}),
        ([map_index], 'greet', 1, aerospike.MAP_RETURN_VALUE, 3, True, [0], ['hello', 'hi'], { 'first': {'numbers': [3, 1, 2]},
         'second': {'nested': {4,5,6,}, 'hundred': 100}, 'third': {'one': {'cat': 'dog', 'barn': {'horse': 'shoe', 'fish': 'pond'},
         'cage': ['bird']}, 'two': []}}),
        ([map_key, map_index, map_value], 'fish', 0, aerospike.MAP_RETURN_VALUE, 2, False, ['third',0,{'horse': 'shoe', 'fish': 'pond'}], ['pond', 'shoe'],
         { 'first': {'greet': 'hi', 'numbers': [3, 1, 2], 3: 'hello'}, 'second': {'nested': {4,5,6}, 'hundred': 100},
         'third': {'one': {'cat': 'dog', 'barn': {}, 'cage': ['bird']}, 'two': []}}),
        ([map_key, map_rank], 'barn', 0, aerospike.MAP_RETURN_VALUE, 2, True, ['third',1], ['dog'],
         { 'first': {'greet': 'hi', 'numbers': [3, 1, 2], 3: 'hello'}, 'second': {'nested': {4,5,6}, 'hundred': 100},
         'third': {'one': {'barn': {'horse': 'shoe', 'fish': 'pond'}, 'cage': ['bird']}, 'two': []}}),
    ])
    def test_ctx_map_remove_by_key_index_range_relative(self, ctx_types, key, offset, return_type, count, inverted, list_indexes, expected_val, expected_bin):
        """
        Invoke map_remove_by_key_index_range_relative() to remove elements starting at key for count by relative index.
        """
        ctx = []
        for x in range(0, len(list_indexes)) :
            ctx.append(add_ctx_op(ctx_types[x], list_indexes[x]))

        ops = [
            map_operations.map_remove_by_key_index_range_relative(self.nested_map_bin, key, offset, return_type, count, inverted, ctx)
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        assert res[self.nested_map_bin] == expected_val
        _, _, bins = self.as_connection.get(self.test_key)
        assert bins[self.nested_map_bin] == expected_bin

    @pytest.mark.parametrize("key, offset, return_type, count, inverted, list_indexes, expected", [
        ('greet', 'bad_offset', aerospike.MAP_RETURN_VALUE, 1, False, [3], e.ParamError),
        ('greet', 0, aerospike.MAP_RETURN_VALUE, 1, False, [3], e.OpNotApplicable),
        ('greet', 0, aerospike.MAP_RETURN_VALUE, 'bad_count', False, [3], e.ParamError),
        ('greet', 0, aerospike.MAP_RETURN_VALUE, 1, False, [0,0,0,0], e.OpNotApplicable),
    ])
    def test_ctx_map_remove_by_key_index_range_relative_negative(self, key, offset, return_type, count, inverted, list_indexes, expected):
        """
        Invoke map_remove_by_key_index_range_relative() on a nested map with expected failures.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_map_index(place))

        ops = [
            map_operations.map_remove_by_key_index_range_relative(self.nested_map_bin, key, offset, return_type, count, inverted, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("ctx_types, key, offset, return_type, count, inverted, list_indexes, expected", [
        ([map_index], 'greet', 0, aerospike.MAP_RETURN_VALUE, 1, False, [0], ['hi']),
        ([map_index], 'greet', 1, aerospike.MAP_RETURN_VALUE, 3, True, [0], ['hello', 'hi']),
        ([map_key, map_index, map_value], 'fish', 0, aerospike.MAP_RETURN_VALUE, 2, False, ['third',0,
         {'horse': 'shoe', 'fish': 'pond'}], ['pond', 'shoe']),
        ([map_key, map_rank], 'barn', 0, aerospike.MAP_RETURN_VALUE, 2, True, ['third',1], ['dog']),
    ])
    def test_ctx_map_get_by_key_index_range_relative(self, ctx_types, key, offset, return_type, count, inverted, list_indexes, expected):
        """
        Invoke map_get_by_key_index_range_relative() to get the element at key for count by relative index.
        """
        ctx = []
        for x in range(0, len(list_indexes)) :
            ctx.append(add_ctx_op(ctx_types[x], list_indexes[x]))

        ops = [
            map_operations.map_get_by_key_index_range_relative(self.nested_map_bin, key, offset, return_type, count, inverted, ctx)
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        assert res[self.nested_map_bin] == expected

    @pytest.mark.parametrize("key, offset, return_type, count, inverted, list_indexes, expected", [
        ('greet', 'bad_offset', aerospike.MAP_RETURN_VALUE, 1, False, [3], e.ParamError),
        ('greet', 0, aerospike.MAP_RETURN_VALUE, 1, False, [3], e.OpNotApplicable),
        ('greet', 0, aerospike.MAP_RETURN_VALUE, 'bad_count', False, [3], e.ParamError),
        ('greet', 0, aerospike.MAP_RETURN_VALUE, 1, False, [0,0,0,0], e.OpNotApplicable),
    ])
    def test_ctx_map_get_by_key_index_range_relative_negative(self, key, offset, return_type, count, inverted, list_indexes, expected):
        """
        Invoke map_get_key_index_range_relative() on a nested map with expected failures.
        """
        ctx = []
        for place in list_indexes:
            ctx.append(cdt_ctx.cdt_ctx_map_index(place))

        ops = [
            map_operations.map_get_by_key_index_range_relative(self.nested_map_bin, key, offset, return_type, count, inverted, ctx)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    def test_non_list_ctx(self):
        """
        Test ctx conversion with a non list ctx.
        """
        ctx = [cdt_ctx.cdt_ctx_map_key(1)]

        ops = [
            map_operations.map_get_by_key(self.nested_map_bin, 'greet', aerospike.MAP_RETURN_VALUE, ctx[0])
        ]

        for i in range(10):
            with pytest.raises(e.ParamError):
                self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("key, val", [
        ('new_key', 'val1'),
        (230, 'val1')
    ])
    def test_cdt_ctx_map_key_create_pos(self, key, val):
        """
        Test the map_key_create cdt_ctx type.
        """
        ctx = [cdt_ctx.cdt_ctx_map_key_create(key, aerospike.MAP_KEY_ORDERED)]

        ops = [
            map_operations.map_put(self.nested_map_bin, 'key1', val, None, ctx),
            map_operations.map_get_by_key(self.nested_map_bin, key, aerospike.MAP_RETURN_VALUE)
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        assert(res[self.nested_map_bin] == {'key1': val})

    @pytest.mark.parametrize("key, val, flags, expected", [
        ('new_key', 'val1', ['bad_order'], e.ParamError),
        ('new_key', 'val1', None, e.ParamError)
    ])
    def test_cdt_ctx_map_key_create_neg(self, key, val, flags, expected):
        """
        Test the map_key_create cdt_ctx type.
        """
        ctx = [cdt_ctx.cdt_ctx_map_key_create(key, flags)]

        ops = [
            map_operations.map_put(self.nested_map_bin, 'key1', val, None, ctx),
            map_operations.map_get_by_key(self.nested_map_bin, key, aerospike.MAP_RETURN_VALUE)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)

    @pytest.mark.parametrize("index, val, pad", [
        (10, 'val1', True),
        (2, 'val1', False)
    ])
    def test_cdt_ctx_list_index_create_pos(self, index, val, pad):
        """
        Test the list_index_create cdt_ctx type.
        """
        ctx = [cdt_ctx.cdt_ctx_list_index_create(index, aerospike.LIST_UNORDERED, pad)]

        ops = [
            list_operations.list_append(self.nested_list_bin, 'val1', None, ctx),
            list_operations.list_get_by_index(self.nested_list_bin, index, aerospike.LIST_RETURN_VALUE)
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        assert(res[self.nested_list_bin] == ['val1'])

    @pytest.mark.parametrize("index, val, pad, flags, expected", [
        (10, 'val1', False, aerospike.LIST_ORDERED, e.OpNotApplicable),
        (None, 'val1', False, aerospike.LIST_ORDERED, e.ParamError),
        (2, 'val1', "bad_pad", aerospike.LIST_ORDERED, e.ParamError),
        (2, 'val1', "bad_pad", ["bad_flags"], e.ParamError)
    ])
    def test_cdt_ctx_list_index_create_neg(self, index, val, pad, flags, expected):
        """
        Test the list_index_create cdt_ctx type.
        """
        ctx = [cdt_ctx.cdt_ctx_list_index_create(index, aerospike.LIST_ORDERED, pad)]

        ops = [
            list_operations.list_append(self.nested_list_bin, 'val1', None, ctx),
            list_operations.list_get_by_index(self.nested_list_bin, index, aerospike.LIST_RETURN_VALUE)
        ]

        with pytest.raises(expected):
            self.as_connection.operate(self.test_key, ops)