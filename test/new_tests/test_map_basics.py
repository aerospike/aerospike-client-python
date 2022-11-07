# -*- coding: utf-8 -*-
import pytest
import sys
from .test_base_class import TestBaseClass
from aerospike import exception as e

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)


class TestMapBasics(object):

    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):

        key = ('test', 'map_test', 1)
        rec = {'blank': 'blank'}
        as_connection.put(key, rec)

        def teardown():
            """
            Teardown method.
            """
            key = ('test', 'map_test', 1)
            binname = 'my_map'
            self.as_connection.remove(key)

        request.addfinalizer(teardown)


    @pytest.mark.parametrize("key, binname, map_key, map_value, expected", [
        (('test', 'map_test', 1), 'my_map', 'age', 97, [('age', 97)]),
        (('test', 'map_test', 1), 'my_map', 'bytes',
         bytearray('1234', 'utf-8'), [('bytes', bytearray('1234', 'utf-8'))]),
        (('test', 'map_test', 1), 'my_map', bytearray('1234', 'utf-8'), 1234,
         [(bytearray('1234', 'utf-8'), 1234)]),
        (('test', 'map_test', 1), 'my_map2', 'name', 'dean', [('name', 'dean')])
    ])
    def test_pos_map_put_get(self, key, binname, map_key, map_value, expected):
        """
        Invoke map_put() insert value with correct parameters
        """
        self.as_connection.map_put(key, binname, map_key, map_value)
        bins = self.as_connection.map_get_by_key(key, binname, map_key, aerospike.MAP_RETURN_KEY_VALUE)
        self.as_connection.map_clear(key, binname)
        assert bins == expected

    @pytest.mark.parametrize("key, binname, map_items, map_key, expected", [
        (('test', 'map_test', 1), 'my_map',
            {'item1': 'value1', 'item2': 'value2'},
            'item2', [('item2', 'value2')]),
        (('test', 'map_test', 1), 'my_map',
            {15: 'value15', 23: 'value23'},
            15, [(15, 'value15')]),
        (('test', 'map_test', 1), 'my_map',
            {15: bytearray('1234', 'utf-8'), 23: 'value23'},
            15, [(15, bytearray('1234', 'utf-8'))])
    ])
    def test_pos_map_put_items(self, key, binname, map_items, map_key, expected):
        self.as_connection.map_put_items(key, binname, map_items)
        bins = self.as_connection.map_get_by_key(key, binname, map_key, aerospike.MAP_RETURN_KEY_VALUE)
        self.as_connection.map_clear(key, binname)
        assert bins == expected

    def test_pos_map_clear(self):
        key = ('test', 'map_test', 1)
        binname = 'my_map'
        self.as_connection.map_put(key, binname, 'age', 97)
        size = self.as_connection.map_size(key, binname)
        self.as_connection.map_clear(key, binname)
        newSize = self.as_connection.map_size(key, binname)
        assert size == 1
        assert newSize == 0

    @pytest.mark.parametrize("key, binname, map_key, map_value, map_increment, expected", [
        (('test', 'map_test', 1), 'my_map', 'int', 99, 1, 100),
        (('test', 'map_test', 1), 'my_map', bytearray('1234', 'utf-8'),
         99, 1, 100),
        (('test', 'map_test', 1), 'my_map2', 'float', 2.1, 1.2, 3.3)
    ])
    def test_pos_map_increment(self, key, binname, map_key, map_value, map_increment, expected):
        self.as_connection.map_put(key, binname, map_key, map_value)
        self.as_connection.map_increment(key, binname, map_key, map_increment)
        value = self.as_connection.map_get_by_key(key, binname, map_key, aerospike.MAP_RETURN_VALUE)
        self.as_connection.map_clear(key, binname)
        assert value == expected

    @pytest.mark.parametrize("key, binname, map_key, map_value, map_decrement, expected", [
        (('test', 'map_test', 1), 'my_map', 'int', 99, 1, 98),
        (('test', 'map_test', 1), 'my_map2', 'float', 2.1, 1.1, 1.0),
        (('test', 'map_test', 1), 'my_map', bytearray('1234', 'utf-8'),
         100, 1, 99),
    ])
    def test_pos_map_decrement(self, key, binname, map_key, map_value, map_decrement, expected):
        self.as_connection.map_put(key, binname, map_key, map_value)
        self.as_connection.map_decrement(key, binname, map_key, map_decrement)
        value = self.as_connection.map_get_by_key(key, binname, map_key, aerospike.MAP_RETURN_VALUE)
        self.as_connection.map_clear(key, binname)
        assert value == expected

    def test_pos_map_remove_by_key(self):
        key = ('test', 'map_test', 1)
        binname = 'my_map'

        self.as_connection.map_put(key, binname, 'age', 97)
        value = self.as_connection.map_remove_by_key(key, binname, 'age', aerospike.MAP_RETURN_VALUE)
        value2 = self.as_connection.map_remove_by_key(key, binname, 'age', aerospike.MAP_RETURN_VALUE)
        assert value == 97
        assert value2 is None

        byte_map_bin = bytearray('1234', 'utf-8')
        self.as_connection.map_put(key, binname, byte_map_bin, 97)
        value = self.as_connection.map_remove_by_key(
            key, binname, byte_map_bin, aerospike.MAP_RETURN_VALUE)
        value2 = self.as_connection.map_remove_by_key(
            key, binname, byte_map_bin, aerospike.MAP_RETURN_VALUE)
        assert value == 97
        assert value2 is None

    def test_pos_map_remove_by_key_list(self):
        key = ('test', 'map_test', 1)
        binname = 'my_map'
        self.as_connection.map_put_items(key, binname, {'item1': 'value1', 'item2': 'value2'})
        self.as_connection.map_remove_by_key_list(key, binname, ['item1', 'item2'], aerospike.MAP_RETURN_NONE)
        size = self.as_connection.map_size(key, binname)
        assert size == 0

    def test_pos_map_remove_by_key_list_bytes(self):
        key = ('test', 'map_test', 1)
        binname = 'my_map'
        self.as_connection.map_put(
            key, binname, bytearray('1234', 'utf-8'), 5)
        self.as_connection.map_put(
            key, binname, bytearray('1235', 'utf-8'), 6)
        self.as_connection.map_remove_by_key_list(
            key, binname,
            [bytearray('1234', 'utf-8'), bytearray('1235', 'utf-8')],
            aerospike.MAP_RETURN_NONE)
        size = self.as_connection.map_size(key, binname)
        assert size == 0


    def test_pos_map_remove_by_key_range(self):
        key = ('test', 'map_test', 1)
        binname = 'my_map'
        self.as_connection.map_put_items(key, binname, {'item1': 'value1', 'item2': 'value2'})
        self.as_connection.map_remove_by_key_range(key, binname, 'i', 'j', aerospike.MAP_RETURN_NONE)
        size = self.as_connection.map_size(key, binname)
        assert size == 0

    def test_pos_map_remove_by_key_range_bytes(self):
        key = ('test', 'map_test', 1)
        binname = 'my_map'
        self.as_connection.map_put(
            key, binname, bytearray([1, 2, 3, 4]), 5)
        self.as_connection.map_put(
            key, binname, bytearray([1, 9, 9, 9]), 6)

        self.as_connection.map_remove_by_key_range(
            key, binname, bytearray([1, 2, 3, 4]),
            bytearray([2, 0, 0, 0]), aerospike.MAP_RETURN_NONE)

        size = self.as_connection.map_size(key, binname)
        assert size == 0

    def test_pos_map_remove_by_value(self):
        key = ('test', 'map_test', 1)
        binname = 'my_map'
        self.as_connection.map_put_items(key, binname, {'item1': 'value1', 'item2': 'value2'})
        count1 = self.as_connection.map_remove_by_value(key, binname, 'value1', aerospike.MAP_RETURN_COUNT)
        count2 = self.as_connection.map_remove_by_value(key, binname, 'value2', aerospike.MAP_RETURN_COUNT)
        size = self.as_connection.map_size(key, binname)
        assert count1 == 1
        assert count2 == 1
        assert size == 0

    def test_pos_map_remove_by_value_bytes(self):
        key = ('test', 'map_test', 1)
        binname = 'my_map'
        val1 = bytearray([1, 2, 3, 4])
        val2 = bytearray([1, 2, 3, 5])
        self.as_connection.map_put_items(
            key, binname, {'item1': val1, 'item2': val2})
        count1 = self.as_connection.map_remove_by_value(
            key, binname, val1, aerospike.MAP_RETURN_COUNT)
        count2 = self.as_connection.map_remove_by_value(
            key, binname, val2, aerospike.MAP_RETURN_COUNT)
        size = self.as_connection.map_size(key, binname)
        assert count1 == 1
        assert count2 == 1
        assert size == 0

    def test_pos_map_remove_by_value_list(self):
        key = ('test', 'map_test', 1)
        binname = 'my_map'
        self.as_connection.map_put_items(key, binname, {'item1': 'value1', 'item2': 'value2'})
        self.as_connection.map_remove_by_value_list(key, binname, ['value1', 'value2'], aerospike.MAP_RETURN_NONE)
        size = self.as_connection.map_size(key, binname)
        assert size == 0

    def test_pos_map_remove_by_value_list_bytes(self):
        key = ('test', 'map_test', 1)
        binname = 'my_map'
        val1 = bytearray([1, 2, 3, 4])
        val2 = bytearray([1, 2, 3, 5])
        self.as_connection.map_put_items(
            key, binname, {'item1': val1, 'item2': val2})
        self.as_connection.map_remove_by_value_list(
            key, binname, [val1, val2], aerospike.MAP_RETURN_NONE)
        size = self.as_connection.map_size(key, binname)
        assert size == 0

    def test_pos_map_remove_by_value_range(self):
        key = ('test', 'map_test', 1)
        binname = 'my_map'
        self.as_connection.map_put_items(key, binname, {'item1': 'value1', 'item2': 'value2'})
        self.as_connection.map_remove_by_value_range(key, binname, 'v', 'w', aerospike.MAP_RETURN_NONE)
        size = self.as_connection.map_size(key, binname)
        assert size == 0

    def test_pos_map_remove_by_value_range_bytes(self):
        key = ('test', 'map_test', 1)
        binname = 'my_map'
        val1 = bytearray([1, 2, 3, 4])
        val2 = bytearray([1, 2, 3, 6])
        self.as_connection.map_put_items(
            key, binname, {'item1': val1, 'item2': val2})
        self.as_connection.map_remove_by_value_range(
            key, binname,  bytearray([1, 2, 3, 3]),
            bytearray([1, 2, 3, 7]), aerospike.MAP_RETURN_NONE)
        size = self.as_connection.map_size(key, binname)
        assert size == 0

    def test_pos_map_remove_by_index(self):
        key = ('test', 'map_test', 1)
        binname = 'my_map'
        self.as_connection.map_set_policy(key, binname, {'map_sort': aerospike.MAP_KEY_ORDERED})
        self.as_connection.map_put_items(key, binname, {'item1': 'value1', 'item2': 'value2'})
        value1 = self.as_connection.map_remove_by_index(key, binname, 1, aerospike.MAP_RETURN_VALUE)
        value2 = self.as_connection.map_remove_by_index(key, binname, 0, aerospike.MAP_RETURN_VALUE)
        size = self.as_connection.map_size(key, binname)
        assert size == 0
        assert value1 == 'value2'
        assert value2 == 'value1'

    def test_pos_map_remove_by_index_range(self):
        key = ('test', 'map_test', 1)
        binname = 'my_map'
        self.as_connection.map_put_items(key, binname, {'item1': 'value1', 'item2': 'value2'})
        count = self.as_connection.map_remove_by_index_range(key, binname, 0, 2, aerospike.MAP_RETURN_COUNT)
        size = self.as_connection.map_size(key, binname)
        assert count == 2
        assert size == 0

    def test_pos_map_remove_by_rank(self):
        key = ('test', 'map_test', 1)
        binname = 'my_map'
        self.as_connection.map_put_items(key, binname, {'item1': 10, 'item2': 11})
        theKey = self.as_connection.map_remove_by_rank(key, binname, 1, aerospike.MAP_RETURN_KEY)
        size = self.as_connection.map_size(key, binname)
        self.as_connection.map_clear(key, binname)
        assert theKey == 'item2'
        assert size == 1

    def test_pos_map_remove_by_rank_range(self):
        key = ('test', 'map_test', 1)
        binname = 'my_map'
        self.as_connection.map_put_items(key, binname, {'item1': 10, 'item2': 11, 'item3': 12})
        count = self.as_connection.map_remove_by_rank_range(key, binname, 0, 2, aerospike.MAP_RETURN_COUNT)
        size = self.as_connection.map_size(key, binname)
        self.as_connection.map_clear(key, binname)
        assert count == 2
        assert size == 1

    def test_pos_map_get_by_key_range(self):
        key = ('test', 'map_test', 1)
        binname = 'my_map'
        self.as_connection.map_put_items(key, binname, {'item1': 'value1', 'item2': 'value2'})
        values = self.as_connection.map_get_by_key_range(key, binname, 'i', 'j', aerospike.MAP_RETURN_VALUE)
        self.as_connection.map_clear(key, binname)
        assert values == ['value1', 'value2']

    def test_pos_map_get_by_key_range_bytes(self):
        key = ('test', 'map_test', 1)
        binname = 'my_map'
        self.as_connection.map_put(
            key, binname, bytearray([1, 2, 3, 4]), 5)
        self.as_connection.map_put(
            key, binname, bytearray([1, 9, 9, 9]), 6)

        values = self.as_connection.map_get_by_key_range(
            key, binname, bytearray([1, 2, 3, 4]),
            bytearray([2, 0, 0, 0]), aerospike.MAP_RETURN_VALUE)
        self.as_connection.map_clear(key, binname)
        assert values == [5, 6]

    def test_pos_map_get_by_value(self):
        key = ('test', 'map_test', 1)
        binname = 'my_map'
        self.as_connection.map_set_policy(key, binname, {'map_sort': aerospike.MAP_KEY_ORDERED})
        self.as_connection.map_put_items(key, binname, {'item1': 'value2', 'item2': 'value2'})
        key1 = self.as_connection.map_get_by_value(key, binname, 'value2', aerospike.MAP_RETURN_KEY)
        self.as_connection.map_clear(key, binname)
        assert key1 == ['item1', 'item2']

    def test_pos_map_get_by_value_range(self):
        key = ('test', 'map_test', 1)
        binname = 'my_map'
        self.as_connection.map_put_items(key, binname, {'item1': 'value1', 'item2': 'value2', 'item3': 'value3'})
        keys = self.as_connection.map_get_by_value_range(key, binname, 'value1', 'value3', aerospike.MAP_RETURN_KEY)
        self.as_connection.map_clear(key, binname)
        assert keys == ['item1', 'item2']

    def test_pos_map_get_by_value_range_bytes(self):
        key = ('test', 'map_test', 1)
        binname = 'my_map'
        self.as_connection.map_put(
            key, binname, 5, bytearray([1, 2, 3, 4]))
        self.as_connection.map_put(
            key, binname, 6, bytearray([1, 9, 9, 9]))

        values = self.as_connection.map_get_by_value_range(
            key, binname, bytearray([1, 2, 3, 4]),
            bytearray([2, 0, 0, 0]), aerospike.MAP_RETURN_KEY)
        self.as_connection.map_clear(key, binname)
        assert values == [5, 6]

    @pytest.mark.parametrize(
        'policy, meta',
        [
            (None, None),
            ({'total_timeout': 10000}, {'ttl': -1})
        ])
    def test_map_get_by_value_list(self, policy, meta):
        key = ('test', 'map_test', 1)
        binname = 'my_map'
        map_entry = {'a': 1, 'b': 2, 'c': 3, 'd': 4, 'e': 1}
        self.as_connection.put(key, {binname: map_entry})
        if policy:
            fetched_keys = self.as_connection.map_get_by_value_list(
                key, binname, [1, 3, 4], aerospike.MAP_RETURN_KEY,
                policy=policy, meta=meta)
        else:
            fetched_keys = self.as_connection.map_get_by_value_list(
                key, binname, [1, 3, 4], aerospike.MAP_RETURN_KEY)

        assert set(fetched_keys) == set(['a', 'c', 'd', 'e'])
        self.as_connection.map_clear(key, binname)

    def test_map_get_by_value_list_no_matching_items(self):
        key = ('test', 'map_test', 1)
        binname = 'my_map'
        map_entry = {'a': 1, 'b': 2, 'c': 3, 'd': 4, 'e': 1}
        self.as_connection.put(key, {binname: map_entry})
        fetched_keys = self.as_connection.map_get_by_value_list(
            key, binname, [7, 8, 9], aerospike.MAP_RETURN_KEY,
            policy={'total_timeout': 10000}, meta={'ttl': -1})

        assert fetched_keys == []
        self.as_connection.map_clear(key, binname)

    @pytest.mark.parametrize(
        'policy, meta',
        [
            (None, None),
            ({'total_timeout': 10000}, {'ttl': -1})
        ])
    def test_map_get_by_key_list(self, policy, meta):
        key = ('test', 'map_test', 1)
        binname = 'my_map'
        map_entry = {'a': 1, 'b': 2, 'c': 3, 'd': 4, 'e': 1}
        self.as_connection.put(key, {binname: map_entry})
        if policy:
            fetched_vals = self.as_connection.map_get_by_key_list(
                key, binname, ['a', 'c', 'e'], aerospike.MAP_RETURN_VALUE,
                policy=policy, meta=meta)
        else:
            fetched_vals = self.as_connection.map_get_by_key_list(
                key, binname, ['a', 'c', 'e'], aerospike.MAP_RETURN_VALUE)

        fetched_vals.sort()
        assert fetched_vals == [1, 1, 3]

        self.as_connection.map_clear(key, binname)

    def test_map_get_by_key_list_no_matching(self):
        key = ('test', 'map_test', 1)
        binname = 'my_map'
        map_entry = {'a': 1, 'b': 2, 'c': 3, 'd': 4, 'e': 1}
        self.as_connection.put(key, {binname: map_entry})
        fetched_vals = self.as_connection.map_get_by_key_list(
            key, binname, ['aero', 'spike'], aerospike.MAP_RETURN_VALUE)

        assert fetched_vals == []

        self.as_connection.map_clear(key, binname)

    def test_pos_map_get_by_index(self):
        key = ('test', 'map_test', 1)
        binname = 'my_map'
        self.as_connection.map_set_policy(key, binname, {'map_sort': aerospike.MAP_KEY_ORDERED})
        self.as_connection.map_put_items(key, binname, {'item1': 'value1', 'item2': 'value2'})
        value1 = self.as_connection.map_get_by_index(key, binname, 0, aerospike.MAP_RETURN_VALUE)
        value2 = self.as_connection.map_get_by_index(key, binname, 1, aerospike.MAP_RETURN_VALUE)
        self.as_connection.map_clear(key, binname)
        assert value1 == 'value1'
        assert value2 == 'value2'

    def test_pos_map_get_get_by_index_range(self):
        key = ('test', 'map_test', 1)
        binname = 'my_map'
        self.as_connection.map_set_policy(key, binname, {'map_sort': aerospike.MAP_KEY_ORDERED})
        self.as_connection.map_put_items(key, binname, {'item1': 'value1', 'item2': 'value2'})
        values = self.as_connection.map_get_by_index_range(key, binname, 0, 2, aerospike.MAP_RETURN_VALUE)
        self.as_connection.map_clear(key, binname)
        assert values == ['value1', 'value2']

    def test_pos_map_get_by_rank(self):
        key = ('test', 'map_test', 1)
        binname = 'my_map'
        self.as_connection.map_put_items(key, binname, {'item1': 10, 'item2': 11})
        theKey = self.as_connection.map_get_by_rank(key, binname, 1, aerospike.MAP_RETURN_KEY)
        self.as_connection.map_clear(key, binname)
        assert theKey == 'item2'

    def test_pos_map_get_by_rank_range(self):
        key = ('test', 'map_test', 1)
        binname = 'my_map'
        self.as_connection.map_put_items(key, binname, {'item1': 10, 'item2': 11, 'item3': 12})
        count = self.as_connection.map_get_by_rank_range(key, binname, 0, 2, aerospike.MAP_RETURN_COUNT)
        self.as_connection.map_clear(key, binname)
        assert count == 2

    def test_pos_map_insert_empty_map_into_cleared_map(self):
        key = ('test', 'map_test', 1)
        binname = 'my_map'
        self.as_connection.map_put_items(key, binname, {'item1': 3})
        self.as_connection.map_clear(key, binname)
        self.as_connection.map_put_items(key, binname, {})
        size = self.as_connection.map_size(key, binname)
        assert size == 0

    def test_pos_map_insert_none_value(self):
        key = ('test', 'map_test', 1)
        binname = 'my_map'
        self.as_connection.map_put(key, binname, 'my_key', None)
        size = self.as_connection.map_size(key, binname)
        assert size == 1
        value = self.as_connection.map_get_by_key(key, binname, 'my_key', aerospike.MAP_RETURN_VALUE)
        assert value is None
        self.as_connection.map_clear(key, binname)

    def test_pos_map_insert_empty_value(self):
        key = ('test', 'map_test', 1)
        binname = 'my_map'
        self.as_connection.map_put(key, binname, 'my_key', {})
        size = self.as_connection.map_size(key, binname)
        assert size == 1
        value = self.as_connection.map_get_by_key(key, binname, 'my_key', aerospike.MAP_RETURN_VALUE)
        assert value == {}
        self.as_connection.map_clear(key, binname)

    def test_map_get_by_value_list_non_list(self):
        key = ('test', 'demo', 1)
        binname = 'my_map'
        with pytest.raises(e.ParamError):
            self.as_connection.map_get_by_value_list(
                key, binname, 'A value', aerospike.MAP_RETURN_KEY)

    def test_map_get_by_key_list_non_list(self):
        key = ('test', 'demo', 1)
        binname = 'my_map'
        with pytest.raises(e.ParamError):
            self.as_connection.map_get_by_key_list(
                key, binname, 'A key, and another', aerospike.MAP_RETURN_KEY)

    def test_map_get_by_value_list_no_return_type(self):
        key = ('test', 'demo', 1)
        binname = 'my_map'
        with pytest.raises(TypeError):
            self.as_connection.map_get_by_value_list(
                key, binname, [1, 2, 3])

    def test_map_get_by_key_list_no_return_type(self):
        key = ('test', 'demo', 1)
        binname = 'my_map'
        with pytest.raises(TypeError):
            self.as_connection.map_get_by_key_list(
                key, binname, ['k1', 'k2', 'k3'])

    def test_map_size_nonexistent_bin(self):
        key = ('test', 'map_test', 1)
        binname = 'non_a_real_bin'
        size = self.as_connection.map_size(key, binname)
        assert size == 0

    def test_map_insert_invalid_key(self):
        key = ()
        binname = 'my_map'
        with pytest.raises(e.ParamError):
            self.as_connection.map_put(key, binname, 'my_key', {})

    @pytest.mark.parametrize(
        "method_name",
        [
            "map_set_policy",
            "map_put",
            "map_put_items",
            "map_increment",
            "map_decrement",
            "map_size",
            "map_clear",
            "map_remove_by_key",
            "map_remove_by_key_list",
            "map_remove_by_key_range",
            "map_remove_by_value",
            "map_remove_by_value_list",
            "map_remove_by_value_range",
            "map_remove_by_index",
            "map_remove_by_rank",
            "map_remove_by_rank_range",
            "map_get_by_key",
            "map_get_by_key_range",
            "map_get_by_value",
            "map_get_by_value_range",
            "map_get_by_index",
            "map_get_by_index_range",
            "map_get_by_rank",
            "map_get_by_rank_range",
            "map_get_by_key_list",
            "map_get_by_value_list"
        ])
    def test_call_to_methods_with_no_args(self, method_name):
        '''
        Call each of the map_* methods with no arguments
        '''
        method = getattr(self.as_connection, method_name)
        with pytest.raises(TypeError):
            method()
