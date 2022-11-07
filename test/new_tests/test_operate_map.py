# -*- coding: utf-8 -*-
import sys

import pytest

from .test_base_class import TestBaseClass
aerospike = pytest.importorskip("aerospike")

try:
    import aerospike
    from aerospike import exception as e
except:
    print("Please install aerospike python client.")
    sys.exit(1)

# aerospike.OP_MAP_SET_POLICY
# aerospike.OP_MAP_PUT
# aerospike.OP_MAP_PUT_ITEMS
# aerospike.OP_MAP_INCREMENT
# aerospike.OP_MAP_DECREMENT
# aerospike.OP_MAP_SIZE
# aerospike.OP_MAP_CLEAR
# aerospike.OP_MAP_REMOVE_BY_KEY
# aerospike.OP_MAP_REMOVE_BY_KEY_LIST
# aerospike.OP_MAP_REMOVE_BY_KEY_RANGE
# aerospike.OP_MAP_REMOVE_BY_VALUE
# aerospike.OP_MAP_REMOVE_BY_VALUE_LIST
# aerospike.OP_MAP_REMOVE_BY_VALUE_RANGE
# aerospike.OP_MAP_REMOVE_BY_INDEX
# aerospike.OP_MAP_REMOVE_BY_INDEX_RANGE
# aerospike.OP_MAP_REMOVE_BY_RANK
# aerospike.OP_MAP_REMOVE_BY_RANK_RANGE
# aerospike.OP_MAP_GET_BY_KEY
# aerospike.OP_MAP_GET_BY_KEY_RANGE
# aerospike.OP_MAP_GET_BY_VALUE
# aerospike.OP_MAP_GET_BY_VALUE_RANGE
# aerospike.OP_MAP_GET_BY_INDEX
# aerospike.OP_MAP_GET_BY_INDEX_RANGE
# aerospike.OP_MAP_GET_BY_RANK
# aerospike.OP_MAP_GET_BY_RANK_RANGE


class TestOperate(object):

    def setup_class(cls):
        """
        Setup class.
        """
        cls.client_no_typechecks = TestBaseClass.get_new_connection(
          {'strict_types': False})

    def teardown_class(cls):
        TestOperate.client_no_typechecks.close()

    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        """
        Setup Method
        """
        key = ('test', 'demo', 'test_op_map')
        self.test_map_key = key
        test_map = {'a': 1, 'b': 2, 'c': 3, 'd': 4, 'e': 5}
        self.test_map = test_map
        self.test_map_bin = 'test_map'
        key_order_policy = {'map_order': aerospike.MAP_KEY_ORDERED}
        as_connection.map_put_items(
            key, bin='test_map', items=test_map,
            map_policy=key_order_policy)
        as_connection.map_put_items(
            key, bin='test_map2', items=test_map,
            map_policy=key_order_policy)

        def teardown():
            try:
                as_connection.remove(key)
            except:
                pass
            """
            Teardown Method
            """
        request.addfinalizer(teardown)

    @pytest.mark.parametrize("key, llist, expected", [
        (('test', 'map_test', 1),
            [{"op": aerospike.OP_MAP_PUT,
              "bin": "my_map",
              "key": "age",
              "val": 97},
             {"op": aerospike.OP_MAP_INCREMENT,
              "bin": "my_map",
              "key": "age",
              "val": 1},
             {"op": aerospike.OP_MAP_GET_BY_KEY,
              "bin": "my_map",
              "key": "age",
              "return_type": aerospike.MAP_RETURN_VALUE}],
            {'my_map': 98}),
        (('test', 'map_test', 1),
            [{"op": aerospike.OP_MAP_PUT,
              "bin": "my_map",
              "key": "age",
              "val": 22},
             {"op": aerospike.OP_MAP_DECREMENT,
              "bin": "my_map",
              "key": "age",
              "val": 1},
             {"op": aerospike.OP_MAP_GET_BY_KEY,
              "bin": "my_map",
              "key": "age",
              "return_type": aerospike.MAP_RETURN_VALUE}],
            {'my_map': 21}),
        (('test', 'map_test', 1),
            [{"op": aerospike.OP_MAP_PUT_ITEMS,
              "bin": "my_map",
              "val": {'name': 'bubba', 'occupation': 'dancer'}},
             {"op": aerospike.OP_MAP_GET_BY_KEY,
              "bin": "my_map",
              "key": "name",
              "return_type": aerospike.MAP_RETURN_KEY_VALUE}],
            {'my_map': ['name', 'bubba']})
    ])
    def test_pos_operate_with_correct_paramters(self, key, llist, expected):
        """
        Invoke operate() with correct parameters
        """
        key, _, bins = self.as_connection.operate(key, llist)

        assert bins == expected
        self.as_connection.remove(key)

    @pytest.mark.skip()
    @pytest.mark.parametrize("key, llist, expected", [
        (('test', 'map_test', 1),
            [{"op": aerospike.OP_MAP_PUT,
              "bin": "my_map",
              "key": "age",
              "val": 97},
             {"op": aerospike.OP_MAP_INCREMENT,
              "bin": "my_map",
              "key": "age",
              "val": 1},
             {"op": aerospike.OP_MAP_GET_BY_KEY,
              "bin": "my_map",
              "key": "age",
              "return_type": aerospike.MAP_RETURN_VALUE}],
            [None, None, ('my_map', 98)]),
        (('test', 'map_test', 1),
            [{"op": aerospike.OP_MAP_PUT,
              "bin": "my_map",
              "key": "age",
              "val": 22},
             {"op": aerospike.OP_MAP_DECREMENT,
              "bin": "my_map",
              "key": "age",
              "val": 1},
             {"op": aerospike.OP_MAP_GET_BY_KEY,
              "bin": "my_map",
              "key": "age",
              "return_type": aerospike.MAP_RETURN_VALUE}],
            [None, None, ('my_map', 21)]),
        (('test', 'map_test', 1),
            [{"op": aerospike.OP_MAP_PUT_ITEMS,
              "bin": "my_map",
              "val": {'name': 'bubba', 'occupation': 'dancer'}},
             {"op": aerospike.OP_MAP_GET_BY_KEY,
              "bin": "my_map",
              "key": "name",
              "return_type": aerospike.MAP_RETURN_KEY_VALUE}],
            [None, ('my_map', [('name', 'bubba')])])
    ])
    def test_pos_operate_ordered_with_correct_paramters(self, key, llist, expected):
        """
        Invoke operate() with correct parameters
        """
        key, _, bins = self.as_connection.operate_ordered(key, llist)

        assert bins == expected
        self.as_connection.remove(key)

    def test_pos_operate_set_map_policy(self):
        key = ('test', 'map_test', 1)
        llist = [{"op": aerospike.OP_MAP_SET_POLICY,
                  "bin": "my_map",
                  "map_policy": {'map_sort': aerospike.MAP_KEY_ORDERED}}]
        key, _, _ = self.as_connection.operate(key, llist)
        self.as_connection.remove(key)
        pass

    def test_pos_map_clear(self):
        key = ('test', 'map_test', 1)
        binname = 'my_map'
        llist = [{"op": aerospike.OP_MAP_PUT,
                  "bin": binname,
                  "key": "age",
                  "val": 97}]
        key, _, _ = self.as_connection.operate(key, llist)

        llist = [{"op": aerospike.OP_MAP_SIZE,
                  "bin": binname}]
        key, _, bins = self.as_connection.operate(key, llist)

        assert bins == {binname: 1}

        key, _, _ = self.as_connection.operate(key, llist)
        llist = [{"op": aerospike.OP_MAP_CLEAR,
                  "bin": binname}]
        key, _, _ = self.as_connection.operate(key, llist)

        llist = [{"op": aerospike.OP_MAP_SIZE,
                  "bin": binname}]
        key, _, bins = self.as_connection.operate(key, llist)

        assert bins == {binname: 0}
        self.as_connection.remove(key)

    def test_map_remove_by_index_range_correct(self):
        ops = [{
            'op': aerospike.OP_MAP_REMOVE_BY_INDEX_RANGE,
            'bin': self.test_map_bin,
            'index': 1,
            'val': 3,
        }]
        _, _, bins = self.as_connection.operate(
            self.test_map_key, ops)

        _, _, bins = self.as_connection.get(self.test_map_key)
        assert bins[self.test_map_bin] == {'a': 1, 'e': 5}

    @pytest.mark.xfail(reason="previously worked")
    def test_map_remove_by_index_range_no_index(self):
        ops = [{
            'op': aerospike.OP_MAP_REMOVE_BY_INDEX_RANGE,
            'bin': self.test_map_bin,
            'val': 3,
        }]
        with pytest.raises(e.ParamError):
            _, _, bins = self.as_connection.operate(
                self.test_map_key, ops)

    def test_op_map_put_existing_key(self):
        result_map = self.test_map.copy()
        result_map['a'] = 'b'
        ops = [{
            'op': aerospike.OP_MAP_PUT,
            'bin': self.test_map_bin,
            'key': 'a',
            'val': 'b'
        }]

        self.as_connection.operate(self.test_map_key, ops)
        _, _, bins = self.as_connection.get(self.test_map_key)
        assert bins['test_map'] == result_map

    def test_op_map_put_new_key(self):
        result_map = self.test_map.copy()
        result_map['new'] = 'value'
        ops = [{
            'op': aerospike.OP_MAP_PUT,
            'bin': self.test_map_bin,
            'key': 'new',
            'val': 'value'
        }]

        self.as_connection.operate(self.test_map_key, ops)
        _, _, bins = self.as_connection.get(self.test_map_key)
        assert bins[self.test_map_bin] == result_map

    @pytest.mark.parametrize(
        "required_key",
        (
            'bin',
            'key',
            'val'
        ))
    def test_op_map_put_missing_required_keys(self, required_key):
        op = {
                'op': aerospike.OP_MAP_PUT,
                'bin': 'test_map',
                'key': 'new',
                'val': 'value'
        }
        del op[required_key]

        ops = [op]
        with pytest.raises(e.ParamError):
            self.as_connection.operate(self.test_map_key, ops)

    def test_op_map_put_items(self):
        result_map = self.test_map.copy()
        result_map['new'] = 'value'
        result_map['new2'] = 'value2'
        ops = [{
            'op': aerospike.OP_MAP_PUT_ITEMS,
            'bin': self.test_map_bin,
            'val': {"new": "value", "new2": "value2"}
        }]

        self.as_connection.operate(self.test_map_key, ops)
        _, _, bins = self.as_connection.get(self.test_map_key)
        assert bins[self.test_map_bin] == result_map

    @pytest.mark.parametrize(
        "key",
        (
            'bin',
            'val'
        ))
    def test_op_map_put_items_missing_required_entry(self, key):
        op = {
            'op': aerospike.OP_MAP_PUT_ITEMS,
            'bin': 'test_map',
            'val': {"new": "value", "new2": "value2"}
        }
        del op[key]
        ops = [op]

        with pytest.raises(e.ParamError):
            self.as_connection.operate(self.test_map_key, ops)

    def test_map_increment(self):
        result_map = self.test_map.copy()
        result_map['a'] = result_map['a'] + 2
        ops = [{
            'op': aerospike.OP_MAP_INCREMENT,
            'bin': self.test_map_bin,
            'key': 'a',
            'val': 2
        }]

        self.as_connection.operate(self.test_map_key, ops)
        _, _, bins = self.as_connection.get(self.test_map_key)
        assert bins[self.test_map_bin] == result_map

    @pytest.mark.parametrize(
        "val",
        (
            "str",
            [1, 2, 3],
            (),
            {'a': 'b'}
        ))
    def test_map_increment_invalid_type(self, val):
        ops = [{
            'op': aerospike.OP_MAP_INCREMENT,
            'bin': self.test_map_bin,
            'key': 'a',
            'val': val
        }]

        with pytest.raises(Exception):
            self.as_connection.operate(self.test_map_key, ops)

    @pytest.mark.parametrize(
        "key",
        (
            'bin',
            'key',
            'val'
        ))
    def test_op_map_put_incr_missing_required_entry(self, key):
        op = {
            'op': aerospike.OP_MAP_INCREMENT,
            'bin': self.test_map_bin,
            'key': 'a',
            'val': 1
        }
        del op[key]
        ops = [op]

        with pytest.raises(e.ParamError):
            self.as_connection.operate(self.test_map_key, ops)

    def test_map_decrement(self):
        result_map = self.test_map.copy()
        result_map['a'] = result_map['a'] - 2
        ops = [{
            'op': aerospike.OP_MAP_DECREMENT,
            'bin': self.test_map_bin,
            'key': 'a',
            'val': 2
        }]

        self.as_connection.operate(self.test_map_key, ops)
        _, _, bins = self.as_connection.get(self.test_map_key)
        assert bins[self.test_map_bin] == result_map

    @pytest.mark.parametrize(
        "val",
        (
            "str",
            [1, 2, 3],
            (),
            {'a': 'b'}
        ))
    def test_map_decrement_invalid_type(self, val):
        ops = [{
            'op': aerospike.OP_MAP_DECREMENT,
            'bin': self.test_map_bin,
            'key': 'a',
            'val': val
        }]

        with pytest.raises(Exception):
            self.as_connection.operate(self.test_map_key, ops)

    @pytest.mark.parametrize(
        "key",
        (
            'bin',
            'key',
            'val'
        ))
    def test_op_map_put_decr_missing_required_entry(self, key):
        op = {
            'op': aerospike.OP_MAP_DECREMENT,
            'bin': self.test_map_bin,
            'key': 'a',
            'val': 1
        }
        del op[key]
        ops = [op]

        with pytest.raises(e.ParamError):
            self.as_connection.operate(self.test_map_key, ops)

    def test_map_size(self):
        op = {
            'op': aerospike.OP_MAP_SIZE,
            'bin': self.test_map_bin
        }
        ops = [op]

        _, _, bins = self.as_connection.operate(self.test_map_key, ops)
        assert bins[self.test_map_bin] == len(self.test_map)

    def test_map_size_no_bin(self):
        op = {
            'op': aerospike.OP_MAP_SIZE,
        }
        ops = [op]

        with pytest.raises(e.ParamError):
            self.as_connection.operate(self.test_map_key, ops)

    def test_map_size_non_existent_bin(self):
        op = {
            'op': aerospike.OP_MAP_SIZE,
            'bin': 'fake_bin'
        }
        ops = [op]

        _, _, bins = self.as_connection.operate(self.test_map_key, ops)
        assert bins['fake_bin'] is None

    def test_map_clear(self):
        ops = [{
            'op': aerospike.OP_MAP_CLEAR,
            'bin': self.test_map_bin
        }]

        _, _, bins = self.as_connection.get(self.test_map_key)
        assert len(bins[self.test_map_bin]) == len(self.test_map)

        self.as_connection.operate(self.test_map_key, ops)

        _, _, bins = self.as_connection.get(self.test_map_key)
        assert len(bins[self.test_map_bin]) == 0

    def test_map_remove_by_key(self):
        result_map = self.test_map.copy()
        del result_map["c"]
        ops = [{
            'op': aerospike.OP_MAP_REMOVE_BY_KEY,
            'bin': self.test_map_bin,
            'key': 'c'
        }]

        _, _, bins = self.as_connection.operate(self.test_map_key, ops)
        _, _, bins = self.as_connection.get(self.test_map_key)
        assert 'c' not in bins[self.test_map_bin]

    def test_map_remove_by_key_ret_key(self):
        result_map = self.test_map.copy()
        del result_map["c"]
        ops = [{
            'op': aerospike.OP_MAP_REMOVE_BY_KEY,
            'bin': 'test_map',
            'key': 'c',
            'return_type': aerospike.MAP_RETURN_KEY
        }]

        _, _, bins = self.as_connection.operate(self.test_map_key, ops)
        assert bins[self.test_map_bin] == 'c'

    def test_map_remove_by_key_ret_val(self):
        result_map = self.test_map.copy()
        del result_map["c"]
        ops = [{
            'op': aerospike.OP_MAP_REMOVE_BY_KEY,
            'bin': 'test_map',
            'key': 'c',
            'return_type': aerospike.MAP_RETURN_VALUE
        }]

        _, _, bins = self.as_connection.operate(self.test_map_key, ops)
        assert bins['test_map'] == self.test_map['c']

    def test_map_remove_by_key_ret_key_val(self):
        result_map = self.test_map.copy()
        del result_map["c"]
        ops = [{
            'op': aerospike.OP_MAP_REMOVE_BY_KEY,
            'bin': self.test_map_bin,
            'key': 'c',
            'return_type': aerospike.MAP_RETURN_KEY_VALUE
        }]

        _, _, bins = self.as_connection.operate(self.test_map_key, ops)
        assert bins[self.test_map_bin] == ['c', self.test_map['c']]

    def test_map_remove_by_key_ret_key_val_test_with_list_read_odd(self):
        result_map = self.test_map.copy()
        self.as_connection.put(self.test_map_key, {'cool_list': [1, 2, 3]})
        ops = [
            {
                'op': aerospike.OPERATOR_READ,
                'bin': 'cool_list'
            },
            {
                'op': aerospike.OP_MAP_REMOVE_BY_KEY,
                'bin': self.test_map_bin,
                'key': 'c',
                'return_type': aerospike.MAP_RETURN_KEY_VALUE
            }
        ]

        _, _, bins = self.as_connection.operate(self.test_map_key, ops)
        assert bins['cool_list'] == [1, 2, 3]

    def test_map_remove_by_key_ret_key_val_test_with_list_read_even(self):
        result_map = self.test_map.copy()
        self.as_connection.put(self.test_map_key, {'cool_list': [1, 2, 3, 4]})
        ops = [
            {
                'op': aerospike.OPERATOR_READ,
                'bin': 'cool_list'
            },
            {
                'op': aerospike.OP_MAP_REMOVE_BY_KEY,
                'bin': self.test_map_bin,
                'key': 'c',
                'return_type': aerospike.MAP_RETURN_KEY_VALUE
            }
        ]

        _, _, bins = self.as_connection.operate(self.test_map_key, ops)
        assert bins['cool_list'] == [1, 2, 3, 4]

    @pytest.mark.parametrize(
        "entry", ('bin', 'key'))
    def test_map_remove_by_key_missing_required_entries(self, entry):
        op = {
            'op': aerospike.OP_MAP_REMOVE_BY_KEY,
            'bin': self.test_map_bin,
            'key': 'c',
            'return_type': aerospike.MAP_RETURN_KEY_VALUE
        }

        del op[entry]
        ops = [op]

        with pytest.raises(e.ParamError):
            self.as_connection.operate(self.test_map_key, ops)

    @pytest.mark.parametrize(
        "entry", ([], {}, ()))
    def test_map_remove_by_key_invalid_bin(self, entry):
        op = {
            'op': aerospike.OP_MAP_REMOVE_BY_KEY,
            'bin': entry,
            'key': 'a',
            'return_type': aerospike.MAP_RETURN_KEY_VALUE
        }
        ops = [op]

        with pytest.raises(e.ParamError):
            self.as_connection.operate(self.test_map_key, ops)

    def test_map_remove_by_key_list(self):
        result_map = self.test_map
        del result_map['a']
        del result_map['c']
        del result_map['d']

        ops = [{
            'op': aerospike.OP_MAP_REMOVE_BY_KEY_LIST,
            'bin': self.test_map_bin,
            'val': ['a', 'c', 'd'],
        }]

        self.as_connection.operate(self.test_map_key, ops)
        _, _, bins = self.as_connection.get(self.test_map_key)

        assert bins[self.test_map_bin] == result_map

    @pytest.mark.parametrize(
        "val", ('a', {'a': 'b'}, 2, ('a', 'b')))
    def test_map_remove_by_key_list_wrong_val_type(self, val):
        ops = [{
            'op': aerospike.OP_MAP_REMOVE_BY_KEY_LIST,
            'bin': self.test_map_bin,
            'val': val,
        }]

        with pytest.raises(Exception):
            self.as_connection.operate(self.test_map_key, ops)

    @pytest.mark.parametrize(
        "val", ('bin', 'val'))
    def test_map_remove_by_key_missing_required_entry(self, val):
        ops = [{
            'op': aerospike.OP_MAP_REMOVE_BY_KEY_LIST,
            'bin': self.test_map_bin,
            'val': val,
        }]
        del ops[0][val]

        with pytest.raises(e.ParamError):
            self.as_connection.operate(self.test_map_key, ops)

    def test_map_remove_by_key_range(self):
        ops = [{
            'op': aerospike.OP_MAP_REMOVE_BY_KEY_RANGE,
            'bin': self.test_map_bin,
            'key': 'b',
            'val': 'd',

        }]

        result_map = self.test_map.copy()
        del result_map['b']
        del result_map['c']

        self.as_connection.operate(self.test_map_key, ops)
        _, _, bins = self.as_connection.get(self.test_map_key)
        assert result_map == bins[self.test_map_bin]

    @pytest.mark.parametrize(
        'entry', ('bin', 'key', 'val'))
    def test_map_remove_by_key_range_missing_entry(self, entry):
        op = {
            'op': aerospike.OP_MAP_REMOVE_BY_KEY_RANGE,
            'bin': self.test_map_bin,
            'key': 'b',
            'val': 'd'
        }

        del op[entry]
        ops = [op]

        with pytest.raises(e.ParamError):
            self.as_connection.operate(self.test_map_key, ops)

    def test_map_remove_by_value(self):
        result_map = self.test_map.copy()
        del result_map['d']

        ops = [{
            'op': aerospike.OP_MAP_REMOVE_BY_VALUE,
            'bin': self.test_map_bin,
            'val': 4,
            'return_type': aerospike.MAP_RETURN_KEY
        }]

        _, _, bins = self.as_connection.operate(self.test_map_key, ops)
        assert bins[self.test_map_bin] == ['d']

        _, _, bins = self.as_connection.get(self.test_map_key)
        assert result_map == bins[self.test_map_bin]

    @pytest.mark.parametrize(
        'entry', ('bin', 'val'))
    def test_map_remove_by_value_missing_required_entry(self, entry):
        ops = [{
            'op': aerospike.OP_MAP_REMOVE_BY_VALUE,
            'bin': self.test_map_bin,
            'val': 4,
            'return_type': aerospike.MAP_RETURN_KEY
        }]

        del ops[0][entry]

        with pytest.raises(e.ParamError):
            self.as_connection.operate(self.test_map_key, ops)

    def test_map_remove_by_value_list(self):
        result_map = self.test_map.copy()
        del result_map['b']
        del result_map['d']
        del result_map['e']

        ops = [{
            'op': aerospike.OP_MAP_REMOVE_BY_VALUE_LIST,
            'bin': self.test_map_bin,
            'val': [2, 4, 5],
            'return_type': aerospike.MAP_RETURN_KEY
        }]

        _, _, bins = self.as_connection.operate(self.test_map_key, ops)
        assert bins[self.test_map_bin] == ['b', 'd', 'e']

        _, _, bins = self.as_connection.get(self.test_map_key)
        assert result_map == bins[self.test_map_bin]

    @pytest.mark.parametrize(
        'entry', ('bin', 'val'))
    def test_map_remove_by_value_list_missing_required_entry(self, entry):
        ops = [{
            'op': aerospike.OP_MAP_REMOVE_BY_VALUE_LIST,
            'bin': self.test_map_bin,
            'val': [2, 4, 5],
            'return_type': aerospike.MAP_RETURN_KEY
        }]
        del ops[0][entry]

        with pytest.raises(e.ParamError):
            self.as_connection.operate(self.test_map_key, ops)

    def test_map_remove_by_value_range(self):
        result_map = self.test_map.copy()
        del result_map['b']
        del result_map['c']
        del result_map['d']

        ops = [{
            'op': aerospike.OP_MAP_REMOVE_BY_VALUE_RANGE,
            'bin': self.test_map_bin,
            'val': 2,
            'range': 5,
            'return_type': aerospike.MAP_RETURN_KEY
        }]

        _, _, bins = self.as_connection.operate(self.test_map_key, ops)
        assert bins[self.test_map_bin] == ['b', 'c', 'd']

        _, _, bins = self.as_connection.get(self.test_map_key)
        assert result_map == bins[self.test_map_bin]

    @pytest.mark.parametrize(
        'entry', ('bin', 'val', 'range'))
    def test_map_remove_by_value_range_missing_required_entry(self, entry):
        op = {
            'op': aerospike.OP_MAP_REMOVE_BY_VALUE_RANGE,
            'bin': self.test_map_bin,
            'val': 2,
            'range': 5,
            'return_type': aerospike.MAP_RETURN_KEY
        }

        del op[entry]
        ops = [op]
        with pytest.raises(e.ParamError):
            self.as_connection.operate(self.test_map_key, ops)

    def test_map_remove_by_index(self):
        result_map = self.test_map.copy()
        del result_map['b']
        ops = [{
            "op": aerospike.OP_MAP_REMOVE_BY_INDEX,
            "bin": self.test_map_bin,
            "index": 1,
            "return_type": aerospike.MAP_RETURN_KEY
        }]

        _, _, res = self.as_connection.operate(self.test_map_key, ops)
        assert res[self.test_map_bin] == 'b'

        _, _, bins = self.as_connection.get(self.test_map_key)
        assert bins[self.test_map_bin] == result_map

    @pytest.mark.parametrize(
        'entry', ('bin', 'index'))
    def test_map_remove_by_index_missing_required_entry(self, entry):
        op = {
            "op": aerospike.OP_MAP_REMOVE_BY_INDEX,
            "bin": self.test_map_bin,
            "index": 1,
            "return_type": aerospike.MAP_RETURN_KEY
        }

        del op[entry]
        ops = [op]

        with pytest.raises(e.ParamError):
            self.as_connection.operate(self.test_map_key, ops)

    def test_map_remove_by_index_range(self):
        result_map = self.test_map.copy()
        del result_map['b']
        del result_map['c']
        del result_map['d']
        ops = [{
            "op": aerospike.OP_MAP_REMOVE_BY_INDEX_RANGE,
            "bin": self.test_map_bin,
            "index": 1,
            "val": 3,
            "return_type": aerospike.MAP_RETURN_KEY
        }]

        _, _, res = self.as_connection.operate(self.test_map_key, ops)
        assert res[self.test_map_bin] == ['b', 'c', 'd']

        _, _, bins = self.as_connection.get(self.test_map_key)
        assert bins[self.test_map_bin] == result_map

    @pytest.mark.parametrize(
        'entry', ('bin', 'index', 'val'))
    def test_map_remove_by_index_range_missing_required_entry(self, entry):
        op = {
            "op": aerospike.OP_MAP_REMOVE_BY_INDEX_RANGE,
            "bin": self.test_map_bin,
            "index": 1,
            "val": 3,
            "return_type": aerospike.MAP_RETURN_KEY
        }
        del op[entry]
        ops = [op]

        with pytest.raises(e.ParamError):
            self.as_connection.operate(self.test_map_key, ops)

    @pytest.mark.xfail(reason="This works, but shouldn't")
    def test_map_remove_by_index_range_no_strict_types_val(self):
        result_map = self.test_map.copy()
        del result_map['b']
        del result_map['c']
        del result_map['d']
        ops = [{
            "op": aerospike.OP_MAP_REMOVE_BY_INDEX_RANGE,
            "bin": self.test_map_bin,
            "index": 1,
            "val": 3,
            "return_type": aerospike.MAP_RETURN_KEY
        }]

        _, _, res = self.client_no_typechecks.operate(self.test_map_key, ops)
        assert res[self.test_map_bin] == ['b', 'c', 'd']

        _, _, bins = self.as_connection.get(self.test_map_key)
        assert bins[self.test_map_bin] == result_map

    def test_map_remove_by_rank(self):
        '''
        remove the 3rd item ordered by val
        '''
        result_map = self.test_map.copy()
        del result_map['c']
        ops = [{
            "op": aerospike.OP_MAP_REMOVE_BY_RANK,
            "bin": self.test_map_bin,
            "index": 2,
        }]

        self.as_connection.operate(self.test_map_key, ops)

        _, _, bins = self.as_connection.get(self.test_map_key)

        assert bins[self.test_map_bin] == result_map

    @pytest.mark.parametrize(
        'entry', ('bin', 'index'))
    def test_map_remove_by_rank_missing_required_entry(self, entry):
        op = {
            "op": aerospike.OP_MAP_REMOVE_BY_RANK,
            "bin": self.test_map_bin,
            "index": 2,
        }
        del op[entry]
        ops = [op]

        with pytest.raises(e.ParamError):
            self.as_connection.operate(self.test_map_key, ops)

    def test_map_remove_by_rank_range(self):
        result_map = self.test_map.copy()
        del result_map['c']
        del result_map['d']
        ops = [{
            "op": aerospike.OP_MAP_REMOVE_BY_RANK_RANGE,
            "bin": self.test_map_bin,
            "index": 2,
            "val": 2,
            "return_type": aerospike.MAP_RETURN_KEY
        }]

        _, _, res = self.as_connection.operate(self.test_map_key, ops)
        assert res[self.test_map_bin] == ['c', 'd']

        _, _, bins = self.as_connection.get(self.test_map_key)
        assert bins[self.test_map_bin] == result_map

    def test_map_remove_by_rank_range_val_instead_of_range(self):
        '''
        TODO: Should not work
        '''
        result_map = self.test_map.copy()
        del result_map['c']
        del result_map['d']
        ops = [{
            "op": aerospike.OP_MAP_REMOVE_BY_RANK_RANGE,
            "bin": self.test_map_bin,
            "index": 2,
            "val": 2,
            "return_type": aerospike.MAP_RETURN_KEY
        }]

        _, _, res = self.as_connection.operate(self.test_map_key, ops)
        assert res[self.test_map_bin] == ['c', 'd']

        _, _, bins = self.as_connection.get(self.test_map_key)
        assert bins[self.test_map_bin] == result_map

    def test_op_map_get_by_key(self):
        ops = [{
            'op': aerospike.OP_MAP_GET_BY_KEY,
            'bin': self.test_map_bin,
            'key': 'b',
            'return_type': aerospike.MAP_RETURN_VALUE
        }]

        _, _, res = self.as_connection.operate(self.test_map_key, ops)

        assert res[self.test_map_bin] == 2

    @pytest.mark.parametrize(
        'entry', ('bin', 'key'))
    def test_op_map_get_by_key_missing_required_entry(self, entry):
        op = {
            'op': aerospike.OP_MAP_GET_BY_KEY,
            'bin': self.test_map_bin,
            'key': 'b',
            'return_type': aerospike.MAP_RETURN_VALUE
        }
        del op[entry]
        ops = [op]
        with pytest.raises(e.ParamError):
            self.as_connection.operate(self.test_map_key, ops)

    def test_op_map_get_by_key_range(self):
        ops = [{
            'op': aerospike.OP_MAP_GET_BY_KEY_RANGE,
            'bin': self.test_map_bin,
            'key': 'b',
            'range': 'e',
            'return_type': aerospike.MAP_RETURN_VALUE
        }]

        _, _, res = self.as_connection.operate(self.test_map_key, ops)

        assert res[self.test_map_bin] == [2, 3, 4]

    @pytest.mark.parametrize(
        'entry', ('bin', 'key', 'range'))
    def test_op_map_get_by_key_range_missing_required_entry(self, entry):
        '''
        TODO Figure out if this is correct
        '''
        op = {
            'op': aerospike.OP_MAP_GET_BY_KEY_RANGE,
            'bin': self.test_map_bin,
            'key': 'b',
            'range': 'e',
            'return_type': aerospike.MAP_RETURN_VALUE
        }
        del op[entry]
        ops = [op]

        with pytest.raises(e.ParamError):
            self.as_connection.operate(self.test_map_key, ops)

    def test_op_map_get_by_value(self):
        ops = [{
            'op': aerospike.OP_MAP_GET_BY_VALUE,
            'bin': self.test_map_bin,
            'val': 2,
            'return_type': aerospike.MAP_RETURN_KEY
        }]

        _, _, res = self.as_connection.operate(self.test_map_key, ops)

        assert res[self.test_map_bin] == ['b']

    @pytest.mark.parametrize(
        'entry', ('bin', 'val'))
    def test_op_map_get_by_value_missing_required_entry(self, entry):
        op = {
            'op': aerospike.OP_MAP_GET_BY_VALUE,
            'bin': self.test_map_bin,
            'val': 2,
            'return_type': aerospike.MAP_RETURN_KEY
        }

        del op[entry]
        ops = [op]

        with pytest.raises(e.ParamError):
            self.as_connection.operate(self.test_map_key, ops)

    def test_op_map_get_by_value_range(self):
        ops = [{
            'op': aerospike.OP_MAP_GET_BY_VALUE_RANGE,
            'bin': self.test_map_bin,
            'val': 2,
            'range': 5,
            'return_type': aerospike.MAP_RETURN_KEY
        }]

        _, _, res = self.as_connection.operate(self.test_map_key, ops)
        assert res[self.test_map_bin] == ['b', 'c', 'd']

    @pytest.mark.parametrize(
        'entry', ('bin', 'val', 'range'))
    def test_op_map_get_by_value_range_missing_required_entry(self, entry):
        op = {
            'op': aerospike.OP_MAP_GET_BY_VALUE_RANGE,
            'bin': self.test_map_bin,
            'val': 2,
            'range': 5,
            'return_type': aerospike.MAP_RETURN_KEY
        }
        del op[entry]

        ops = [op]
        with pytest.raises(e.ParamError):
            self.as_connection.operate(self.test_map_key, ops)

    def test_op_map_get_by_index(self):
        ops = [{
            'op': aerospike.OP_MAP_GET_BY_INDEX,
            'bin': self.test_map_bin,
            'index': 2,
            'return_type': aerospike.MAP_RETURN_KEY
        }]

        _, _, res = self.as_connection.operate(self.test_map_key, ops)
        assert res[self.test_map_bin] == 'c'

    @pytest.mark.parametrize(
        'entry', ('bin', 'index'))
    def test_op_map_get_by_index_missing_required_entry(self, entry):
        op = {
            'op': aerospike.OP_MAP_GET_BY_INDEX,
            'bin': self.test_map_bin,
            'index': 2,
            'return_type': aerospike.MAP_RETURN_KEY
        }
        del op[entry]
        ops = [op]

        with pytest.raises(e.ParamError):
            self.as_connection.operate(self.test_map_key, ops)

    def test_op_map_get_by_index_range(self):
        ops = [{
            "op": aerospike.OP_MAP_GET_BY_INDEX_RANGE,
            "bin": self.test_map_bin,
            "index": 1,
            "val": 2,
            "return_type": aerospike.MAP_RETURN_KEY
        }]

        _, _, res = self.as_connection.operate(self.test_map_key, ops)
        assert res[self.test_map_bin] == ['b', 'c']

    @pytest.mark.parametrize(
        'entry', ('bin', 'index', 'val'))
    def test_op_map_get_by_index_range_missing_required(self, entry):
        op = {
            "op": aerospike.OP_MAP_GET_BY_INDEX_RANGE,
            "bin": self.test_map_bin,
            "index": 1,
            "val": 2,
            "return_type": aerospike.MAP_RETURN_KEY
        }
        del op[entry]
        ops = [op]

        with pytest.raises(e.ParamError):
            self.as_connection.operate(self.test_map_key, ops)

    def test_op_map_get_by_rank(self):
        ops = [{
            'op': aerospike.OP_MAP_GET_BY_RANK,
            'bin': self.test_map_bin,
            'index': 1,
            "return_type": aerospike.MAP_RETURN_KEY
        }]

        _, _, res = self.as_connection.operate(self.test_map_key, ops)
        assert res[self.test_map_bin] == 'b'

    @pytest.mark.parametrize(
        'entry', ('bin', 'index'))
    def test_op_map_get_by_rank_missing_required_entry(self, entry):
        '''
        TODO: this shouldn't be needed
        '''
        op = {
            'op': aerospike.OP_MAP_GET_BY_RANK,
            'bin': self.test_map_bin,
            'index': 1,
            "return_type": aerospike.MAP_RETURN_KEY
        }
        del op[entry]
        ops = [op]

        with pytest.raises(e.ParamError):
            self.as_connection.operate(self.test_map_key, ops)

    def test_op_map_get_by_rank_range(self):

        ops = [{
            'op': aerospike.OP_MAP_GET_BY_RANK_RANGE,
            'bin': self.test_map_bin,
            'index': 1,
            'val': 2,
            'return_type': aerospike.MAP_RETURN_KEY
        }]

        _, _, res = self.as_connection.operate(self.test_map_key, ops)
        assert res[self.test_map_bin] == ['b', 'c']

    @pytest.mark.parametrize(
        'entry', ('bin', 'index', 'val'))
    def test_op_map_get_by_rank_range_val_missing_required_values(self, entry):
        op = {
            'op': aerospike.OP_MAP_GET_BY_RANK_RANGE,
            'bin': self.test_map_bin,
            'index': 1,
            'val': 2,
            'return_type': aerospike.MAP_RETURN_KEY
        }

        del op[entry]
        ops = [op]

        with pytest.raises(e.ParamError):
            self.as_connection.operate(self.test_map_key, ops)
