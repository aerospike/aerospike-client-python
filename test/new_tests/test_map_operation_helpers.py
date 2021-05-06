# -*- coding: utf-8 -*-
import pytest
import sys
from aerospike import exception as e
from aerospike_helpers.operations import map_operations as map_ops

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)


def get_map_result_from_operation(client, key, operations, res_bin):
    '''
    Map operations return a 1 bin record. 
    This operation extracts the dictionary from it.
    '''
    _, _, result_bins = client.operate(key, operations)
    return result_bins[res_bin]


def maps_have_same_keys(map1, map2):
    '''
    Return True iff the two maps have an identical set of keys
    '''
    if len(map1) != len(map2):
        return False
    
    for key in map1:
        if key not in map2:
            return False
    return True


def maps_have_same_values(map1, map2):
    if len(map1) != len(map2):
        return False
    
    for key in map1:
        if key not in map2 or map1[key] != map2[key]:
            return False
    return True


def sort_map(client, test_key, test_bin):
    map_policy = {"map_write_mode": aerospike.MAP_CREATE_ONLY, "map_order": aerospike.MAP_KEY_ORDERED}
    operations = [map_ops.map_set_policy(test_bin, map_policy)]
    client.operate(test_key, operations)

class TestNewListOperationsHelpers(object):

    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        """
        Setup Method
        """
        self.keys = []
        self.test_map = {
            "a" : 5,
            "b" : 4,
            "c" : 3,
            "d" : 2,
            "e" : 1,
            "f" : True,
            "g" : False
        }

        self.test_key = 'test', 'demo', 'new_map_op'
        self.test_bin = 'map'
        self.as_connection.put(self.test_key, {self.test_bin: self.test_map})
        self.keys.append(self.test_key)

        yield

        for key in self.keys:
            try:
                self.as_connection.remove(key)
            except e.AerospikeError:
                pass

    def test_map_set_policy(self):
        '''
        Test setting map policy with an operation
        '''
        map_policy = {"map_write_mode": aerospike.MAP_CREATE_ONLY, "map_order": aerospike.MAP_KEY_VALUE_ORDERED}
        operations = [map_ops.map_set_policy(self.test_bin, map_policy)]

        self.as_connection.operate(self.test_key, operations)

    def test_map_put(self):
        operations =[map_ops.map_put(self.test_bin, "new", "map_put")]
        self.as_connection.operate(self.test_key, operations)
        res_map = self.as_connection.get(self.test_key)[2][self.test_bin]
        assert res_map["new"] == "map_put"

    def test_map_put_items(self):
        new_items = {"new": 1, "new2": 3}
        expected_items = {
            "a" : 5,
            "b" : 4,
            "c" : 3,
            "d" : 2,
            "e" : 1,
            "f" : True,
            "g" : False,
            "new": 1,
            "new2": 3
        }
        operations = [map_ops.map_put_items(self.test_bin, new_items)]
        self.as_connection.operate(self.test_key, operations)
        res_map = self.as_connection.get(self.test_key)[2][self.test_bin]

        assert res_map == expected_items

    def test_map_increment(self):
        operations = [map_ops.map_increment(self.test_bin, "a", 100)]
        self.as_connection.operate(self.test_key, operations)
        res_map = self.as_connection.get(self.test_key)[2][self.test_bin]
        assert res_map["a"] == self.test_map["a"] + 100

    def test_map_decrement(self):
        operations = [map_ops.map_decrement(self.test_bin, "a", 100)]
        self.as_connection.operate(self.test_key, operations)
        res_map = self.as_connection.get(self.test_key)[2][self.test_bin]
        assert res_map["a"] == self.test_map["a"] - 100

    def test_map_size(self):
        operations = [map_ops.map_size(self.test_bin)]
        assert (
            get_map_result_from_operation(self.as_connection, self.test_key, operations, self.test_bin)
            == len(self.test_map)
        )

    def test_map_clear(self):
        operations = [map_ops.map_clear(self.test_bin)]
        self.as_connection.operate(self.test_key, operations)
        res_map = self.as_connection.get(self.test_key)[2][self.test_bin]
        assert res_map == {}
    
    def test_map_remove_by_key(self):
        operations = [map_ops.map_remove_by_key(
            self.test_bin, "a", return_type=aerospike.MAP_RETURN_VALUE)]
        assert (get_map_result_from_operation(self.as_connection, self.test_key, operations, self.test_bin)
            == self.test_map["a"])

        res_map = self.as_connection.get(self.test_key)[2][self.test_bin]
        assert "a" not in res_map

    def test_map_remove_by_key_list(self):
        operations = [map_ops.map_remove_by_key_list(
            self.test_bin, ["a", "b", "c"],  return_type=aerospike.MAP_RETURN_VALUE)]
        ret_vals = get_map_result_from_operation(self.as_connection, self.test_key, operations, self.test_bin)

        assert set(ret_vals) == set([self.test_map["a"], self.test_map["b"], self.test_map["c"]])

        res_map = self.as_connection.get(self.test_key)[2][self.test_bin]
        assert "a" not in res_map
        assert "b" not in res_map
        assert "c" not in res_map

    def test_map_remove_by_key_range(self):
        operations = [map_ops.map_remove_by_key_range(
            self.test_bin, "a", "d",  return_type=aerospike.MAP_RETURN_VALUE)]
        ret_vals = get_map_result_from_operation(self.as_connection, self.test_key, operations, self.test_bin)

        assert set(ret_vals) == set([self.test_map["a"], self.test_map["b"], self.test_map["c"]])

        res_map = self.as_connection.get(self.test_key)[2][self.test_bin]
        assert "a" not in res_map
        assert "b" not in res_map
        assert "c" not in res_map

    def test_map_remove_by_value(self):
        operations = [map_ops.map_remove_by_value(
            self.test_bin, self.test_map["a"], return_type=aerospike.MAP_RETURN_KEY)]
        assert (get_map_result_from_operation(self.as_connection, self.test_key, operations, self.test_bin)
            == ["a"])

        res_map = self.as_connection.get(self.test_key)[2][self.test_bin]
        assert "a" not in res_map


    def test_map_remove_by_value_list(self):
        operations = [map_ops.map_remove_by_value_list(
            self.test_bin,
            [self.test_map["a"], self.test_map["b"], self.test_map["c"]],
            return_type=aerospike.MAP_RETURN_KEY)]
        ret_vals = get_map_result_from_operation(self.as_connection, self.test_key, operations, self.test_bin)

        assert set(ret_vals) == set(["a", "b", "c"])

        res_map = self.as_connection.get(self.test_key)[2][self.test_bin]
        assert "a" not in res_map
        assert "b" not in res_map
        assert "c" not in res_map

    def test_map_remove_by_value_range(self):
        operations = [map_ops.map_remove_by_value_range(
            self.test_bin, 1, 4,  return_type=aerospike.MAP_RETURN_KEY)]
        ret_vals = get_map_result_from_operation(self.as_connection, self.test_key, operations, self.test_bin)

        assert set(ret_vals) == set(["e", "d", "c"])

        res_map = self.as_connection.get(self.test_key)[2][self.test_bin]
        assert "e" not in res_map
        assert "d" not in res_map
        assert "c" not in res_map

    def test_map_remove_by_index(self):
        sort_map(self.as_connection, self.test_key, self.test_bin)
        operations = [
            map_ops.map_remove_by_index(self.test_bin, 1, return_type=aerospike.MAP_RETURN_KEY)
        ]
        ret_vals = get_map_result_from_operation(self.as_connection, self.test_key, operations, self.test_bin)
        assert ret_vals == "b"
        res_map = self.as_connection.get(self.test_key)[2][self.test_bin]
        assert "b" not in res_map

    def test_map_remove_by_index_range(self):
        sort_map(self.as_connection, self.test_key, self.test_bin)
        operations = [
            map_ops.map_remove_by_index_range(self.test_bin, 1, 2, return_type=aerospike.MAP_RETURN_KEY)
        ]
        ret_vals = get_map_result_from_operation(self.as_connection, self.test_key, operations, self.test_bin)
        assert ret_vals == ["b", "c"]
        res_map = self.as_connection.get(self.test_key)[2][self.test_bin]
        assert "b" not in res_map
        assert "c" not in res_map
    
    def test_map_remove_by_rank(self):
        operations = [map_ops.map_remove_by_rank(self.test_bin, 1, return_type=aerospike.MAP_RETURN_KEY)]
        ret_vals = get_map_result_from_operation(self.as_connection, self.test_key, operations, self.test_bin)
        assert ret_vals == "d"
        res_map = self.as_connection.get(self.test_key)[2][self.test_bin]
        assert "d" not in res_map

    def test_map_remove_by_rank_range(self):
        operations = [map_ops.map_remove_by_rank_range(self.test_bin, 1, 2, return_type=aerospike.MAP_RETURN_KEY)]
        ret_vals = get_map_result_from_operation(self.as_connection, self.test_key, operations, self.test_bin)
        assert set(ret_vals) == set(["d", "c"])
        res_map = self.as_connection.get(self.test_key)[2][self.test_bin]
        assert "d" not in res_map
        assert "c" not in res_map


    def test_map_get_by_key(self):
        operations = [map_ops.map_get_by_key(
            self.test_bin, "a", return_type=aerospike.MAP_RETURN_VALUE)]
        assert (get_map_result_from_operation(self.as_connection, self.test_key, operations, self.test_bin)
            == self.test_map["a"])

    def test_map_get_by_key_list(self):
        operations = [map_ops.map_get_by_key_list(
            self.test_bin, ["a", "b", "c"],  return_type=aerospike.MAP_RETURN_VALUE)]
        ret_vals = get_map_result_from_operation(self.as_connection, self.test_key, operations, self.test_bin)

        assert set(ret_vals) == set([self.test_map["a"], self.test_map["b"], self.test_map["c"]])

    def test_map_get_by_key_range(self):
        operations = [map_ops.map_get_by_key_range(
            self.test_bin, "a", "d",  return_type=aerospike.MAP_RETURN_VALUE)]
        ret_vals = get_map_result_from_operation(self.as_connection, self.test_key, operations, self.test_bin)

        assert set(ret_vals) == set([self.test_map["a"], self.test_map["b"], self.test_map["c"]])

    def test_map_get_by_value(self):
        operations = [map_ops.map_get_by_value(
            self.test_bin, self.test_map["a"], return_type=aerospike.MAP_RETURN_KEY)]
        assert (get_map_result_from_operation(self.as_connection, self.test_key, operations, self.test_bin)
            == ["a"])

    def test_map_get_by_value_list(self):
        operations = [map_ops.map_get_by_value_list(
            self.test_bin,
            [self.test_map["a"], self.test_map["b"], self.test_map["c"]],
            return_type=aerospike.MAP_RETURN_KEY)]
        ret_vals = get_map_result_from_operation(self.as_connection, self.test_key, operations, self.test_bin)

        assert set(ret_vals) == set(["a", "b", "c"])

    def test_map_get_by_value_range(self):
        operations = [map_ops.map_get_by_value_range(
            self.test_bin, 1, 4,  return_type=aerospike.MAP_RETURN_KEY)]
        ret_vals = get_map_result_from_operation(self.as_connection, self.test_key, operations, self.test_bin)

        assert set(ret_vals) == set(["e", "d", "c"])

    def test_map_get_by_index(self):
        sort_map(self.as_connection, self.test_key, self.test_bin)
        operations = [
            map_ops.map_get_by_index(self.test_bin, 1, return_type=aerospike.MAP_RETURN_KEY)
        ]
        ret_vals = get_map_result_from_operation(self.as_connection, self.test_key, operations, self.test_bin)
        assert ret_vals == "b"

    def test_map_get_by_index_range(self):
        sort_map(self.as_connection, self.test_key, self.test_bin)
        operations = [
            map_ops.map_get_by_index_range(self.test_bin, 1, 2, return_type=aerospike.MAP_RETURN_KEY)
        ]
        ret_vals = get_map_result_from_operation(self.as_connection, self.test_key, operations, self.test_bin)
        assert ret_vals == ["b", "c"]

    def test_map_get_by_rank(self):
        operations = [map_ops.map_get_by_rank(self.test_bin, 1, return_type=aerospike.MAP_RETURN_KEY)]
        ret_vals = get_map_result_from_operation(self.as_connection, self.test_key, operations, self.test_bin)
        assert ret_vals == "d"

    def test_map_get_by_rank_range(self):
        sort_map(self.as_connection, self.test_key, self.test_bin)
        operations = [
            map_ops.map_get_by_rank_range(self.test_bin, 1, 2, return_type=aerospike.MAP_RETURN_KEY)
        ]
        ret_vals = get_map_result_from_operation(self.as_connection, self.test_key, operations, self.test_bin)
        assert ret_vals == ["d", "c"]