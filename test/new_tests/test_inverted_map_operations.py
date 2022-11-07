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
            "e" : 1
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

    def test_map_remove_by_key_list(self):
        operations = [map_ops.map_remove_by_key_list(
            self.test_bin, ["a", "b", "c"],  return_type=aerospike.MAP_RETURN_VALUE, inverted=True)]
        ret_vals = get_map_result_from_operation(self.as_connection, self.test_key, operations, self.test_bin)

        assert set(ret_vals) == set([self.test_map["e"], self.test_map["d"]])

        res_map = self.as_connection.get(self.test_key)[2][self.test_bin]
        assert "d" not in res_map
        assert "e" not in res_map

    def test_map_remove_by_key_range(self):
        operations = [map_ops.map_remove_by_key_range(
            self.test_bin, "a", "d",  return_type=aerospike.MAP_RETURN_VALUE, inverted=True)]
        ret_vals = get_map_result_from_operation(self.as_connection, self.test_key, operations, self.test_bin)

        assert set(ret_vals) == set([self.test_map["e"], self.test_map["d"]])

        res_map = self.as_connection.get(self.test_key)[2][self.test_bin]
        assert "d" not in res_map
        assert "e" not in res_map

    def test_map_remove_by_value(self):
        operations = [map_ops.map_remove_by_value(
            self.test_bin, self.test_map["a"], return_type=aerospike.MAP_RETURN_KEY, inverted=True)]
        assert (set(get_map_result_from_operation(self.as_connection, self.test_key, operations, self.test_bin))
            == set(["b", "c", "d", "e"]))

        res_map = self.as_connection.get(self.test_key)[2][self.test_bin]
        assert "b" not in res_map
        assert "c" not in res_map
        assert "d" not in res_map
        assert "e" not in res_map

    def test_map_remove_by_value_list(self):
        operations = [map_ops.map_remove_by_value_list(
            self.test_bin,
            [self.test_map["a"], self.test_map["b"], self.test_map["c"]],
            return_type=aerospike.MAP_RETURN_KEY, inverted=True)]
        ret_vals = get_map_result_from_operation(self.as_connection, self.test_key, operations, self.test_bin)

        assert set(ret_vals) == set(["d", "e"])

        res_map = self.as_connection.get(self.test_key)[2][self.test_bin]
        assert "d" not in res_map
        assert "e" not in res_map

    def test_map_remove_by_value_range(self):
        operations = [map_ops.map_remove_by_value_range(
            self.test_bin, 1, 4,  return_type=aerospike.MAP_RETURN_KEY, inverted=True)]
        ret_vals = get_map_result_from_operation(self.as_connection, self.test_key, operations, self.test_bin)

        assert set(ret_vals) == set(["a","b"])

        res_map = self.as_connection.get(self.test_key)[2][self.test_bin]
        assert "a" not in res_map
        assert "b" not in res_map

    def test_map_remove_by_index_range(self):
        sort_map(self.as_connection, self.test_key, self.test_bin)
        operations = [
            map_ops.map_remove_by_index_range(
                self.test_bin, 1, 3, return_type=aerospike.MAP_RETURN_KEY, inverted=True)
        ]
        ret_vals = get_map_result_from_operation(self.as_connection, self.test_key, operations, self.test_bin)
        assert ret_vals == ["a", "e"]
        res_map = self.as_connection.get(self.test_key)[2][self.test_bin]
        assert "a" not in res_map
        assert "e" not in res_map

    def test_map_remove_by_rank_range(self):
        operations = [map_ops.map_remove_by_rank_range(
            self.test_bin, 1, 3, return_type=aerospike.MAP_RETURN_KEY, inverted=True)]
        ret_vals = get_map_result_from_operation(self.as_connection, self.test_key, operations, self.test_bin)
        assert set(ret_vals) == set(["a", "e"])
        res_map = self.as_connection.get(self.test_key)[2][self.test_bin]
        assert "a" not in res_map
        assert "e" not in res_map


    def test_map_get_by_key_list(self):
        operations = [map_ops.map_get_by_key_list(
            self.test_bin, ["a", "b", "c"],  return_type=aerospike.MAP_RETURN_VALUE, inverted=True)]
        ret_vals = get_map_result_from_operation(self.as_connection, self.test_key, operations, self.test_bin)

        assert set(ret_vals) == set([self.test_map["d"], self.test_map["e"]])

    def test_map_get_by_key_range(self):
        operations = [map_ops.map_get_by_key_range(
            self.test_bin, "a", "d",  return_type=aerospike.MAP_RETURN_VALUE, inverted=True)]
        ret_vals = get_map_result_from_operation(self.as_connection, self.test_key, operations, self.test_bin)

        assert set(ret_vals) == set([self.test_map["d"], self.test_map["e"]])

    def test_map_get_by_value(self):
        operations = [map_ops.map_get_by_value(
            self.test_bin, self.test_map["a"], return_type=aerospike.MAP_RETURN_KEY, inverted=True)]
        assert (set(get_map_result_from_operation(self.as_connection, self.test_key, operations, self.test_bin))
            == set(["b", "c", "d", "e"]))

    def test_map_get_by_value_list(self):
        operations = [map_ops.map_get_by_value_list(
            self.test_bin,
            [self.test_map["a"], self.test_map["b"], self.test_map["c"]],
            return_type=aerospike.MAP_RETURN_KEY, inverted=True)]
        ret_vals = get_map_result_from_operation(self.as_connection, self.test_key, operations, self.test_bin)

        assert set(ret_vals) == set(["d", "e"])

    def test_map_get_by_value_range(self):
        operations = [map_ops.map_get_by_value_range(
            self.test_bin, 1, 4,  return_type=aerospike.MAP_RETURN_KEY, inverted=True)]
        ret_vals = get_map_result_from_operation(self.as_connection, self.test_key, operations, self.test_bin)

        assert set(ret_vals) == set(["a", "b"])

    def test_map_get_by_index_range(self):
        sort_map(self.as_connection, self.test_key, self.test_bin)
        operations = [
            map_ops.map_get_by_index_range(
                self.test_bin, 1, 2, return_type=aerospike.MAP_RETURN_KEY, inverted=True)
        ]
        ret_vals = get_map_result_from_operation(self.as_connection, self.test_key, operations, self.test_bin)
        assert set(ret_vals) == set(["a", "d", "e"])

    def test_map_get_by_rank_range(self):
        sort_map(self.as_connection, self.test_key, self.test_bin)
        operations = [
            map_ops.map_get_by_rank_range(
                self.test_bin, 1, 2, return_type=aerospike.MAP_RETURN_KEY, inverted=True)
        ]
        ret_vals = get_map_result_from_operation(self.as_connection, self.test_key, operations, self.test_bin)
        assert set(ret_vals) == set(["a", "b", "e"])
