# -*- coding: utf-8 -*-
import pytest
import sys
from aerospike import exception as e
from aerospike_helpers.operations import list_operations
from aerospike_helpers.operations import map_operations as map_ops
aerospike = pytest.importorskip("aerospike")

try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)


def get_list_result_from_operation(client, key, operation, binname):
    _, _, result_bins = client.operate(key, [operation])
    return result_bins[binname]

def skip_less_than_430(version):
    if version < [4, 3]:
        print(version)
        pytest.skip("Requires server > 4.3.0 to work")

class TestNewRelativeListOperations(object):

    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        """
        Setup Method
        """
        skip_less_than_430(self.server_version)
        self.keys = []
        # INDEXES    0, 1, 2, 3, 4, 05
        # RINDEX     5, 4, 3, 2, 1, 0
        # RANK       2, 1, 0, 3, 4, 5
        # RRANK     -4,-5,-6,-3,-2,-1
        self.test_list = [3, 1, 5, 7, 9, 11]

        self.test_key = 'test', 'demo', 'new_list_op'
        self.test_bin = 'list'
        self.as_connection.put(self.test_key, {self.test_bin: self.test_list})

        # Make sure the list is ordered, in order to get expected return order.
        ops = [list_operations.list_sort("list", 0),
               list_operations.list_set_order("list", aerospike.LIST_ORDERED)]
        self.as_connection.operate(self.test_key, ops)
        self.keys.append(self.test_key)

        yield

        for key in self.keys:
            try:
                self.as_connection.remove(key)
            except e.AerospikeError:
                pass

    @pytest.mark.parametrize(
        "value, offset, count, expected",
        (
            (5, 0, None, [5, 7, 9, 11]),
            (5, 0, 2, [5, 7]),
            (4, -2, 3, [1, 3, 5]),
            (0, 0, 3, [1, 3, 5]),
            (150, 0, None, [])
        ))
    def test_list_get_by_value_rank_range_relative(self, value, offset, count, expected):
        '''
        Without a return type this should return the value
        '''
        operation = list_operations.list_get_by_value_rank_range_relative(
            self.test_bin, value, offset, return_type=aerospike.LIST_RETURN_VALUE,
            count=count)

        result = get_list_result_from_operation(
            self.as_connection, self.test_key, operation, self.test_bin)

        assert result == expected


    @pytest.mark.parametrize(
        "value, offset, count, expected",
        (
            (5, 0, None, [5, 7, 9, 11]),
            (5, 0, 2, [5, 7]),
            (4, -2, 3, [1, 3, 5]),
            (0, 0, 3, [1, 3, 5]),
            (150, 0, None, [])
        ))
    def test_list_remove_by_value_rank_range_relative(self, value, offset, count, expected):
        '''
        Without a return type this should return the value
        '''
        operation = list_operations.list_remove_by_value_rank_range_relative(
            self.test_bin, value, offset, return_type=aerospike.LIST_RETURN_VALUE,
            count=count)

        result = get_list_result_from_operation(
            self.as_connection, self.test_key, operation, self.test_bin)
        assert result == expected

        _, _, bins = self.as_connection.get(self.test_key)
        list_items = bins[self.test_bin]

        # Ensure that the correct number of items were removed
        assert len(list_items) == (len(self.test_list) - len(expected))

        # Ensure that the expected items were removed
        for item in list_items:
            assert item not in expected

    @pytest.mark.parametrize(
        "return_type, expected",
        (
            (aerospike.LIST_RETURN_VALUE, 5),
            (aerospike.LIST_RETURN_INDEX, 2),
            (aerospike.LIST_RETURN_RANK, 2),
            (aerospike.LIST_RETURN_REVERSE_RANK, 3),
            (aerospike.LIST_RETURN_REVERSE_INDEX, 3),
        ))
    def test_list_remove_by_value_rank_range_relative(self, return_type, expected):
        '''
        Without a return type this should return the value
        '''
        operation = list_operations.list_remove_by_value_rank_range_relative(
            self.test_bin, 5, 0, return_type=return_type,
            count=1)

        result = get_list_result_from_operation(
            self.as_connection, self.test_key, operation, self.test_bin)
        assert result == [expected]

    def test_list_remove_by_value_rank_range_relative_ret_none(self):
        '''
        Without a return type this should return the value
        '''
        operation = list_operations.list_remove_by_value_rank_range_relative(
            self.test_bin, 5, 0, return_type=aerospike.LIST_RETURN_NONE,
            count=1)

        _, _, bins = self.as_connection.operate(self.test_key, [operation])
        assert bins == {}

    def test_list_remove_by_value_rank_range_relative_ret_count(self):
        '''
        Without a return type this should return the value
        '''
        operation = list_operations.list_remove_by_value_rank_range_relative(
            self.test_bin, 5, 0, return_type=aerospike.LIST_RETURN_COUNT,
            count=1)

        result = get_list_result_from_operation(
            self.as_connection, self.test_key, operation, self.test_bin)
        assert result == 1

    @pytest.mark.parametrize(
        "value, offset, count, expected_err",
        (
            (5, "zero", None, e.ParamError),
            (5, 0, "None", e.ParamError),
            (5, 1.5, "None", e.ParamError),
            (5, 0, 1.5, e.ParamError),
        ))
    def test_list_remove_by_value_rank_invalid_params(self, value, offset, count, expected_err):
        '''
        Without a return type this should return the value
        '''
        operation = list_operations.list_remove_by_value_rank_range_relative(
            self.test_bin, value, offset, return_type=aerospike.LIST_RETURN_VALUE,
            count=count)

        with pytest.raises(expected_err):
            self.as_connection.operate(self.test_key, [operation])

    @pytest.mark.parametrize(
        "value, offset, count, expected_err",
        (
            (5, "zero", None, e.ParamError),
            (5, 0, "None", e.ParamError),
            (5, 1.5, "None", e.ParamError),
            (5, 0, 1.5, e.ParamError),
        ))
    def test_list_get_by_value_rank_invalid_params(self, value, offset, count, expected_err):
        '''
        Without a return type this should return the value
        '''
        operation = list_operations.list_get_by_value_rank_range_relative(
            self.test_bin, value, offset, return_type=aerospike.LIST_RETURN_VALUE,
            count=count)

        with pytest.raises(expected_err):
            self.as_connection.operate(self.test_key, [operation])

    # INVERTED TESTS
    # LIST = [3, 1, 5, 7, 9, 11]
    @pytest.mark.parametrize(
        "value, offset, count, expected",
        (
            (5, 0, None, [1, 3]),
            (5, 0, 2, [ 1, 3, 9, 11]),
            (4, -2, 3, [7, 9, 11]),
            (0, 0, 3, [7, 9, 11]),
            (150, 0, None, [1, 3, 5, 7, 9, 11])
        ))
    def test_list_get_by_value_rank_range_relative_inverted(self, value, offset, count, expected):
        '''
        Without a return type this should return the value
        '''
        operation = list_operations.list_get_by_value_rank_range_relative(
            self.test_bin, value, offset, return_type=aerospike.LIST_RETURN_VALUE,
            count=count, inverted=True)

        result = get_list_result_from_operation(
            self.as_connection, self.test_key, operation, self.test_bin)

        assert result == expected


    @pytest.mark.parametrize(
        "value, offset, count, expected",
        (
            (5, 0, None, [1, 3]),
            (5, 0, 2, [1, 3, 9, 11]),
            (4, -2, 3, [7, 9, 11]),
            (0, 0, 3, [7, 9, 11]),
            (150, 0, None, [1, 3, 5, 7, 9, 11])
        ))
    def test_list_remove_by_value_rank_range_relative_inverted(self, value, offset, count, expected):
        '''
        Without a return type this should return the value
        '''
        operation = list_operations.list_remove_by_value_rank_range_relative(
            self.test_bin, value, offset, return_type=aerospike.LIST_RETURN_VALUE,
            count=count, inverted=True)

        result = get_list_result_from_operation(
            self.as_connection, self.test_key, operation, self.test_bin)
        assert result == expected

        _, _, bins = self.as_connection.get(self.test_key)
        list_items = bins[self.test_bin]

        # Ensure that the correct number of items were removed
        assert len(list_items) == (len(self.test_list) - len(expected))

        # Ensure that the expected items were removed
        for item in list_items:
            assert item not in expected


def get_list_result_from_operation(client, key, operation, binname):
    _, _, result_bins = client.operate(key, [operation])
    return result_bins[binname]


class TestNewRelativeMapOperations(object):

    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        """
        Setup Method
        """

        skip_less_than_430(self.server_version)

        self.keys = []
        # INDEXES    0, 1, 2, 3, 4, 05
        # RINDEX     5, 4, 3, 2, 1, 0
        # RANK       2, 1, 0, 3, 4, 5
        # RRANK     -4,-5,-6,-3,-2,-1
        self.test_map = {
            0: 6,
            5: 12,
            10: 18,
            15: 24
        }

        self.test_key = 'test', 'demo', 'new_map_op'
        self.test_bin = 'map'
        self.as_connection.put(self.test_key, {self.test_bin: self.test_map})

        map_policy = {"map_write_mode": aerospike.MAP_CREATE_ONLY, "map_order": aerospike.MAP_KEY_ORDERED}
        operations = [map_ops.map_set_policy(self.test_bin, map_policy)]
        self.as_connection.operate(self.test_key, operations)
        self.keys.append(self.test_key)

        yield

        for key in self.keys:
            try:
                self.as_connection.remove(key)
            except e.AerospikeError:
                pass

    @pytest.mark.parametrize(
        "value, offset, count, expected",
        (
            (7, 0, None, [5, 10, 15]),
            (7, 1, None, [10, 15]),
            (7, -1, 2, [0, 5]),
        )
    )
    def test_get_by_value_rank_range_rel(self, value, offset, count, expected):
        operation = map_ops.map_get_by_value_rank_range_relative(
            self.test_bin, value, offset, return_type=aerospike.MAP_RETURN_KEY, count=count,
            inverted=False
        )
        res = get_list_result_from_operation(
            self.as_connection, self.test_key, operation, self.test_bin)

        assert res == expected

    @pytest.mark.parametrize(
        "value, offset, count, expected",
        (
            (7, 0, None, [5, 10, 15]),
            (7, 1, None, [10, 15]),
            (7, -1, 2, [0, 5]),
        )
    )
    def test_remove_by_value_rank_range_rel(self, value, offset, count, expected):
        operation = map_ops.map_remove_by_value_rank_range_relative(
            self.test_bin, value, offset, return_type=aerospike.MAP_RETURN_KEY, count=count,
            inverted=False
        )
        res = get_list_result_from_operation(
            self.as_connection, self.test_key, operation, self.test_bin)

        assert res == expected
        test_map = self.as_connection.get(self.test_key)[2][self.test_bin]

        assert len(test_map) == (len(self.test_map) - len(expected))

        for item in expected:
            assert item not in test_map


    @pytest.mark.parametrize(
        "key, offset, count, expected",
        (
            (4, 0, None, [5, 10, 15]),
            (4, 1, None, [10, 15]),
            (4, -1, 2, [0, 5]),
        )
    )
    def test_get_by_key_index_range_rel(self, key, offset, count, expected):
        operation = map_ops.map_get_by_key_index_range_relative(
            self.test_bin, key, offset, return_type=aerospike.MAP_RETURN_KEY, count=count,
            inverted=False
        )
        res = get_list_result_from_operation(
            self.as_connection, self.test_key, operation, self.test_bin)

        assert res == expected

    @pytest.mark.parametrize(
        "key, offset, count, expected",
        (
            (4, 0, None, [5, 10, 15]),
            (4, 1, None, [10, 15]),
            (4, -1, 2, [0, 5]),
        )
    )
    def test_remove_by_key_index_range_rel(self, key, offset, count, expected):
        operation = map_ops.map_remove_by_key_index_range_relative(
            self.test_bin, key, offset, return_type=aerospike.MAP_RETURN_KEY, count=count,
            inverted=False
        )
        res = get_list_result_from_operation(
            self.as_connection, self.test_key, operation, self.test_bin)

        assert res == expected
        test_map = self.as_connection.get(self.test_key)[2][self.test_bin]

        assert len(test_map) == (len(self.test_map) - len(expected))

        for item in expected:
            assert item not in test_map


    @pytest.mark.parametrize(
        "return_type, expected",
        (
            (aerospike.MAP_RETURN_INDEX, [1]),
            (aerospike.MAP_RETURN_REVERSE_INDEX, [2]),
            (aerospike.MAP_RETURN_RANK, [1]),
            (aerospike.MAP_RETURN_REVERSE_RANK, [2]),
            (aerospike.MAP_RETURN_VALUE, [12]),
            (aerospike.MAP_RETURN_KEY_VALUE, [5, 12]),
            (aerospike.MAP_RETURN_COUNT, 1)
        ))
    def test_list_get_by_key_index_range_relative_rettype(self, return_type, expected):
        '''
        Without a return type this should return the value
        '''
        operation = map_ops.map_get_by_key_index_range_relative(
            self.test_bin, 5, 0, return_type=return_type, count=1,
            inverted=False
        )
        res = get_list_result_from_operation(
            self.as_connection, self.test_key, operation, self.test_bin)

        assert res == expected


    @pytest.mark.parametrize(
        "function",
        (
            map_ops.map_get_by_value_rank_range_relative,
            map_ops.map_get_by_key_index_range_relative,
            map_ops.map_remove_by_key_index_range_relative,
            map_ops.map_remove_by_value_rank_range_relative
        )
    )
    @pytest.mark.parametrize(
        "key, offset, count, err",
        (
            (4, "zero", None, e.ParamError),
            (4, 1, "count", e.ParamError),
        )
    )
    def test_remove_by_key_index_range_invalid(self, function, key, offset, count, err):
        operation = function(
            self.test_bin, key, offset, return_type=aerospike.MAP_RETURN_KEY, count=count,
            inverted=False
        )

        with pytest.raises(err):
            self.as_connection.operate(self.test_key, [operation])
