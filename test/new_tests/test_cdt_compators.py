# These fail if we don't have a new server
# -*- coding: utf-8 -*-
import pytest
import aerospike
from aerospike import exception as e
from aerospike_helpers.operations import list_operations as lo
from aerospike_helpers.operations import map_operations as map_ops

def get_list_result_from_operation(client, key, operation, binname):
    '''
    Just perform a single operation and return the bins portion of the
    result
    '''
    _, _, result_bins = client.operate(key, [operation])
    return result_bins[binname]


@pytest.mark.xfail(reason="This requires server 4.3.13")
class TestNewRelativeCDTValues(object):
    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        """
        Setup Method
        """
        self.keys = []
        cdt_list_val = [
            [0, "a"],
            [1, "b"],
            [1, "c"],
            [1, "d", "e"],
            [2, "f"],
            [2, "two"],
            [3, "g"]
        ]
        cdt_map_val = {
            'a': [0, 'a'],
            'b': [1, 'b'],
            'c': [1, 'c'],
            'd': [1, 'd', 'e'],
            'e': [2, 'f'],
            'f': [2, 'two'],
            'g': [3, 'g']
        }

        self.cdt_key = ('test', 'cdt_values', 'wildcard')
        self.cdt_list_bin = "cdt_list_bin"
        self.cdt_map_bin = "cdt_map_bin"

        self.as_connection.put(
            self.cdt_key,
            {
                self.cdt_list_bin: cdt_list_val,
                self.cdt_map_bin: cdt_map_val
            }
        )
        # Make sure the list is ordered, in order to get expected return order.
        ops = [lo.list_sort(self.cdt_list_bin, 0),
               lo.list_set_order(self.cdt_list_bin, aerospike.LIST_ORDERED)]
        self.as_connection.operate(self.cdt_key, ops)

        self.keys.append(self.cdt_key)

        yield

        for rec_key in self.keys:
            try:
                self.as_connection.remove(rec_key)
            except e.AerospikeError:
                pass

    def test_cdt_wild_card_list_value_multi_element(self):
        operation = lo.list_get_by_value(
            self.cdt_list_bin, [1, aerospike.CDTWildcard()], aerospike.LIST_RETURN_VALUE)            

        result = get_list_result_from_operation(
            self.as_connection,
            self.keys[0],
            operation,
            self.cdt_list_bin
            )

        # All items starting with 1
        assert len(result) == 3
        for lst in result:
            assert lst[0] == 1

    def test_cdt_wild_card_list_value(self):
        # This is does gthe value match [*]
        operation = lo.list_get_by_value(
            self.cdt_list_bin, aerospike.CDTWildcard(), aerospike.LIST_RETURN_VALUE)            

        result = get_list_result_from_operation(
            self.as_connection,
            self.keys[0],
            operation,
            self.cdt_list_bin
            )

        # All items starting with 1
        assert len(result) == 7

    def test_cdt_infinite_list_range_value(self):
        operation = lo.list_get_by_value_range(
            self.cdt_list_bin,
            aerospike.LIST_RETURN_VALUE,
            [1, aerospike.null()],
            [1, aerospike.CDTInfinite()]
        )            

        result = get_list_result_from_operation(
            self.as_connection,
            self.keys[0],
            operation,
            self.cdt_list_bin
            )

        # All items starting with 1
        assert len(result) == 3
        for lst in result:
            assert lst[0] == 1

    def test_map_value_wildcard(self):
        operation = map_ops.map_get_by_value(
            self.cdt_map_bin,
            aerospike.CDTWildcard(),
            aerospike.MAP_RETURN_KEY
        )
        result = get_list_result_from_operation(
            self.as_connection,
            self.cdt_key,
            operation,
            self.cdt_map_bin
        )

        assert set(result) == set(('a', 'b', 'c', 'd', 'e', 'f', 'g'))

    def test_map_value_wildcard(self):
        operation = map_ops.map_get_by_value(
            self.cdt_map_bin,
            [1, aerospike.CDTWildcard()],
            aerospike.MAP_RETURN_KEY,
        )
        result = get_list_result_from_operation(
            self.as_connection,
            self.cdt_key,
            operation,
            self.cdt_map_bin
        )

        assert set(result) == set(('b', 'c', 'd'))

    def test_map_value_null_infinity_range(self):
        operation = map_ops.map_get_by_value_range(
            self.cdt_map_bin,
            [1, aerospike.null()],
            [1, aerospike.CDTInfinite()],
            aerospike.MAP_RETURN_KEY
        )
        result = get_list_result_from_operation(
            self.as_connection,
            self.cdt_key,
            operation,
            self.cdt_map_bin
        )

        assert set(result) == set(('b', 'c', 'd'))
