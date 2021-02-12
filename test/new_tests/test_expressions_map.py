# -*- coding: utf-8 -*-

import pytest
import sys
from .test_base_class import TestBaseClass
from aerospike import exception as e
from .as_status_codes import AerospikeStatus
from aerospike_helpers import cdt_ctx
from aerospike_helpers.expressions import *
from aerospike_helpers.operations import map_operations
from aerospike_helpers.operations import list_operations
from aerospike_helpers.operations import hll_operations
from aerospike_helpers.operations import operations
from math import sqrt, ceil, floor

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)

# Constants
_NUM_RECORDS = 9

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

GEO_POLY = aerospike.GeoJSON(
                            {"type": "Polygon",
                            "coordinates": [[[-122.500000, 37.000000],
                                            [-121.000000, 37.000000],
                                            [-121.000000, 38.080000],
                                            [-122.500000, 38.080000],
                                            [-122.500000, 37.000000]]]})

GEO_POLY1 = aerospike.GeoJSON(
                            {"type": "Polygon",
                            "coordinates": [[[-132.500000, 47.000000],
                                            [-131.000000, 47.000000],
                                            [-131.000000, 48.080000],
                                            [-132.500000, 48.080000],
                                            [-132.500000, 47.000000]]]})

GEO_POLY2 = aerospike.GeoJSON(
                            {"type": "Polygon",
                            "coordinates": [[[-132.500000, 47.000000],
                                            [-131.000000, 47.000000],
                                            [-131.000000, 48.080000],
                                            [-132.500000, 48.080000],
                                            [-132.500000, 80.000000]]]})

def add_ctx_op(ctx_type, value):
    ctx_func = ctx_ops[ctx_type]
    return ctx_func(value)

def verify_multiple_expression_result(client, test_ns, test_set, expr, op_bin, expected):
    keys = [(test_ns, test_set, i) for i in range(_NUM_RECORDS + 1)]

    # batch get
    res = [rec for rec in client.get_many(keys, policy={'expressions': expr}) if rec[2]]

    assert len(res) == expected


class TestUsrDefinedClass():

    __test__ = False

    def __init__(self, i):
        self.data = i


LIST_BIN_EXAMPLE = [
                None,
                8,
                "string_test" + str(8),
                [26, 27, 28, 8],
                {32: 32, 33: 33, 8: 8, 31: 31},
                bytearray("bytearray_test" + str(8), "utf8"),
                ("bytes_test" + str(8)).encode("utf8"),
                8 % 2 == 1,
                aerospike.null,
                TestUsrDefinedClass(8),
                float(8),
                GEO_POLY
]


class TestExpressions(TestBaseClass):

    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        self.test_ns = 'test'
        self.test_set = 'demo'

        for i in range(_NUM_RECORDS):
            key = ('test', u'demo', i)
            rec = {'name': 'name%s' % (str(i)), 't': True,
                    'age': i,
                    'balance': i * 10,
                    'key': i, 'alt_name': 'name%s' % (str(i)),
                    'list_bin': [
                        None,
                        i,
                        "string_test" + str(i),
                        [26, 27, 28, i],
                        {31: 31, 32: 32, 33: 33, i: i},
                        bytearray("bytearray_test" + str(i), "utf8"),
                        ("bytes_test" + str(i)).encode("utf8"),
                        i % 2 == 1,
                        aerospike.null,
                        TestUsrDefinedClass(i),
                        float(i),
                        GEO_POLY
                    ],
                    'mlist_bin': [
                        {1: 2},
                        {1: 3},
                        {1: 4}
                    ],
                    'imap_bin': {
                        1: 1,
                        2: 2,
                        3: 6,
                    },
                    'smap_bin': {
                        'b': 'b',
                        'd': 'd',
                        'f': 'f'
                    },
                    'lmap_bin': {
                        1: [1, 2],
                        2: [1, 3],
                        3: [1, 4]
                    },
                    'mmap_bin': {
                        1: {1: 2},
                        2: {1: 3},
                        3: {1: 4}
                    },
                    'bymap_bin': {
                        1: 'b'.encode("utf8"),
                        2: 'd'.encode("utf8"),
                        3: 'f'.encode("utf8")
                    },
                    'bomap_bin': {
                        1: False,
                        2: False,
                        3: True
                    },
                    'nmap_bin': {
                        1: None,
                        2: aerospike.null,
                        3: aerospike.null
                    },
                    'blmap_bin': {
                        1: TestUsrDefinedClass(1),
                        2: TestUsrDefinedClass(3),
                        3: TestUsrDefinedClass(4)
                    },
                    'fmap_bin': {
                        1.0: 1.0,
                        2.0: 2.0,
                        6.0: 6.0
                    },
                    'gmap_bin': {
                        1: GEO_POLY,
                        2: GEO_POLY1,
                        3: GEO_POLY2
                    }
                }
            self.as_connection.put(key, rec)

        def teardown():
            for i in range(_NUM_RECORDS):
                key = ('test', u'demo', i)
                as_connection.remove(key)

        request.addfinalizer(teardown)

    @pytest.mark.parametrize("bin, values, keys, expected", [
        ("imap_bin", [ResultType.INTEGER, 6, 2, 7, [2, 6], 1, ResultType.INTEGER], [3, 2, 4, [2, 3], 2], [2, [2, 6], [2, 6]]),
        ("smap_bin", [ResultType.INTEGER, 'f', 'd', 'g', ['d', 'f'], 'b', ResultType.STRING], ['f', 'd', 'g', ['d', 'f'], 'd'], ['d', ['d', 'f'], ['d', 'f']]),
        ("lmap_bin", [ResultType.INTEGER, [1, 4], [1, 3], [1, 5], [[1, 3], [1, 4]], [1, 2], ResultType.LIST], [3, 2, 4, [2, 3], 2], [[1, 3], [[1, 3], [1, 4]], [[1, 3], [1, 4]]]),
    ])
    def test_map_read_ops_pos(self, bin, values, keys, expected):
        """
        Invoke various map read expressions with many value types.
        """

        expr = And(
            Eq(
                MapGetByKey(None, aerospike.MAP_RETURN_RANK, values[0], keys[0], bin),
                2
            ),
            Eq(
                MapGetByValue(None, aerospike.MAP_RETURN_RANK, values[1], bin),
                [2]
            ),
            Eq(
                MapGetByIndex(None, aerospike.MAP_RETURN_RANK, values[0], 1, bin),
                1
            ),
            Eq(
                MapGetByRank(None, aerospike.MAP_RETURN_VALUE, values[6], 1, bin),
                expected[0]
            ),
            Eq(
                ListGetByIndexRangeToEnd(None, aerospike.LIST_RETURN_VALUE, 0,
                    ListSort(None, aerospike.LIST_SORT_DEFAULT,
                        MapGetByKeyRange(None, aerospike.MAP_RETURN_VALUE, keys[1], keys[2], bin))),
                expected[1]
            ),
            Eq(
                ListGetByIndexRangeToEnd(None, aerospike.LIST_RETURN_VALUE, 0,
                    ListSort(None, aerospike.LIST_SORT_DEFAULT,
                        MapGetByKeyList(None, aerospike.MAP_RETURN_INDEX, keys[3], bin))),
                [1, 2]
            ),
            Eq(
                ListGetByIndexRangeToEnd(None, aerospike.LIST_RETURN_COUNT, 0,
                    ListSort(None, aerospike.LIST_SORT_DEFAULT,
                        MapGetByKeyRelIndexRangeToEnd(None, aerospike.MAP_RETURN_VALUE, keys[4], 1, bin))),
                1
            ),
            Eq(
                ListGetByIndexRangeToEnd(None, aerospike.LIST_RETURN_COUNT, 0,
                    ListSort(None, aerospike.LIST_SORT_DEFAULT,
                        MapGetByKeyRelIndexRange(None, aerospike.MAP_RETURN_VALUE, keys[4], 0, 2, bin))),
                2
            ),
            Eq(
                ListGetByIndexRangeToEnd(None, aerospike.LIST_RETURN_VALUE, 0,
                    ListSort(None, aerospike.LIST_SORT_DEFAULT,
                        MapGetByValueRange(None, aerospike.MAP_RETURN_VALUE, values[2], values[3], bin))),
                expected[2]
            ),
            Eq(
                ListGetByIndexRangeToEnd(None, aerospike.LIST_RETURN_VALUE, 0,
                    ListSort(None, aerospike.LIST_SORT_DEFAULT,
                        MapGetByValueList(None, aerospike.MAP_RETURN_INDEX, values[4], bin))),
                [1, 2]
            ),
            Eq(
                ListGetByIndexRangeToEnd(None, aerospike.LIST_RETURN_COUNT, 0,
                    ListSort(None, aerospike.LIST_SORT_DEFAULT,
                        MapGetByValueRelRankRangeToEnd(None, aerospike.MAP_RETURN_VALUE, values[5], 1, bin))),
                2
            ),
            Eq(
                ListGetByIndexRangeToEnd(None, aerospike.LIST_RETURN_COUNT, 0,
                    ListSort(None, aerospike.LIST_SORT_DEFAULT,
                        MapGetByValueRelRankRange(None, aerospike.MAP_RETURN_VALUE, values[5], 0, 2, bin))),
                2
            ),
            Eq(
                ListGetByIndexRangeToEnd(None, aerospike.LIST_RETURN_COUNT, 0,
                    ListSort(None, aerospike.LIST_SORT_DEFAULT,
                        MapGetByIndexRangeToEnd(None, aerospike.MAP_RETURN_VALUE, 1, bin))),
                2
            ),
            Eq(
                ListGetByIndexRangeToEnd(None, aerospike.LIST_RETURN_VALUE, 0,
                    ListSort(None, aerospike.LIST_SORT_DEFAULT,
                        MapGetByIndexRange(None, aerospike.MAP_RETURN_VALUE, 1, 2, bin))),
                expected[2]
            ),
            Eq(
                ListGetByIndexRangeToEnd(None, aerospike.LIST_RETURN_COUNT, 0,
                    ListSort(None, aerospike.LIST_SORT_DEFAULT,
                        MapGetByRankRangeToEnd(None, aerospike.MAP_RETURN_VALUE, 1, bin))),
                2
            ),
            Eq(
                ListGetByIndexRangeToEnd(None, aerospike.LIST_RETURN_VALUE, 0,
                    ListSort(None, aerospike.LIST_SORT_DEFAULT,
                        MapGetByRankRange(None, aerospike.MAP_RETURN_VALUE, 1, 2, bin))),
                expected[2]
            ),
        )

        verify_multiple_expression_result(self.as_connection, self.test_ns, self.test_set, expr.compile(), bin, _NUM_RECORDS)

    @pytest.mark.parametrize("bin, bin_name, ctx, policy, key, value, expected", [
        ("imap_bin", "imap_bin", None, None, 3, 6, [12]),
        ("fmap_bin", "fmap_bin", None, None, 6.0, 6.0, [12.0]),
        (ListBin("mlist_bin"), "mlist_bin", [cdt_ctx.cdt_ctx_list_index(0)], None, 1, 4, [6])
    ])
    def test_map_increment_pos(self, bin, bin_name, ctx, policy, key, value, expected):
        """
        Invoke MapIncrement() on various integer and float bins.
        """
        expr = Eq(
                    MapGetByValue(ctx, aerospike.MAP_RETURN_VALUE, expected[0], 
                        MapIncrement(ctx, policy, key, value, bin)), 
                    expected).compile()
        verify_multiple_expression_result(self.as_connection, self.test_ns, self.test_set, expr, bin_name, _NUM_RECORDS)

    @pytest.mark.parametrize("bin, ctx, policy, values", [
        (
            "imap_bin",
            None,
            {'map_write_flags': aerospike.MAP_WRITE_FLAGS_NO_FAIL}, 
            [aerospike.CDTInfinite, 10, 1, 1, 3, 6],
        ),
        (
            "smap_bin",
            None,
            {}, 
            ['j', 'j', 'b', 'b', 'f', 'f'],
        ),
        (
            "lmap_bin",
            None,
            {}, 
            [7, [1, 8], 1, [1, 2], 3, [1, 4]],
        ),
        (
            "mmap_bin",
            None,
            {}, 
            [7, {1: 8}, 1, {1: 2}, 3, {1: 4}],
        ),
        (
            "bymap_bin",
            None,
            {}, 
            [7, 'j'.encode("utf8"), 1, 'b'.encode("utf8"), 3, 'f'.encode("utf8")],
        ),
        (
            "fmap_bin",
            None,
            {}, 
            [8.0, 10.0, 1.0, 1.0, 6.0, 6.0],
        )
    ])
    def test_map_mod_ops_pos(self, bin, ctx, policy, values):
        """
        Invoke various map modify expressions with many value types.
        """

        expr =  And(
            Eq(MapGetByValue(ctx, aerospike.MAP_RETURN_KEY, values[1], 
                    MapPut(ctx, policy, values[0], values[1], bin)),
                [values[0]]
            ),
            Eq(MapGetByValue(ctx, aerospike.MAP_RETURN_KEY, values[1], 
                    MapPutItems(ctx, policy, {values[0]: values[1]}, bin)),
                [values[0]]
            ),
            # Eq(MapClear(ctx, bin), NOTE: not valid, const map comparison
            #     {1:1}
            # )
            Eq(
                MapSize(None,
                    MapClear(ctx, bin)),
                0
            ),
            Eq(MapGetByValue(ctx, aerospike.MAP_RETURN_COUNT, values[3], 
                    MapRemoveByKey(ctx, values[2], bin)),
                0
            ),
            Eq(MapGetByValue(ctx, aerospike.MAP_RETURN_COUNT, values[3], 
                    MapRemoveByKeyList(ctx, [values[2]], bin)),
                0
            ),
            Eq(MapGetByKeyRange(ctx, aerospike.MAP_RETURN_VALUE, values[2], values[0],
                    MapRemoveByKeyRange(ctx, values[2], values[4], bin)),
                [values[5]]
            ),
            Eq(MapGetByKeyRange(ctx, aerospike.MAP_RETURN_VALUE, values[2], values[0],
                    MapRemoveByKeyRelIndexRangeToEnd(ctx, values[2], 1, bin)),
                [values[3]]
            ),
            Eq(MapGetByKeyRange(ctx, aerospike.MAP_RETURN_VALUE, values[2], values[0],
                    MapRemoveByKeyRelIndexRange(ctx, values[2], 1, 3, bin)),
                [values[3]]
            ),
            #
            Eq(MapGetByValue(ctx, aerospike.MAP_RETURN_COUNT, values[3], 
                    MapRemoveByValue(ctx, values[3], bin)),
                0
            ),
            Eq(MapGetByValue(ctx, aerospike.MAP_RETURN_COUNT, values[3], 
                    MapRemoveByValueList(ctx, [values[3]], bin)),
                0
            ),
            Eq(MapGetByKeyRange(ctx, aerospike.MAP_RETURN_VALUE, values[2], values[0],
                    MapRemoveByValueRange(ctx, values[3], values[5], bin)),
                [values[5]]
            ),
            Eq(MapGetByKeyRange(ctx, aerospike.MAP_RETURN_VALUE, values[2], values[0],
                    MapRemoveByValueRelRankRangeToEnd(ctx, values[3], 1, bin)),
                [values[3]]
            ),
            Eq(MapGetByKeyRange(ctx, aerospike.MAP_RETURN_VALUE, values[2], values[0],
                    MapRemoveByValueRelRankRange(ctx, values[3], 1, 3, bin)),
                [values[3]]
            ),
            #
            Eq(MapGetByValue(ctx, aerospike.MAP_RETURN_VALUE, values[3], 
                    MapRemoveByIndex(ctx, 0, bin)),
                []
            ),
            Eq(MapGetByKeyRange(ctx, aerospike.MAP_RETURN_VALUE, values[2], values[0],
                    MapRemoveByIndexRange(ctx, 1, 3, bin)),
                [values[3]]
            ),
            Eq(MapGetByKeyRange(ctx, aerospike.MAP_RETURN_VALUE, values[2], values[0],
                    MapRemoveByIndexRangeToEnd(ctx, 1, bin)),
                [values[3]]
            ),
            #
            Eq(MapGetByValue(ctx, aerospike.MAP_RETURN_VALUE, values[3], 
                    MapRemoveByRank(ctx, 0, bin)),
                []
            ),
            Eq(MapGetByKeyRange(ctx, aerospike.MAP_RETURN_VALUE, values[2], values[0],
                    MapRemoveByRankRange(ctx, 1, 3, bin)),
                [values[3]]
            ),
            Eq(MapGetByKeyRange(ctx, aerospike.MAP_RETURN_VALUE, values[2], values[0],
                    MapRemoveByRankRangeToEnd(ctx, 1, bin)),
                [values[3]]
            ),
        )

        verify_multiple_expression_result(self.as_connection, self.test_ns, self.test_set, expr.compile(), bin, _NUM_RECORDS)