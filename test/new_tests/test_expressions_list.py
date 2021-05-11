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
                    'ilist_bin': [
                        1,
                        2,
                        6,
                    ],
                    'slist_bin': [
                        'b',
                        'd',
                        'f'
                    ],
                    'llist_bin': [
                        [1, 2],
                        [1, 3],
                        [1, 4]
                    ],
                    'mlist_bin': [
                        {1: 2},
                        {1: 3},
                        {1: 4}
                    ],
                    'bylist_bin': [
                        'b'.encode("utf8"),
                        'd'.encode("utf8"),
                        'f'.encode("utf8")
                    ],
                    'bolist_bin': [
                        False,
                        False,
                        True
                    ],
                    'nlist_bin': [
                        None,
                        aerospike.null,
                        aerospike.null
                    ],
                    'bllist_bin': [
                        TestUsrDefinedClass(1),
                        TestUsrDefinedClass(3),
                        TestUsrDefinedClass(4)
                    ],
                    'flist_bin': [
                        1.0,
                        2.0,
                        6.0
                    ]
                }
            self.as_connection.put(key, rec)

        def teardown():
            for i in range(_NUM_RECORDS):
                key = ('test', u'demo', i)
                as_connection.remove(key)

        request.addfinalizer(teardown)

    @pytest.mark.parametrize("ctx_types, ctx_indexes, bin_type, index, return_type, check, expected", [
        (None, None, ResultType.INTEGER, 1, aerospike.LIST_RETURN_VALUE, 8, 1),
        (None, None, ResultType.STRING, 2, aerospike.LIST_RETURN_VALUE, "string_test3", 1),
        (None, None, ResultType.BLOB, 6, aerospike.LIST_RETURN_VALUE, "bytes_test3".encode("utf8"), 1),
        (None, None, ResultType.BLOB, 5, aerospike.LIST_RETURN_VALUE, bytearray("bytearray_test3", "utf8"), 1),
        (None, None, ResultType.LIST, 3, aerospike.LIST_RETURN_VALUE, [26, 27, 28, 6], 1),
        ([list_index], [3], ResultType.INTEGER, 3, aerospike.LIST_RETURN_VALUE, 6, 1),
    ])
    def test_list_get_by_index_pos(self, ctx_types, ctx_indexes, bin_type, index, return_type, check, expected):
        """
        Invoke ListGetByIndex().
        """

        if ctx_types is not None:
            ctx = []
            for ctx_type, p in zip(ctx_types, ctx_indexes) :
                ctx.append(add_ctx_op(ctx_type, p))
        else:
            ctx = None
        
        expr = Eq(ListGetByIndex(ctx, return_type, bin_type, index, 'list_bin'), check)         
        verify_multiple_expression_result(self.as_connection, self.test_ns, self.test_set, expr.compile(), 'list_bin', expected)

    @pytest.mark.parametrize("ctx_types, ctx_indexes, value, return_type, check, expected", [
        (None, None, 8, aerospike.LIST_RETURN_VALUE, [8], 1),
        (None, None, "string_test3", aerospike.LIST_RETURN_VALUE, ["string_test3"], 1),
        (None, None, "bytes_test3".encode("utf8"), aerospike.LIST_RETURN_VALUE, ["bytes_test3".encode("utf8")], 1),
        (None, None, bytearray("bytearray_test3", "utf8"), aerospike.LIST_RETURN_VALUE, [bytearray("bytearray_test3", "utf8")], 1),
        #(None, None, True, aerospike.LIST_RETURN_VALUE, [True], 9), NOTE: this won't work because booleans are not serialized by default in expressions.
        (None, None, None, aerospike.LIST_RETURN_VALUE, [None], _NUM_RECORDS),
        (None, None, [26, 27, 28, 6], aerospike.LIST_RETURN_VALUE, [[26, 27, 28, 6]], 1),
        ([list_index], [3], 6, aerospike.LIST_RETURN_VALUE, [6], 1),
        (None, None, {31: 31, 32: 32, 33: 33, 8: 8}, aerospike.LIST_RETURN_VALUE, [{31: 31, 32: 32, 33: 33, 8: 8}], 1),
        (None, None, aerospike.null, aerospike.LIST_RETURN_VALUE, [aerospike.null], _NUM_RECORDS),
        (None, None, GEO_POLY, aerospike.LIST_RETURN_VALUE, [GEO_POLY], _NUM_RECORDS),
        (None, None, TestUsrDefinedClass(4), aerospike.LIST_RETURN_VALUE, [TestUsrDefinedClass(4)], 1)
    ])
    def test_list_get_by_value_pos(self, ctx_types, ctx_indexes, value, return_type, check, expected):
        """
        Invoke ListGetByValue().
        """

        if ctx_types is not None:
            ctx = []
            for ctx_type, index in zip(ctx_types, ctx_indexes) :
                ctx.append(add_ctx_op(ctx_type, index))
        else:
            ctx = None
        
        expr = Eq(ListGetByValue(ctx, return_type, value, 'list_bin'), check)
        verify_multiple_expression_result(self.as_connection, self.test_ns, self.test_set, expr.compile(), 'list_bin', expected)

    @pytest.mark.parametrize("ctx_types, ctx_indexes, begin, end, return_type, check, expected", [
        (None, None, 4, 7, aerospike.LIST_RETURN_VALUE, [[4], [5], [6]], 3),
        # (None, None, 5, aerospike.CDTInfinite(), aerospike.LIST_RETURN_COUNT, [10, 10, 10], 4), temporarily failing because of bool jump rank
        # (None, None, 4, 7, aerospike.LIST_RETURN_RANK, [[2], [2], [2]], 3), temporarily failing because of bool jump rank
        # (None, None, 5, aerospike.CDTInfinite(), aerospike.LIST_RETURN_COUNT, [10, 10, 10], 5), temporarily failing because of bool jump rank
        # (None, None, 4, 7, aerospike.LIST_RETURN_RANK, [[1], [1], [1]], 3), temporarily failing because of bool jump rank
        (None, None, "string_test3","string_test6", aerospike.LIST_RETURN_INDEX, [[2], [2], [2]], 3),
        (None, None, "bytes_test6".encode("utf8"), "bytes_test9".encode("utf8"), aerospike.LIST_RETURN_COUNT, [1, 1, 1], 3),
        (None, None, bytearray("bytearray_test3", "utf8"), bytearray("bytearray_test6", "utf8"), aerospike.LIST_RETURN_REVERSE_INDEX, [[6], [6], [6]], 3),
        (None, None, [26, 27, 28, 6], [26, 27, 28, 9], aerospike.LIST_RETURN_VALUE, [[[26, 27, 28, 6]], [[26, 27, 28, 7]], [[26, 27, 28, 8]]], 3),
        ([list_index], [3], 5, 9, aerospike.LIST_RETURN_REVERSE_RANK, [[3], [3], [3]], 4),
        (None, None, GEO_POLY, aerospike.CDTInfinite(), aerospike.LIST_RETURN_VALUE, [[GEO_POLY], [GEO_POLY], [GEO_POLY]], _NUM_RECORDS),
        (None, None, TestUsrDefinedClass(4), TestUsrDefinedClass(7), aerospike.LIST_RETURN_VALUE, [[TestUsrDefinedClass(4)], [TestUsrDefinedClass(5)], [TestUsrDefinedClass(6)]], 3) #NOTE py_bytes cannot be compard directly server side
    ])
    def test_list_get_by_value_range_pos(self, ctx_types, ctx_indexes, begin, end, return_type, check, expected):
        """
        Invoke ListGetByValueRange().
        """

        if ctx_types is not None:
            ctx = []
            for ctx_type, index in zip(ctx_types, ctx_indexes) :
                ctx.append(add_ctx_op(ctx_type, index))
        else:
            ctx = None
        
        expr = Or(
                    Eq(ListGetByValueRange(ctx, return_type, begin, end, 'list_bin'), check[0]),
                    Eq(ListGetByValueRange(ctx, return_type, begin, end, 'list_bin'), check[1]),
                    Eq(ListGetByValueRange(ctx, return_type, begin, end, 'list_bin'), check[2]),
        )

        verify_multiple_expression_result(self.as_connection, self.test_ns, self.test_set, expr.compile(), 'list_bin', expected)

    @pytest.mark.parametrize("ctx, begin, end, return_type, check, expected", [
        ("bad ctx", 10, 13, aerospike.LIST_RETURN_VALUE, [[10], [11], [12]], e.ParamError),
        (None, 10, 13, aerospike.LIST_RETURN_VALUE, [[10], [11], 12], e.InvalidRequest)
    ])
    def test_list_get_by_value_range_neg(self, ctx, begin, end, return_type, check, expected):
        """
        Invoke ListGetByValue() with expected failures.
        """
        
        expr = Or(
                    Eq(ListGetByValueRange(ctx, return_type, begin, end, 'list_bin'), check[0]),
                    Eq(ListGetByValueRange(ctx, return_type, begin, end, 'list_bin'), check[1]),
                    Eq(ListGetByValueRange(ctx, return_type, begin, end, 'list_bin'), check[2]),
        )

        with pytest.raises(expected):
            verify_multiple_expression_result(self.as_connection, self.test_ns, self.test_set, expr.compile(), 'list_bin', expected)

    @pytest.mark.parametrize("ctx_types, ctx_indexes, value, return_type, check, expected", [
        (None, None, [8, [26, 27, 28, 8]], aerospike.LIST_RETURN_VALUE, [8, [26, 27, 28, 8]], 1),
        (None, None, ["string_test3", 3], aerospike.LIST_RETURN_VALUE, [3, "string_test3"], 1),
        (None, None, ["string_test3", 3], aerospike.LIST_RETURN_VALUE, ["string_test3", 3], 0),
        (None, None, ["bytes_test8".encode("utf8"), 8, GEO_POLY], aerospike.LIST_RETURN_VALUE, [8, "bytes_test8".encode("utf8"), GEO_POLY], 1),
        (None, None, LIST_BIN_EXAMPLE, aerospike.LIST_RETURN_VALUE, LIST_BIN_EXAMPLE, 1),
        (None, None, LIST_BIN_EXAMPLE, aerospike.LIST_RETURN_INDEX, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9 ,10, 11], 1),
        (None, None, LIST_BIN_EXAMPLE, aerospike.LIST_RETURN_REVERSE_INDEX, [11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0], 1),
        (None, None, LIST_BIN_EXAMPLE, aerospike.LIST_RETURN_COUNT, 12, 1),
        # (None, None, [8], aerospike.LIST_RETURN_RANK, [1], 1), temporarily failing because of bool jump rank
        ([list_index], [3], [26, 6], aerospike.LIST_RETURN_INDEX, [0, 3], 1),
    ])
    def test_list_get_by_value_list_pos(self, ctx_types, ctx_indexes, value, return_type, check, expected):
        """
        Invoke ListGetByValueList().
        """

        if ctx_types is not None:
            ctx = []
            for ctx_type, index in zip(ctx_types, ctx_indexes):
                ctx.append(add_ctx_op(ctx_type, index))
        else:
            ctx = None
        
        expr = Eq(ListGetByValueList(ctx, return_type, value, 'list_bin'), check)
        verify_multiple_expression_result(self.as_connection, self.test_ns, self.test_set, expr.compile(), 'list_bin', expected)

    @pytest.mark.parametrize("ctx_types, ctx_indexes, value, return_type, check, expected", [
        (None, None, [10, [26, 27, 28, 10]], aerospike.LIST_RETURN_VALUE, (10, [26, 27, 28, 10]), e.InvalidRequest)
    ])
    def test_list_get_by_value_list_neg(self, ctx_types, ctx_indexes, value, return_type, check, expected):
        """
        Invoke ListGetByValueList() with expected failures.
        """

        if ctx_types is not None:
            ctx = []
            for ctx_type, index in zip(ctx_types, ctx_indexes) :
                ctx.append(add_ctx_op(ctx_type, index))
        else:
            ctx = None
        
        expr = Eq(ListGetByValueList(ctx, return_type, value, 'list_bin'), check)
        with pytest.raises(expected):
            verify_multiple_expression_result(self.as_connection, self.test_ns, self.test_set, expr.compile(), 'list_bin', expected)

    @pytest.mark.parametrize("ctx_types, ctx_indexes, value, rank, return_type, check, expected", [
        ([list_index], [3], 26, 0, aerospike.LIST_RETURN_COUNT, 3, _NUM_RECORDS),
        ([list_index], [3], 7, 1, aerospike.LIST_RETURN_COUNT, 3, 2),
        ([list_index], [3], 7, 2, aerospike.LIST_RETURN_VALUE, [27, 28], 2),
        # (None, None, "string_test8", 0,  aerospike.LIST_RETURN_COUNT, 10, 1), temporarily failing because of bool jump rank
    ])
    def test_list_get_by_value_rel_rank_range_to_end_pos(self, ctx_types, ctx_indexes, value, rank, return_type, check, expected):
        """
        Invoke ListGetByValueRelRankRangeToEnd().
        """

        if ctx_types is not None:
            ctx = []
            for ctx_type, index in zip(ctx_types, ctx_indexes) :
                ctx.append(add_ctx_op(ctx_type, index))
        else:
            ctx = None
        
        expr = Eq(ListGetByValueRelRankRangeToEnd(ctx, return_type, value, rank, 'list_bin'), check)
        verify_multiple_expression_result(self.as_connection, self.test_ns, self.test_set, expr.compile(), 'list_bin', expected)

    @pytest.mark.parametrize("ctx_types, ctx_indexes, value, rank, return_type, expected", [
        ([list_index], [3], 26, "bad_rank", "bad_return_type", e.ParamError)
    ])
    def test_list_get_by_value_rel_rank_range_to_end_neg(self, ctx_types, ctx_indexes, value, rank, return_type, expected):
        """
        Invoke ListGetByValueRelRankRangeToEnd() with expected failures.
        """

        if ctx_types is not None:
            ctx = []
            for ctx_type, index in zip(ctx_types, ctx_indexes) :
                ctx.append(add_ctx_op(ctx_type, index))
        else:
            ctx = None
        
        expr = ListGetByValueRelRankRangeToEnd(ctx, return_type, value, rank, 'list_bin')
        with pytest.raises(expected):
            verify_multiple_expression_result(self.as_connection, self.test_ns, self.test_set, expr.compile(), 'list_bin', expected)


    @pytest.mark.parametrize("ctx_types, ctx_indexes, value, rank, count, return_type, check, expected", [
        ([list_index], [3], 26, 0, 3, aerospike.LIST_RETURN_COUNT, 3, _NUM_RECORDS),
        ([list_index], [3], 26, 0, 2, aerospike.LIST_RETURN_VALUE, [27, 26], _NUM_RECORDS),
        (None, None, "string_test10", 0, 1, aerospike.LIST_RETURN_INDEX, [3], 2),
    ])
    def test_list_get_by_value_rel_rank_range_pos(self, ctx_types, ctx_indexes, value, rank, count, return_type, check, expected):
        """
        Invoke ListGetByValueRelRankRange().
        """

        if ctx_types is not None:
            ctx = []
            for ctx_type, index in zip(ctx_types, ctx_indexes) :
                ctx.append(add_ctx_op(ctx_type, index))
        else:
            ctx = None
        
        expr = Eq(ListGetByValueRelRankRange(ctx, return_type, value, rank, count, 'list_bin'), check)
        verify_multiple_expression_result(self.as_connection, self.test_ns, self.test_set, expr.compile(), 'list_bin', expected)

    @pytest.mark.parametrize("bin, values", [
        ("ilist_bin", [ResultType.INTEGER, 6, 1, 7, [2, 6], 1]),
        ("slist_bin", [ResultType.STRING, "f", "b", "g", ["d", "f"], "b"]),
        ("llist_bin", [ResultType.LIST, [1, 4], [1, 2], [1, 6], [[1, 3], [1, 4]], [1, 2]]),
        ("bylist_bin", [ResultType.BLOB, "f".encode("utf8"), "b".encode("utf8"), "g".encode("utf8"), ["d".encode("utf8"), "f".encode("utf8")], "b".encode("utf8")]),
        ("flist_bin", [ResultType.FLOAT, 6.0, 1.0, 7.0, [2.0, 6.0], 1.0]),
    ])
    def test_list_read_ops_pos(self, bin, values):
        """
        Invoke various list read expressions with many value types.
        """
        
        expr = And(
            Eq(
                ListGetByValueRelRankRange(None, aerospike.LIST_RETURN_COUNT, 
                    ListGetByIndex(None, aerospike.LIST_RETURN_VALUE, values[0], 0, bin), 1, 3, bin),
                2),
            Eq(
                ListGetByValue(None, aerospike.LIST_RETURN_INDEX, values[1],
                    ListGetByValueRange(None, aerospike.LIST_RETURN_VALUE, values[2], values[3], bin)),
                [2]),
            Eq(
                ListGetByValueList(None, aerospike.LIST_RETURN_COUNT, values[4], 
                    ListGetByValueRelRankRangeToEnd(None, aerospike.LIST_RETURN_VALUE, values[5], 1, bin)),
                2),
            Eq(
                ListGetByIndexRangeToEnd(None, aerospike.LIST_RETURN_COUNT, 1,
                    ListGetByIndexRange(None, aerospike.LIST_RETURN_VALUE, 1, 3, bin,)),
                1),
            Eq(
                ListGetByRank(None, aerospike.LIST_RETURN_RANK, ResultType.INTEGER, 1, #lets 20 pass with slist_bin
                    ListGetByRankRangeToEnd(None, aerospike.LIST_RETURN_VALUE, 1, bin)),
                1),
            Eq(
                ListGetByRankRange(None, aerospike.LIST_RETURN_COUNT, 1, ListSize(None, bin), bin),
                2
            )
        )

        verify_multiple_expression_result(self.as_connection, self.test_ns, self.test_set, expr.compile(), bin, _NUM_RECORDS)

    @pytest.mark.parametrize("bin, ctx, policy, values, expected", [
        (
            "ilist_bin",
            None,
            {'write_flags': aerospike.LIST_WRITE_ADD_UNIQUE}, 
            [
                20,
                [3, 9],
                4,
                [24, 25], #
                10,
                1, #
                [2, 6],
                None, #
                3,
                6,
                2 #
            ], 
            [
                [1, 2, 3, 4, 6, 9, 20],
                [10, 24, 25],
                [1],
                []
            ]
        ),
        (
            "slist_bin",
            None,
            {}, 
            [
                'h',
                ['e', 'g'],
                'c',
                ['x', 'y'], #
                'b',
                'b', #
                ['d', 'f'],
                'b', #
                None,
                'f',
                'd' #
            ], 
            [
                ['b', 'c', 'd', 'e', 'f', 'g', 'h'],
                ['b', 'x', 'y',],
                ['b'],
                ['b', 'd']
            ]
        ),
        (
            "llist_bin",
            None,
            {}, 
            [
                [1, 20],
                [[1, 6], [1, 9]],
                [1, 5],
                [[1, 24], [1, 25]], #
                [1, 10],
                [1, 2], #
                [[1, 3], [1, 4]],
                [1, 2], #
                [1, 4],
                [1, 4],
                [1, 3] #
            ], 
            [
                [[1, 2], [1, 3], [1, 4], [1, 5], [1, 6], [1,9], [1, 20]],
                [[1, 10], [1, 24], [1, 25]],
                [[1, 2]],
                []
            ]
        ),
        (
            "mlist_bin",
            None,
            {}, 
            [
                {1: 20},
                [{1: 6}, {1: 9}],
                {1: 5},
                [{1: 24}, {1: 25}], #
                {1: 10},
                {1: 2}, #
                [{1: 3}, {1: 4}],
                {1: 2}, #
                {1: 4},
                {1: 4},
                {1: 3} #
            ], 
            [
                [{1: 2}, {1: 3}, {1: 4}, {1: 5}, {1: 6}, {1: 9}, {1: 20}],
                [{1: 10}, {1: 24}, {1: 25}],
                [{1: 2}],
                []
            ]
        ),
        (
            "bylist_bin",
            None,
            {}, 
            [
                b'h',
                [b'e', b'g'],
                b'c',
                [b'x', b'y'], #
                b'b',
                b'b', #
                [b'd', b'f'],
                b'b', #
                b'e',
                b'f',
                b'd' #
            ], 
            [
                [b'b', b'c', b'd', b'e', b'f', b'g', b'h'],
                [b'b', b'x', b'y'],
                [b'b'],
                []
            ]
        ),
        (
            "flist_bin",
            None,
            {}, 
            [
                20.0,
                [3.0, 9.0],
                4.0,
                [24.0, 25.0], #
                10.0,
                1.0, #
                [2.0, 6.0],
                1.0, #
                3.0,
                6.0,
                2.0 #
            ], 
            [
                [1.0, 2.0, 3.0, 4.0, 6.0, 9.0, 20.0],
                [10.0, 24.0, 25.0],
                [1.0],
                []
            ]
        ),
    ])
    def test_list_mod_ops_pos(self, bin, ctx, policy, values, expected):
        """
        Invoke various list modify expressions with many value types.
        """
        
        expr = And(
            Eq(
                ListGetByIndexRangeToEnd(ctx, aerospike.LIST_RETURN_VALUE, 0,                 
                    ListSort(ctx, aerospike.LIST_SORT_DEFAULT,      
                        ListAppend(ctx, policy, values[0],
                            ListAppendItems(ctx, policy, values[1],
                                ListInsert(ctx, policy, 1, values[2], bin))))), #NOTE: invalid on ordered lists
                expected[0]
            ),
            Eq( 
                ListSort(ctx, aerospike.LIST_SORT_DEFAULT,
                    ListGetByRankRangeToEnd(ctx, aerospike.LIST_RETURN_VALUE, 0,
                        ListInsertItems(ctx, policy, 0, values[3],
                            ListSet(ctx, policy, 0, values[4],
                                ListClear(ctx, bin))))),
                expected[1]
            ),
            Eq(
                ListRemoveByValue(ctx, values[5],
                    ListRemoveByValueList(ctx, values[6], bin)),
                []
            ),
            Eq(
                ListRemoveByValueRange(ctx, values[7], values[8],
                    ListRemoveByValueRelRankToEnd(ctx, values[9], 0, bin)),
                expected[3]
            ),
            Eq(
                ListRemoveByValueRelRankRange(ctx, values[10], 0, 2,
                    ListRemoveByIndex(ctx, 0, bin)),
                []
            ), 
            Eq(
                ListRemoveByIndexRange(ctx, 0, 1,
                    ListRemoveByIndexRangeToEnd(ctx, 1, bin)),
                []
            ),
            Eq(
                ListRemoveByRank(ctx, 0, 
                    ListRemoveByRankRangeToEnd(ctx, 1, bin)),
                []
            ),
            Eq(
                ListRemoveByRankRange(ctx, 1, 2, bin),
                expected[2]
            )
        )

        verify_multiple_expression_result(self.as_connection, self.test_ns, self.test_set, expr.compile(), bin, _NUM_RECORDS)