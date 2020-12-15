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

def verify_multiple_expression_avenues(client, test_ns, test_set, expr, op_bin, expected):
    keys = [(test_ns, test_set, i) for i in range(20)]

    # operate
    ops = [
        operations.read(op_bin)
    ]
    res = []
    for key in keys:
        try:
            res.append(client.operate(key, ops, policy={'expressions': expr})[2])
        except e.FilteredOut:
            pass

    assert len(res) == expected

    # operate ordered
    ops = [
        operations.read(op_bin)
    ]
    res = []
    for key in keys:
        try:
            res.append(client.operate_ordered(key, ops, policy={'expressions': expr})[2])
        except e.FilteredOut:
            pass
    
    # batch get
    res = [rec for rec in client.get_many(keys, policy={'expressions': expr}) if rec[2]]

    assert len(res) == expected

    # scan results
    scan_obj = client.scan(test_ns, test_set)
    records = scan_obj.results({'expressions': expr})
    assert len(records) == expected


    # other scan methods
    # execute_background tested test_scan_execute_background.py 
    # foreach tested test_scan.py

    # query results
    query_obj = client.query(test_ns, test_set)
    records = query_obj.results({'expressions': expr})
    assert len(records) == expected

    # other query methods
    # execute background tested in test_query_execute_background.py 
    # foreach tested in test_query.py


class TestUsrDefinedClass():

    __test__ = False

    def __init__(self, i):
        self.data = i


LIST_BIN_EXAMPLE = [
                None,
                10,
                "string_test" + str(10),
                [26, 27, 28, 10],
                {32: 32, 33: 33, 10: 10, 31: 31},
                bytearray("bytearray_test" + str(10), "utf8"),
                ("bytes_test" + str(10)).encode("utf8"),
                10 % 2 == 1,
                aerospike.null,
                TestUsrDefinedClass(10),
                float(10),
                GEO_POLY
]


class TestExpressions(TestBaseClass):

    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        self.test_ns = 'test'
        self.test_set = 'demo'

        for i in range(19):
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
                    },
                    '1bits_bin': bytearray([1] * 8),
                }
            self.as_connection.put(key, rec)
        
        self.as_connection.put(('test', u'demo', 19), {'extra': 'record'}, policy={'key': aerospike.POLICY_KEY_SEND})
        self.as_connection.put(('test', u'demo', 'k_string'), {'test': 'data'}, policy={'key': aerospike.POLICY_KEY_SEND})
        self.as_connection.put(('test', u'demo', bytearray('b_string', 'utf-8')), {'test': 'b_data'}, policy={'key': aerospike.POLICY_KEY_SEND})

        ctx_sort_nested_map1 = [
            cdt_ctx.cdt_ctx_list_index(4)
        ]

        HLL_ops = [
            hll_operations.hll_add('hll_bin', ['key%s' % str(i) for i in range(10000)], 15, 49),
            hll_operations.hll_add('hll_bin2', ['key%s' % str(i) for i in range(5000, 15000)], 15, 49),
            hll_operations.hll_add('hll_bin3', ['key%s' % str(i) for i in range(20000, 30000)], 15, 49)
        ]

        #apply map order policy
        for i in range(19):
            _, _, _ = self.as_connection.operate(('test', u'demo', i), HLL_ops)

        def teardown():
            for i in range(19):
                key = ('test', u'demo', i)
                as_connection.remove(key)
        
            as_connection.remove(('test', u'demo', 19))
            as_connection.remove(('test', u'demo', 'k_string'))
            as_connection.remove(('test', u'demo', bytearray('b_string', 'utf-8')))

        request.addfinalizer(teardown)

    def relative_count_error(self, n_index_bits, expected):
        return (expected * (1.04 / sqrt(2 ** n_index_bits)) * 8)

    def relative_intersect_error(self, n_index_bits, bin_counts, bin_intersect_count):
        sigma = (1.04 / sqrt(2 ** n_index_bits))
        rel_err_sum  = 0
        for count in bin_counts:
            rel_err_sum += ((sigma * count) ** 2)
        rel_err_sum += (sigma * (bin_intersect_count ** 2))

        return sqrt(rel_err_sum)

    def test_DeviceSize_pos(self):
        expr = Eq(DeviceSize(), 0)
        record = self.as_connection.get(('test', u'demo', 19), policy={'expressions': expr.compile()})
        assert(record[2]['extra'] == 'record')

    def test_TTL_pos(self):
        expr = NE(TTL(), 0)
        record = self.as_connection.get(('test', u'demo', 19), policy={'expressions': expr.compile()})
        assert(record[2]['extra'] == 'record')

    def test_VoidTime_pos(self):
        expr = NE(VoidTime(), 0)
        record = self.as_connection.get(('test', u'demo', 19), policy={'expressions': expr.compile()})
        assert(record[2]['extra'] == 'record')

    def test_remove_with_expressions_neg(self):
        self.as_connection.put(('test', u'demo', 25), {'test': 'test_data'})

        expr = Eq(KeyInt(), 26)
        with pytest.raises(e.FilteredOut):
            record = self.as_connection.remove(('test', u'demo', 25), policy={'expressions': expr.compile()})

    def test_scan_with_results_method_and_expressions(self):
        ns = 'test'
        st = 'demo'

        expr =  Eq(KeyInt(), 19)
        record = self.as_connection.get(('test', u'demo', 19), policy={'expressions': expr.compile()})
        assert(record[2]['extra'] == 'record')


        expr =  Eq(KeyStr(), 'k_string')
        record = self.as_connection.get(('test', u'demo', 'k_string'), policy={'expressions': expr.compile()})
        assert(record[2]['test'] == 'data')

        expr =  Eq(KeyBlob(), bytearray('b_string', 'utf-8'))
        record = self.as_connection.get(('test', u'demo', bytearray('b_string', 'utf-8')), policy={'expressions': expr.compile()})
        assert(record[2]['test'] == 'b_data')

        expr =  KeyExists()
        record = self.as_connection.get(('test', u'demo', bytearray('b_string', 'utf-8')), policy={'expressions': expr.compile()})
        assert(record[2]['test'] == 'b_data')

        expr = And(
            BinExists("age"),
            Eq(SetName(), 'demo'),
            NE(LastUpdateTime(), 0),
            NE(SinceUpdateTime(), 0),
            Not(IsTombstone()),
            Eq(DigestMod(2), 0)
        )

        scan_obj = self.as_connection.scan(ns, st)
        records = scan_obj.results({'expressions': expr.compile()})
        assert(10 == len(records))

    @pytest.mark.parametrize("bin, expected_bin_type", [
        ("ilist_bin", aerospike.AS_BYTES_LIST),
        ("age", aerospike.AS_BYTES_INTEGER),
        ("imap_bin", aerospike.AS_BYTES_MAP)
    ])
    def test_BinType_pos(self, bin, expected_bin_type):
        """
        Invoke BinType() on various kinds of bins.
        """
        expr = Eq(BinType(bin), expected_bin_type).compile()
        verify_multiple_expression_avenues(self.as_connection, self.test_ns, self.test_set, expr, bin, 19)

    @pytest.mark.parametrize("ctx_types, ctx_indexes, bin_type, index, return_type, check, expected", [
        (None, None, ResultType.INTEGER, 1, aerospike.LIST_RETURN_VALUE, 10, 1),
        (None, None, ResultType.STRING, 2, aerospike.LIST_RETURN_VALUE, "string_test3", 1),
        (None, None, ResultType.BLOB, 6, aerospike.LIST_RETURN_VALUE, "bytes_test3".encode("utf8"), 1),
        (None, None, ResultType.BLOB, 5, aerospike.LIST_RETURN_VALUE, bytearray("bytearray_test3", "utf8"), 1),
        (None, None, ResultType.LIST, 3, aerospike.LIST_RETURN_VALUE, [26, 27, 28, 6], 1),
        ([list_index], [3], ResultType.INTEGER, 3, aerospike.LIST_RETURN_VALUE, 6, 1),
    ])
    def test_ListGetByIndex_pos(self, ctx_types, ctx_indexes, bin_type, index, return_type, check, expected):
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
        verify_multiple_expression_avenues(self.as_connection, self.test_ns, self.test_set, expr.compile(), 'list_bin', expected)

    @pytest.mark.parametrize("ctx_types, ctx_indexes, value, return_type, check, expected", [
        (None, None, 10, aerospike.LIST_RETURN_VALUE, [10], 1),
        (None, None, "string_test3", aerospike.LIST_RETURN_VALUE, ["string_test3"], 1),
        (None, None, "bytes_test3".encode("utf8"), aerospike.LIST_RETURN_VALUE, ["bytes_test3".encode("utf8")], 1),
        (None, None, bytearray("bytearray_test3", "utf8"), aerospike.LIST_RETURN_VALUE, [bytearray("bytearray_test3", "utf8")], 1),
        (None, None, True, aerospike.LIST_RETURN_VALUE, [True], 9),
        (None, None, None, aerospike.LIST_RETURN_VALUE, [None], 19),
        (None, None, [26, 27, 28, 6], aerospike.LIST_RETURN_VALUE, [[26, 27, 28, 6]], 1),
        ([list_index], [3], 6, aerospike.LIST_RETURN_VALUE, [6], 1),
        (None, None, {31: 31, 32: 32, 33: 33, 12: 12}, aerospike.LIST_RETURN_VALUE, [{31: 31, 32: 32, 33: 33, 12: 12}], 1),
        (None, None, aerospike.null, aerospike.LIST_RETURN_VALUE, [aerospike.null], 19),
        (None, None, GEO_POLY, aerospike.LIST_RETURN_VALUE, [GEO_POLY], 19),
        (None, None, TestUsrDefinedClass(4), aerospike.LIST_RETURN_VALUE, [TestUsrDefinedClass(4)], 1)
    ])
    def test_ListGetByValue_pos(self, ctx_types, ctx_indexes, value, return_type, check, expected):
        """
        Invoke ListGetByValue().
        """
        #breakpoint()

        if ctx_types is not None:
            ctx = []
            for ctx_type, index in zip(ctx_types, ctx_indexes) :
                ctx.append(add_ctx_op(ctx_type, index))
        else:
            ctx = None
        
        expr = Eq(ListGetByValue(ctx, return_type, value, 'list_bin'), check)
        verify_multiple_expression_avenues(self.as_connection, self.test_ns, self.test_set, expr.compile(), 'list_bin', expected)

    @pytest.mark.parametrize("ctx_types, ctx_indexes, begin, end, return_type, check, expected", [
        (None, None, 10, 13, aerospike.LIST_RETURN_VALUE, [[10], [11], [12]], 3),
        (None, None, 10, aerospike.CDTInfinite(), aerospike.LIST_RETURN_COUNT, [10, 10, 10], 10),
        (None, None, 10, 13, aerospike.LIST_RETURN_RANK, [[1], [1], [1]], 3),
        (None, None, "string_test3","string_test6", aerospike.LIST_RETURN_INDEX, [[2], [2], [2]], 3),
        (None, None, "bytes_test6".encode("utf8"), "bytes_test9".encode("utf8"), aerospike.LIST_RETURN_COUNT, [1, 1, 1], 3),
        (None, None, bytearray("bytearray_test3", "utf8"), bytearray("bytearray_test6", "utf8"), aerospike.LIST_RETURN_REVERSE_INDEX, [[6], [6], [6]], 3),
        (None, None, [26, 27, 28, 6], [26, 27, 28, 9], aerospike.LIST_RETURN_VALUE, [[[26, 27, 28, 6]], [[26, 27, 28, 7]], [[26, 27, 28, 8]]], 3),
        ([list_index], [3], 5, 9, aerospike.LIST_RETURN_REVERSE_RANK, [[3], [3], [3]], 4),
        (None, None, GEO_POLY, aerospike.CDTInfinite(), aerospike.LIST_RETURN_VALUE, [[GEO_POLY], [GEO_POLY], [GEO_POLY]], 19),
        (None, None, TestUsrDefinedClass(4), TestUsrDefinedClass(7), aerospike.LIST_RETURN_VALUE, [[TestUsrDefinedClass(4)], [TestUsrDefinedClass(5)], [TestUsrDefinedClass(6)]], 3) #NOTE py_bytes cannot be compard directly server side
    ])
    def test_ListGetByValueRange_pos(self, ctx_types, ctx_indexes, begin, end, return_type, check, expected):
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

        verify_multiple_expression_avenues(self.as_connection, self.test_ns, self.test_set, expr.compile(), 'list_bin', expected)

    @pytest.mark.parametrize("ctx, begin, end, return_type, check, expected", [
        ("bad ctx", 10, 13, aerospike.LIST_RETURN_VALUE, [[10], [11], [12]], e.ParamError),
        (None, 10, 13, aerospike.LIST_RETURN_VALUE, [[10], [11], 12], e.InvalidRequest)
    ])
    def test_ListGetByValueRange_neg(self, ctx, begin, end, return_type, check, expected):
        """
        Invoke ListGetByValue() with expected failures.
        """
        
        expr = Or(
                    Eq(ListGetByValueRange(ctx, return_type, begin, end, 'list_bin'), check[0]),
                    Eq(ListGetByValueRange(ctx, return_type, begin, end, 'list_bin'), check[1]),
                    Eq(ListGetByValueRange(ctx, return_type, begin, end, 'list_bin'), check[2]),
        )

        with pytest.raises(expected):
            verify_multiple_expression_avenues(self.as_connection, self.test_ns, self.test_set, expr.compile(), 'list_bin', expected)

    @pytest.mark.parametrize("ctx_types, ctx_indexes, value, return_type, check, expected", [
        (None, None, [10, [26, 27, 28, 10]], aerospike.LIST_RETURN_VALUE, [10, [26, 27, 28, 10]], 1),
        (None, None, ["string_test3", 3], aerospike.LIST_RETURN_VALUE, [3, "string_test3"], 1),
        (None, None, ["string_test3", 3], aerospike.LIST_RETURN_VALUE, ["string_test3", 3], 0),
        (None, None, ["bytes_test18".encode("utf8"), 18, GEO_POLY], aerospike.LIST_RETURN_VALUE, [18, "bytes_test18".encode("utf8"), GEO_POLY], 1),
        (None, None, LIST_BIN_EXAMPLE, aerospike.LIST_RETURN_VALUE, LIST_BIN_EXAMPLE, 1),
        (None, None, LIST_BIN_EXAMPLE, aerospike.LIST_RETURN_INDEX, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11], 1),
        (None, None, LIST_BIN_EXAMPLE, aerospike.LIST_RETURN_REVERSE_INDEX, [11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0], 1),
        (None, None, LIST_BIN_EXAMPLE, aerospike.LIST_RETURN_COUNT, 12, 1),
        (None, None, [10], aerospike.LIST_RETURN_RANK, [1], 1),
        ([list_index], [3], [26, 6], aerospike.LIST_RETURN_INDEX, [0, 3], 1),
    ])
    def test_ListGetByValueList_pos(self, ctx_types, ctx_indexes, value, return_type, check, expected):
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
        verify_multiple_expression_avenues(self.as_connection, self.test_ns, self.test_set, expr.compile(), 'list_bin', expected)

    @pytest.mark.parametrize("ctx_types, ctx_indexes, value, return_type, check, expected", [
        (None, None, [10, [26, 27, 28, 10]], aerospike.LIST_RETURN_VALUE, (10, [26, 27, 28, 10]), e.InvalidRequest)
    ])
    def test_ListGetByValueList_neg(self, ctx_types, ctx_indexes, value, return_type, check, expected):
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
            verify_multiple_expression_avenues(self.as_connection, self.test_ns, self.test_set, expr.compile(), 'list_bin', expected)

    @pytest.mark.parametrize("ctx_types, ctx_indexes, value, rank, return_type, check, expected", [
        ([list_index], [3], 26, 0, aerospike.LIST_RETURN_COUNT, 3, 19),
        ([list_index], [3], 10, 1, aerospike.LIST_RETURN_COUNT, 3, 9),
        ([list_index], [3], 10, 2, aerospike.LIST_RETURN_VALUE, [27, 28], 9),
        (None, None, "string_test10", 0,  aerospike.LIST_RETURN_COUNT, 10, 17),
    ])
    def test_ListGetByValueRelRankRangeToEnd_pos(self, ctx_types, ctx_indexes, value, rank, return_type, check, expected):
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
        verify_multiple_expression_avenues(self.as_connection, self.test_ns, self.test_set, expr.compile(), 'list_bin', expected)

    @pytest.mark.parametrize("ctx_types, ctx_indexes, value, rank, return_type, expected", [
        ([list_index], [3], 26, "bad_rank", "bad_return_type", e.ParamError)
    ])
    def test_ListGetByValueRelRankRangeToEnd_neg(self, ctx_types, ctx_indexes, value, rank, return_type, expected):
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
            verify_multiple_expression_avenues(self.as_connection, self.test_ns, self.test_set, expr.compile(), 'list_bin', expected)


    @pytest.mark.parametrize("ctx_types, ctx_indexes, value, rank, count, return_type, check, expected", [
        ([list_index], [3], 26, 0, 3, aerospike.LIST_RETURN_COUNT, 3, 19),
        ([list_index], [3], 26, 0, 2, aerospike.LIST_RETURN_VALUE, [27, 26], 19),
        (None, None, "string_test10", 0, 1, aerospike.LIST_RETURN_INDEX, [3], 2),
    ])
    def test_ListGetByValueRelRankRange_pos(self, ctx_types, ctx_indexes, value, rank, count, return_type, check, expected):
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
        verify_multiple_expression_avenues(self.as_connection, self.test_ns, self.test_set, expr.compile(), 'list_bin', expected)

    @pytest.mark.parametrize("bin, values", [
        ("ilist_bin", [ResultType.INTEGER, 6, 1, 7, [2, 6], 1]),
        ("slist_bin", [ResultType.STRING, "f", "b", "g", ["d", "f"], "b"]),
        ("llist_bin", [ResultType.LIST, [1, 4], [1, 2], [1, 6], [[1, 3], [1, 4]], [1, 2]]),
        ("bylist_bin", [ResultType.BLOB, "f".encode("utf8"), "b".encode("utf8"), "g".encode("utf8"), ["d".encode("utf8"), "f".encode("utf8")], "b".encode("utf8")]),
        ("flist_bin", [ResultType.FLOAT, 6.0, 1.0, 7.0, [2.0, 6.0], 1.0]),
    ])
    def test_ListReadOps_pos(self, bin, values):
        """
        Invoke various list read expressions with many value types.
        """
        
        expr = And(
            Eq(
                ListGetByValueRelRankRange(None, aerospike.LIST_RETURN_COUNT, 
                    ListGetByIndex(None, aerospike.LIST_RETURN_VALUE, values[0], 0, bin), 1, 3, bin), #why did this fail with aerospike.CDTInfinite for count?
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

        verify_multiple_expression_avenues(self.as_connection, self.test_ns, self.test_set, expr.compile(), bin, 19)

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
    def test_ListModOps_pos(self, bin, ctx, policy, values, expected):
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

        verify_multiple_expression_avenues(self.as_connection, self.test_ns, self.test_set, expr.compile(), bin, 19)
    

    @pytest.mark.parametrize("bin, values, keys, expected", [
        ("imap_bin", [ResultType.INTEGER, 6, 2, 7, [2, 6], 1, ResultType.INTEGER], [3, 2, 4, [2, 3], 2], [2, [2, 6], [2, 6]]),
        ("smap_bin", [ResultType.INTEGER, 'f', 'd', 'g', ['d', 'f'], 'b', ResultType.STRING], ['f', 'd', 'g', ['d', 'f'], 'd'], ['d', ['d', 'f'], ['d', 'f']]),
        ("lmap_bin", [ResultType.INTEGER, [1, 4], [1, 3], [1, 5], [[1, 3], [1, 4]], [1, 2], ResultType.LIST], [3, 2, 4, [2, 3], 2], [[1, 3], [[1, 3], [1, 4]], [[1, 3], [1, 4]]]),
    ])
    def test_MapReadOps_pos(self, bin, values, keys, expected):
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

        verify_multiple_expression_avenues(self.as_connection, self.test_ns, self.test_set, expr.compile(), bin, 19)

    @pytest.mark.parametrize("bin, bin_name, ctx, policy, key, value, expected", [
        ("imap_bin", "imap_bin", None, None, 3, 6, [12]),
        ("fmap_bin", "fmap_bin", None, None, 6.0, 6.0, [12.0]),
        (ListBin("mlist_bin"), "mlist_bin", [cdt_ctx.cdt_ctx_list_index(0)], None, 1, 4, [6])
    ])
    def test_MapIncrement_pos(self, bin, bin_name, ctx, policy, key, value, expected):
        """
        Invoke MapIncrement() on various integer and float bins.
        """
        expr = Eq(
                    MapGetByValue(ctx, aerospike.MAP_RETURN_VALUE, expected[0], 
                        MapIncrement(ctx, policy, key, value, bin)), 
                    expected).compile()
        verify_multiple_expression_avenues(self.as_connection, self.test_ns, self.test_set, expr, bin_name, 19)

    @pytest.mark.parametrize("bin, ctx, policy, values", [
        (
            "imap_bin",
            None,
            {'map_write_flags': aerospike.MAP_WRITE_FLAGS_NO_FAIL}, 
            [4, 10, 1, 1, 3, 6],
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
    def test_MapModOps_pos(self, bin, ctx, policy, values):
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

        verify_multiple_expression_avenues(self.as_connection, self.test_ns, self.test_set, expr.compile(), bin, 19)

    @pytest.mark.parametrize("policy, bytes_size, flags, bin, expected", [
        (None, 10, None, '1bits_bin', bytearray([1])),
        ({'bit_write_flags': aerospike.BIT_WRITE_UPDATE_ONLY}, 10, None, '1bits_bin', bytearray([1])),
        (None, 10, aerospike.BIT_RESIZE_FROM_FRONT, '1bits_bin', bytearray([0]))
    ])
    def test_BitResize_pos(self, policy, bytes_size, flags, bin, expected):
        """
        Test BitResize expression.
        """

        expr = Eq(
                    BitGet(8, 8, 
                        BitResize(policy, bytes_size, flags, bin)),
                    expected
                )

        verify_multiple_expression_avenues(self.as_connection, self.test_ns, self.test_set, expr.compile(), bin, 19)

    @pytest.mark.parametrize("policy, byte_offset, byte_size, bin, expected", [
        (None, 0, 1, '1bits_bin', bytearray([0] * 1)),
        ({'bit_write_flags': aerospike.BIT_WRITE_UPDATE_ONLY}, 0, 1, '1bits_bin', bytearray([0] * 1))
    ])
    def test_BitRemoveOps_pos(self, policy, byte_offset, byte_size, bin, expected):
        """
        Test BitRemove expression.
        """

        expr = Eq(
                    BitRemove(policy, byte_offset, byte_size, bin),
                    bytearray([1] * 7)
                )

        verify_multiple_expression_avenues(self.as_connection, self.test_ns, self.test_set, expr.compile(), bin, 19)

    @pytest.mark.parametrize("policy", [
        (None),
        ({'bit_write_flags': aerospike.BIT_WRITE_UPDATE_ONLY})
    ])
    def test_BitInsert_pos(self, policy):
        """
        Test BitInsert expression.
        """

        expr = Eq(
                    BitInsert(policy, 1, bytearray([3]), '1bits_bin'),
                    bytearray([1, 3, 1, 1, 1, 1, 1, 1, 1])
                )

        verify_multiple_expression_avenues(self.as_connection, self.test_ns, self.test_set, expr.compile(), '1bits_bin', 19)

    @pytest.mark.parametrize("policy", [
        (None),
        ({'bit_write_flags': aerospike.BIT_WRITE_UPDATE_ONLY})
    ])
    def test_BitSet_pos(self, policy):
        """
        Test BitSet expression.
        """

        expr = Eq(
                    BitSet(policy, 7, 1, bytearray([0]), '1bits_bin'),
                    bytearray([0] + [1] * 7)
                )

        verify_multiple_expression_avenues(self.as_connection, self.test_ns, self.test_set, expr.compile(), '1bits_bin', 19)

    @pytest.mark.parametrize("policy", [
        (None),
        ({'bit_write_flags': aerospike.BIT_WRITE_UPDATE_ONLY})
    ])
    def test_BitOr_pos(self, policy):
        """
        Test BitOr expression.
        """

        expr = Eq(
                    BitOr(policy, 0, 8, bytearray([8]), '1bits_bin'),
                    bytearray([9] + [1] * 7)
                )

        verify_multiple_expression_avenues(self.as_connection, self.test_ns, self.test_set, expr.compile(), '1bits_bin', 19)

    @pytest.mark.parametrize("policy", [
        (None),
        ({'bit_write_flags': aerospike.BIT_WRITE_UPDATE_ONLY})
    ])
    def test_BitXor_pos(self, policy):
        """
        Test BitXor expression.
        """

        expr = Eq(
                    BitXor(policy, 0, 8, bytearray([1]), '1bits_bin'),
                    bytearray([0] + [1] * 7)
                )

        verify_multiple_expression_avenues(self.as_connection, self.test_ns, self.test_set, expr.compile(), '1bits_bin', 19)

    @pytest.mark.parametrize("policy", [
        (None),
        ({'bit_write_flags': aerospike.BIT_WRITE_UPDATE_ONLY})
    ])
    def test_BitAnd_pos(self, policy):
        """
        Test BitAnd expression.
        """

        expr = Eq(
                    BitAnd(policy, 0, 8, bytearray([0]), '1bits_bin'),
                    bytearray([0] + [1] * 7)
                )

        verify_multiple_expression_avenues(self.as_connection, self.test_ns, self.test_set, expr.compile(), '1bits_bin', 19)

    @pytest.mark.parametrize("policy", [
        (None),
        ({'bit_write_flags': aerospike.BIT_WRITE_UPDATE_ONLY})
    ])
    def test_BitNot_pos(self, policy):
        """
        Test BitNot expression.
        """

        expr = Eq(
                    BitNot(policy, 0, 64, '1bits_bin'),
                    bytearray([254] * 8)
                )

        verify_multiple_expression_avenues(self.as_connection, self.test_ns, self.test_set, expr.compile(), '1bits_bin', 19)

    @pytest.mark.parametrize("policy", [
        (None),
        ({'bit_write_flags': aerospike.BIT_WRITE_UPDATE_ONLY})
    ])
    def test_BitLeftShift_pos(self, policy):
        """
        Test BitLeftShift expression.
        """

        expr = Eq(
                    BitLeftShift(policy, 0, 8, 3, '1bits_bin'),
                    bytearray([8] + [1] * 7)
                )

        verify_multiple_expression_avenues(self.as_connection, self.test_ns, self.test_set, expr.compile(), '1bits_bin', 19)

    @pytest.mark.parametrize("policy", [
        (None),
        ({'bit_write_flags': aerospike.BIT_WRITE_UPDATE_ONLY})
    ])
    def test_BitRightShift_pos(self, policy):
        """
        Test BitRightShift expression.
        """

        expr = Eq(
                    BitRightShift(policy, 0, 8, 1, 
                        BitLeftShift(None, 0, 8, 3, '1bits_bin')),
                    bytearray([4] + [1] * 7)
                )

        verify_multiple_expression_avenues(self.as_connection, self.test_ns, self.test_set, expr.compile(), '1bits_bin', 19)

    @pytest.mark.parametrize("policy, bit_offset, bit_size, value, action, bin, expected", [
        (None, 8, 8, 1, aerospike.BIT_OVERFLOW_FAIL, '1bits_bin', bytearray([1] + [2] + [1] * 6)),
        ({'bit_write_flags': aerospike.BIT_WRITE_UPDATE_ONLY}, 8, 8, 1, aerospike.BIT_OVERFLOW_FAIL, '1bits_bin', bytearray([1] + [2] + [1] * 6))
    ])
    def test_BitAdd_pos(self, policy, bit_offset, bit_size, value, action, bin, expected):
        """
        Test BitAdd expression.
        """

        expr = Eq(
                    BitAdd(policy, bit_offset, bit_size, value, action, bin),
                    expected
                )

        verify_multiple_expression_avenues(self.as_connection, self.test_ns, self.test_set, expr.compile(), bin, 19)

    @pytest.mark.parametrize("policy, bit_offset, bit_size, value, action, bin, expected", [
        (None, 8, 8, 1, aerospike.BIT_OVERFLOW_FAIL, '1bits_bin', bytearray([1] + [0] + [1] * 6)),
        ({'bit_write_flags': aerospike.BIT_WRITE_UPDATE_ONLY}, 8, 8, 1, aerospike.BIT_OVERFLOW_FAIL, '1bits_bin', bytearray([1] + [0] + [1] * 6))
    ])
    def test_BitSubtract_pos(self, policy, bit_offset, bit_size, value, action, bin, expected):
        """
        Test BitSubtract expression.
        """

        expr = Eq(
                    BitSubtract(policy, bit_offset, bit_size, value, action, bin),
                    expected
                )

        verify_multiple_expression_avenues(self.as_connection, self.test_ns, self.test_set, expr.compile(), bin, 19)

    @pytest.mark.parametrize("policy", [
        (None),
        ({'bit_write_flags': aerospike.BIT_WRITE_UPDATE_ONLY})
    ])
    def test_BitSetInt_pos(self, policy):
        """
        Test BitSetInt expression.
        """

        expr = Eq(
                    BitSetInt(policy, 7, 1, 0, '1bits_bin'),
                    bytearray([0] + [1] * 7)
                )

        verify_multiple_expression_avenues(self.as_connection, self.test_ns, self.test_set, expr.compile(), '1bits_bin', 19)

    @pytest.mark.parametrize("bit_offset, bit_size, bin, expected", [
        (8, 8, '1bits_bin', bytearray([1]))
    ])
    def test_BitGet_pos(self, bit_offset, bit_size, bin, expected):
        """
        Test BitGet expression.
        """

        expr = Eq(
                    BitGet(bit_offset, bit_size, bin),
                    expected
                )

        verify_multiple_expression_avenues(self.as_connection, self.test_ns, self.test_set, expr.compile(), bin, 19)

    @pytest.mark.parametrize("bit_offset, bit_size, bin, expected", [
        (16, 8 * 3, '1bits_bin', 3)
    ])
    def test_BitCount_pos(self, bit_offset, bit_size, bin, expected):
        """
        Test BitCount expression.
        """

        expr = Eq(
                    BitCount(bit_offset, bit_size, bin),
                    expected
                )

        verify_multiple_expression_avenues(self.as_connection, self.test_ns, self.test_set, expr.compile(), bin, 19)

    @pytest.mark.parametrize("bit_offset, bit_size, value, bin, expected", [
        (0, 8, ExpTrue(), '1bits_bin', 7)
    ])
    def test_BitLeftScan_pos(self, bit_offset, bit_size, value, bin, expected):
        """
        Test BitLeftScan expression.
        """

        expr = Eq(
                    BitLeftScan(bit_offset, bit_size, value, bin),
                    expected
                )

        verify_multiple_expression_avenues(self.as_connection, self.test_ns, self.test_set, expr.compile(), bin, 19)

    @pytest.mark.parametrize("bit_offset, bit_size, value, bin, expected", [
        (0, 8, ExpTrue(), '1bits_bin', 7)
    ])
    def test_BitRightScan_pos(self, bit_offset, bit_size, value, bin, expected):
        """
        Test BitRightScan expression.
        """

        expr = Eq(
                    BitRightScan(bit_offset, bit_size, value, bin),
                    expected
                )

        verify_multiple_expression_avenues(self.as_connection, self.test_ns, self.test_set, expr.compile(), bin, 19)

    @pytest.mark.parametrize("bit_offset, bit_size, bin, expected", [
        (0, 8, '1bits_bin', 1)
    ])
    def test_BitGetInt_pos(self, bit_offset, bit_size, bin, expected):
        """
        Test BitGetInt expression.
        """

        expr = Eq(
                    BitGetInt(bit_offset, bit_size, ExpTrue(), bin),
                    expected
                )

        verify_multiple_expression_avenues(self.as_connection, self.test_ns, self.test_set, expr.compile(), bin, 19)

    @pytest.mark.parametrize("policy, listp, index_bc, mh_bc, bin, expected", [ 
        (None, ['key%s' % str(i) for i in range(11000, 16000)], 15, None, 'hll_bin', 15000),
        (None, ['key%s' % str(i) for i in range(11000, 16000)], None, None, 'hll_bin', 15000),
        (None, ['key%s' % str(i) for i in range(11000, 16000)], 15, 49, 'hll_bin', 15000),
        ({'flags': aerospike.HLL_WRITE_NO_FAIL}, ['key%s' % str(i) for i in range(11000, 16000)], None, None, 'hll_bin', 15000)
    ])
    def test_HLLAdd_pos(self, policy, listp, index_bc, mh_bc, bin, expected):
        """
        Test the HLLAdd expression.
        """

        upper_lim = ceil(expected + self.relative_count_error(15, expected))
        lower_lim = floor(expected - self.relative_count_error(15, expected))
        expr = And(
                GE(
                    HLLGetCount(
                        HLLAdd(policy, listp, index_bc, mh_bc, bin)),
                    lower_lim
                ),
                LE(
                    HLLGetCount(
                        HLLAdd(policy, listp, index_bc, mh_bc, bin)),
                    upper_lim
                ),
        )

        verify_multiple_expression_avenues(self.as_connection, self.test_ns, self.test_set, expr.compile(), bin, 19)

    @pytest.mark.parametrize("bin, expected", [
        ('hll_bin', 25000)
    ])
    def test_HLLGetUnion_pos(self, bin, expected):
        """
        Test the HLLGetUnion expression.
        """

        upper_lim = ceil(expected + self.relative_count_error(10, expected))
        lower_lim = floor(expected - self.relative_count_error(10, expected))
        record = self.as_connection.get(('test', u'demo', 0))
        records = [record[2]['hll_bin'], record[2]['hll_bin2'], record[2]['hll_bin3']]
        expr = And(
                    GE(
                        HLLGetCount(
                            HLLGetUnion(records, bin)),
                        lower_lim
                    ),
                    LE(
                        HLLGetCount(
                            HLLGetUnion(records, bin)),
                        upper_lim
                    ),
        )

        verify_multiple_expression_avenues(self.as_connection, self.test_ns, self.test_set, expr.compile(), bin, 19)

    @pytest.mark.parametrize("bin, expected", [
        ('hll_bin', 25000)
    ])
    def test_HLLGetUnionCount_pos(self, bin, expected):
        """
        Test the HLLGetUnionCount expression.
        """

        upper_lim = ceil(expected + self.relative_count_error(10, expected))
        lower_lim = floor(expected - self.relative_count_error(10, expected))
        record = self.as_connection.get(('test', u'demo', 0))
        records = [record[2]['hll_bin'], record[2]['hll_bin2'], record[2]['hll_bin3']]
        expr = And(
                    GT(
                        HLLGetUnionCount(records, bin),
                        lower_lim
                    ),
                    LE(
                        HLLGetUnionCount(records, bin),
                        upper_lim
                    ),
        )

        verify_multiple_expression_avenues(self.as_connection, self.test_ns, self.test_set, expr.compile(), bin, 19)

    @pytest.mark.parametrize("bin, expected", [
        ('hll_bin', 5000)
    ])
    def test_HLLGetIntersectCount_pos(self, bin, expected):
        """
        Test the HLLGetIntersectCount expression.
        """

        upper_lim = ceil(expected + self.relative_intersect_error(10, [10000, 10000], 5000))
        lower_lim = floor(expected - self.relative_intersect_error(10, [10000, 10000], 5000))
        record = self.as_connection.get(('test', u'demo', 0))
        records = [record[2]['hll_bin2']]
        expr = And(
                    GE(
                        HLLGetIntersectCount(records, bin),
                        lower_lim
                    ),
                    LE(
                        HLLGetIntersectCount(records, bin),
                        upper_lim
                    ),
        )

        verify_multiple_expression_avenues(self.as_connection, self.test_ns, self.test_set, expr.compile(), bin, 19)

    @pytest.mark.parametrize("bin, expected", [
        ('hll_bin', 0.33)
    ])
    def test_HLLGetSimilarity_pos(self, bin, expected):
        """
        Test the HLLGetSimilarity expression.
        """

        record = self.as_connection.get(('test', u'demo', 0))
        records = [record[2]['hll_bin2']]
        expr = And(
                    GE(
                        HLLGetSimilarity(records, bin),
                        expected - 0.03
                    ),
                    LE(
                        HLLGetSimilarity(records, bin),
                        expected + 0.03
                    ),
        )

        verify_multiple_expression_avenues(self.as_connection, self.test_ns, self.test_set, expr.compile(), bin, 19)

    @pytest.mark.parametrize("bin, expected", [
        ('hll_bin', [15, 49])
    ])
    def test_HLLDescribe_pos(self, bin, expected):
        """
        Test the HLLDescribe expression.
        """

        expr = Eq(HLLDescribe(bin), expected)
        verify_multiple_expression_avenues(self.as_connection, self.test_ns, self.test_set, expr.compile(), bin, 19)

    @pytest.mark.parametrize("bin", [
        ('hll_bin')
    ])
    def test_HLLMayContain_pos(self, bin):
        """
        Test the HLLMayContain expression.
        """

        expr = Eq(HLLMayContain(["key1", "key2", "key3"], HLLBin(bin)), 1)
        verify_multiple_expression_avenues(self.as_connection, self.test_ns, self.test_set, expr.compile(), bin, 19)