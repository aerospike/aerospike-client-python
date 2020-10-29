# -*- coding: utf-8 -*-

import pytest
import sys
from .test_base_class import TestBaseClass
from aerospike import exception as e
from .as_status_codes import AerospikeStatus
from aerospike_helpers import cdt_ctx
from aerospike_helpers.predexp import *
from aerospike_helpers.operations import map_operations
from aerospike_helpers.operations import list_operations
from aerospike import exception as e

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


class TestUsrDefinedClass():

    __test__ = False

    def __init__(self, i):
        self.data = i

# arranged by order
LIST_BIN_EXAMPLE = [
                None,
                aerospike.null,
                10 % 2 == 1,
                10,
                "string_test" + str(10),
                [26, 27, 28, 10],
                {32: 32, 33: 33, 10: 10, 31: 31},
                bytearray("bytearray_test" + str(10), "utf8"),
                ("bytes_test" + str(10)).encode("utf8"),
                TestUsrDefinedClass(10),
                float(10),
                GEO_POLY
]


class TestPred2(TestBaseClass):

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
                        {1: 1, 2: 2},
                        {1: 1, 3: 3},
                        {1: 1, 4: 4}
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
                    'glist_bin': [
                        GEO_POLY,
                        GEO_POLY1,
                        GEO_POLY2
                    ],
                    '1bits_bin': bytearray([1] * 8)
                }
            self.as_connection.put(key, rec)
        
        self.as_connection.put(('test', u'demo', 100), {'extra': 'record'})

        ctx_sort_nested_map1 = [
            cdt_ctx.cdt_ctx_list_index(4)
        ]

        sort_ops = [
            #list_operations.list_set_order('ilist_bin', aerospike.LIST_ORDERED),
            list_operations.list_set_order('slist_bin', aerospike.LIST_ORDERED),
            list_operations.list_set_order('bllist_bin', aerospike.LIST_ORDERED),
            list_operations.list_set_order('bylist_bin', aerospike.LIST_ORDERED),
            #map_operations.map_set_policy('list_bin', {'map_order': aerospike.MAP_KEY_ORDERED}, ctx_sort_nested_map1),
        ]

        #apply map order policy
        for i in range(19):
            _, _, _ = self.as_connection.operate(('test', u'demo', i), sort_ops)
        
        _, _, rec = self.as_connection.get(('test', u'demo', 10))
        print(rec)

        def teardown():
            for i in range(19):
                key = ('test', u'demo', i)
                as_connection.remove(key)

        request.addfinalizer(teardown)

    def test_scan_with_results_method_and_predexp(self):

        ns = 'test'
        st = 'demo'

        expr = And(
            EQ(IntBin("age"), 10),
            EQ(IntBin("age"), IntBin("key")),
            NE(23, IntBin("balance")),
            GT(IntBin("balance"), 99),
            GE(IntBin("balance"), 100),
            LT(IntBin("balance"), 101),
            LE(IntBin("balance"), 100),
            Or(
                LE(IntBin("balance"), 100),
                Not(
                    EQ(IntBin("age"), IntBin("balance"))
                )
            ),
            EQ(MetaDigestMod(2), 0),
            GE(MetaDeviceSize(), 1),
            NE(MetaLastUpdateTime(), 0),
            NE(MetaVoidTime(), 0),
            NE(MetaTTL(), 0),
            MetaKeyExists(), #needs debugging
            EQ(MetaSetName(), 'demo'),
            EQ(ListGetByIndex(ResultType.INTEGER, None, aerospike.LIST_RETURN_VALUE, 0, 'list_bin'), 5),
            GE(ListSize(None, 'list_bin'), 2),
            
        )

        #expr = EQ(MetaSetName(), 'demo')

        print(MetaKeyExists().compile())

        #print(expr.compile())

        scan_obj = self.as_connection.scan(ns, st)

        records = scan_obj.results({'predexp2': expr.compile()})
        #print(records)
        assert(1 == len(records))
    
    @pytest.mark.parametrize("ctx_types, ctx_indexes, bin_type, index, return_type, check, expected", [
        (None, None, ResultType.INTEGER, 1, aerospike.LIST_RETURN_VALUE, 10, 1),
        (None, None, ResultType.STRING, 2, aerospike.LIST_RETURN_VALUE, "string_test3", 1),
        (None, None, ResultType.BLOB, 6, aerospike.LIST_RETURN_VALUE, "bytes_test3".encode("utf8"), 1),
        (None, None, ResultType.BLOB, 5, aerospike.LIST_RETURN_VALUE, bytearray("bytearray_test3", "utf8"), 1),
        (None, None, ResultType.BLOB, 7, aerospike.LIST_RETURN_VALUE, True, 9), #TODO needs debuging
        #(None, None, ResultType.BLOB, 5, aerospike.LIST_RETURN_VALUE, None, 19), #TODO use this for negative testing as it used to cause a crash
        #(None, None, 0, 5, aerospike.LIST_RETURN_VALUE, None, 19), #TODO needs investigating build_call - error 4 invalid result_type 0
        (None, None, ResultType.LIST, 3, aerospike.LIST_RETURN_VALUE, [26, 27, 28, 6], 1),
        ([list_index], [3], ResultType.INTEGER, 3, aerospike.LIST_RETURN_VALUE, 6, 1), #TODO needs debugging
        (None, None, ResultType.MAP, 4, aerospike.LIST_RETURN_VALUE, {31: 31, 32: 32, 33: 33, 12: 12}, 1),
        #(None, None, 6, 8, aerospike.LIST_RETURN_VALUE, aerospike.null, 19),
        #(None, None, 8, 9, aerospike.LIST_RETURN_VALUE, GEO_POLY, 19), #TODO needs debugging 'build_compare - error 4 cannot compare type 8'
        (None, None, ResultType.BLOB, 0, aerospike.LIST_RETURN_VALUE, TestUsrDefinedClass(10), 1) #TODO needs debugging
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
        
        #breakpoint()
        expr = EQ(ListGetByIndex(bin_type, ctx, return_type, index, 'list_bin'), check)
        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)
        records = scan_obj.results({'predexp2': expr.compile()})
        assert(len(records) == expected)

# Oct 06 2020 12:08:36 GMT: WARNING (particle): (msgpack_in.c:1099) msgpack_sz_internal: invalid at i 1 count 2
# Oct 06 2020 12:08:36 GMT: WARNING (exp): (exp.c:755) invalid instruction at offset 60
# Oct 06 2020 12:08:36 GMT: WARNING (scan): (scan.c:752) basic scan job failed predexp processing
# Oct 06 2020 12:08:36 GMT: WARNING (particle): (msgpack_in.c:1099) msgpack_sz_internal: invalid at i 1 count 2
# Oct 06 2020 12:08:36 GMT: WARNING (exp): (exp.c:755) invalid instruction at offset 86
# Oct 06 2020 12:08:36 GMT: WARNING (scan): (scan.c:752) basic scan job failed predexp processing

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
        
        expr = EQ(ListGetByValue(ctx, value, return_type, 'list_bin'), check)
        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)
        print('\nhi*************************', expr.compile(), '\n')
        records = scan_obj.results({'predexp2': expr.compile()})
        #print(records[3])
        assert(len(records) == expected)

    @pytest.mark.parametrize("ctx_types, ctx_indexes, begin, end, return_type, check, expected", [
        (None, None, 10, 13, aerospike.LIST_RETURN_VALUE, [[10], [11], [12]], 3),
        (None, None, 10, aerospike.CDTInfinite(), aerospike.LIST_RETURN_COUNT, [9, 9, 9], 10),
        (None, None, 10, 13, aerospike.LIST_RETURN_RANK, [[1], [1], [1]], 3),
        (None, None, "string_test3","string_test6", aerospike.LIST_RETURN_INDEX, [[2], [2], [2]], 3),
        (None, None, "bytes_test6".encode("utf8"), "bytes_test9".encode("utf8"), aerospike.LIST_RETURN_COUNT, [1, 1, 1], 3),
        (None, None, bytearray("bytearray_test3", "utf8"), bytearray("bytearray_test6", "utf8"), aerospike.LIST_RETURN_REVERSE_INDEX, [[5], [5], [5]], 3),
        (None, None, [26, 27, 28, 6], [26, 27, 28, 9], aerospike.LIST_RETURN_VALUE, [[[26, 27, 28, 6]], [[26, 27, 28, 7]], [[26, 27, 28, 8]]], 3),
        ([list_index], [3], 5, 9, aerospike.LIST_RETURN_REVERSE_RANK, [[3], [3], [3]], 4),
        #(None, None, {12: 12, 31: 31, 32: 32, 33: 33}, {15: 15, 31: 31, 32: 32, 33: 33}, aerospike.LIST_RETURN_INDEX, [[4], [4], [4]], 3), #TODO why 6 matches? WHat is expected?
        (None, None, GEO_POLY, aerospike.CDTInfinite(), aerospike.LIST_RETURN_VALUE, [[GEO_POLY], [GEO_POLY], [GEO_POLY]], 19), #why doesn't CDTWildCard() get same matches as inf?
        (None, None, TestUsrDefinedClass(4), TestUsrDefinedClass(7), aerospike.LIST_RETURN_VALUE, [[TestUsrDefinedClass(4)], [TestUsrDefinedClass(5)], [TestUsrDefinedClass(6)]], 3)
    ])
    def test_ListGetByValueRange_pos(self, ctx_types, ctx_indexes, begin, end, return_type, check, expected):
        """
        Invoke ListGetByValue().
        """

        if ctx_types is not None:
            ctx = []
            for ctx_type, index in zip(ctx_types, ctx_indexes) :
                ctx.append(add_ctx_op(ctx_type, index))
        else:
            ctx = None
        
        expr = Or(
                    EQ(ListGetByValueRange(ctx, return_type, begin, end, 'list_bin'), check[0]),
                    EQ(ListGetByValueRange(ctx, return_type, begin, end, 'list_bin'), check[1]),
                    EQ(ListGetByValueRange(ctx, return_type, begin, end, 'list_bin'), check[2]),
        )

        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)
        records = scan_obj.results({'predexp2': expr.compile()})
        for record in records:
            print(record[2]['list_bin'][7])
        assert(len(records) == expected)

    @pytest.mark.parametrize("ctx, begin, end, return_type, check, expected", [
        ("bad ctx", 10, 13, aerospike.LIST_RETURN_VALUE, [[10], [11], [12]], e.ParamError),
        (None, 10, 13, aerospike.LIST_RETURN_VALUE, [[10], [11], 12], e.InvalidRequest)
    ])
    def test_ListGetByValueRange_neg(self, ctx, begin, end, return_type, check, expected):
        """
        Invoke ListGetByValue() with expected failures.
        """
        
        expr = Or(
                    EQ(ListGetByValueRange(ctx, return_type, begin, end, 'list_bin'), check[0]),
                    EQ(ListGetByValueRange(ctx, return_type, begin, end, 'list_bin'), check[1]),
                    EQ(ListGetByValueRange(ctx, return_type, begin, end, 'list_bin'), check[2]),
        )

        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)
        with pytest.raises(expected):
            scan_obj.results({'predexp2': expr.compile()})

    @pytest.mark.parametrize("ctx_types, ctx_indexes, value, return_type, check, expected", [
        (None, None, [10, [26, 27, 28, 10]], aerospike.LIST_RETURN_VALUE, [10, [26, 27, 28, 10]], 1),
        (None, None, ["string_test3", 3], aerospike.LIST_RETURN_VALUE, [3, "string_test3"], 1),
        (None, None, ["string_test3", 3], aerospike.LIST_RETURN_VALUE, ["string_test3", 3], 0), #why doesn't this work like above? is order
        (None, None, ["bytes_test18".encode("utf8"), 18, GEO_POLY], aerospike.LIST_RETURN_VALUE, [18, "bytes_test18".encode("utf8"), GEO_POLY], 1),
        (None, None, LIST_BIN_EXAMPLE, aerospike.LIST_RETURN_VALUE, LIST_BIN_EXAMPLE, 1),
        (None, None, LIST_BIN_EXAMPLE, aerospike.LIST_RETURN_INDEX, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 1),
        (None, None, LIST_BIN_EXAMPLE, aerospike.LIST_RETURN_REVERSE_INDEX, [10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0], 1),
        (None, None, LIST_BIN_EXAMPLE, aerospike.LIST_RETURN_COUNT, 11, 1),
        (None, None, [10], aerospike.LIST_RETURN_RANK, [1], 1),
        ([list_index], [3], [26, 6], aerospike.LIST_RETURN_INDEX, [0, 3], 1),
    ])
    def test_ListGetByValueList_pos(self, ctx_types, ctx_indexes, value, return_type, check, expected):
        """
        Invoke ListGetByValueList().
        """
        #breakpoint()

        if ctx_types is not None:
            ctx = []
            for ctx_type, index in zip(ctx_types, ctx_indexes):
                ctx.append(add_ctx_op(ctx_type, index))
        else:
            ctx = None
        
        expr = EQ(ListGetByValueList(ctx, return_type, value, 'list_bin'), check)
        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)
        records = scan_obj.results({'predexp2': expr.compile()})
        #print(records[3])
        #print(LIST_BIN_EXAMPLE == LIST_BIN_EXAMPLE)
        assert(len(records) == expected)

    @pytest.mark.parametrize("ctx_types, ctx_indexes, value, return_type, check, expected", [
        (None, None, [10, [26, 27, 28, 10]], aerospike.LIST_RETURN_VALUE, (10, [26, 27, 28, 10]), e.InvalidRequest) # bad tuple
    ])
    def test_ListGetByValueList_neg(self, ctx_types, ctx_indexes, value, return_type, check, expected):
        """
        Invoke ListGetByValueList() with expected failures.
        """
        #breakpoint()

        if ctx_types is not None:
            ctx = []
            for ctx_type, index in zip(ctx_types, ctx_indexes) :
                ctx.append(add_ctx_op(ctx_type, index))
        else:
            ctx = None
        
        expr = EQ(ListGetByValueList(ctx, return_type, value, 'list_bin'), check)
        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)
        #records = scan_obj.results({'predexp2': expr.compile()})
        with pytest.raises(expected):
            records = scan_obj.results({'predexp2': expr.compile()})

    @pytest.mark.parametrize("ctx_types, ctx_indexes, value, rank, return_type, check, expected", [ #TODO more tests
        ([list_index], [3], 26, 0, aerospike.LIST_RETURN_COUNT, 3, 19),
        ([list_index], [3], 10, 1, aerospike.LIST_RETURN_COUNT, 3, 9),
        ([list_index], [3], 10, 2, aerospike.LIST_RETURN_VALUE, [27, 28], 9),
        (None, None, "string_test10", 0,  aerospike.LIST_RETURN_COUNT, 9, 1),
        # (None, None, ["bytes_test18".encode("utf8"), 18, GEO_POLY], aerospike.LIST_RETURN_VALUE, [18, "bytes_test18".encode("utf8"), GEO_POLY], 1),
        # (None, None, LIST_BIN_EXAMPLE, aerospike.LIST_RETURN_VALUE, LIST_BIN_EXAMPLE, 1),
        # (None, None, LIST_BIN_EXAMPLE, aerospike.LIST_RETURN_INDEX, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 1),
        # (None, None, LIST_BIN_EXAMPLE, aerospike.LIST_RETURN_REVERSE_INDEX, [10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0], 1),
        # (None, None, LIST_BIN_EXAMPLE, aerospike.LIST_RETURN_COUNT, 11, 1),
        # (None, None, [10], aerospike.LIST_RETURN_RANK, [1], 1),
        # ([list_index], [6], [26, 6], aerospike.LIST_RETURN_INDEX, [0, 3], 1),
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
        
        expr = EQ(ListGetByValueRelRankRangeToEnd(ctx, return_type, value, rank, 'list_bin'), check)
        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)
        records = scan_obj.results({'predexp2': expr.compile()})
        #print(records)
        for record in records:
            print(record[2])
        assert(len(records) == expected)

    @pytest.mark.parametrize("ctx_types, ctx_indexes, value, rank, return_type, expected", [
        ([list_index], [3], 26, "bad_rank", "bad_return_type", e.ParamError)
    ])
    def test_ListGetByValueRelRankRangeToEnd_neg(self, ctx_types, ctx_indexes, value, rank, return_type, expected):
        """
        Invoke ListGetByValueRelRankRangeToEnd() with expected failures.
        """
        #breakpoint()

        if ctx_types is not None:
            ctx = []
            for ctx_type, index in zip(ctx_types, ctx_indexes) :
                ctx.append(add_ctx_op(ctx_type, index))
        else:
            ctx = None
        
        expr = ListGetByValueRelRankRangeToEnd(ctx, return_type, value, rank, 'list_bin')
        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)
        with pytest.raises(expected):
            records = scan_obj.results({'predexp2': expr.compile()})


    @pytest.mark.parametrize("ctx_types, ctx_indexes, value, rank, count, return_type, check, expected", [ #TODO more tests
        ([list_index], [3], 26, 0, 3, aerospike.LIST_RETURN_COUNT, 3, 19),
        ([list_index], [3], 26, 0, 2, aerospike.LIST_RETURN_VALUE, [26, 27], 19),
        (None, None, "string_test10", 0, 1, aerospike.LIST_RETURN_INDEX, [3], 1),
        # (None, None, ["bytes_test18".encode("utf8"), 18, GEO_POLY], aerospike.LIST_RETURN_VALUE, [18, "bytes_test18".encode("utf8"), GEO_POLY], 1),
        # (None, None, LIST_BIN_EXAMPLE, aerospike.LIST_RETURN_VALUE, LIST_BIN_EXAMPLE, 1),
        # (None, None, LIST_BIN_EXAMPLE, aerospike.LIST_RETURN_INDEX, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 1),
        # (None, None, LIST_BIN_EXAMPLE, aerospike.LIST_RETURN_REVERSE_INDEX, [10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0], 1),
        # (None, None, LIST_BIN_EXAMPLE, aerospike.LIST_RETURN_COUNT, 11, 1),
        # (None, None, [10], aerospike.LIST_RETURN_RANK, [1], 1),
        # ([list_index], [6], [26, 6], aerospike.LIST_RETURN_INDEX, [0, 3], 1),
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
        
        expr = EQ(ListGetByValueRelRankRange(ctx, return_type, value, rank, count, 'list_bin'), check)
        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)
        records = scan_obj.results({'predexp2': expr.compile()})
        assert(len(records) == expected)

    @pytest.mark.parametrize("bin, values", [
        ("ilist_bin", [ResultType.INTEGER, 6, 1, 7, [2, 6], 1]), #what was happening with everything being true when values[0] == 1?
        ("slist_bin", [ResultType.STRING, "f", "b", "g", ["d", "f"], "b"]),
        ("llist_bin", [ResultType.LIST, [1, 4], [1, 2], [1, 6], [[1, 3], [1, 4]], [1, 2]]),
        # ("bllist_bin", [ResultType.BLOB, TestUsrDefinedClass(4), TestUsrDefinedClass(1), TestUsrDefinedClass(6), [TestUsrDefinedClass(3), TestUsrDefinedClass(4)], TestUsrDefinedClass(1)]) comparison with anything other than AS_BYTES_BLOB is unsupported.
        #("mlist_bin", [ResultType.MAP, {1: 1, 4: 4}, {1: 1, 2: 2}, {1: 1, 6: 6}, [{1: 1, 3: 3}, {1: 1, 4: 4}], {1: 1, 2: 2}]) # pending investigation
        ("bylist_bin", [ResultType.BLOB, "f".encode("utf8"), "b".encode("utf8"), "g".encode("utf8"), ["d".encode("utf8"), "f".encode("utf8")], "b".encode("utf8")]),
        #("bolist_bin", [ResultType.BLOB, True, False, True, [False, True], False]) comparison with anything other than AS_BYTES_BLOB is unsupported.
        #TODO nlist_bin test
        ("flist_bin", [ResultType.FLOAT, 6.0, 1.0, 7.0, [2.0, 6.0], 1.0]),
        #("flist_bin", [ResultType.GEOJSON, GEO_POLY2, GEO_POLY, GEO_POLY2, [GEO_POLY1, GEO_POLY2], GEO_POLY]) ask about geojson support
    ])
    def test_ListReadOps_pos(self, bin, values):
        """
        Invoke various list read expressions with many value types.
        """
        
        expr = And(
            EQ(
                ListGetByValueRelRankRange(None, aerospike.LIST_RETURN_COUNT, 
                    ListGetByIndex(values[0], None, aerospike.LIST_RETURN_VALUE, 0, bin), 1, 3, bin), #why did this fail with aerospike.CDTInfinite for count?
                2),
            EQ(
                ListGetByValue(None,  values[1], aerospike.LIST_RETURN_INDEX,
                    ListGetByValueRange(None, aerospike.LIST_RETURN_VALUE, values[2], values[3], bin)),
                [2]),
            EQ(
                ListGetByValueList(None, aerospike.LIST_RETURN_COUNT, values[4], 
                    ListGetByValueRelRankRangeToEnd(None, aerospike.LIST_RETURN_VALUE, values[5], 1, bin)),
                2),
            EQ(
                ListGetByIndexRangeToEnd(None, aerospike.LIST_RETURN_COUNT, 1,
                    ListGetByIndexRelRankRange(None, aerospike.LIST_RETURN_VALUE, 1, 3, bin,)),
                1),
            EQ(
                ListGetByRank(None, aerospike.LIST_RETURN_RANK, ResultType.INTEGER, 1, #lets 20 pass with slist_bin
                    ListGetByRankRangeToEnd(None, aerospike.LIST_RETURN_VALUE, 1, bin)),
                1),
            EQ(
                ListGetByRankRange(None, aerospike.LIST_RETURN_COUNT, 1, ListSize(None, bin), bin),
                2
            )
        )

        # ops = [
        #     list_operations.list_get_by_index(bin, 0, aerospike.LIST_RETURN_VALUE)
        # ]

        # _, _, res = self.as_connection.operate(('test', u'demo', 10), ops)
        # # print("******* ", res[bin].data)
        # x = res[bin]

        # expr =             EQ(
        #         ListGetByValueRelRankRange(None, aerospike.LIST_RETURN_COUNT, 
        #             x, 1, 3, 'bllist_bin'), #why did this fail with aerospike.CDTInfinite for count?
        #         2)
        
        # expr = EQ(
        #     ListGetByIndex(ResultType.MAP, None, aerospike.LIST_RETURN_VALUE, 0, bin),
        #     {1: 1, 2: 2}
        # )

        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)
        records = scan_obj.results({'predexp2': expr.compile()})
        #print(records) TestUsrDefinedClass(1)
        # for record in records:
        #     print(record[2])
        assert(len(records) == 19)


    @pytest.mark.parametrize("bin, ctx, policy, values, expected", [
        (
            "ilist_bin",
            None,
            {}, 
            [
                20,
                [3, 9],
                4,
                [24, 25], #
                10,
                1, #
                [2, 6],
                1, #
                2,
                6,
                2 #
            ], 
            [
                [1, 2, 3, 4, 6, 9, 20],
                [10, 24, 25, 2, 6],
                [1]
            ]
        )
    ])
    def test_ListModOps_pos(self, bin, ctx, policy, values, expected):
        """
        Invoke various list modify expressions with many value types.
        """
        
        expr = And(
            EQ(
                ListGetByIndexRangeToEnd(ctx, aerospike.LIST_RETURN_VALUE, 0,                 
                    ListSort(ctx, aerospike.LIST_SORT_DEFAULT, #TODO can't compare with constant list (server issue)        
                        ListAppend(ctx, policy, values[0],
                            ListAppendItems(ctx, policy, values[1],
                                ListInsert(ctx, policy, 1, values[2], bin))))),
                expected[0]
            ),
            EQ(
                ListGetByRankRangeToEnd(ctx, aerospike.LIST_RETURN_VALUE, 0,
                    ListInsertItems(ctx, policy, 1, values[3],
                        ListSet(ctx, policy, 0, values[4],
                            ListClear(ctx, bin)))),
                expected[1]
            ),
            EQ(
                ListRemoveByValue(ctx, values[5],
                    ListRemoveByValueList(ctx, values[6], bin)),
                []
            ),
            EQ(
                ListRemoveByValueRange(ctx, values[7], values[8],
                    ListRemoveByValueRelRankToEnd(ctx, values[9], 2, bin)),
                []
            ),
            EQ(
                ListRemoveByValueRelRankRange(ctx, values[10], 0, 2,
                    ListRemoveByIndex(ctx, 0, bin)),
                []
            ), 
            EQ(
                ListRemoveByIndexRangeToEnd(ctx, 1,
                    ListRemoveByIndexRange(ctx, 0, 1, bin)),
                []
            ),
            EQ(
                ListRemoveByRank(ctx, 0, 
                    ListRemoveByRankRangeToEnd(ctx, 1, bin)),
                [] #empty
            ),
            EQ(
                ListRemoveByRankRange(ctx, 1, 2, bin),
                expected[2]
            )
        )

        # ListIncrement(ctx, policy, 1, )) TODO needs it's own always int case

        expr =  EQ(
                ListGetByRankRangeToEnd(ctx, aerospike.LIST_RETURN_VALUE, 0,
                    # ListSort(ctx, aerospike.LIST_SORT_DEFAULT,
                        ListInsertItems(ctx, policy, 1, values[3],
                            #ListSet(ctx, policy, 0, values[4],
                                ListClear(ctx, bin))),
                [24, 25]
            )

        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)
        records = scan_obj.results({'predexp2': expr.compile()})
        #print(records) TestUsrDefinedClass(1)
        # for record in records:
        #     print(record[2])
        assert(len(records) == 19)
    

    @pytest.mark.parametrize("policy, bytes_size, flags, bin, expected", [
        (None, 10, None, '1bits_bin', bytearray([0] * 1))
    ])
    def test_BitModOps_pos(self, policy, bytes_size, flags, bin, expected):
        """
        Test various bit expressions.
        """

        expr = EQ(
                    BitGet(9, 2, 
                        BitResize(policy, bytes_size, flags, bin)),
                    bytearray([0] * 1)
                )

        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)
        records = scan_obj.results({'predexp2': expr.compile()})
        #print(records) TestUsrDefinedClass(1)
        # for record in records:
        #     print(record[2])
        assert(len(records) == 19)