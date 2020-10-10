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

def add_ctx_op(ctx_type, value):
    ctx_func = ctx_ops[ctx_type]
    return ctx_func(value)


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
                    'list_bin' : [
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
                        GEO_POLY
                    ]
                }
            self.as_connection.put(key, rec)

        ctx_sort_nested_map1 = [
            cdt_ctx.cdt_ctx_list_index(4)
        ]

        sort_ops = [
            list_operations.list_set_order('list_bin', aerospike.LIST_ORDERED),
            #map_operations.map_set_policy('list_bin', {'map_order': aerospike.MAP_KEY_ORDERED}, ctx_sort_nested_map1),
        ]

        #apply map order policy
        for i in range(19):
            _, _, _ = self.as_connection.operate(('test', u'demo', i), sort_ops)
        
        _, _, rec = self.as_connection.get(('test', u'demo', 10))
        print(rec)

        key = ('test', u'demo', 122)
        llist = [{"op": aerospike.OPERATOR_APPEND,
                  "bin": bytearray("asd;adk\0kj", "utf-8"),
                  "val": u"john"}]
        # Creates a record with the key 122, with one bytearray key.
        self.bytearray_bin = bytearray("asd;adk\0kj", "utf-8")
        as_connection.operate(key, llist)
        self.record_count = 20

        def teardown():
            for i in range(19):
                key = ('test', u'demo', i)
                as_connection.remove(key)

            key = ('test', 'demo', 122)
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
            EQ(ListGetByIndex('list_bin', ResultType.INTEGER, 0, aerospike.LIST_RETURN_VALUE), 5),
            GE(ListSize('list_bin'), 2),
            
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
        #(None, None, ResultType.BLOB, 7, aerospike.LIST_RETURN_VALUE, True, 9), #TODO needs debuging
        #(None, None, ResultType.BLOB, 5, aerospike.LIST_RETURN_VALUE, None, 19), #TODO use this for negative testing as it used to cause a crash
        #(None, None, 6, 5, aerospike.LIST_RETURN_VALUE, None, 19), #TODO needs investigating
        (None, None, ResultType.LIST, 3, aerospike.LIST_RETURN_VALUE, [26, 27, 28, 6], 1),
        #([list_index], [6], ResultType.INTEGER, 6, aerospike.LIST_RETURN_VALUE, [6], 1),
        #(None, None, ResultType.MAP, 7, aerospike.LIST_RETURN_VALUE, {31: 31, 32: 32, 33: 33, 12: 12}, 1),
        #(None, None, ResultType.BLOB, 8, aerospike.LIST_RETURN_VALUE, aerospike.null, 19),
        #(None, None, ResultType.BLOB, 9, aerospike.LIST_RETURN_VALUE, GEO_POLY, 19) #TODO needs debugging
        #(None, None, ResultType.BLOB, 10, aerospike.LIST_RETURN_VALUE, TestUsrDefinedClass, 19) #TODO needs debugging
    ])
    def test_ListGetByIndex_pos(self, ctx_types, ctx_indexes, bin_type, index, return_type, check, expected):
        """
        Invoke ListGetByIndex().
        """

        if ctx_types is not None:
            ctx = []
            for ctx_type, index in zip(ctx_types, ctx_indexes) :
                ctx.append(add_ctx_op(ctx_type, index))
        else:
            ctx = None
        
        expr = EQ(ListGetByIndex(bin_type, ctx, return_type, index, 'list_bin'), check)
        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)
        records = scan_obj.results({'predexp2': expr.compile()})
        #print(records[3])
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
        #(None, None, {12: 12, 31: 31, 32: 32, 33: 33}, {15: 15, 31: 31, 32: 32, 33: 33}, aerospike.LIST_RETURN_INDEX, [[7], [7], [7]], 3), #TODO why 6 matches? WHat is expected?
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
            for ctx_type, index in zip(ctx_types, ctx_indexes) :
                ctx.append(add_ctx_op(ctx_type, index))
        else:
            ctx = None
        
        expr = EQ(ListGetByValueList(ctx, return_type, value, 'list_bin'), check)
        print('hi', expr.compile())
        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)
        records = scan_obj.results({'predexp2': expr.compile()})
        #print(records[3])
        #print(LIST_BIN_EXAMPLE == LIST_BIN_EXAMPLE)
        assert(len(records) == expected)

    @pytest.mark.parametrize("ctx_types, ctx_indexes, value, return_type, check, expected", [
        (None, None, [10, [26, 27, 28, 10]], aerospike.LIST_RETURN_VALUE, (10, [26, 27, 28, 10]), e.InvalidRequest), # bad tuple
        (None, None, (10, [26, 27, 28, 10]), aerospike.LIST_RETURN_VALUE, [10, [26, 27, 28, 10]], e.ParamError), # bad tuple
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

    # @pytest.mark.parametrize("ctx_types, ctx_indexes, value, return_type, check, expected", [
    #     (None, None, [10, [26, 27, 28, 10]], aerospike.LIST_RETURN_VALUE, [10, [26, 27, 28, 10]], 1),
    #     (None, None, ["string_test3", 3], aerospike.LIST_RETURN_VALUE, [3, "string_test3"], 1),
    #     (None, None, ["string_test3", 3], aerospike.LIST_RETURN_VALUE, ["string_test3", 3], 0), #why doesn't this work like above? is order
    #     (None, None, ["bytes_test18".encode("utf8"), 18, GEO_POLY], aerospike.LIST_RETURN_VALUE, [18, "bytes_test18".encode("utf8"), GEO_POLY], 1),
    #     (None, None, LIST_BIN_EXAMPLE, aerospike.LIST_RETURN_VALUE, LIST_BIN_EXAMPLE, 1),
    #     (None, None, LIST_BIN_EXAMPLE, aerospike.LIST_RETURN_INDEX, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 1),
    #     (None, None, LIST_BIN_EXAMPLE, aerospike.LIST_RETURN_REVERSE_INDEX, [10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0], 1),
    #     (None, None, LIST_BIN_EXAMPLE, aerospike.LIST_RETURN_COUNT, 11, 1),
    #     (None, None, [10], aerospike.LIST_RETURN_RANK, [1], 1),
    #     ([list_index], [6], [26, 6], aerospike.LIST_RETURN_INDEX, [0, 3], 1),
    # ])
    # def test_ListGetByValueRelRankRangeToEnd_pos(self, ctx_types, ctx_indexes, value, return_type, check, expected):
    #     """
    #     Invoke ListGetByValueRelRankRangeToEnd().
    #     """

    #     if ctx_types is not None:
    #         ctx = []
    #         for ctx_type, index in zip(ctx_types, ctx_indexes) :
    #             ctx.append(add_ctx_op(ctx_type, index))
    #     else:
    #         ctx = None
        
    #     expr = EQ(ListGetByValueList('list_bin', return_type, value, ctx), check)
    #     scan_obj = self.as_connection.scan(self.test_ns, self.test_set)
    #     records = scan_obj.results({'predexp2': expr.compile()})
    #     #print(records[3])
    #     assert(len(records) == expected)
