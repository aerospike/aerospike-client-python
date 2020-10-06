# -*- coding: utf-8 -*-

import pytest
import sys
from .test_base_class import TestBaseClass
from aerospike import exception as e
from .as_status_codes import AerospikeStatus
from aerospike_helpers import cdt_ctx
from aerospike_helpers.predexp import *

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

def add_ctx_op(ctx_type, value):
    ctx_func = ctx_ops[ctx_type]
    return ctx_func(value)

class TestPred2(TestBaseClass):

    class TestUsrDefinedClass():

        def __init__(self, i):
            self.data = i

    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        self.test_ns = 'test'
        self.test_set = 'demo'

        for i in range(19):
            key = ('test', u'demo', i)
            rec = {'name': 'name%s' % (str(i)),
                    'age': i,
                    'balance': i * 10,
                    'key': i, 'alt_name': 'name%s' % (str(i)),
                    'list_bin': [
                                    i,
                                    "string_test" + str(i),
                                    ("bytes_test" + str(i)).encode("utf8"),
                                    bytearray("bytearray_test" + str(i), "utf8"),
                                    i % 2 == 1,
                                    None,
                                    [26, 27, 28, i],
                                    {31: 31, 32: 32, 33: 33, i: i},
                                    aerospike.null,
                                    aerospike.GeoJSON(
                                        {"type": "Polygon",
                                        "coordinates": [[[-122.500000, 37.000000],
                                                        [-121.000000, 37.000000],
                                                        [-121.000000, 38.080000],
                                                        [-122.500000, 38.080000],
                                                        [-122.500000, 37.000000]]]}),
                                    self.TestUsrDefinedClass(i)
                    ]
                }
            self.as_connection.put(key, rec)

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
    

    @pytest.mark.parametrize("ctx_types, ctx_indexes, value, return_type, check, expected", [
        (None, None, 10, aerospike.LIST_RETURN_VALUE, [10], 1),
        (None, None, "string_test3", aerospike.LIST_RETURN_VALUE, ["string_test3"], 1),
    ])
    def test_ListGetByValue_pos(self, ctx_types, ctx_indexes, value, return_type, check, expected):
        """
        Invoke ListGetByValue().
        """

        if ctx_types is not None:
            ctx = []
            for ctx_type, index in zip(ctx_types, ctx_indexes) :
                ctx.append(add_ctx_op(ctx_type, index))
        else:
            ctx = None
        
        expr = EQ(ListGetByValue('list_bin', value, return_type, ctx), check)
        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)
        records = scan_obj.results({'predexp2': expr.compile()})

        assert(len(records) == expected)




