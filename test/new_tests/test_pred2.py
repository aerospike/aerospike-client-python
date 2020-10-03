# -*- coding: utf-8 -*-

import pytest
import sys
from .test_base_class import TestBaseClass
from aerospike import exception as e
from .as_status_codes import AerospikeStatus
from aerospike_helpers.predexp import *

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)


class TestPred2(TestBaseClass):

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
                    'list_bin': [5, 10, 32]
                }
            as_connection.put(key, rec)

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
            #MetaKeyExists(), needs debugging
            EQ(MetaSetName(), 'demo'),
            EQ(ListGetByIndex('list_bin', ResultType.INTEGER, 0, aerospike.LIST_RETURN_VALUE), 5),
            GE(ListSize('list_bin'), 2),
            
        )

        print(MetaKeyExists().compile())

        #print(expr.compile())

        scan_obj = self.as_connection.scan(ns, st)

        records = scan_obj.results({'predexp2': expr.compile()})
        #print(records)
        assert(1 == len(records))

