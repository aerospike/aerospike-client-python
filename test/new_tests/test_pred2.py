# -*- coding: utf-8 -*-

import pytest
import sys
from .test_base_class import TestBaseClass
from aerospike import exception as e
from .as_status_codes import AerospikeStatus
from aerospike_helpers import predexp

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
            rec = {'name': 'name%s' % (str(i)), 'age': i, 'balance': i * 10, 'key': i, 'alt_name': 'name%s' % (str(i))}
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

        expr = predexp.And(
            predexp.EQ(predexp.IntBin("age"), 10),
            predexp.EQ(predexp.IntBin("age"), predexp.IntBin("key")),
            predexp.NE(23, predexp.IntBin("balance")),
            predexp.GT(predexp.IntBin("balance"), 99),
            predexp.GE(predexp.IntBin("balance"), 100),
            predexp.LT(predexp.IntBin("balance"), 101),
            predexp.LE(predexp.IntBin("balance"), 100),
            predexp.Or(
                predexp.LE(predexp.IntBin("balance"), 100),
                predexp.Not(
                    predexp.EQ(predexp.IntBin("age"), predexp.IntBin("balance"))
                )
            ),
            predexp.EQ(predexp.MetaDigestMod(2), 0),
            predexp.GE(predexp.MetaDeviceSize(), 1),
            predexp.NE(predexp.MetaLastUpdateTime(), 0),
            predexp.NE(predexp.MetaVoidTime(), 0),
            predexp.NE(predexp.MetaTTL(), 0),
            predexp.MetaKeyExists(),
            predexp.EQ(predexp.MetaSetName(), 'demo')

        )

        #print(expr.compile())

        scan_obj = self.as_connection.scan(ns, st)

        records = scan_obj.results({'predexp2': expr.compile()})
        #print(records)
        assert(1 == len(records))

