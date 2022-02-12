# -*- coding: utf-8 -*-

import sys

import pytest

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)

from aerospike_helpers.expressions import *
from aerospike_helpers.operations import expression_operations as expressions
from aerospike_helpers import batch_records as br
from aerospike_helpers.operations import operations as op
from aerospike import exception as e
from .test_base_class import TestBaseClass


class TestBatchOperate(TestBaseClass):

    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        if self.server_version < [5, 6]:
            pytest.mark.xfail(reason="Servers older than 5.6 do not support arithmetic expressions.")
            pytest.xfail()
        
        self.test_ns = 'test'
        self.test_set = 'demo'
        self.keys = []
        self.batch_size = 5

        for i in range(self.batch_size):
            key = ('test', u'demo', i)
            rec = {
                'name': 'name10',
                't': True,
                'age': 10,
                'balance': 100,
                'key': 10,
                'ilist_bin': [
                    1,
                    2,
                    6,
                ],
                'imap_bin': {
                    1: 1,
                    2: 2,
                    3: 6,
                }
            }
            as_connection.put(key, rec)
            self.keys.append(key)

        def teardown():
            for i in range(self.batch_size):
                key = ('test', u'demo', i)
                as_connection.remove(key)

        request.addfinalizer(teardown)

    @pytest.mark.parametrize("name, batch_records, expected", [
        (
            "test1",
            br.BatchRecords(
                [
                    br.BatchWrite(
                        ("test", "demo", 1),
                        [
                            op.write("new", 10)
                        ],
                        policy=[]
                    )
                ]
            ),
            # TODO expected
        ),
    ])
    def test_read_pos(self, expr, flags, name, expected):
        """
        Test expression read operation with correct parameters.
        """
        ops = [
            expressions.expression_read(name, expr.compile(), flags)
        ]
        meta = {'gen': 1}
        policy = {'timeout': 1001}
        #print(self.keys)
        res = self.as_connection.batch_get_ops(self.keys, ops, meta, policy)
        """
        res are in the format of (status-tuple, ((meta-dict, result-dict), status-tuple, exception), ...)
        """
        status = res[0]
        recs = res[1:]
        # print("\ntest_read_pos status:", status)
        for i in range(self.batch_size):
            # print("results: ", recs[i])
            assert recs[i][0][1] == expected
