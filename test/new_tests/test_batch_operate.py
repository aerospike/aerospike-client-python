# -*- coding: utf-8 -*-

import sys

import pytest

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)

from aerospike_helpers import expressions as exp
from aerospike_helpers.batch import records as br
from aerospike_helpers.operations import operations as op
from aerospike_helpers.operations import list_operations as lop
from aerospike import exception as e
from .test_base_class import TestBaseClass
from .as_status_codes import AerospikeStatus
modules = dir()


class TestBatchOperate(TestBaseClass):

    @pytest.fixture(autouse=True)
    def setup(self, request, connection_with_config_funcs):
        as_connection = connection_with_config_funcs

        if self.server_version < [5, 6]:
            pytest.mark.xfail(reason="Servers older than 5.6 do not support arithmetic expressions.")
            pytest.xfail()
        
        self.test_ns = 'test'
        self.test_set = 'demo'
        self.keys = []
        self.batch_size = 5

        for i in range(self.batch_size):
            key = ('test', 'demo', i)
            rec = {
                "count": i,
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
                key = ('test', 'demo', i)
                as_connection.remove(key)

        request.addfinalizer(teardown)

    @pytest.mark.parametrize("name, keys, ops, policy_batch, policy_batch_write, exp_res, exp_rec", [
        (
            "simple-write",
            [
                ("test", "demo", 1)
            ],
            [
                op.write("count", 2),
                op.read("count")
            ],
            {},
            {},
            [AerospikeStatus.AEROSPIKE_OK],
            [{"count": 2}]
        ),
    ])
    def test_batch_operate_pos(self, name, keys, ops, policy_batch, policy_batch_write, exp_res, exp_rec):
        """
        Test batch_operate positive
        """

        res = self.as_connection.batch_operate(keys, ops, policy_batch, policy_batch_write)
        import pprint
        p = pprint.PrettyPrinter()
        print("HI")
        p.pprint(res.batch_records)
        
        for i, batch_rec in enumerate(res.batch_records):

            print(batch_rec.key)
            print(batch_rec.record)
            print(batch_rec.result)
            print(batch_rec.in_doubt)

            assert batch_rec.result == exp_res[i]
            assert batch_rec.record[2] == exp_rec[i]