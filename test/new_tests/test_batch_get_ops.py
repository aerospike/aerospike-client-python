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
from aerospike import exception as e
from .test_base_class import TestBaseClass


class TestBatchExpressionsOperations(TestBaseClass):

    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        # TODO this should be changed to 6.0 before release.
        if self.server_version < [5, 6]:
            pytest.mark.xfail(reason="Servers older than 5.6 do not support batch get ops.")
            pytest.xfail()
        
        self.test_ns = 'test'
        self.test_set = 'demo'
        self.keys = []
        self.rec_count = 5

        for i in range(self.rec_count):
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
            for i in range(self.rec_count):
                key = ('test', u'demo', i)
                as_connection.remove(key)

        request.addfinalizer(teardown)

    @pytest.mark.parametrize("expr, flags, name, expected", [
        (
            Let(Def("bal", IntBin("balance")),
                Var("bal")
            ),
            aerospike.EXP_READ_DEFAULT,
            "test_name",
            {"test_name": 100}
        ),
        (
            Let(Def("bal", IntBin("balance")),
                Cond(
                    GE(Var("bal"), 50),
                        Add(Var("bal"), 50),
                    Unknown())
            ),
            aerospike.EXP_READ_DEFAULT,
            "test_name2",
            {"test_name2": 150}
        ),
        (
            Let(Def("bal", IntBin("balance")),
                Def("age", IntBin("age")),
                Cond(
                    GE(Var("bal"), 50),
                        Mul(Var("bal"), Var("age")),
                    Unknown())
            ),
            aerospike.EXP_READ_DEFAULT,
            "test_mul1",
            {"test_mul1": 1000}
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

        res = self.as_connection.batch_get_ops(self.keys, ops, meta, policy)
        """
        res are in the format of (status-tuple, ((meta-dict, result-dict), status-tuple, exception), ...)
        """

        status = res[0]
        recs = res[1:]
        for i in range(self.rec_count):
            assert recs[i][0][1] == expected

    @pytest.mark.parametrize("expr, flags, name, expected", [
        (
            "bad_expr",
            aerospike.EXP_READ_DEFAULT,
            "test_name2",
            e.ParamError
        ),
        (
            Let(Def("bal", IntBin("balance")),
                Cond(
                    LT(Var("bal"), 50),
                        Add(Var("bal"), 50),
                    Unknown())
            ).compile(),
            "bad_flags",
            "test_name3",
            e.ParamError
        ),
    ])
    def test_read_neg(self, expr, flags, name, expected):
        """
        Test expression read operation expecting failure.
        """
        with pytest.raises(expected):
            ops = [
                expressions.expression_read(name, expr, flags)
            ]
            res = self.as_connection.batch_get_ops(self.keys, ops)
            # print("test_read_neg: ", res)