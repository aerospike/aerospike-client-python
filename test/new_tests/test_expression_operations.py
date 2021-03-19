# -*- coding: utf-8 -*-

import pytest
import sys
from .test_base_class import TestBaseClass
from aerospike import exception as e
from .as_status_codes import AerospikeStatus
from aerospike_helpers import cdt_ctx
from aerospike_helpers.expressions import *
from aerospike_helpers.operations import operations
from aerospike_helpers.operations import expression_operations as expressions
from math import sqrt, ceil, floor
from aerospike import exception as e

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)


class TestExpressionsOperations(TestBaseClass):

    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        self.test_ns = 'test'
        self.test_set = 'demo'

        self.key = ('test', u'demo', 5)
        self.rec = {
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
        self.as_connection.put(self.key, self.rec)

        def teardown():
            as_connection.remove(self.key)

        request.addfinalizer(teardown)

    @pytest.mark.parametrize("expr, flags, expected", [
        (
            Let(Def("bal", IntBin("balance")),
                Var("bal")
            ),
            aerospike.EXP_READ_DEFAULT,
            {"": 100}
        ),
        (
            Let(Def("bal", IntBin("balance")),
                Cond(
                    GE(Var("bal"), 50),
                        Add(Var("bal"), 50),
                    Unknown())
            ),
            aerospike.EXP_READ_DEFAULT,
            {"": 150}
        ),
    ])
    def test_read_pos(self, expr, flags, expected):
        """
        Test expression read operation with correct parameters.
        """
        
        ops = [
            expressions.expression_read(expr.compile(), flags)
        ]
        _, _, res = self.as_connection.operate(self.key, ops)
        assert res == expected

    @pytest.mark.parametrize("expr, flags, expected", [
        (
            Let(Def("bal", IntBin("balance")),
                Cond(
                    LT(Var("bal"), 50),
                        Add(Var("bal"), 50),
                    Unknown())
            ).compile(),
            aerospike.EXP_READ_DEFAULT,
            e.OpNotApplicable # Because Unknown will be returned.
        ),
        (
            "bad_expr",
            aerospike.EXP_READ_DEFAULT,
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
            e.ParamError
        ),
    ])
    def test_read_neg(self, expr, flags, expected):
        """
        Test expression read operation expecting failure.
        """
        
        with pytest.raises(expected):
            ops = [
                expressions.expression_read(expr, flags)
            ]
            _, _, res = self.as_connection.operate(self.key, ops)

    @pytest.mark.parametrize("expr, flags, bin, expected", [
        (
            Let(Def("bal", IntBin("balance")),
                Var("bal")
            ),
            aerospike.EXP_WRITE_DEFAULT,
            "balance",
            100
        ),
        (
            Let(Def("bal", IntBin("balance")),
                Cond(
                    GE(Var("bal"), 50),
                        Add(Var("bal"), 50),
                    Unknown())
            ),
            aerospike.EXP_READ_DEFAULT,
            "balance",
            150
        ),

    ])
    def test_write_pos(self, expr, flags, bin, expected):
        """
        Test expression write operation with correct parameters.
        """
        
        ops = [
            expressions.expression_write(bin, expr.compile(), flags)
        ]
        self.as_connection.operate(self.key, ops)
        _, _, res = self.as_connection.get(self.key)
        assert res[bin] == expected

    @pytest.mark.parametrize("expr, flags, bin, expected", [
        (
            "bad_expr",
            aerospike.EXP_WRITE_DEFAULT,
            "balance",
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
            "balance",
            e.ParamError
        ),
        (
            Let(Def("bal", IntBin("balance")),
                Cond(
                    LT(Var("bal"), 50),
                        Add(Var("bal"), 50),
                    Unknown())
            ).compile(),
            aerospike.EXP_READ_DEFAULT,
            "balance",
            e.OpNotApplicable
        ),

    ])
    def test_write_neg(self, expr, flags, bin, expected):
        """
        Test expression write operation expecting failure.
        """
        
        with pytest.raises(expected):
            ops = [
                expressions.expression_write(bin, expr, flags)
            ]
            self.as_connection.operate(self.key, ops)