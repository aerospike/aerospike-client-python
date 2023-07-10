# -*- coding: utf-8 -*-


import pytest
from aerospike_helpers.operations import map_operations as mh

import aerospike

from aerospike_helpers.expressions import Add, Cond, Def, GE, IntBin, LT, Let, Mul, Unknown, Var
from aerospike_helpers.operations import expression_operations as expressions
from aerospike import exception as e
from .test_base_class import TestBaseClass


@pytest.mark.skip("client.batch_get_ops() is deprecated")
class TestBatchExpressionsOperations(TestBaseClass):
    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        # TODO this should be changed to 6.0 before release.
        if self.server_version < [5, 6]:
            pytest.mark.xfail(reason="Servers older than 5.6 do not support batch get ops.")
            pytest.xfail()

        self.test_ns = "test"
        self.test_set = "demo"
        self.keys = []
        self.rec_count = 5

        for i in range(self.rec_count):
            key = ("test", "demo", i)
            rec = {
                "name": "name10",
                "t": True,
                "age": 10,
                "balance": 100,
                "key": 10,
                "ilist_bin": [
                    1,
                    2,
                    6,
                ],
                "imap_bin": {
                    1: 1,
                    2: 2,
                    3: 6,
                },
            }
            as_connection.put(key, rec)
            self.keys.append(key)

        def teardown():
            for i in range(self.rec_count):
                key = ("test", "demo", i)
                as_connection.remove(key)

        request.addfinalizer(teardown)

    @pytest.mark.parametrize(
        "expr, flags, name, expected",
        [
            (
                Let(Def("bal", IntBin("balance")), Var("bal")),
                aerospike.EXP_READ_DEFAULT,
                "test_name",
                {"test_name": 100},
            ),
            (
                Let(Def("bal", IntBin("balance")), Cond(GE(Var("bal"), 50), Add(Var("bal"), 50), Unknown())),
                aerospike.EXP_READ_DEFAULT,
                "test_name2",
                {"test_name2": 150},
            ),
            (
                Let(
                    Def("bal", IntBin("balance")),
                    Def("age", IntBin("age")),
                    Cond(GE(Var("bal"), 50), Mul(Var("bal"), Var("age")), Unknown()),
                ),
                aerospike.EXP_READ_DEFAULT,
                "test_mul1",
                {"test_mul1": 1000},
            ),
        ],
    )
    def test_read_pos(self, expr, flags, name, expected):
        """
        Test expression read operation with correct parameters.
        """
        ops = [expressions.expression_read(name, expr.compile(), flags)]
        meta = {"gen": 1}

        res = self.as_connection.batch_get_ops(self.keys, ops, meta)
        """
        res are in the format of (status-tuple, ((meta-dict, result-dict), status-tuple, exception), ...)
        """
        # print(res)
        for i in range(self.rec_count):
            assert res[0][2] == expected

    @pytest.mark.parametrize(
        "expr, flags, name, expected",
        [
            ("bad_expr", aerospike.EXP_READ_DEFAULT, "test_name2", e.ParamError),
            (
                Let(Def("bal", IntBin("balance")), Cond(LT(Var("bal"), 50), Add(Var("bal"), 50), Unknown())).compile(),
                "bad_flags",
                "test_name3",
                e.ParamError,
            ),
        ],
    )
    def test_read_neg(self, expr, flags, name, expected):
        """
        Test expression read operation expecting failure.
        """
        with pytest.raises(expected):
            ops = [expressions.expression_read(name, expr, flags)]
            self.as_connection.batch_get_ops(self.keys, ops)
            # print("test_read_neg: ", res)

    def test_batch_result_output_format(self):
        # pp = pprint.PrettyPrinter(2, 80)
        policy = {"key": aerospike.POLICY_KEY_SEND}
        map_policy = {
            "map_write_mode": aerospike.MAP_UPDATE,
            "map_order": aerospike.MAP_KEY_ORDERED,
        }

        key1 = ("test", "demo", "batch-ops-k1")
        scores1 = {"u1": 123, "u2": 234, "u7": 789, "u8": 890, "u9": 901}
        ops = [mh.map_put_items("scores", scores1, map_policy)]
        self.as_connection.operate(key1, ops, policy=policy)
        key2 = ("test", "demo", "batch-ops-k2")
        scores2 = {"z1": 321, "z2": 432, "z7": 987, "z8": 98, "z9": 109}
        ops = [mh.map_put_items("scores", scores2, map_policy)]
        self.as_connection.operate(key2, ops, policy=policy)

        ops = [mh.map_get_by_rank_range("scores", -3, 3, aerospike.MAP_RETURN_KEY_VALUE)]
        non_existent_key = ("test", "demo", "batch-ops-non_existent_key")
        rec = self.as_connection.batch_get_ops([key1, key2, non_existent_key], ops, policy)

        # print("\nThe record from batch_get_ops")
        # pp.pprint(rec)

        assert rec[0][-1] is not None
        assert rec[1][-1] is not None
        assert rec[2][-2] == e.RecordNotFound
        assert rec[2][-1] is None

        # rec = self.as_connection.select_many([non_existent_key], ['name'])
        # print("\nFor comparison, here's batch-read (select_many) is an array of records")
        # pp.pprint(rec)

        # rec = self.as_connection.get_many([key1, key2, non_existent_key], policy)
        # print("\nFor comparison, here's batch-read (get_many) is an array of records")
        # pp.pprint(rec)

        # print("\nThe record coming from opreate")
        # rec = self.as_connection.operate(key1, ops, policy=policy)
        # pp.pprint(rec)

        self.as_connection.remove(key1)
        self.as_connection.remove(key2)
