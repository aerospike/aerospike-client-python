# -*- coding: utf-8 -*-


import pytest

import aerospike

from aerospike_helpers import expressions as exp
from aerospike_helpers.operations import operations as op
from aerospike import exception as e
from .test_base_class import TestBaseClass
from .as_status_codes import AerospikeStatus


class TestBatchOperate(TestBaseClass):
    @pytest.fixture(autouse=True)
    def setup(self, request, connection_with_config_funcs):
        as_connection = connection_with_config_funcs

        if self.server_version < [6, 0]:
            pytest.mark.xfail(reason="Servers older than 6.0 do not support batch operate.")
            pytest.xfail()

        self.test_ns = "test"
        self.test_set = "demo"
        self.keys = []
        self.batch_size = 5

        for i in range(self.batch_size):
            key = ("test", "demo", i)
            rec = {
                "count": i,
                "name": "name10",
                "t": True,
                "age": 10,
                "balance": 100,
                "key": 10,
                "ilist_bin": [
                    i,
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
            for i in range(self.batch_size):
                key = ("test", "demo", i)
                as_connection.remove(key)

        request.addfinalizer(teardown)

    @pytest.mark.parametrize(
        "name, keys, ops, policy_batch, policy_batch_write, exp_res, exp_rec",
        [
            (
                "simple-write",
                [("test", "demo", 0)],
                [op.write("count", 2), op.read("count")],
                None,
                None,
                [AerospikeStatus.AEROSPIKE_OK],
                [{"count": 2}],
            ),
            (
                "simple-write2",
                [("test", "demo", 1)],
                [op.write("count", 3), op.read("count")],
                {},
                {},
                [AerospikeStatus.AEROSPIKE_OK],
                [{"count": 3}],
            ),
            (
                "simple-write-policy-batch",
                [("test", "demo", 0)],
                [op.write("count", 7), op.read("count")],
                {
                    "max_retries": 2,
                    "allow_inline_ssd": True,
                    "respond_all_keys": False,
                    "expressions": exp.Eq(
                        exp.ListGetByRank(
                            None, aerospike.LIST_RETURN_VALUE, exp.ResultType.INTEGER, 0, exp.ListBin("ilist_bin")
                        ),
                        0,
                    ).compile(),
                },
                {},
                [AerospikeStatus.AEROSPIKE_OK],
                [{"count": 7}],
            ),
            (
                "simple-write-policy-batch-write",
                [("test", "demo", 0)],
                [op.write("count", 7), op.read("count")],
                {},
                {
                    "key": aerospike.POLICY_KEY_SEND,
                    "commit_level": aerospike.POLICY_COMMIT_LEVEL_MASTER,
                    "gen": aerospike.POLICY_GEN_IGNORE,
                    "exists": aerospike.POLICY_EXISTS_UPDATE,
                    "durable_delete": False,
                    "expressions": exp.Eq(exp.IntBin("count"), 0).compile(),
                },
                [AerospikeStatus.AEROSPIKE_OK],
                [{"count": 7}],
            ),
            (
                "simple-write-policy-batch-write-with-ttl",
                [("test", "demo", 0)],
                [
                    op.write("count", 7),
                    op.read("count")
                ],
                {},
                {
                    "ttl": 200
                },
                [AerospikeStatus.AEROSPIKE_OK],
                [{"count": 7}],
            ),
            (
                "simple-write-policy-both",
                [("test", "demo", 0)],
                [op.write("count", 7), op.read("count")],
                {
                    "max_retries": 2,
                    "allow_inline_ssd": True,
                    "respond_all_keys": False,
                    "expressions": exp.Eq(
                        exp.ListGetByRank(
                            None, aerospike.LIST_RETURN_VALUE, exp.ResultType.INTEGER, 0, exp.ListBin("ilist_bin")
                        ),
                        1,
                    ).compile(),
                },
                {
                    "key": aerospike.POLICY_KEY_SEND,
                    "commit_level": aerospike.POLICY_COMMIT_LEVEL_MASTER,
                    "gen": aerospike.POLICY_GEN_IGNORE,
                    "exists": aerospike.POLICY_EXISTS_UPDATE,
                    "durable_delete": False,
                    "expressions": exp.Eq(exp.IntBin("count"), 0).compile(),  # this expression takes precedence
                },
                [AerospikeStatus.AEROSPIKE_OK],
                [{"count": 7}],
            ),
        ],
    )
    def test_batch_operate_pos(self, name, keys, ops, policy_batch, policy_batch_write, exp_res, exp_rec):
        """
        Test batch_operate positive.
        """

        res = self.as_connection.batch_operate(keys, ops, policy_batch, policy_batch_write)

        for i, batch_rec in enumerate(res.batch_records):
            assert batch_rec.result == exp_res[i]
            assert batch_rec.record[2] == exp_rec[i]
            assert batch_rec.key[:3] == keys[i]  # checking key
            assert batch_rec.record[0][:3] == keys[i]  # checking key in record

    def test_batch_operate_many_pos(self):
        """
        Test batch operate with many keys.
        """

        keys = [("test", "demo", i) for i in range(100, 1000)]

        ops = [
            op.write("count", 10),
            op.read("count"),
        ]

        policy_batch = {}

        policy_batch_write = {}

        try:
            for key in keys:
                self.as_connection.put(key, {"count": 0})

            res = self.as_connection.batch_operate(keys, ops, policy_batch, policy_batch_write)

            for i, batch_rec in enumerate(res.batch_records):
                assert batch_rec.result == AerospikeStatus.AEROSPIKE_OK
                assert batch_rec.record[2] == {"count": 10}
                assert batch_rec.key[:3] == keys[i]  # checking key
                assert batch_rec.record[0][:3] == keys[i]  # checking key in record
        except Exception as ex:
            raise (ex)
        finally:
            for key in keys:
                self.as_connection.remove(key)

    @pytest.mark.parametrize(
        "name, keys, ops, policy_batch, policy_batch_write, exp_res",
        [
            (
                "bad-key",
                [("bad-key", i) for i in range(1000)],
                [op.write("count", 2), op.read("count")],
                {},
                {},
                e.ParamError,
            ),
            (
                "bad-ops",
                [("test", "demo", 1)],
                [op.write("count", 2), op.read("count"), ["bad-op"]],
                {
                    "respond_all_keys": False,
                },
                {},
                e.ParamError,
            ),
            (
                "bad-ops2",
                [("test", "demo", 1)],
                {"bad": "ops"},
                {
                    "respond_all_keys": False,
                },
                {},
                e.ParamError,
            ),
            (
                "bad-batch-policy",
                [("test", "demo", 1)],
                [
                    op.write("count", 2),
                    op.read("count"),
                ],
                ["bad-batch-policy"],
                {},
                e.ParamError,
            ),
            (
                "bad-batch-write-policy",
                [("test", "demo", 1)],
                [
                    op.write("count", 2),
                    op.read("count"),
                ],
                {},
                ["bad-batch-write-policy"],
                e.ParamError,
            ),
            (
                "bad-batch-write-policy-ttl",
                [("test", "demo", 1)],
                [
                    op.write("count", 2),
                ],
                {},
                {
                    # Out of bounds
                    "ttl": 2**32
                },
                e.ParamError,
            ),
        ],
    )
    def test_batch_operate_neg(self, name, keys, ops, policy_batch, policy_batch_write, exp_res):
        """
        Test batch_operate negative.
        """

        with pytest.raises(exp_res):
            self.as_connection.batch_operate(keys, ops, policy_batch, policy_batch_write)

    def test_batch_operate_neg_connection(self):
        """
        Test batch_operate negative with bad connection.
        """

        keys = []
        ops = ["doesn't_matter"]

        exp_res = e.ClientError

        with pytest.raises(exp_res):
            bad_client = aerospike.client({"hosts": [("bad_addr", 3000)]})
            bad_client.batch_operate(keys, ops)
