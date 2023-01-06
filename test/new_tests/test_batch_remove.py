# -*- coding: utf-8 -*-


import pytest

import aerospike

from aerospike_helpers import expressions as exp
from aerospike import exception as e
from .test_base_class import TestBaseClass
from .as_status_codes import AerospikeStatus


class TestBatchRemove(TestBaseClass):
    @pytest.fixture(autouse=True)
    def setup(self, request, connection_with_config_funcs):
        as_connection = connection_with_config_funcs

        if self.server_version < [6, 0]:
            pytest.mark.xfail(reason="Servers older than 6.0 do not support batch remove.")
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

                try:
                    as_connection.remove(key)
                except e.RecordNotFound:
                    continue

        request.addfinalizer(teardown)

    @pytest.mark.parametrize(
        "name, keys, policy_batch, policy_batch_remove, exp_res, exp_rec",
        [
            ("simple-write", [("test", "demo", 0)], None, None, [AerospikeStatus.AEROSPIKE_OK], [{}]),
            ("simple-write2", [("test", "demo", 1)], {}, {}, [AerospikeStatus.AEROSPIKE_OK], [{}]),
            (
                "simple-write-policy-batch",
                [("test", "demo", 0)],
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
                [{}],
            ),
            (
                "simple-write-policy-batch-remove",
                [("test", "demo", 0)],
                {},
                {
                    "key": aerospike.POLICY_KEY_SEND,
                    "commit_level": aerospike.POLICY_COMMIT_LEVEL_MASTER,
                    "gen": aerospike.POLICY_GEN_IGNORE,
                    "durable_delete": False,
                    "generation": 2,
                    "expressions": exp.Eq(exp.IntBin("count"), 0).compile(),
                },
                [AerospikeStatus.AEROSPIKE_OK],
                [{}],
            ),
            (
                "simple-write-policy-both",
                [("test", "demo", 0)],
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
                    "durable_delete": False,
                    "generation": 2,
                    "expressions": exp.Eq(exp.IntBin("count"), 0).compile(),  # this expression takes precedence
                },
                [AerospikeStatus.AEROSPIKE_OK],
                [{}],
            ),
        ],
    )
    def test_batch_remove_pos(self, name, keys, policy_batch, policy_batch_remove, exp_res, exp_rec):
        """
        Test batch_remove positive.
        """

        res = self.as_connection.batch_remove(keys, policy_batch, policy_batch_remove)

        for i, batch_rec in enumerate(res.batch_records):
            assert batch_rec.result == exp_res[i]
            assert batch_rec.key[:3] == keys[i]  # checking key
            assert batch_rec.record[2] == exp_rec[i]  # checking bins
            assert batch_rec.record[0][:3] == keys[i]  # checking key in record

    def test_batch_remove_many_pos(self):
        """
        Test batch operate with many keys.
        """

        keys = [("test", "demo", i) for i in range(100, 1000)]

        policy_batch = {}

        policy_batch_remove = {}

        try:
            for key in keys:
                self.as_connection.put(key, {"count": 0})

            res = self.as_connection.batch_remove(keys, policy_batch, policy_batch_remove)

            for i, batch_rec in enumerate(res.batch_records):
                assert batch_rec.result == AerospikeStatus.AEROSPIKE_OK
                assert batch_rec.key[:3] == keys[i]  # checking key
                assert batch_rec.record[2] == {}  # checking bins
                assert batch_rec.record[0][:3] == keys[i]  # checking key in record
        except Exception as ex:
            raise (ex)
        finally:
            for key in keys:
                try:
                    self.as_connection.remove(key)
                except e.RecordNotFound:
                    continue

    @pytest.mark.parametrize(
        "name, keys, policy_batch, policy_batch_remove, exp_res",
        [
            ("bad-key", [("bad-key", i) for i in range(1000)], {}, {}, e.ParamError),
            ("bad-batch-policy", [("test", "demo", 1)], ["bad-batch-policy"], {}, e.ParamError),
            ("bad-batch-write-policy", [("test", "demo", 1)], {}, ["bad-batch-write-policy"], e.ParamError),
        ],
    )
    def test_batch_remove_neg(self, name, keys, policy_batch, policy_batch_remove, exp_res):
        """
        Test batch_remove negative.
        """

        with pytest.raises(exp_res):
            self.as_connection.batch_remove(keys, policy_batch, policy_batch_remove)

    def test_batch_remove_neg_connection(self):
        """
        Test batch_remove negative with bad connection.
        """

        keys = []

        exp_res = e.ClientError

        with pytest.raises(exp_res):
            bad_client = aerospike.client({"hosts": [("bad_addr", 3000)]})
            bad_client.batch_remove(keys)
