# -*- coding: utf-8 -*-


import pytest

import aerospike

from aerospike_helpers import expressions as exp
from aerospike import exception as e
from .test_base_class import TestBaseClass
from .as_status_codes import AerospikeStatus


def add_indexes_and_udfs(client):
    """
    Load the UDFs used in the tests and setup indexes
    """
    policy = {}
    try:
        client.index_integer_create("test", "demo", "age", "age_index", policy)
    except e.IndexFoundError:
        pass
    try:
        client.index_integer_create("test", "demo", "age1", "age_index1", policy)
    except e.IndexFoundError:
        pass

    udf_type = 0
    udf_files = ("sample.lua", "test_record_udf.lua", "udf_basic_ops.lua")

    for module in udf_files:
        client.udf_put(module, udf_type, policy)


def remove_indexes_and_udfs(client):
    """
    Remove all of the UDFS and indexes created for these tests
    """
    policy = {}

    try:
        client.index_remove("test", "age_index", policy)
    except e.IndexNotFound:
        pass

    try:
        client.index_remove("test", "age_index1", policy)
    except e.IndexNotFound:
        pass

    udf_files = ("sample.lua", "test_record_udf.lua", "udf_basic_ops.lua")

    for module in udf_files:
        client.udf_remove(module, policy)


class TestBatchApply(TestBaseClass):
    def setup_class(cls):
        # Register setup and teardown functions
        cls.connection_setup_functions = [add_indexes_and_udfs]
        cls.connection_teardown_functions = [remove_indexes_and_udfs]

    @pytest.fixture(autouse=True)
    def setup(self, request, connection_with_config_funcs):
        as_connection = connection_with_config_funcs

        if self.server_version < [6, 0]:
            pytest.mark.xfail(reason="Servers older than 6.0 do not support batch apply.")
            pytest.xfail()

        self.test_ns = "test"
        self.test_set = "demo"
        self.keys = []
        self.batch_size = 5

        for i in range(self.batch_size):
            key = ("test", "demo", i)
            rec = {
                "name": ["name%s" % (str(i))],
                "addr": "name%s" % (str(i)),
                "age": i,
                "no": i,
                "basic_map": {"k30": 6, "k20": 5, "k10": 1},
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
        "name, keys, module, function, args, policy_batch, policy_batch_apply, exp_res, exp_rec, exp_change, \
            change_bin",
        [
            (
                "list-append",
                [("test", "demo", 0), ("test", "demo", 2), ("test", "demo", 4)],
                "sample",
                "list_append",
                ["name", 1],
                None,
                None,
                [AerospikeStatus.AEROSPIKE_OK, AerospikeStatus.AEROSPIKE_OK, AerospikeStatus.AEROSPIKE_OK],
                [{"SUCCESS": 0}, {"SUCCESS": 0}, {"SUCCESS": 0}],
                [
                    ["name0", 1],
                    ["name2", 1],
                    ["name4", 1],
                ],
                "name",
            ),
            (
                "return-bool",
                [("test", "demo", 0)],
                "test_record_udf",
                "bool_check",
                [],
                {},
                {},
                [AerospikeStatus.AEROSPIKE_OK],
                [{"SUCCESS": True}],
                [],
                None,
            ),
            (
                "map-iterate",
                [("test", "demo", 0)],
                "test_record_udf",
                "map_iterate",
                ["basic_map", 555],
                {},
                {},
                [AerospikeStatus.AEROSPIKE_OK],
                [{"SUCCESS": None}],
                [{"k30": 555, "k20": 555, "k10": 555}],
                "basic_map",
            ),
            (
                "return-bool-policy-batch",
                [("test", "demo", 0)],
                "test_record_udf",
                "bool_check",
                [],
                {
                    "max_retries": 2,
                    "allow_inline_ssd": True,
                    "respond_all_keys": False,
                    "expressions": exp.Eq(
                        exp.ListGetByRank(
                            None, aerospike.LIST_RETURN_VALUE, exp.ResultType.STRING, 0, exp.ListBin("name")
                        ),
                        "name0",
                    ).compile(),
                },
                {},
                [AerospikeStatus.AEROSPIKE_OK],
                [{"SUCCESS": True}],
                [],
                None,
            ),
            (
                "return-bool-policy-batch-apply",
                [("test", "demo", 0)],
                "test_record_udf",
                "bool_check",
                [],
                {},
                {
                    "key": aerospike.POLICY_KEY_SEND,
                    "commit_level": aerospike.POLICY_COMMIT_LEVEL_MASTER,
                    "ttl": aerospike.TTL_DONT_UPDATE,
                    "durable_delete": False,
                    "expressions": exp.Eq(exp.IntBin("no"), 0).compile(),
                },
                [AerospikeStatus.AEROSPIKE_OK],
                [{"SUCCESS": True}],
                [],
                None,
            ),
        ],
    )
    def test_batch_apply_pos(
        self,
        name,
        keys,
        module,
        function,
        args,
        policy_batch,
        policy_batch_apply,
        exp_res,
        exp_rec,
        exp_change,
        change_bin,
    ):
        """
        Test batch_apply positive.
        """

        res = self.as_connection.batch_apply(keys, module, function, args, policy_batch, policy_batch_apply)

        for i, batch_rec in enumerate(res.batch_records):
            assert batch_rec.result == exp_res[i]
            assert batch_rec.key[:3] == keys[i]  # checking key
            assert batch_rec.record[2] == exp_rec[i]  # checking bins
            assert batch_rec.record[0][:3] == keys[i]  # checking key in record

            if exp_change:
                changed_rec = self.as_connection.get(keys[i])
                assert changed_rec[2][change_bin] == exp_change[i]

    def test_batch_apply_many_pos(self):
        """
        Test batch operate with many keys.
        """

        keys = [("test", "demo", i) for i in range(100, 1000)]
        module = "sample"
        func = "list_append"
        args = ["name", 10]
        policy_batch = {}
        policy_batch_apply = {}

        try:
            for i, key in enumerate(keys):
                self.as_connection.put(key, {"name": ["name" + str(i)]})

            res = self.as_connection.batch_apply(keys, module, func, args, policy_batch, policy_batch_apply)

            for i, batch_rec in enumerate(res.batch_records):
                assert batch_rec.result == AerospikeStatus.AEROSPIKE_OK
                assert batch_rec.key[:3] == keys[i]  # checking key
                assert batch_rec.record[2] == {"SUCCESS": 0}  # checking bins
                assert batch_rec.record[0][:3] == keys[i]  # checking key in record

            res = self.as_connection.get_many(keys)
            for i, rec in enumerate(res):
                assert rec[2]["name"] == ["name" + str(i), 10]

        except Exception as ex:
            raise (ex)
        finally:
            for key in keys:
                try:
                    self.as_connection.remove(key)
                except e.RecordNotFound:
                    continue

    @pytest.mark.parametrize(
        "name, keys, module, function, args, policy_batch, policy_batch_apply, exp_res",
        [
            (
                "bad-key",
                [("bad-key", i) for i in range(1000)],
                "sample",
                "list_append",
                ["name", 1],
                {},
                {},
                e.ParamError,
            ),
            ("bad-module", [("test", "demo", 1)], {"bad": "mod"}, "list_append", ["name", 1], {}, {}, e.ParamError),
            # ( NOTE These aren't raised but should be reflected in BatchRecords.result
            #     "non-existent-module",
            #     [
            #         ("test", "demo", 1)
            #     ],
            #     "fake_mod",
            #     "list_append",
            #     ["name", 1],
            #     {},
            #     {},
            #     e.AerospikeError
            # ),
            ("bad-func", [("test", "demo", 1)], "sample", {"bad": "func"}, ["name", 1], {}, {}, e.ParamError),
            # (
            #     "non-existent-func",
            #     [
            #         ("test", "demo", 1)
            #     ],
            #     "sample",
            #     "fake-func",
            #     ["name", 1],
            #     {},
            #     {},
            #     e.AerospikeError
            # ),
            ("bad-args", [("test", "demo", 1)], "sample", "list_append", {"bad": "args"}, {}, {}, e.ParamError),
            (
                "bad-batch-policy",
                [("test", "demo", 1)],
                "sample",
                "list_append",
                ["name", 1],
                ["bad-batch-policy"],
                {},
                e.ParamError,
            ),
            (
                "bad-batch-apply-policy",
                [("test", "demo", 1)],
                "sample",
                "list_append",
                ["name", 1],
                {},
                ["bad-batch-apply-policy"],
                e.ParamError,
            ),
        ],
    )
    def test_batch_apply_neg(self, name, keys, module, function, args, policy_batch, policy_batch_apply, exp_res):
        """
        Test batch_apply negative.
        """

        with pytest.raises(exp_res):
            self.as_connection.batch_apply(keys, module, function, args, policy_batch, policy_batch_apply)

    def test_batch_apply_neg_connection(self):
        """
        Test batch_apply negative with bad connection.
        """

        module = "lua_mod"
        function = "lua_func"
        args = []
        keys = []

        exp_res = e.ClientError

        with pytest.raises(exp_res):
            bad_client = aerospike.client({"hosts": [("bad_addr", 3000)]})
            bad_client.batch_apply(keys, module, function, args)
