# -*- coding: utf-8 -*-


import pytest

import aerospike

from aerospike_helpers import expressions as exp
from aerospike_helpers.batch import records as br
from aerospike_helpers.operations import operations as op
from aerospike_helpers.operations import list_operations as lop
from aerospike import exception as e
from .test_base_class import TestBaseClass
from .as_status_codes import AerospikeStatus


def add_udfs(client):
    """
    Load the UDFs used in the tests
    """
    policy = {}
    udf_type = 0
    udf_files = ("sample.lua", "test_record_udf.lua", "udf_basic_ops.lua")

    for module in udf_files:
        client.udf_put(module, udf_type, policy)


def remove_udfs(client):
    """
    Remove all of the UDFS created for these tests
    """
    policy = {}

    udf_files = ("sample.lua", "test_record_udf.lua", "udf_basic_ops.lua")

    for module in udf_files:
        client.udf_remove(module, policy)


class TestBatchWrite(TestBaseClass):
    def setup_class(cls):
        # Register setup and teardown functions
        cls.connection_setup_functions = [add_udfs]
        cls.connection_teardown_functions = [remove_udfs]

    @pytest.fixture(autouse=True)
    def setup(self, request, connection_with_config_funcs):
        as_connection = connection_with_config_funcs

        if self.server_version < [6, 0]:
            pytest.mark.xfail(reason="Servers older than 6.0 do not support batch writes.")
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
                    # records may have been removed by out of order remove batch ops
                    pass

        request.addfinalizer(teardown)

    @pytest.mark.parametrize(
        "name, batch_records, policy, exp_res, exp_rec",
        [
            (
                "simple-write",
                br.BatchRecords([br.Write(("test", "demo", 1), [op.write("new", 10), op.read("new")])]),
                {},
                [AerospikeStatus.AEROSPIKE_OK],
                [{"new": 10}],
            ),
            (
                "write-with-batch-policy",
                br.BatchRecords(
                    [
                        br.Write(
                            ("test", "demo", 1),
                            [op.write("new", 10), op.read("new")],
                        )
                    ]
                ),
                {"max_retries": 2, "allow_inline_ssd": True, "respond_all_keys": False},
                [AerospikeStatus.AEROSPIKE_OK],
                [{"new": 10}],
            ),
            (
                "write-with-policy",
                br.BatchRecords(
                    [
                        br.Write(
                            ("test", "demo", 1),
                            [op.write("new", 10), op.read("new")],
                            meta={"gen": 1, "ttl": aerospike.TTL_NEVER_EXPIRE},
                            policy={
                                "key": aerospike.POLICY_KEY_SEND,
                                "commit_level": aerospike.POLICY_COMMIT_LEVEL_MASTER,
                                "gen": aerospike.POLICY_GEN_IGNORE,
                                "exists": aerospike.POLICY_EXISTS_UPDATE,
                                "durable_delete": False,
                                "expressions": exp.Eq(exp.IntBin("count"), 1).compile(),
                            },
                        )
                    ]
                ),
                {},
                [AerospikeStatus.AEROSPIKE_OK],
                [{"new": 10}],
            ),
            (
                "simple-read",
                br.BatchRecords(
                    [
                        br.Read(
                            ("test", "demo", 1),
                            [op.read("count")],
                        )
                    ]
                ),
                {},
                [AerospikeStatus.AEROSPIKE_OK],
                [{"count": 1}],
            ),
            (
                "read-all-bins",
                br.BatchRecords([br.Read(("test", "demo", 1), ops=None, read_all_bins=True)]),
                {},
                [AerospikeStatus.AEROSPIKE_OK],
                [
                    {
                        "count": 1,
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
                ],
            ),
            (
                "read-with-policy",
                br.BatchRecords(
                    [
                        br.Write(
                            ("test", "demo", 1),
                            [op.write("new", 10), op.read("new")],
                            meta={"gen": 1, "ttl": aerospike.TTL_NEVER_EXPIRE},
                            policy={
                                "read_mode_ap": aerospike.POLICY_READ_MODE_AP_ONE,
                                "expressions": exp.Eq(exp.IntBin("count"), 1).compile(),
                            },
                        )
                    ]
                ),
                {},
                [AerospikeStatus.AEROSPIKE_OK],
                [{"new": 10}],
            ),
            (
                "simple-remove",
                br.BatchRecords(
                    [
                        br.Remove(("test", "demo", 1)),
                        br.Write(
                            ("test", "demo", 1),
                            [op.write("new", 10), op.read("new")],
                        ),
                    ]
                ),
                {},
                [AerospikeStatus.AEROSPIKE_OK, AerospikeStatus.AEROSPIKE_OK],
                [{}, {"new": 10}],
            ),
            (
                "remove-with-policy",
                br.BatchRecords(
                    [
                        br.Remove(
                            ("test", "demo", 1),
                            policy={
                                "key": aerospike.POLICY_KEY_SEND,
                                "commit_level": aerospike.POLICY_COMMIT_LEVEL_MASTER,
                                "gen": aerospike.POLICY_GEN_IGNORE,
                                "durable_delete": False,
                                "expressions": exp.Eq(exp.IntBin("count"), 1).compile(),
                            },
                        ),
                        br.Write(("test", "demo", 1), [op.write("new", 10), op.read("new")]),
                    ]
                ),
                {},
                [AerospikeStatus.AEROSPIKE_OK, AerospikeStatus.AEROSPIKE_OK],
                [{}, {"new": 10}],
            ),
            (
                "simple-apply",
                br.BatchRecords(
                    [
                        br.Apply(("test", "demo", 1), "sample", "list_append", ["ilist_bin", 200]),
                        br.Read(
                            ("test", "demo", 1),
                            [
                                lop.list_get_by_rank("ilist_bin", 0, aerospike.LIST_RETURN_VALUE),
                            ],
                        ),
                    ]
                ),
                {},
                [AerospikeStatus.AEROSPIKE_OK, AerospikeStatus.AEROSPIKE_OK],
                [{"SUCCESS": 0}, {"ilist_bin": 1}],
            ),
            (
                "apply-with-policy",
                br.BatchRecords(
                    [
                        br.Apply(
                            ("test", "demo", 1),
                            "sample",
                            "list_append",
                            ["ilist_bin", 200],
                            policy={
                                "key": aerospike.POLICY_KEY_DIGEST,
                                "commit_level": aerospike.POLICY_COMMIT_LEVEL_MASTER,
                                "ttl": aerospike.TTL_NEVER_EXPIRE,
                                "durable_delete": False,
                                "expressions": exp.Eq(exp.IntBin("count"), 1).compile(),
                            },
                        ),
                        br.Read(
                            ("test", "demo", 1),
                            [
                                lop.list_get_by_rank("ilist_bin", 0, aerospike.LIST_RETURN_VALUE),
                            ],
                        ),
                    ]
                ),
                {},
                [AerospikeStatus.AEROSPIKE_OK, AerospikeStatus.AEROSPIKE_OK],
                [{"SUCCESS": 0}, {"ilist_bin": 1}],
            ),
            (
                "write-read",
                br.BatchRecords(
                    [
                        br.Write(("test", "demo", 1), [op.write("new", 11), op.read("new")]),
                        br.Read(
                            ("test", "demo", 1),
                            [lop.list_get_by_rank("ilist_bin", -1, aerospike.LIST_RETURN_VALUE), op.read("balance")],
                        ),
                    ]
                ),
                {},
                [AerospikeStatus.AEROSPIKE_OK, AerospikeStatus.AEROSPIKE_OK],
                [{"new": 11}, {"ilist_bin": 6, "balance": 100}],
            ),
            (
                "complex",
                br.BatchRecords(
                    [
                        br.Write(
                            ("test", "demo", 1),
                            [
                                op.write("ilist_bin", [2, 6]),
                                op.write("balance", 100),
                                op.read("ilist_bin"),
                                op.read("balance"),
                            ],
                        ),
                        br.Read(
                            ("test", "demo", 1), [lop.list_get_by_rank("ilist_bin", -1, aerospike.LIST_RETURN_VALUE)]
                        ),
                        br.Apply(
                            ("test", "demo", 3),
                            "sample",
                            "list_append",
                            ["ilist_bin", 200],
                            policy={"expressions": exp.Eq(exp.IntBin("count"), 3).compile()},
                        ),
                        br.Read(
                            ("test", "demo", 3),
                            [lop.list_get_by_rank("ilist_bin", 0, aerospike.LIST_RETURN_VALUE), op.read("balance")],
                        ),
                    ]
                ),
                {},
                [
                    AerospikeStatus.AEROSPIKE_OK,
                    AerospikeStatus.AEROSPIKE_OK,
                    AerospikeStatus.AEROSPIKE_OK,
                    AerospikeStatus.AEROSPIKE_OK,
                    AerospikeStatus.AEROSPIKE_OK,
                ],
                [
                    {"balance": 100, "ilist_bin": [2, 6]},
                    {"ilist_bin": 6},
                    {"SUCCESS": 0},
                    {"balance": 100, "ilist_bin": 1},
                ],
            ),
            (
                "read-many",
                br.BatchRecords(
                    [
                        br.Read(
                            ("test", "demo", i),
                            [
                                op.read("count"),
                            ],
                        )
                        for i in range(5)
                    ]
                ),
                {},
                [
                    AerospikeStatus.AEROSPIKE_OK,
                    AerospikeStatus.AEROSPIKE_OK,
                    AerospikeStatus.AEROSPIKE_OK,
                    AerospikeStatus.AEROSPIKE_OK,
                    AerospikeStatus.AEROSPIKE_OK,
                ],
                [
                    {"count": 0},
                    {"count": 1},
                    {"count": 2},
                    {"count": 3},
                    {"count": 4},
                ],
            ),
        ],
    )
    def test_batch_write_pos(self, name, batch_records, policy, exp_res, exp_rec):
        """
        Test batch_write positive
        """
        res = self.as_connection.batch_write(batch_records, policy)

        for i, batch_rec in enumerate(res.batch_records):
            # print("name:", name)
            assert batch_rec.result == exp_res[i]
            assert batch_rec.record[2] == exp_rec[i]

    @pytest.mark.parametrize(
        "name, batch_records, policy, exp_res",
        [
            ("bad-batch-records", ["bad", "batch", "records"], {}, e.ParamError),
            (
                "bad-batch-record",
                br.BatchRecords(
                    [
                        br.Read(
                            ("test", "demo", 1),
                            [
                                op.read("count"),
                            ],
                        ),
                        "bad_batch_record",
                    ]
                ),
                {},
                e.ParamError,
            ),
            (
                "bad-batch-record-key",
                br.BatchRecords(
                    [
                        br.Read(
                            "bad_key",
                            [
                                op.read("count"),
                            ],
                        )
                    ]
                ),
                {},
                e.ParamError,
            ),
            ("bad-batch-record-ops", br.BatchRecords([br.Read(("test", "demo", 1), {"bad": "ops"})]), {}, e.ParamError),
            (
                "bad-batch-record-policy",
                br.BatchRecords(
                    [
                        br.Read(
                            ("test", "demo", 1),
                            [
                                op.read("count"),
                            ],
                            meta=None,
                            policy="bad policy",
                        )
                    ]
                ),
                {},
                e.ParamError,
            ),
            (
                "bad-batch-policy",
                br.BatchRecords(
                    [
                        br.Read(
                            ("test", "demo", 1),
                            [
                                op.read("count"),
                            ],
                        )
                    ]
                ),
                "bad policy",
                e.ParamError,
            ),
        ],
    )
    def test_batch_write_neg(self, name, batch_records, policy, exp_res):
        """
        Test batch_write positive
        """

        with pytest.raises(exp_res):
            self.as_connection.batch_write(batch_records, policy)

    def test_batch_write_neg_connection(self):
        """
        Test batch_write negative with bad connection.
        """

        batch_records = []

        exp_res = e.ClientError

        with pytest.raises(exp_res):
            bad_client = aerospike.client({"hosts": [("bad_addr", 3000)]})
            bad_client.batch_write(batch_records)

    def test_batch_read_invalid_policy_value(self):
        batch_records = br.BatchRecords(
            [
                br.Read(
                    key=("test", "demo", 1),
                    ops=[
                        op.read("count"),
                    ],
                    policy={
                        "read_touch_ttl_percent": "fail"
                    }
                )
            ]
        )
        with pytest.raises(e.ParamError) as excinfo:
            self.as_connection.batch_write(batch_records)
        assert excinfo.value.msg == "batch_type: Read, failed to convert policy"

    @pytest.mark.parametrize(
        "policy_name, batch_record",
        [
            (
                "batch_write",
                br.Write(
                    key=("test", "demo", 0),
                    ops=[
                        op.write(bin_name="a", write_item=1)
                    ]
                )
            ),
            (
                "batch_apply",
                br.Apply(
                    key=("test", "demo", 0),
                    module="sample",
                    function="list_append",
                    args=["ilist_bin", 200]
                ),
            )
        ]
    )
    def test_global_batch_write_and_apply_policies(self, policy_name: str, batch_record: br.BatchRecord):
        config = TestBaseClass.get_connection_config()
        config["policies"][policy_name] = {
            "key": aerospike.POLICY_KEY_SEND
        }
        c = aerospike.client(config)
        batch_records = br.BatchRecords(
            batch_records=[
                batch_record
            ]
        )
        c.batch_write(batch_records)

        query = self.as_connection.query(self.test_ns, self.test_set)
        records = query.results()
        # Only the record with primary key 0 should have its PK returned from the query
        # The other records should not have a PK returned from the server
        for record_tuple in records:
            key_tuple = record_tuple[0]
            pk = key_tuple[2]
            # Use count bin to identify record's PK
            bins = record_tuple[2]
            if bins["count"] == 0:
                # Get key tuple with PK 0
                expected_key_tuple = self.keys[0]
                assert pk == expected_key_tuple[2]
            else:
                assert pk is None

    def test_global_batch_remove_policy(self):
        config = TestBaseClass.get_connection_config()
        config["policies"]["batch_remove"] = {
            # Record's real generation should be 1 since it was just written before this test case
            "gen": aerospike.POLICY_GEN_EQ,
            "generation": 42
        }
        c = aerospike.client(config)
        batch_records = br.BatchRecords(
            batch_records=[
                br.Remove(
                    key=("test", "demo", 0),
                )
            ]
        )
        brs = c.batch_write(batch_records)
        assert brs.result == AerospikeStatus.AEROSPIKE_BATCH_FAILED
        assert brs.batch_records[0].result == AerospikeStatus.AEROSPIKE_ERR_RECORD_GENERATION
