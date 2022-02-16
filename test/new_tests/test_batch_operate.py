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
from aerospike_helpers import batch_records as br
from aerospike_helpers.operations import operations as op
from aerospike_helpers.operations import list_operations as lop
from aerospike import exception as e
from .test_base_class import TestBaseClass
from .as_status_codes import AerospikeStatus


def add_udfs(client):
    '''
    Load the UDFs used in the tests
    '''
    policy = {}
    print("running")
    udf_type = 0
    udf_files = ("sample.lua", "test_record_udf.lua", "udf_basic_ops.lua")

    for module in udf_files:
        client.udf_put(module, udf_type, policy)


def remove_udfs(client):
    '''
    Remove all of the UDFS created for these tests
    '''
    policy = {}

    udf_files = ("sample.lua", "test_record_udf.lua", "udf_basic_ops.lua")

    for module in udf_files:
        client.udf_remove(module, policy)


class TestBatchOperate(TestBaseClass):

    def setup_class(cls):
        # Register setup and teardown functions
        cls.connection_setup_functions = [add_udfs]
        cls.connection_teardown_functions = [remove_udfs]

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

    @pytest.mark.parametrize("name, batch_records, policy, exp_res, exp_rec", [
        (
            "simple-write",
            br.BatchRecords(
                [
                    br.BatchWrite(
                        ("test", "demo", 1),
                        [
                            op.write("new", 10),
                            op.read("new"),
                        ],
                        policy={}
                    )
                ]
            ),
            {},
            [AerospikeStatus.AEROSPIKE_OK],
            [{"new": 10}]
        ),
        (
            "write-with-batch-policy",
            br.BatchRecords(
                [
                    br.BatchWrite(
                        ("test", "demo", 1),
                        [
                            op.write("new", 10),
                            op.read("new"),
                        ],
                        policy={}
                    )
                ]
            ),
            {
                "total_timeout": 2000,
                "max_retries": 2,
                "allow_inline_ssd": True,
                "respond_all_keys": False
            },
            [AerospikeStatus.AEROSPIKE_OK],
            [{"new": 10}]
        ),
        (
            "write-with-policy",
            br.BatchRecords(
                [
                    br.BatchWrite(
                        ("test", "demo", 1),
                        [
                            op.write("new", 10),
                            op.read("new"),
                        ],
                        policy={
                            "key": aerospike.POLICY_KEY_SEND,
                            "commit_level": aerospike.POLICY_COMMIT_LEVEL_MASTER,
                            "gen": aerospike.POLICY_GEN_IGNORE,
                            "exists": aerospike.POLICY_EXISTS_UPDATE,
                            "durable_delete": False,
                            "expressions": exp.Eq(exp.IntBin("count"), 1).compile()
                        }
                    )
                ]
            ),
            {},
            [AerospikeStatus.AEROSPIKE_OK],
            [{"new": 10}]
        ),
        (
            "simple-read",
            br.BatchRecords(
                [
                    br.BatchRead(
                        ("test", "demo", 1),
                        [
                            op.read("count"),
                        ],
                        policy={}
                    )
                ]
            ),
            {},
            [AerospikeStatus.AEROSPIKE_OK],
            [{"count": 1}]
        ),
        (
            "read-with-policy",
            br.BatchRecords(
                [
                    br.BatchWrite(
                        ("test", "demo", 1),
                        [
                            op.write("new", 10),
                            op.read("new"),
                        ],
                        policy={
                            "read_mode_ap": aerospike.POLICY_READ_MODE_AP_ONE,
                            "expressions": exp.Eq(exp.IntBin("count"), 1).compile()
                        }
                    )
                ]
            ),
            {},
            [AerospikeStatus.AEROSPIKE_OK],
            [{"new": 10}]
        ),
        (
            "simple-remove",
            br.BatchRecords(
                [
                    br.BatchRemove(
                        ("test", "demo", 1),
                        policy={}
                    ),
                    br.BatchWrite(
                        ("test", "demo", 1),
                        [
                            op.write("new", 10),
                            op.read("new"),
                        ],
                        policy={}
                    )
                ]
            ),
            {},
            [
                AerospikeStatus.AEROSPIKE_OK,
                AerospikeStatus.AEROSPIKE_OK
            ],
            [
                {},
                {"new": 10}
            ]
        ),
        (
            "remove-with-policy",
            br.BatchRecords(
                [
                    br.BatchRemove(
                        ("test", "demo", 1),
                        policy={
                            "key": aerospike.POLICY_KEY_SEND,
                            "commit_level": aerospike.POLICY_COMMIT_LEVEL_MASTER,
                            "gen": aerospike.POLICY_GEN_IGNORE,
                            "durable_delete": False,
                            "expressions": exp.Eq(exp.IntBin("count"), 1).compile()
                        }
                    ),
                    br.BatchWrite(
                        ("test", "demo", 1),
                        [
                            op.write("new", 10),
                            op.read("new"),
                        ],
                        policy={}
                    )
                ]
            ),
            {},
            [
                AerospikeStatus.AEROSPIKE_OK,
                AerospikeStatus.AEROSPIKE_OK
            ],
            [
                {},
                {"new": 10}
            ]
        ),
        (
            "simple-apply",
            br.BatchRecords(
                [
                    br.BatchApply(
                        ("test", "demo", 1),
                        "sample",
                        "list_append",
                        ["ilist_bin", 200],
                        policy={}
                    ),
                    br.BatchRead(
                        ("test", "demo", 1),
                        [
                            lop.list_get_by_rank("ilist_bin", -1, aerospike.LIST_RETURN_VALUE),
                        ],
                        policy={}
                    )
                ]
            ),
            {},
            [
                AerospikeStatus.AEROSPIKE_OK,
                AerospikeStatus.AEROSPIKE_OK
            ],
            [
                {'SUCCESS': 0},
                {"ilist_bin": 200}
            ]
        ),
        (
            "apply-with-policy",
            br.BatchRecords(
                [
                    br.BatchApply(
                        ("test", "demo", 1),
                        "sample",
                        "list_append",
                        ["ilist_bin", 200],
                        policy={                            
                            "key": aerospike.POLICY_KEY_DIGEST,
                            "commit_level": aerospike.POLICY_COMMIT_LEVEL_MASTER,
                            "ttl": aerospike.TTL_NEVER_EXPIRE,
                            "durable_delete": False,
                            "expressions": exp.Eq(exp.IntBin("count"), 1).compile()
                        }
                    ),
                    br.BatchRead(
                        ("test", "demo", 1),
                        [
                            lop.list_get_by_rank("ilist_bin", -1, aerospike.LIST_RETURN_VALUE),
                        ],
                        policy={
                            "expressions": exp.Eq(exp.TTL(), -1).compile()
                        }
                    )
                ]
            ),
            {},
            [
                AerospikeStatus.AEROSPIKE_OK,
                AerospikeStatus.AEROSPIKE_OK
            ],
            [
                {'SUCCESS': 0},
                {"ilist_bin": 200}
            ]
        ),
        (
            "write-read",
            br.BatchRecords(
                [
                    br.BatchWrite(
                        ("test", "demo", 1),
                        [
                            op.write("new", 11),
                            op.read("new")
                        ],
                        policy={}
                    ),
                    br.BatchRead(
                        ("test", "demo", 1),
                        [
                            lop.list_get_by_rank("ilist_bin", -1, aerospike.LIST_RETURN_VALUE),
                            op.read("balance")
                        ],
                        policy={}
                    )
                ]
            ),
            {},
            [
                AerospikeStatus.AEROSPIKE_OK,
                AerospikeStatus.AEROSPIKE_OK
            ],
            [
                {"new": 11},
                {"ilist_bin": 6, "balance": 100}
            ]
        ),
        (
            "complex",
            br.BatchRecords(
                [
                    br.BatchRemove(
                        ("test", "demo", 1),
                        policy={}
                    ),
                    br.BatchWrite(
                        ("test", "demo", 1),
                        [
                            op.write("ilist_bin", [2, 6]),
                            op.write("balance", 100),
                            op.read("ilist_bin"),
                            op.read("balance"),
                        ],
                        policy={}
                    ),
                    br.BatchRead(
                        ("test", "demo", 1),
                        [
                            lop.list_get_by_rank("ilist_bin", -1, aerospike.LIST_RETURN_VALUE)
                        ],
                        policy={}
                    ),
                    br.BatchApply(
                        ("test", "demo", 3),
                        "sample",
                        "list_append",
                        ["ilist_bin", 200],
                        policy={                            
                            "expressions": exp.Eq(exp.IntBin("count"), 3).compile()
                        }
                    ),
                    br.BatchRead(
                        ("test", "demo", 3),
                        [
                            lop.list_get_by_rank("ilist_bin", -1, aerospike.LIST_RETURN_VALUE),
                            op.read("balance")
                        ],
                        policy={}
                    ),
                ]
            ),
            {},
            [
                AerospikeStatus.AEROSPIKE_OK,
                AerospikeStatus.AEROSPIKE_OK,
                AerospikeStatus.AEROSPIKE_OK,
                AerospikeStatus.AEROSPIKE_OK,
                AerospikeStatus.AEROSPIKE_OK
            ],
            [
                {},
                {"balance": 100, "ilist_bin": [2, 6]},
                {"ilist_bin": 6},
                {'SUCCESS': 0},
                {"balance": 100, "ilist_bin": 200}
            ]
        ),
        (
            "read-many",
            br.BatchRecords(
                [
                    br.BatchRead(
                        ("test", "demo", i),
                        [
                            op.read("count"),
                        ],
                        policy={}
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
                AerospikeStatus.AEROSPIKE_OK
            ],
            [
                {"count": 0},
                {"count": 1},
                {"count": 2},
                {"count": 3},
                {"count": 4},
            ]
        ),
    ])
    def test_read_pos(self, name, batch_records, policy, exp_res, exp_rec):
        """
        Test batch_operate positive
        """
        res = self.as_connection.batch_operate(batch_records, policy)

        for i, batch_rec in enumerate(res.batch_records):
            assert batch_rec.result == exp_res[i]
            assert batch_rec.record[2] == exp_rec[i]

    @pytest.mark.parametrize("name, batch_records, policy, exp_res", [
        (
            "bad-batch-records",
            [],
            {},
            e.ParamError
        ),
    ])
    def test_read_neg(self, name, batch_records, policy, exp_res):
        """
        Test batch_operate positive
        """
        with pytest.raises(exp_res):
            self.as_connection.batch_operate(batch_records, policy)