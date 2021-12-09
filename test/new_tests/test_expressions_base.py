# -*- coding: utf-8 -*-

import pytest
import sys
from .test_base_class import TestBaseClass
from aerospike import exception as e
from .as_status_codes import AerospikeStatus
from aerospike_helpers import cdt_ctx
from aerospike_helpers.expressions import *
from aerospike_helpers.operations import map_operations
from aerospike_helpers.operations import list_operations
from aerospike_helpers.operations import hll_operations
from aerospike_helpers.operations import expression_operations as expressions
from aerospike_helpers.operations import operations
from math import sqrt, ceil, floor

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)


# Constants
_NUM_RECORDS = 8


def verify_multiple_expression_avenues(client, test_ns, test_set, expr, op_bin, expected):
    keys = [(test_ns, test_set, i) for i in range(_NUM_RECORDS + 1)]

    # operate
    ops = [
        operations.read(op_bin)
    ]
    res = []
    for key in keys:
        try:
            res.append(client.operate(key, ops, policy={'expressions': expr})[2])
        except e.FilteredOut:
            pass

    assert len(res) == expected

    # operate ordered
    ops = [
        operations.read(op_bin)
    ]
    res = []
    for key in keys:
        try:
            res.append(client.operate_ordered(key, ops, policy={'expressions': expr})[2])
        except e.FilteredOut:
            pass
    
    # batch get
    res = [rec for rec in client.get_many(keys, policy={'expressions': expr}) if rec[2]]

    assert len(res) == expected

    # scan results
    scan_obj = client.scan(test_ns, test_set)
    records = scan_obj.results({'expressions': expr})
    assert len(records) == expected


    # other scan methods
    # execute_background tested test_scan_execute_background.py 
    # foreach tested test_scan.py

    # query results
    query_obj = client.query(test_ns, test_set)
    records = query_obj.results({'expressions': expr})
    assert len(records) == expected

    # other query methods
    # execute background tested in test_query_execute_background.py 
    # foreach tested in test_query.py


class TestExpressions(TestBaseClass):

    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        self.test_ns = 'test'
        self.test_set = 'demo'

        for i in range(_NUM_RECORDS):
            key = ('test', u'demo', i)
            rec = {'name': 'name%s' % (str(i)), 't': True, 'f': False,
                    'age': i,
                    'balance': i * 10,
                    'key': i, 'alt_name': 'name%s' % (str(i)),
                    'list_bin': [
                        None,
                        i,
                        "string_test" + str(i),
                        [26, 27, 28, i],
                        {31: 31, 32: 32, 33: 33, i: i},
                        bytearray("bytearray_test" + str(i), "utf8"),
                        ("bytes_test" + str(i)).encode("utf8"),
                        i % 2 == 1,
                        aerospike.null,
                        float(i)
                    ],
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
            self.as_connection.put(key, rec)
        
        self.as_connection.put(('test', u'demo', _NUM_RECORDS), {'extra': 'record'}, policy={'key': aerospike.POLICY_KEY_SEND})
        self.as_connection.put(('test', u'demo', 'k_string'), {'test': 'data'}, policy={'key': aerospike.POLICY_KEY_SEND})
        self.as_connection.put(('test', u'demo', bytearray('b_string', 'utf-8')), {'test': 'b_data'}, policy={'key': aerospike.POLICY_KEY_SEND})

        def teardown():
            for i in range(_NUM_RECORDS):
                key = ('test', u'demo', i)
                as_connection.remove(key)
        
            as_connection.remove(('test', u'demo', _NUM_RECORDS))
            as_connection.remove(('test', u'demo', 'k_string'))
            as_connection.remove(('test', u'demo', bytearray('b_string', 'utf-8')))

        request.addfinalizer(teardown)

    @pytest.mark.xfail(reason="Will fail on storage engine device.")
    def test_device_size_pos(self):
        expr = Eq(DeviceSize(), 0)
        record = self.as_connection.get(('test', u'demo', _NUM_RECORDS), policy={'expressions': expr.compile()})
        assert(record[2]['extra'] == 'record')

    def test_TTL_pos(self):
        expr = NE(TTL(), 0)
        record = self.as_connection.get(('test', u'demo', _NUM_RECORDS), policy={'expressions': expr.compile()})
        assert(record[2]['extra'] == 'record')

    def test_void_time_pos(self):
        expr = NE(VoidTime(), 0)
        record = self.as_connection.get(('test', u'demo', _NUM_RECORDS), policy={'expressions': expr.compile()})
        assert(record[2]['extra'] == 'record')

    def test_remove_with_expressions_neg(self):
        self.as_connection.put(('test', u'demo', 25), {'test': 'test_data'})

        expr = Eq(KeyInt(), 26)
        with pytest.raises(e.FilteredOut):
            record = self.as_connection.remove(('test', u'demo', 25), policy={'expressions': expr.compile()})

    def test_scan_with_results_method_and_expressions(self):
        ns = 'test'
        st = 'demo'

        expr =  Eq(KeyInt(), _NUM_RECORDS)
        record = self.as_connection.get(('test', u'demo', _NUM_RECORDS), policy={'expressions': expr.compile()})
        assert(record[2]['extra'] == 'record')

        expr =  Eq(KeyStr(), 'k_string')
        record = self.as_connection.get(('test', u'demo', 'k_string'), policy={'expressions': expr.compile()})
        assert(record[2]['test'] == 'data')

        expr =  Eq(KeyBlob(), bytearray('b_string', 'utf-8'))
        record = self.as_connection.get(('test', u'demo', bytearray('b_string', 'utf-8')), policy={'expressions': expr.compile()})
        assert(record[2]['test'] == 'b_data')

        expr =  KeyExists()
        record = self.as_connection.get(('test', u'demo', bytearray('b_string', 'utf-8')), policy={'expressions': expr.compile()})
        assert(record[2]['test'] == 'b_data')

        expr = And(
            BinExists("age"),
            Eq(SetName(), 'demo'),
            NE(LastUpdateTime(), 0),
            NE(SinceUpdateTime(), 0),
            Not(IsTombstone()),
            Eq(DigestMod(2), 0)
        )

        scan_obj = self.as_connection.scan(ns, st)
        records = scan_obj.results({'expressions': expr.compile()})
        assert(5 == len(records))

    @pytest.mark.parametrize("bin, expected_bin_type", [
        ("ilist_bin", aerospike.AS_BYTES_LIST),
        ("age", aerospike.AS_BYTES_INTEGER),
        ("imap_bin", aerospike.AS_BYTES_MAP)
    ])
    def test_bin_type_pos(self, bin, expected_bin_type):
        """
        Invoke BinType() on various kinds of bins.
        """
        expr = Eq(BinType(bin), expected_bin_type).compile()
        verify_multiple_expression_avenues(self.as_connection, self.test_ns, self.test_set, expr, bin, _NUM_RECORDS)

    def test_predexp_and_expressions(self):
        self.as_connection.put(('test', u'demo', 25), {'test': 'test_data'})

        expr = Eq(KeyInt(), 25)
        with pytest.raises(e.ParamError):
            record = self.as_connection.remove(('test', u'demo', 25), policy={'expressions': expr.compile(), 'predexp': expr})

    def test_nested_logic_pos(self):
        """
        Test nested logical operators expression.
        """

        expr = Or(
                Or(Eq(ListBin("ilist_bin"), [1, 2, 7]), Eq(ListBin("ilist_bin"), [1, 2, 6])),
                And(LT(IntBin("age"), 22), GT(IntBin("age"), -1)),
                And(
                    Or(Eq(ListBin("ilist_bin"), [1, 2, 7]), Eq(ListBin("ilist_bin"), [1, 2, 6])),
                    And(LT(IntBin("age"), 22), GT(IntBin("age"), -1)),
                )
            ).compile()
        verify_multiple_expression_avenues(self.as_connection, self.test_ns, self.test_set, expr, "ilist_bin", _NUM_RECORDS)

    def test_bool_bin_true(self):
        if self.server_version < [5, 6]:
            pytest.mark.xfail(reason="Servers older than 5.6 do not support 6.0.0 expressions")
            pytest.xfail()

        config = TestBaseClass.get_connection_config()
        config["send_bool_as"] = aerospike.AS_BOOL
        test_client = aerospike.client(config).connect(config['user'], config['password'])

        expr = BoolBin("t")
        ops = [
            operations.write("t", True),
            expressions.expression_read("", expr.compile())
        ]
        _, _, res = test_client.operate(('test', u'demo', _NUM_RECORDS - 1), ops)
        test_client.close()
        assert res[""]

    def test_bool_bin_false(self):
        if self.server_version < [5, 6]:
            pytest.mark.xfail(reason="Servers older than 5.6 do not support 6.0.0 expressions")
            pytest.xfail()

        config = TestBaseClass.get_connection_config()
        config["send_bool_as"] = aerospike.AS_BOOL
        test_client = aerospike.client(config).connect(config['user'], config['password'])

        expr = Not(BoolBin("t"))
        ops = [
            operations.write("t", True),
            expressions.expression_read("", expr.compile())
        ]
        _, _, res = test_client.operate(('test', u'demo', _NUM_RECORDS - 1), ops)
        test_client.close()
        assert not res[""]

    def test_exclusive_pos(self):
        if self.server_version < [5, 6]:
            pytest.mark.xfail(reason="Servers older than 5.6 do not support 6.0.0 expressions")
            pytest.xfail()

        expr = Exclusive(
                GT(IntBin("age"), _NUM_RECORDS // 2),
                GT(IntBin("age"), _NUM_RECORDS - 1))
        record = self.as_connection.get(('test', u'demo', _NUM_RECORDS - 2), policy={'expressions': expr.compile()})
        assert(record[2]['age'] == _NUM_RECORDS - 2)

    def test_exclusive_neg(self):
        if self.server_version < [5, 6]:
            pytest.mark.xfail(reason="Servers older than 5.6 do not support 6.0.0 expressions")
            pytest.xfail()

        expr = Exclusive(
                GT(IntBin("age"), _NUM_RECORDS // 2),
                GT(IntBin("age"), _NUM_RECORDS // 2))
        with pytest.raises(e.FilteredOut):
            self.as_connection.get(('test', u'demo', _NUM_RECORDS - 2), policy={'expressions': expr.compile()})

    def test_let_def_var_pos(self):
        if self.server_version < [5, 6]:
            pytest.mark.xfail(reason="Servers older than 5.6 do not support 6.0.0 expressions")
            pytest.xfail()

        expr = Let(Def("a", IntBin("age")),
                Cond(
                    LT(Var("a"), 50),
                        True,
                    Unknown()))
        record = self.as_connection.get(('test', u'demo', _NUM_RECORDS - 1), policy={'expressions': expr.compile()})
        assert(record[2]['age'] == _NUM_RECORDS - 1)

    def test_let_def_var_neg(self):
        if self.server_version < [5, 6]:
            pytest.mark.xfail(reason="Servers older than 5.6 do not support 6.0.0 expressions")
            pytest.xfail()

        expr = Let(Def("a", IntBin("age")),
                Cond(
                    LT(Var("a"), 0),
                        True,
                    Unknown()))
        with pytest.raises(e.FilteredOut):
            self.as_connection.get(('test', u'demo', _NUM_RECORDS - 1), policy={'expressions': expr.compile()})

    def test_cond_pos(self):
        if self.server_version < [5, 6]:
            pytest.mark.xfail(reason="Servers older than 5.6 do not support 6.0.0 expressions")
            pytest.xfail()

        expr = Cond(
                    GE(IntBin("age"), 2),
                        True,
                    Unknown())
        record = self.as_connection.get(('test', u'demo', _NUM_RECORDS - 1), policy={'expressions': expr.compile()})
        assert(record[2]['age'] == _NUM_RECORDS - 1)

    def test_cond_neg(self):
        if self.server_version < [5, 6]:
            pytest.mark.xfail(reason="Servers older than 5.6 do not support 6.0.0 expressions")
            pytest.xfail()

        expr = Cond(
                    GT(IntBin("age"), _NUM_RECORDS),
                        True,
                    Unknown())
        with pytest.raises(e.FilteredOut):
            record = self.as_connection.get(('test', u'demo', _NUM_RECORDS - 1), policy={'expressions': expr.compile()})