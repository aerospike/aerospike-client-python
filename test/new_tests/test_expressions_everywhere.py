# -*- coding: utf-8 -*-
import sys
import random
import unittest
from datetime import datetime
import time

import pytest

from aerospike import exception as e
from aerospike_helpers import expressions as exp
from aerospike_helpers import cdt_ctx
from aerospike_helpers.operations import list_operations
from aerospike_helpers.operations import map_operations
from aerospike_helpers.operations import operations


aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)

geo_circle = aerospike.GeoJSON(
    {"type": "AeroCircle", "coordinates": [[-132.0, 37.5], 1000]}
)

geo_point = aerospike.GeoJSON(
    {"type": "Point", "coordinates": [-132.0, 37.5]}
)

geo_point2 = aerospike.GeoJSON(
    {"type": "Point", "coordinates": [-60.5, 20]}
)

class TestPredEveryWhere(object):

    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        """
        Setup Method
        """
        self.keys = []
        self.test_data = [ 
            {
                'account_id': j,
                'user_name': 'user' + str(j),
                'acct_balance': j * 10,
                'charges': [j + 5, j + 10],
                'meta': {'date': '11/4/2019'}
            } for j in range(1,5)
        ]
        self.test_data.append({'string_list': ['s1', 's2', 's3', 's4']})
        self.test_data.append({'map_bin': {'k1': 1, 'k2': 2, 'k3': 3, 'k4': 4}})

        georec = {
            'id': 1,
            'point': geo_point,
            'region': geo_circle,
            'geolist': [geo_point]
        }

        self.test_data.append(georec)
        self.test_data_bin = 'test_data'
        self.keys = [('test', 'pred_evry', i+1) for i, _ in enumerate(self.test_data)]
        # print('self keys is: ', self.keys)

        for key, data in zip(self.keys, self.test_data):
            self.as_connection.put(
                key,
                data
            )

        # cleanup
        yield

        for key in self.keys:
            try:
                self.as_connection.remove(key)
            except e.AerospikeError:
                pass

    @pytest.mark.parametrize("ops, expressions, expected_bins, expected_res, key_num", [
        (# test integer equal
            [
                operations.increment("account_id", 1)
            ],
            exp.Eq(3, exp.IntBin('account_id')),
            {
                'account_id': 4,
                'user_name': 'user3',
                'acct_balance': 30,
                'charges': [8, 13],
                'meta': {'date': '11/4/2019'}
            },
            {},
            3
        ),
        (# test string equal
            [
                operations.increment("account_id", 1)
            ],
            exp.Eq('user3', exp.StrBin('user_name')),
            {
                'account_id': 4,
                'user_name': 'user3',
                'acct_balance': 30,
                'charges': [8, 13],
                'meta': {'date': '11/4/2019'}
            },
            {},
            3
        ),
        (# test and
            [
                list_operations.list_remove_by_index_range('charges', 0, 3, aerospike.LIST_RETURN_COUNT),
                operations.increment("acct_balance", -23)
            ],
            exp.And(
                exp.GE(exp.IntBin('acct_balance'), 10),
                exp.LE(exp.IntBin('acct_balance'), 50)
            ),
            {
                'account_id': 4,
                'user_name': 'user4',
                'acct_balance': 17,
                'charges': [],
                'meta': {'date': '11/4/2019'}
            },
            {'charges': [0, 1]},
            4
        ),
        (# test or
            [
                map_operations.map_put('meta', 'lupdated', 'now')
            ],
            exp.Or(
                exp.Eq(exp.StrBin('user_name'), 'user2'),
                exp.GE(exp.IntBin('acct_balance'), 50)
            ),
            {
                'account_id': 2,
                'user_name': 'user2',
                'acct_balance': 20,
                'charges': [7, 12],
                'meta': {'date': '11/4/2019', 'lupdated': 'now'}
            },
            {'meta': 2},
            2
        ),
        (# test integer greater
            [
                map_operations.map_clear('meta')
            ],
            exp.GT(exp.IntBin('account_id'), 2),
            {
                'account_id': 3,
                'user_name': 'user3',
                'acct_balance': 30,
                'charges': [8, 13],
                'meta': {}
            },
            {'meta': None},
            3
        ),
        (# test integer greatereq
            [
                map_operations.map_clear('meta')
            ],
            exp.GE(exp.IntBin('account_id'), 2),
            {
                'account_id': 3,
                'user_name': 'user3',
                'acct_balance': 30,
                'charges': [8, 13],
                'meta': {}
            },
            {'meta': None},
            3
        ),
        (# test integer less
            [
                list_operations.list_clear('charges')
            ],
            exp.LT(exp.IntBin('account_id'), 5),
            {
                'account_id': 4,
                'user_name': 'user4',
                'acct_balance': 40,
                'charges': [],
                'meta': {'date': '11/4/2019'}
            },
            {},
            4
        ),
        (# test integer lesseq
            [
                list_operations.list_clear('charges')
            ],
            exp.LE(exp.IntBin('account_id'), 4),
            {
                'account_id': 4,
                'user_name': 'user4',
                'acct_balance': 40,
                'charges': [],
                'meta': {'date': '11/4/2019'}
            },
            {},
            4
        ),
        (# test string unequal
            [
                list_operations.list_append('charges', 2)
            ],
            exp.NE(exp.StrBin('user_name'), 'user2'),
            {
                'account_id': 4,
                'user_name': 'user4',
                'acct_balance': 40,
                'charges': [9, 14, 2],
                'meta': {'date': '11/4/2019'}
            },
            {'charges': 3},
            4
        ),
        (# test not
            [
                list_operations.list_append('charges', 2)
            ],
            exp.Not(
                exp.NE(exp.StrBin('user_name'), 'user4')),
            {
                'account_id': 4,
                'user_name': 'user4',
                'acct_balance': 40,
                'charges': [9, 14, 2],
                'meta': {'date': '11/4/2019'}
            },
            {'charges': 3},
            4
        ),
        (# test string regex
            [
                list_operations.list_append('charges', 2)
            ],
            exp.CmpRegex(aerospike.REGEX_ICASE, '.*4.*', exp.StrBin('user_name')),
            {
                'account_id': 4,
                'user_name': 'user4',
                'acct_balance': 40,
                'charges': [9, 14, 2],
                'meta': {'date': '11/4/2019'}
            },
            {'charges': 3},
            4
        ),
        (# test list or int
            [
                list_operations.list_append('charges', 2)
            ],
            exp.Eq(
                exp.ListGetByValue(None, aerospike.LIST_RETURN_COUNT, 14, 'charges'),
                1
            ),
            {
                'account_id': 4,
                'user_name': 'user4',
                'acct_balance': 40,
                'charges': [9, 14, 2],
                'meta': {'date': '11/4/2019'}
            },
            {'charges': 3},
            4
        ),
        (# test list and int
            [
                list_operations.list_append('charges', 2)
            ],
            exp.LT(
                exp.ListGetByRank(None, aerospike.LIST_RETURN_VALUE, exp.ResultType.INTEGER, -1, 'charges'),
                120
            ),
            {
                'account_id': 4,
                'user_name': 'user4',
                'acct_balance': 40,
                'charges': [9, 14, 2],
                'meta': {'date': '11/4/2019'}
            },
            {'charges': 3},
            4
        ),
        (# test list or str
            [
                list_operations.list_append('string_list', 's5')
            ],
            exp.Eq(
                exp.ListGetByValue(None, aerospike.LIST_RETURN_COUNT, 's2', 'string_list'),
                1
            ),
            {
                'string_list': ['s1', 's2', 's3', 's4', 's5']
            },
            {'string_list': 5},
            5
        ),
        # (# test list and str TODO Not sure how to write this in expressions
        #     [
        #         list_operations.list_remove_by_index_range('string_list', 0, aerospike.LIST_RETURN_VALUE, 2)
        #     ],
        #     exp.LT(
        #         exp.ListGetByRank(None, aerospike.LIST_RETURN_VALUE, exp.ResultType.INTEGER, -1, 'charges'),
        #         120
        #     ),
        #     exp.CmpRegex(aerospike.REGEX_ICASE, '.*s.*', exp.StrBin('user_name')),
        #     [
        #         exp.string_var('list_val'),
        #         exp.string_value('.*s.*'),
        #         exp.string_regex(aerospike.REGEX_ICASE),
        #         exp.list_bin('string_list'),
        #         exp.list_iterate_and('list_val')
        #     ],
        #     {
        #         'string_list': ['s3', 's4']
        #     },
        #     {'string_list': ['s1', 's2']},
        #     5
        # ),
        (# test map_key_iterate_or
            [
                map_operations.map_put('map_bin', 'k5', 5)
            ],
            exp.Eq(
                exp.MapGetByKey(None, aerospike.MAP_RETURN_COUNT, exp.ResultType.INTEGER, 'k3', 'map_bin'),
                1
            ),
            {
                'map_bin': {'k1': 1, 'k2': 2, 'k3': 3, 'k4': 4, 'k5': 5}
            },
            {'map_bin': 5},
            6
        ),
        (# test map_key_iterate_and
            [
                map_operations.map_put('map_bin', 'k5', 5)
            ],
            exp.Eq(
                exp.MapGetByKey(None, aerospike.MAP_RETURN_COUNT, exp.ResultType.INTEGER, 'k7', 'map_bin'),
                0
            ),
            {
                'map_bin': {'k1': 1, 'k2': 2, 'k3': 3, 'k4': 4, 'k5': 5}
            },
            {'map_bin': 5},
            6
        ),
        (# test mapval_iterate_and
            [
                map_operations.map_put('map_bin', 'k5', 5)
            ],
            exp.Eq(
                exp.MapGetByValue(None, aerospike.MAP_RETURN_COUNT, 7, 'map_bin'),
                0
            ),
            {
                'map_bin': {'k1': 1, 'k2': 2, 'k3': 3, 'k4': 4, 'k5': 5}
            },
            {'map_bin': 5},
            6
        ),
        (# test mapval_iterate_or
            [
                map_operations.map_get_by_key('map_bin', 'k1', aerospike.MAP_RETURN_VALUE)
            ],
            exp.Eq(
                exp.MapGetByValue(None, aerospike.MAP_RETURN_COUNT, 3, 'map_bin'),
                1
            ),
            {
                'map_bin': {'k1': 1, 'k2': 2, 'k3': 3, 'k4': 4}
            },
            {'map_bin': 1},
            6
        )
    ])
    def test_expressions_key_operate(self, ops, expressions, expected_bins, expected_res, key_num):
        """
        Invoke the C client aerospike_key_operate with expressions.
        """
        key = ('test', 'pred_evry', key_num)

        _, _, res = self.as_connection.operate(key, ops, policy={'expressions': expressions.compile()})
        assert res  == expected_res

        _, _, bins = self.as_connection.get(key)
        assert bins == expected_bins

    @pytest.mark.parametrize("ops, expressions, expected_bins, expected_res, key_num", [
    (# test mapval_iterate_or
        [
            map_operations.map_put_items('map_bin', {'k5': 5, 'k6': 6}),
            map_operations.map_get_by_key('map_bin', 'k1', aerospike.MAP_RETURN_VALUE)
        ],
        exp.LT(
            exp.MapGetByRank(None, aerospike.MAP_RETURN_VALUE, exp.ResultType.INTEGER, 0, 'map_bin'),
            3
        ),
        {
            'map_bin': {'k1': 1, 'k2': 2, 'k3': 3, 'k4': 4, 'k5': 5, 'k6': 6}
        },
        [('map_bin', 6), ('map_bin', 1)],
        6
    )])
    def test_expressions_key_operate_ordered(self, ops, expressions, expected_bins, expected_res, key_num):
        """
        Invoke the C client aerospike_key_operate with expressions using operate_ordered.
        """
        key = ('test', 'pred_evry', key_num)

        _, _, res = self.as_connection.operate_ordered(key, ops, policy={'expressions': expressions.compile()})
        assert res  == expected_res

        _, _, bins = self.as_connection.get(key)
        assert bins == expected_bins


    @pytest.mark.parametrize("ops, expressions, expected, key_num", [
    (# test mapval_iterate_or
        [
            map_operations.map_get_by_key('map_bin', 'k1', aerospike.MAP_RETURN_VALUE)
        ],
        exp.Not(
            exp.Eq(
                exp.MapGetByRank(None, aerospike.MAP_RETURN_VALUE, exp.ResultType.INTEGER, 0, 'map_bin'),
                1
            )),
        e.FilteredOut,
        6
    )])
    def test_expressions_key_operate_ordered_negative(self, ops, expressions, expected, key_num):
        """
        Invoke the C client aerospike_key_operate with expressions using operate_ordered with expected failures.
        """
        key = ('test', 'pred_evry', key_num)

        with pytest.raises(expected):
            _, _, res = self.as_connection.operate_ordered(key, ops, policy={'expressions': expressions.compile()})


    @pytest.mark.parametrize("ops, expressions, key_num, bin", [
        (# test geojson_within
            [
                operations.increment('id', 1)
            ],
            exp.CmpGeo(exp.GeoBin('point'), geo_circle),
            7,
            'point'
        ),
        (# test geojson_contains
            [
                operations.increment('id', 1)
            ],
            exp.CmpGeo(exp.GeoBin('region'), geo_point),
            7,
            'point'
        ),
    ])
    def test_expressions_key_operate_geojson(self, ops, expressions, key_num, bin):
        """
        Invoke the C client aerospike_key_operate with expressions.
        """
        key = ('test', 'pred_evry', key_num)

        _, _, _ = self.as_connection.operate(key, ops, policy={'expressions': expressions.compile()})

        _, _, bins = self.as_connection.get(key)
        assert bins['id'] == 2

    # NOTE: may fail due to clock skew
    def test_expressions_key_operate_record_last_updated(self):
        """
        Invoke the C client aerospike_key_operate with a record_last_updated expressions.
        """

        for i in range(5):
            key = 'test', 'pred_lut', i
            self.as_connection.put(key, {'time': 'earlier'})
        
        cutoff_nanos = (10 ** 9) * int(time.time() + 2)
        time.sleep(5)

        for i in range(5, 10):
            key = 'test', 'pred_lut', i
            self.as_connection.put(key, {'time': 'later'})
        
        results = []

        expr = exp.LT(exp.LastUpdateTime(), cutoff_nanos)
        ops = [
            operations.read('time')
        ]

        for i in range(10):
            try:
                key = 'test', 'pred_lut', i
                _, _, res = self.as_connection.operate(key, ops, policy={'expressions': expr.compile()})
                results.append(res)
            except:
                pass
            self.as_connection.remove(key)

        assert len(results) == 5
        for res in results:
            assert res['time'] == 'earlier'

    # NOTE: may fail due to clock skew
    def test_expressions_key_operate_record_void_time(self):
        """
        Invoke the C client aerospike_key_operate with a rec_void_time expressions.
        """

        for i in range(5):
            key = 'test', 'pred_ttl', i
            self.as_connection.put(key, {'time': 'earlier'}, meta={'ttl': 100})
        

        # 150 second range for record TTLs should be enough, we are storing with
        # Current time + 100s and current time +5000s, so only one of the group should be found
        void_time_range_start = (10 ** 9) * int(time.time() + 50)
        void_time_range_end = (10 ** 9) * int(time.time() + 150)

        for i in range(5, 10):
            key = 'test', 'pred_ttl', i
            self.as_connection.put(key, {'time': 'later'}, meta={'ttl': 1000})
        
        results = []

        expr = exp.And(
            exp.GT(exp.VoidTime(), void_time_range_start),
            exp.LT(exp.VoidTime(), void_time_range_end)
        ).compile()

        ops = [
            operations.read('time')
        ]

        for i in range(10):
            try:
                key = 'test', 'pred_ttl', i
                _, _, res = self.as_connection.operate(key, ops, policy={'expressions': expr})
                results.append(res)
            except:
                pass
            self.as_connection.remove(key)

        assert len(results) == 5
        for res in results:
            assert res['time'] == 'earlier'

    def test_expressions_key_operate_record_digest_modulo(self):
        """
        Invoke the C client aerospike_key_operate with a rec_digest_modulo expressions.
        """

        less_than_128 = 0
        for i in range(100):
            key = 'test', 'demo', i
            if aerospike.calc_digest(*key)[-1] < 128:
                less_than_128 += 1
            self.as_connection.put(key, {'dig_id': i})

        
        results = []

        expr =  exp.LT(exp.DigestMod(256), 128).compile()

        ops = [
            operations.read('dig_id')
        ]

        for i in range(100):
            try:
                key = 'test', 'demo', i
                _, _, res = self.as_connection.operate(key, ops, policy={'expressions': expr})
                results.append(res)
            except:
                pass
            self.as_connection.remove(key)

        assert len(results) == less_than_128

    @pytest.mark.parametrize("ops, expressions, expected, key_num", [
        (# filtered out
            [
                operations.increment("account_id", 1)
            ],
            exp.Eq(exp.IntBin('account_id'), 5).compile(),
            e.FilteredOut,
            3
        ),
        (# incorrect bin type
            [
                list_operations.list_remove_by_index_range('charges', 0, 3, aerospike.LIST_RETURN_COUNT),
                operations.increment("acct_balance", -23)
            ],
            exp.And(
                exp.GE(exp.StrBin('acct_balance'), 10),
                exp.LE(exp.IntBin('acct_balance'), 50)
            ).compile(),
            e.InvalidRequest,
            4
        ),
        (# filtered out
            [
                map_operations.map_put('meta', 'lupdated', 'now')
            ],
            exp.Not(
                exp.Or(
                    exp.Eq(exp.StrBin('user_name'), 'user2'),
                    exp.GE(exp.IntBin('acct_balance'), 50)
                )
            ).compile(),
            e.FilteredOut,
            2
        ),
        (# empty expressions list
            [
                map_operations.map_put('meta', 'lupdated', 'now')
            ],
            [],
            e.ParamError,
            2
        ),
        (# expressions not in list
            [
                map_operations.map_put('meta', 'lupdated', 'now')
            ],
            ('bad expressions',),
            e.ParamError,
            2
        ),
    ])
    def test_expressions_key_operate_negative(self, ops, expressions, expected, key_num):
        """
        Invoke the C client aerospike_key_operate with expressions. Expecting failures.
        """
        key = ('test', 'pred_evry', key_num)

        with pytest.raises(expected):
            self.as_connection.operate(key, ops, policy={'expressions': expressions})

    @pytest.mark.parametrize("expressions, rec_place, rec_bin, expected", [
        (
            exp.Eq(exp.IntBin('account_id'), 2),
            1,
            'account_id',
            2
        ),
        (
            exp.Eq(exp.StrBin('user_name'), 'user2'),
            1,
            'account_id',
            2
        ),
        (
            exp.Or(
                exp.Eq(exp.StrBin('user_name'), 'user2'),
                exp.GE(exp.IntBin('acct_balance'), 30)
            ),
            2,
            'account_id',
            3
        )
    ])
    def test_pos_get_many_with_expressions(self, expressions, rec_place, rec_bin, expected):
        '''
        Proper call to get_many with expressions in policy
        '''
        records = self.as_connection.get_many(self.keys, {'expressions': expressions.compile()})

        #assert isinstance(records, list)
        # assert records[2][2]['age'] == 2
        assert records[rec_place][2][rec_bin] == expected

    def test_pos_get_many_with_large_expressions(self):
        '''
        Proper call to get_many with expressions in policy.
        '''
        expr = exp.Or(
            exp.Eq(exp.IntBin('account_id'), 4),
            exp.Eq(exp.StrBin('user_name'), 'user3'),
            exp.LT(exp.ListGetByRank(None, aerospike.LIST_RETURN_VALUE, exp.ResultType.INTEGER, -1, 'charges'), 12)
        )

        matched_recs = []
        records = self.as_connection.get_many(self.keys, {'expressions': expr.compile()})
        for rec in records:
            if rec[2] is not None:
                matched_recs.append(rec[2])
        
        assert len(matched_recs) == 3
        for rec in matched_recs:
            assert rec['account_id'] == 1 or rec['account_id'] == 3 or rec['account_id'] == 4

    def test_pos_select_many_with_large_expressions(self):
        '''
        Proper call to select_many with expressions in policy.
        '''
        expr = exp.Or(
            exp.Eq(exp.IntBin('account_id'), 4),
            exp.Eq(exp.StrBin('user_name'), 'user3'),
            exp.LT(exp.ListGetByRank(None, aerospike.LIST_RETURN_VALUE, exp.ResultType.INTEGER, -1, 'charges'), 12)
        )

        matched_recs = []
        records = self.as_connection.select_many(self.keys, ['account_id'], {'expressions': expr.compile()})
        for rec in records:
            if rec[2] is not None:
                matched_recs.append(rec[2])
        
        assert len(matched_recs) == 3
        for rec in matched_recs:
            assert rec['account_id'] == 1 or rec['account_id'] == 3 or rec['account_id'] == 4

    def test_pos_remove_with_expressions(self):
        '''
        Call remove with expressions in policy.
        '''
        expr = exp.Eq(exp.IntBin('account_id'), 1)
        records = self.as_connection.remove(self.keys[0], {'expressions': expr.compile()})

        rec = self.as_connection.exists(self.keys[0])
        assert rec[1] is None

    def test_remove_with_expressions_filtered_out(self):
        '''
        Call remove with expressions in policy with expected failure.
        '''
        expr = exp.Eq(exp.IntBin('account_id'), 3)
        with pytest.raises(e.FilteredOut):
            self.as_connection.remove(self.keys[0], policy={'expressions': expr.compile()})

    def test_remove_bin_with_expressions(self):
        '''
        Call remove_bin with expressions in policy.
        '''
        expr = exp.Eq(exp.IntBin('account_id'), 1)
        self.as_connection.remove_bin(self.keys[0], ['account_id', 'user_name'], policy={'expressions': expr.compile()})

        rec = self.as_connection.get(self.keys[0])
        assert rec[2].get('account_id') is None and rec[2].get('user_name') is None

    def test_remove_bin_with_expressions_filtered_out(self):
        '''
        Call remove_bin with expressions in policy with expected failure.
        '''
        expr = exp.Eq(exp.IntBin('account_id'), 4)
        with pytest.raises(e.FilteredOut):
            self.as_connection.remove_bin(self.keys[0], ['account_id', 'user_name'], policy={'expressions': expr.compile()})

    def test_put_with_expressions(self):
        '''
        Call put with expressions in policy.
        '''
        expr = exp.Eq(exp.IntBin('account_id'), 1)
        self.as_connection.put(self.keys[0], {'newkey': 'newval'}, policy={'expressions': expr.compile()})

        rec = self.as_connection.get(self.keys[0])
        assert rec[2]['newkey'] == 'newval'

    def test_put_new_record_with_expressions(self): # should this fail?
        '''
        Call put a new record with expressions in policy.
        '''
        expr = exp.Eq(exp.IntBin('account_id'), 1)
        key = ("test", "demo", 10)
        self.as_connection.put(key, {'newkey': 'newval'}, policy={'expressions': expr.compile()})

        rec = self.as_connection.get(key)
        self.as_connection.remove(key)
        assert rec[2]['newkey'] == 'newval'

    def test_put_with_expressions_filtered_out(self):
        '''
        Call put with expressions in policy with expected failure.
        '''
        expr = exp.Eq(exp.IntBin('account_id'), 4)
        with pytest.raises(e.FilteredOut):
            self.as_connection.put(self.keys[0], {'newkey': 'newval'}, policy={'expressions': expr.compile()})

    def test_get_with_expressions(self):
        '''
        Call to get with expressions in policy.
        '''
        expr = exp.Eq(exp.IntBin('account_id'), 1)
        record = self.as_connection.get(self.keys[0], {'expressions': expr.compile()})

        assert record[2]['account_id'] == 1

    def test_get_with_expressions_filtered_out(self):
        '''
        Call to get with expressions in policy with expected failures.
        '''
        expr = exp.Eq(exp.IntBin('account_id'), 3)
        with pytest.raises(e.FilteredOut):
            self.as_connection.get(self.keys[0], {'expressions': expr.compile()})

    def test_select_with_expressions(self):
        '''
        Call to select with expressions in policy.
        '''

        expr = exp.And(
            exp.Eq(exp.IntBin('acct_balance'), 20),
            exp.LT(exp.ListGetByRank(None, aerospike.LIST_RETURN_VALUE, exp.ResultType.INTEGER, -1, 'charges'), 20)
        )

        result = self.as_connection.select(self.keys[1], ['account_id', 'acct_balance'], {'expressions': expr.compile()})
        assert result[2]['account_id'] == 2 and result[2]['acct_balance'] == 20

    def test_select_with_expressions_filtered_out(self):
        '''
        Call to select with expressions in policy with expected failures.
        '''
        expr = exp.And(
            exp.Eq(exp.IntBin('acct_balance'), 20),
            exp.LT(exp.ListGetByRank(None, aerospike.LIST_RETURN_VALUE, exp.ResultType.INTEGER, -1, 'charges'), 10)
        )

        with pytest.raises(e.FilteredOut):
            self.as_connection.select(self.keys[1], ['account_id', 'acct_balance'], {'expressions': expr.compile()})

    def test_exists_many_with_large_expressions(self):
        '''
        Proper call to exists_many with expressions in policy.
        '''

        expr = exp.Or(
            exp.Eq(exp.IntBin('account_id'), 4),
            exp.Eq(exp.StrBin('user_name'), 'user3'),
            exp.LT(exp.ListGetByRank(None, aerospike.LIST_RETURN_VALUE, exp.ResultType.INTEGER, -1, 'charges'), 12)
        )

        matched_recs = []
        records = self.as_connection.exists_many(self.keys, {'expressions': expr.compile()})
        for rec in records:
            if rec[1] is not None:
                matched_recs.append(rec[1])
        
        assert len(matched_recs) == 3
