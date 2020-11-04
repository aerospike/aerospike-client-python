# -*- coding: utf-8 -*-
import sys
import random
import unittest
from datetime import datetime
import time

import pytest

from aerospike import exception as e
#from aerospike import predexp as as_predexp
from aerospike_helpers import predexp as exp
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

    @pytest.mark.parametrize("ops, predexp, expected_bins, expected_res, key_num", [
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
            exp.Eq('user3', exp.StrBin('user-name')),
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
        # (# test integer greater
        #     [
        #         map_operations.map_clear('meta')
        #     ],
        #     exp.G
        #     [
        #         exp.integer_bin('account_id'),
        #         exp.integer_value(2),
        #         exp.integer_greater()
        #     ],
        #     {
        #         'account_id': 3,
        #         'user_name': 'user3',
        #         'acct_balance': 30,
        #         'charges': [8, 13],
        #         'meta': {}
        #     },
        #     {'meta': None},
        #     3
        # ),
        # (# test integer greatereq
        #     [
        #         map_operations.map_clear('meta')
        #     ],
        #     [
        #         exp.integer_bin('account_id'),
        #         exp.integer_value(2),
        #         exp.integer_greatereq()
        #     ],
        #     {
        #         'account_id': 3,
        #         'user_name': 'user3',
        #         'acct_balance': 30,
        #         'charges': [8, 13],
        #         'meta': {}
        #     },
        #     {'meta': None},
        #     3
        # ),
        # (# test integer less
        #     [
        #         list_operations.list_clear('charges')
        #     ],
        #     [
        #         exp.integer_bin('account_id'),
        #         exp.integer_value(5),
        #         exp.integer_less()
        #     ],
        #     {
        #         'account_id': 4,
        #         'user_name': 'user4',
        #         'acct_balance': 40,
        #         'charges': [],
        #         'meta': {'date': '11/4/2019'}
        #     },
        #     {},
        #     4
        # ),
        # (# test integer lesseq
        #     [
        #         list_operations.list_clear('charges')
        #     ],
        #     [
        #         exp.integer_bin('account_id'),
        #         exp.integer_value(4),
        #         exp.integer_lesseq()
        #     ],
        #     {
        #         'account_id': 4,
        #         'user_name': 'user4',
        #         'acct_balance': 40,
        #         'charges': [],
        #         'meta': {'date': '11/4/2019'}
        #     },
        #     {},
        #     4
        # ),
        # (# test string unequal
        #     [
        #         list_operations.list_append('charges', 2)
        #     ],
        #     [
        #         exp.string_bin('user_name'),
        #         exp.string_value('user2'),
        #         exp.string_unequal()
        #     ],
        #     {
        #         'account_id': 4,
        #         'user_name': 'user4',
        #         'acct_balance': 40,
        #         'charges': [9, 14, 2],
        #         'meta': {'date': '11/4/2019'}
        #     },
        #     {'charges': 3},
        #     4
        # ),
        # (# test not
        #     [
        #         list_operations.list_append('charges', 2)
        #     ],
        #     [
        #         exp.string_bin('user_name'),
        #         exp.string_value('user4'),
        #         exp.string_unequal(),
        #         exp.predexp_not()
        #     ],
        #     {
        #         'account_id': 4,
        #         'user_name': 'user4',
        #         'acct_balance': 40,
        #         'charges': [9, 14, 2],
        #         'meta': {'date': '11/4/2019'}
        #     },
        #     {'charges': 3},
        #     4
        # ),
        # (# test string regex
        #     [
        #         list_operations.list_append('charges', 2)
        #     ],
        #     [
        #         exp.string_bin('user_name'),
        #         exp.string_value('.*4.*'),
        #         exp.string_regex(aerospike.REGEX_ICASE)
        #     ],
        #     {
        #         'account_id': 4,
        #         'user_name': 'user4',
        #         'acct_balance': 40,
        #         'charges': [9, 14, 2],
        #         'meta': {'date': '11/4/2019'}
        #     },
        #     {'charges': 3},
        #     4
        # ),
        # (# test list or int
        #     [
        #         list_operations.list_append('charges', 2)
        #     ],
        #     [
        #         exp.integer_var('list_val'),
        #         exp.integer_value(14),
        #         exp.integer_equal(),
        #         exp.list_bin('charges'),
        #         exp.list_iterate_or('list_val')
        #     ],
        #     {
        #         'account_id': 4,
        #         'user_name': 'user4',
        #         'acct_balance': 40,
        #         'charges': [9, 14, 2],
        #         'meta': {'date': '11/4/2019'}
        #     },
        #     {'charges': 3},
        #     4
        # ),
        # (# test list and int
        #     [
        #         list_operations.list_append('charges', 2)
        #     ],
        #     [
        #         exp.integer_var('list_val'),
        #         exp.integer_value(120),
        #         exp.integer_less(),
        #         exp.list_bin('charges'),
        #         exp.list_iterate_or('list_val')
        #     ],
        #     {
        #         'account_id': 4,
        #         'user_name': 'user4',
        #         'acct_balance': 40,
        #         'charges': [9, 14, 2],
        #         'meta': {'date': '11/4/2019'}
        #     },
        #     {'charges': 3},
        #     4
        # ),
        # (# test list or str
        #     [
        #         list_operations.list_append('string_list', 's5')
        #     ],
        #     [
        #         exp.string_var('list_val'),
        #         exp.string_value('s2'),
        #         exp.string_equal(),
        #         exp.list_bin('string_list'),
        #         exp.list_iterate_or('list_val')
        #     ],
        #     {
        #         'string_list': ['s1', 's2', 's3', 's4', 's5']
        #     },
        #     {'string_list': 5},
        #     5
        # ),
        # (# test list and str
        #     [
        #         list_operations.list_remove_by_index_range('string_list', 0, aerospike.LIST_RETURN_VALUE, 2)
        #     ],
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
        # (# test map_key_iterate_or
        #     [
        #         map_operations.map_put('map_bin', 'k5', 5)
        #     ],
        #     [
        #         exp.string_var('map_key'),
        #         exp.string_value('k3'),
        #         exp.string_equal(),
        #         exp.map_bin('map_bin'),
        #         exp.mapkey_iterate_or('map_key')
        #     ],
        #     {
        #         'map_bin': {'k1': 1, 'k2': 2, 'k3': 3, 'k4': 4, 'k5': 5}
        #     },
        #     {'map_bin': 5},
        #     6
        # ),
        # (# test map_key_iterate_and
        #     [
        #         map_operations.map_put('map_bin', 'k5', 5)
        #     ],
        #     [
        #         exp.string_var('map_key'),
        #         exp.string_value('k7'),
        #         exp.string_unequal(),
        #         exp.map_bin('map_bin'),
        #         exp.mapkey_iterate_and('map_key')
        #     ],
        #     {
        #         'map_bin': {'k1': 1, 'k2': 2, 'k3': 3, 'k4': 4, 'k5': 5}
        #     },
        #     {'map_bin': 5},
        #     6
        # ),
        # (# test mapkey_iterate_and
        #     [
        #         map_operations.map_put('map_bin', 'k5', 5)
        #     ],
        #     [
        #         exp.string_var('map_key'),
        #         exp.string_value('k7'),
        #         exp.string_unequal(),
        #         exp.map_bin('map_bin'),
        #         exp.mapkey_iterate_and('map_key')
        #     ],
        #     {
        #         'map_bin': {'k1': 1, 'k2': 2, 'k3': 3, 'k4': 4, 'k5': 5}
        #     },
        #     {'map_bin': 5},
        #     6
        # ),
        # (# test mapval_iterate_and
        #     [
        #         map_operations.map_put('map_bin', 'k5', 5)
        #     ],
        #     [
        #         exp.integer_var('map_val'),
        #         exp.integer_value(7),
        #         exp.integer_unequal(),
        #         exp.map_bin('map_bin'),
        #         exp.mapval_iterate_and('map_val')
        #     ],
        #     {
        #         'map_bin': {'k1': 1, 'k2': 2, 'k3': 3, 'k4': 4, 'k5': 5}
        #     },
        #     {'map_bin': 5},
        #     6
        # ),
        # (# test mapval_iterate_or
        #     [
        #         map_operations.map_get_by_key('map_bin', 'k1', aerospike.MAP_RETURN_VALUE)
        #     ],
        #     [
        #         exp.integer_var('map_val'),
        #         exp.integer_value(3),
        #         exp.integer_less(),
        #         exp.map_bin('map_bin'),
        #         exp.mapval_iterate_or('map_val')
        #     ],
        #     {
        #         'map_bin': {'k1': 1, 'k2': 2, 'k3': 3, 'k4': 4}
        #     },
        #     {'map_bin': 1},
        #     6
        # )
    ])
    def test_predexp_key_operate(self, ops, predexp, expected_bins, expected_res, key_num):
        """
        Invoke the C client aerospike_key_operate with predexp.
        """
        key = ('test', 'pred_evry', key_num)

        _, _, res = self.as_connection.operate(key, ops, policy={'expresions': predexp.compile()})
        assert res  == expected_res

        _, _, bins = self.as_connection.get(key)
        assert bins == expected_bins

    # @pytest.mark.parametrize("ops, predexp, expected_bins, expected_res, key_num", [
    # (# test mapval_iterate_or
    #     [
    #         map_operations.map_put_items('map_bin', {'k5': 5, 'k6': 6}),
    #         map_operations.map_get_by_key('map_bin', 'k1', aerospike.MAP_RETURN_VALUE)
    #     ],
    #     [
    #         exp.integer_var('map_val'),
    #         exp.integer_value(3),
    #         exp.integer_less(),
    #         exp.map_bin('map_bin'),
    #         exp.mapval_iterate_or('map_val')
    #     ],
    #     {
    #         'map_bin': {'k1': 1, 'k2': 2, 'k3': 3, 'k4': 4, 'k5': 5, 'k6': 6}
    #     },
    #     [('map_bin', 6), ('map_bin', 1)],
    #     6
    # )])
    # def test_predexp_key_operate_ordered(self, ops, predexp, expected_bins, expected_res, key_num):
    #     """
    #     Invoke the C client aerospike_key_operate with predexp using operate_ordered.
    #     """
    #     key = ('test', 'pred_evry', key_num)

    #     _, _, res = self.as_connection.operate_ordered(key, ops, policy={'predexp': predexp})
    #     assert res  == expected_res

    #     _, _, bins = self.as_connection.get(key)
    #     assert bins == expected_bins


    # @pytest.mark.parametrize("ops, predexp, expected, key_num", [
    # (# test mapval_iterate_or
    #     [
    #         map_operations.map_get_by_key('map_bin', 'k1', aerospike.MAP_RETURN_VALUE)
    #     ],
    #     [
    #         exp.integer_var('map_val'),
    #         exp.integer_value(3),
    #         exp.integer_less(),
    #         exp.map_bin('map_bin'),
    #         exp.mapval_iterate_or('map_val'),
    #         exp.predexp_not()
    #     ],
    #     e.FilteredOut,
    #     6
    # )])
    # def test_predexp_key_operate_ordered_negative(self, ops, predexp, expected, key_num):
    #     """
    #     Invoke the C client aerospike_key_operate with predexp using operate_ordered with expected failures.
    #     """
    #     key = ('test', 'pred_evry', key_num)

    #     with pytest.raises(expected):
    #         _, _, res = self.as_connection.operate_ordered(key, ops, policy={'predexp': predexp})


    # @pytest.mark.parametrize("ops, predexp, key_num, bin", [
    #     (# test geojson_within
    #         [
    #             operations.increment('id', 1)
    #         ],
    #         [
    #             exp.geojson_bin('point'),
    #             exp.geojson_value(geo_circle.dumps()),
    #             exp.geojson_within()
    #         ],
    #         7,
    #         'point'
    #     ),
    #     (# test geojson_contains
    #         [
    #             operations.increment('id', 1)
    #         ],
    #         [
    #             exp.geojson_bin('region'),
    #             exp.geojson_value(geo_point.dumps()),
    #             exp.geojson_contains()
    #         ],
    #         7,
    #         'point'
    #     ),
    # ])
    # def test_predexp_key_operate_geojson(self, ops, predexp, key_num, bin):
    #     """
    #     Invoke the C client aerospike_key_operate with predexp.
    #     """
    #     key = ('test', 'pred_evry', key_num)

    #     _, _, _ = self.as_connection.operate(key, ops, policy={'predexp': predexp})

    #     _, _, bins = self.as_connection.get(key)
    #     assert bins['id'] == 2

    # # NOTE: may fail due to clock skew
    # def test_predexp_key_operate_record_last_updated(self):
    #     """
    #     Invoke the C client aerospike_key_operate with a record_last_updated predexp.
    #     """

    #     for i in range(5):
    #         key = 'test', 'pred_lut', i
    #         self.as_connection.put(key, {'time': 'earlier'})
        
    #     cutoff_nanos = (10 ** 9) * int(time.time() + 2)
    #     time.sleep(5)

    #     for i in range(5, 10):
    #         key = 'test', 'pred_lut', i
    #         self.as_connection.put(key, {'time': 'later'})
        
    #     results = []

    #     predexp = [
    #         exp.rec_last_update(),
    #         exp.integer_value(cutoff_nanos),
    #         exp.integer_less()
    #     ]

    #     ops = [
    #         operations.read('time')
    #     ]

    #     for i in range(10):
    #         try:
    #             key = 'test', 'pred_lut', i
    #             _, _, res = self.as_connection.operate(key, ops, policy={'predexp': predexp})
    #             results.append(res)
    #         except:
    #             pass
    #         self.as_connection.remove(key)

    #     assert len(results) == 5
    #     for res in results:
    #         assert res['time'] == 'earlier'

    # # NOTE: may fail due to clock skew
    # def test_predexp_key_operate_record_void_time(self):
    #     """
    #     Invoke the C client aerospike_key_operate with a rec_void_time predexp.
    #     """

    #     for i in range(5):
    #         key = 'test', 'pred_ttl', i
    #         self.as_connection.put(key, {'time': 'earlier'}, meta={'ttl': 100})
        

    #     # 150 second range for record TTLs should be enough, we are storing with
    #     # Current time + 100s and current time +5000s, so only one of the group should be found
    #     void_time_range_start = (10 ** 9) * int(time.time() + 50)
    #     void_time_range_end = (10 ** 9) * int(time.time() + 150)

    #     for i in range(5, 10):
    #         key = 'test', 'pred_ttl', i
    #         self.as_connection.put(key, {'time': 'later'}, meta={'ttl': 1000})
        
    #     results = []

    #     predexp = [
    #         exp.rec_void_time(),
    #         exp.integer_value(void_time_range_start),
    #         exp.integer_greater(),

    #         exp.rec_void_time(),
    #         exp.integer_value(void_time_range_end),
    #         exp.integer_less(),

    #         exp.predexp_and(2)
    #     ]

    #     ops = [
    #         operations.read('time')
    #     ]

    #     for i in range(10):
    #         try:
    #             key = 'test', 'pred_ttl', i
    #             _, _, res = self.as_connection.operate(key, ops, policy={'predexp': predexp})
    #             results.append(res)
    #         except:
    #             pass
    #         self.as_connection.remove(key)

    #     assert len(results) == 5
    #     for res in results:
    #         assert res['time'] == 'earlier'

    # def test_predexp_key_operate_record_digest_modulo(self):
    #     """
    #     Invoke the C client aerospike_key_operate with a rec_digest_modulo predexp.
    #     """

    #     less_than_128 = 0
    #     for i in range(100):
    #         key = 'test', 'demo', i
    #         if aerospike.calc_digest(*key)[-1] < 128:
    #             less_than_128 += 1
    #         self.as_connection.put(key, {'dig_id': i})

        
    #     results = []

    #     predexp = [
    #         exp.rec_digest_modulo(256),
    #         exp.integer_value(128),
    #         exp.integer_less()
    #     ]

    #     ops = [
    #         operations.read('dig_id')
    #     ]

    #     for i in range(100):
    #         try:
    #             key = 'test', 'demo', i
    #             _, _, res = self.as_connection.operate(key, ops, policy={'predexp': predexp})
    #             results.append(res)
    #         except:
    #             pass
    #         self.as_connection.remove(key)

    #     assert len(results) == less_than_128

    # @pytest.mark.parametrize("ops, predexp, expected, key_num", [
    #     (# filtered out
    #         [
    #             operations.increment("account_id", 1)
    #         ],
    #         [
    #             exp.integer_bin('account_id'),
    #             exp.integer_value(5),
    #             exp.integer_equal()
    #         ],
    #         e.FilteredOut,
    #         3
    #     ),
    #     (# incorrect bin type
    #         [
    #             list_operations.list_remove_by_index_range('charges', 0, 3, aerospike.LIST_RETURN_COUNT),
    #             operations.increment("acct_balance", -23)
    #         ],
    #         [
    #             exp.integer_bin('acct_balance'),
    #             exp.string_value(10), #incorrect bin type
    #             exp.integer_greatereq(),
    #             exp.integer_bin('acct_balance'),
    #             exp.integer_value(50),
    #             exp.integer_lesseq(),
    #             exp.predexp_and(2)
    #         ],
    #         e.ParamError,
    #         4
    #     ),
    #     (# filtered out
    #         [
    #             map_operations.map_put('meta', 'lupdated', 'now')
    #         ],
    #         [
    #             exp.string_bin('user_name'),
    #             exp.string_value('user2'),
    #             exp.string_equal(),
    #             exp.integer_bin('acct_balance'),
    #             exp.integer_value(50),
    #             exp.integer_greatereq(),
    #             exp.predexp_or(2),
    #             exp.predexp_not()
    #         ],
    #         e.FilteredOut,
    #         2
    #     ),
    #     (# empty predexp list
    #         [
    #             map_operations.map_put('meta', 'lupdated', 'now')
    #         ],
    #         [],
    #         e.InvalidRequest,
    #         2
    #     ),
    #     (# predexp not in list
    #         [
    #             map_operations.map_put('meta', 'lupdated', 'now')
    #         ],
    #         'bad predexp',
    #         e.ParamError,
    #         2
    #     ),
    # ])
    # def test_predexp_key_operate_negative(self, ops, predexp, expected, key_num):
    #     """
    #     Invoke the C client aerospike_key_operate with predexp. Expecting failures.
    #     """
    #     key = ('test', 'pred_evry', key_num)

    #     with pytest.raises(expected):
    #         self.as_connection.operate(key, ops, policy={'predexp': predexp})

    # @pytest.mark.parametrize("predexp, rec_place, rec_bin, expected", [
    #     (
    #         [
    #             exp.integer_bin('account_id'),
    #             exp.integer_value(2),
    #             exp.integer_equal()
    #         ],
    #         1,
    #         'account_id',
    #         2
    #     ),
    #     (
    #         [
    #             exp.string_bin('user_name'),
    #             exp.string_value('user2'),
    #             exp.string_equal(),
    #         ],
    #         1,
    #         'account_id',
    #         2
    #     ),
    #     (
    #         [
    #             exp.string_bin('user_name'),
    #             exp.string_value('user2'),
    #             exp.string_equal(),
    #             exp.integer_bin('acct_balance'),
    #             exp.integer_value(30),
    #             exp.integer_greatereq(),
    #             exp.predexp_or(2)
    #         ],
    #         2,
    #         'account_id',
    #         3
    #     )
    # ])
    # def test_pos_get_many_with_predexp(self, predexp, rec_place, rec_bin, expected):
    #     '''
    #     Proper call to get_many with predexp in policy
    #     '''
    #     records = self.as_connection.get_many(self.keys, {'predexp': predexp})

    #     #assert isinstance(records, list)
    #     # assert records[2][2]['age'] == 2
    #     assert records[rec_place][2][rec_bin] == expected

    # def test_pos_get_many_with_large_predexp(self):
    #     '''
    #     Proper call to get_many with predexp in policy.
    #     '''
    #     predexp = [
    #         exp.integer_bin('account_id'),
    #         exp.integer_value(4),
    #         exp.integer_equal(),
    #         exp.string_bin('user_name'),
    #         exp.string_value('user3'),
    #         exp.string_equal(),
    #         exp.integer_var('list_val'),
    #         exp.integer_value(12),
    #         exp.integer_less(),
    #         exp.list_bin('charges'),
    #         exp.list_iterate_and('list_val'),
    #         exp.predexp_or(3)
    #     ]

    #     matched_recs = []
    #     records = self.as_connection.get_many(self.keys, {'predexp': predexp})
    #     for rec in records:
    #         if rec[2] is not None:
    #             matched_recs.append(rec[2])
        
    #     assert len(matched_recs) == 3
    #     for rec in matched_recs:
    #         assert rec['account_id'] == 1 or rec['account_id'] == 3 or rec['account_id'] == 4

    # def test_pos_select_many_with_large_predexp(self):
    #     '''
    #     Proper call to select_many with predexp in policy.
    #     '''
    #     predexp = [
    #         exp.integer_bin('account_id'),
    #         exp.integer_value(4),
    #         exp.integer_equal(),
    #         exp.string_bin('user_name'),
    #         exp.string_value('user3'),
    #         exp.string_equal(),
    #         exp.integer_var('list_val'),
    #         exp.integer_value(12),
    #         exp.integer_less(),
    #         exp.list_bin('charges'),
    #         exp.list_iterate_and('list_val'),
    #         exp.predexp_or(3)
    #     ]

    #     matched_recs = []
    #     records = self.as_connection.select_many(self.keys, ['account_id'], {'predexp': predexp})
    #     for rec in records:
    #         if rec[2] is not None:
    #             matched_recs.append(rec[2])
        
    #     assert len(matched_recs) == 3
    #     for rec in matched_recs:
    #         assert rec['account_id'] == 1 or rec['account_id'] == 3 or rec['account_id'] == 4

    # def test_pos_remove_with_predexp(self):
    #     '''
    #     Call remove with predexp in policy.
    #     '''
    #     predexp = [
    #         exp.integer_bin('account_id'),
    #         exp.integer_value(1),
    #         exp.integer_equal()
    #     ]
    #     records = self.as_connection.remove(self.keys[0])

    #     rec = self.as_connection.exists(self.keys[0])
    #     assert rec[1] is None

    # def test_remove_with_predexp_filtered_out(self):
    #     '''
    #     Call remove with predexp in policy with expected failure.
    #     '''
    #     predexp = [
    #         exp.integer_bin('account_id'),
    #         exp.integer_value(3),
    #         exp.integer_equal()
    #     ]
    #     with pytest.raises(e.FilteredOut):
    #         self.as_connection.remove(self.keys[0], policy={'predexp': predexp})

    # def test_remove_bin_with_predexp(self):
    #     '''
    #     Call remove_bin with predexp in policy.
    #     '''
    #     predexp = [
    #         exp.integer_bin('account_id'),
    #         exp.integer_value(1),
    #         exp.integer_equal()
    #     ]
    #     self.as_connection.remove_bin(self.keys[0], ['account_id', 'user_name'], policy={'predexp': predexp})

    #     rec = self.as_connection.get(self.keys[0])
    #     assert rec[2].get('account_id') is None and rec[2].get('user_name') is None

    # def test_remove_bin_with_predexp_filtered_out(self):
    #     '''
    #     Call remove_bin with predexp in policy with expected failure.
    #     '''
    #     predexp = [
    #         exp.integer_bin('account_id'),
    #         exp.integer_value(4),
    #         exp.integer_equal()
    #     ]
    #     with pytest.raises(e.FilteredOut):
    #         self.as_connection.remove_bin(self.keys[0], ['account_id', 'user_name'], policy={'predexp': predexp})

    # def test_put_with_predexp(self):
    #     '''
    #     Call put with predexp in policy.
    #     '''
    #     predexp = [
    #         exp.integer_bin('account_id'),
    #         exp.integer_value(1),
    #         exp.integer_equal()
    #     ]
    #     self.as_connection.put(self.keys[0], {'newkey': 'newval'}, policy={'predexp': predexp})

    #     rec = self.as_connection.get(self.keys[0])
    #     assert rec[2]['newkey'] == 'newval'

    # def test_put_new_record_with_predexp(self): # should this fail?
    #     '''
    #     Call put a new record with predexp in policy.
    #     '''
    #     predexp = [
    #         exp.integer_bin('account_id'),
    #         exp.integer_value(1),
    #         exp.integer_equal()
    #     ]
    #     key = ("test", "demo", 10)
    #     self.as_connection.put(key, {'newkey': 'newval'}, policy={'predexp': predexp})

    #     rec = self.as_connection.get(key)
    #     self.as_connection.remove(key)
    #     assert rec[2]['newkey'] == 'newval'

    # def test_put_with_predexp_filtered_out(self):
    #     '''
    #     Call put with predexp in policy with expected failure.
    #     '''
    #     predexp = [
    #         exp.integer_bin('account_id'),
    #         exp.integer_value(4),
    #         exp.integer_equal()
    #     ]
    #     with pytest.raises(e.FilteredOut):
    #         self.as_connection.put(self.keys[0], {'newkey': 'newval'}, policy={'predexp': predexp})

    # def test_get_with_predexp(self):
    #     '''
    #     Call to get with predexp in policy.
    #     '''
    #     predexp = [
    #         exp.integer_bin('account_id'),
    #         exp.integer_value(1),
    #         exp.integer_equal()
    #     ]
    #     record = self.as_connection.get(self.keys[0], {'predexp': predexp})

    #     assert record[2]['account_id'] == 1

    # def test_get_with_predexp_filtered_out(self):
    #     '''
    #     Call to get with predexp in policy with expected failures.
    #     '''
    #     predexp = [
    #         exp.integer_bin('account_id'),
    #         exp.integer_value(3),
    #         exp.integer_equal()
    #     ]
    #     with pytest.raises(e.FilteredOut):
    #         self.as_connection.get(self.keys[0], {'predexp': predexp})

    # def test_select_with_predexp(self):
    #     '''
    #     Call to select with predexp in policy.
    #     '''
    #     predexp = [
    #         exp.integer_bin('acct_balance'),
    #         exp.integer_value(20),
    #         exp.integer_equal(),
    #         exp.integer_var('charge'),
    #         exp.integer_value(20),
    #         exp.integer_less(),
    #         exp.list_bin('charges'),
    #         exp.list_iterate_and('charge'),
    #         exp.predexp_and(2)
    #     ]

    #     result = self.as_connection.select(self.keys[1], ['account_id', 'acct_balance'], {'predexp': predexp})
    #     assert result[2]['account_id'] == 2 and result[2]['acct_balance'] == 20

    # def test_select_with_predexp_filtered_out(self):
    #     '''
    #     Call to select with predexp in policy with expected failures.
    #     '''
    #     predexp = [
    #         exp.integer_bin('acct_balance'),
    #         exp.integer_value(20),
    #         exp.integer_equal(),
    #         exp.integer_var('charge'),
    #         exp.integer_value(10), # charge should trigger a filtered_out
    #         exp.integer_less(),
    #         exp.list_bin('charges'),
    #         exp.list_iterate_and('charge'),
    #         exp.predexp_and(2)
    #     ]

    #     with pytest.raises(e.FilteredOut):
    #         self.as_connection.select(self.keys[1], ['account_id', 'acct_balance'], {'predexp': predexp})

    # def test_exists_many_with_large_predexp(self):
    #     '''
    #     Proper call to exists_many with predexp in policy.
    #     '''
    #     predexp = [
    #         exp.integer_bin('account_id'),
    #         exp.integer_value(4),
    #         exp.integer_equal(),
    #         exp.string_bin('user_name'),
    #         exp.string_value('user3'),
    #         exp.string_equal(),
    #         exp.integer_var('list_val'),
    #         exp.integer_value(12),
    #         exp.integer_less(),
    #         exp.list_bin('charges'),
    #         exp.list_iterate_and('list_val'),
    #         exp.predexp_or(3)
    #     ]

    #     matched_recs = []
    #     records = self.as_connection.exists_many(self.keys, {'predexp': predexp})
    #     for rec in records:
    #         if rec[1] is not None:
    #             matched_recs.append(rec[1])
        
    #     assert len(matched_recs) == 3
