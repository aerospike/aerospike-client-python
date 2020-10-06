# -*- coding: utf-8 -*-
import pytest
import sys
import time

from aerospike import predexp as predexp
from aerospike import exception as e

aerospike = pytest.importorskip('aerospike')
try:
    import aerospike
except:
    print('Please install aerospike python client.')
    sys.exit(1)


def seconds_to_nanos(num):
    '''
    converter seconds to nanoseconds
    '''
    return int(num) * (10 ** 9)

# GeoConstants
geo_object1 = aerospike.GeoJSON(
    {"type": "AeroCircle", "coordinates": [[-122.0, 37.5], 1000]})

geo_object2 = aerospike.GeoJSON(
    {"type": "AeroCircle", "coordinates": [[-132.0, 37.5], 1000]})

geo_point1 = aerospike.GeoJSON(
    {"coordinates": [-122.0, 37.5], "type": "Point"})

geo_point2 = aerospike.GeoJSON(
    {"coordinates": [-132.0, 37.5], "type": "Point"})


def assert_each_record(records, check_func, *args):
    for record in records:
        assert check_func(record, *args)


def assert_each_record_bins(records, check_func):
    for _, _, bins in records:
        assert check_func(bins)


@pytest.fixture(scope='class')
def clean_test_demo_namespace(as_connection):
    names = ['Alice', 'Bob', 'John', 'Jane']
    for i in range(100):
        key = 'test', 'demo', i
        record = {
            'name': names[i % 4],
            'positive_i': i,
            'i_mod_5': i % 5,
            'i_mod_10': i % 10,
            # This is a list: [i, i + 1, i +2 , i+3, i+4, i+5]
            'plus_five_l': list(range(i, i + 6))
        }
        as_connection.put(key, record)

        #  For list tests
        flist1 = ['Alice', 'Bob', 'John']
        flist2 = ['Alice', 'Bob', 'Jane']
        flist3 = ['John', 'Jane']
        key1 = 'test', 'demo2', 'f1'
        key2 = 'test', 'demo2', 'f2'
        key3 = 'test', 'demo2', 'f3'

        as_connection.put(key1, {'slist': flist1})
        as_connection.put(key2, {'slist': flist2})
        as_connection.put(key3, {'slist': flist3})

        #  For map tests:
        flist1 = ['Alice', 'Bob', 'John']
        flist2 = ['Alice', 'Bob', 'Jane']
        flist3 = ['John', 'Jane']

        # dictionary: {'Alice': 5, 'Bob': '3'...}
        map1 = dict([(name, len(name)) for name in flist1])
        map2 = dict([(name, len(name)) for name in flist2])
        map3 = dict([(name, len(name)) for name in flist3])

        key1 = 'test', 'demo3', 'f1'
        key2 = 'test', 'demo3', 'f2'
        key3 = 'test', 'demo3', 'f3'

        as_connection.put(key1, {'map': map1})
        as_connection.put(key2, {'map': map2})
        as_connection.put(key3, {'map': map3})

        geok1 = 'test', 'geo', 1
        geok2 = 'test', 'geo', 2

        georec1 = {
            'id': 1,
            'point': geo_point1,
            'region': geo_object1,
            'geolist': [geo_point1]
        }

        georec2 = {
            'id': 2,
            'point': geo_point2,
            'region': geo_object2,
            'geolist': [geo_point2]
        }

        as_connection.put(geok1, georec1)
        as_connection.put(geok2, georec2)

    yield

    as_connection.truncate('test', None, 0)


@pytest.mark.usefixtures('clean_test_demo_namespace')
class TestQueryPredexp(object):

    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        self.del_keys = []
        self.query = as_connection.query('test', 'demo')

    def test_integer_equals(self):
        predexps = [
            predexp.integer_bin('positive_i'),
            predexp.integer_value(5),
            predexp.integer_equal()
        ]
        self.query.predexp(predexps)
        results = self.query.results()
        assert len(results) == 1
        assert_each_record_bins(results, lambda b: b['positive_i'] == 5)

    def test_integer_greater(self):
        predexps = [
            predexp.integer_bin('positive_i'),
            predexp.integer_value(49),
            predexp.integer_greater()
        ]
        self.query.predexp(predexps)
        results = self.query.results()
        assert len(results) == 50
        assert_each_record_bins(results, lambda b: b['positive_i'] > 49)

    def test_integer_greatereq(self):
        predexps = [
            predexp.integer_bin('positive_i'),
            predexp.integer_value(49),
            predexp.integer_greatereq()
        ]
        self.query.predexp(predexps)
        results = self.query.results()
        assert len(results) == 51
        assert_each_record_bins(results, lambda b: b['positive_i'] >= 49)

    def test_integer_less(self):
        predexps = [
            predexp.integer_bin('positive_i'),
            predexp.integer_value(10),
            predexp.integer_less()
        ]
        self.query.predexp(predexps)
        results = self.query.results()
        assert len(results) == 10
        assert_each_record_bins(results, lambda b: b['positive_i'] < 10)

    def test_integer_lesseq(self):
        predexps = [
            predexp.integer_bin('positive_i'),
            predexp.integer_value(10),
            predexp.integer_lesseq()
        ]
        self.query.predexp(predexps)
        results = self.query.results()
        assert len(results) == 11
        assert_each_record_bins(results, lambda b: b['positive_i'] <= 10)

    # TBD What calculation is the server doing here, I don't get it right now.

    def test_digest_modulo(self):
        # Count of digests whose last byte is < 128
        less_than_128 = 0
        expected_ids = set([])
        for i in range(100):
            key = 'test', 'demo', i
            if aerospike.calc_digest(*key)[-1] < 128:
                expected_ids.add(i)
                less_than_128 += 1

        predexps = [
            predexp.rec_digest_modulo(256),
            predexp.integer_value(128),
            predexp.integer_less()
        ]

        self.query.predexp(predexps)
        results = self.query.results()
        assert len(results) == less_than_128

    def test_string_equal(self):
        predexps = [
            predexp.string_bin('name'),
            predexp.string_value('Alice'),
            predexp.string_equal()
        ]
        self.query.predexp(predexps)
        results = self.query.results()
        assert len(results) == 25
        assert_each_record_bins(
            results,
            lambda b: b['name'] == 'Alice')

    def test_string_unequal(self):
        predexps = [
            predexp.string_bin('name'),
            predexp.string_value('Alice'),
            predexp.string_unequal()
        ]
        self.query.predexp(predexps)
        results = self.query.results()
        assert len(results) == 75
        assert_each_record_bins(
            results,
            lambda b: b['name'] != 'Alice')

    # TODO GEO BIN WITHIN CONTAINS

    def test_geo_within(self):
        predexps = [
            predexp.geojson_bin('point'),
            predexp.geojson_value(geo_object1.dumps()),
            predexp.geojson_within()
        ]

        query = self.as_connection.query('test', 'geo')
        query.predexp(predexps)
        results = query.results()
        assert len(results) == 1
        assert results[0][2]['id'] == 1

    def test_geo_contains(self):
        predexps = [
            predexp.geojson_bin('region'),
            predexp.geojson_value(geo_point2.dumps()),
            predexp.geojson_contains()
        ]

        query = self.as_connection.query('test', 'geo')
        query.predexp(predexps)
        results = query.results()
        assert len(results) == 1
        assert results[0][2]['id'] == 2

    @pytest.mark.xfail(reason="Server does not support this")
    def test_geo_contains2(self):
        predexps = [
            predexp.geojson_bin('region'),
            predexp.geojson_bin('point'),
            predexp.geojson_contains()
        ]

        query = self.as_connection.query('test', 'geo')
        query.predexp(predexps)
        results = query.results()
        assert len(results) == 2

    @pytest.mark.xfail(reason="Server does not support this")
    def test_geo_variable1(self):

        predexps = [
            predexp.geojson_var('list_entry'),
            predexp.geojson_value(geo_object1.dumps()),
            predexp.geojson_within(),
            predexp.list_bin('geolist'),
            predexp.list_iterate_or('list_entry')
        ]

        query = self.as_connection.query('test', 'geo')
        query.predexp(predexps)
        results = query.results()
        assert len(results) == 1
        assert results[0][2]['id'] == 1

    def test_not(self):
        predexps = [
            predexp.integer_bin('positive_i'),
            predexp.integer_value(5),
            predexp.integer_equal(),
            predexp.predexp_not()
        ]
        self.query.predexp(predexps)
        results = self.query.results()

        assert len(results) == 99
        assert_each_record_bins(results, lambda b: b['positive_i'] != 5)

    def test_or(self):
        predexps = [
            predexp.integer_bin('positive_i'),
            predexp.integer_value(5),
            predexp.integer_equal(),
            predexp.integer_bin('positive_i'),
            predexp.integer_value(10),
            predexp.integer_equal(),
            predexp.predexp_or(2)
        ]
        self.query.predexp(predexps)
        results = self.query.results()
        assert len(results) == 2
        assert_each_record_bins(
            results,
            lambda b: b['positive_i'] in (5, 10))

    def test_and(self):
        predexps = [
            predexp.integer_bin('positive_i'),
            predexp.integer_value(10),
            predexp.integer_greater(),
            predexp.integer_bin('positive_i'),
            predexp.integer_value(20),
            predexp.integer_less(),
            predexp.predexp_and(2)
        ]
        self.query.predexp(predexps)
        results = self.query.results()
        assert len(results) == 9
        assert_each_record_bins(
            results,
            lambda b: b['positive_i'] > 10 and b['positive_i'] < 20)

    def test_string_regex(self):
        predexps = [
            predexp.string_bin('name'),
            predexp.string_value('.*O.*'),
            predexp.string_regex(aerospike.REGEX_ICASE)
        ]
        self.query.predexp(predexps)
        results = self.query.results()
        assert len(results) == 50
        assert_each_record_bins(
            results,
            lambda b: b['name'] in ('Bob', 'John'))

    # List Tests
    def test_list_or_int(self):
        predexps = [
            predexp.integer_var('list_val'),
            predexp.integer_value(10),
            predexp.integer_greater(),
            predexp.list_bin('plus_five_l'),
            predexp.list_iterate_or('list_val')
        ]
        self.query.predexp(predexps)
        results = self.query.results()
        assert len(results) == 94  # This isn't true for 0,1,2,3,4,5 so 100 - 6
        assert_each_record_bins(
            results,
            lambda b: b['plus_five_l'][-1] > 10)

    def test_list_and_int(self):
        predexps = [
            predexp.integer_var('list_val'),
            predexp.integer_value(10),
            predexp.integer_greater(),
            predexp.list_bin('plus_five_l'),
            predexp.list_iterate_and('list_val')
        ]
        self.query.predexp(predexps)
        results = self.query.results()
        assert len(results) == 89  # This isn't true for the first 11
        assert_each_record_bins(
            results,
            lambda b: b['plus_five_l'][-1] > 10)

    def test_list_or_str(self):
        predexps = [
            predexp.string_var('list_val'),
            predexp.string_value('Bob'),
            predexp.string_equal(),
            predexp.list_bin('slist'),
            predexp.list_iterate_or('list_val')
        ]
        query = self.as_connection.query('test', 'demo2')
        query.predexp(predexps)
        results = query.results()
        assert len(results) == 2
        assert_each_record_bins(
            results,
            lambda b: any([name == 'Bob' for name in b['slist']]))

    def test_list_and_str(self):

        predexps = [
            predexp.string_var('list_val'),
            predexp.string_value('Bob'),
            predexp.string_unequal(),
            predexp.list_bin('slist'),
            predexp.list_iterate_and('list_val')
        ]
        # Only one friend list without Bob
        query = self.as_connection.query('test', 'demo2')
        query.predexp(predexps)
        results = query.results()
        assert len(results) == 1
        assert_each_record_bins(
            results,
            lambda b: all([name != 'Bob' for name in b['slist']]))

    # Mapkey Tests
    def test_mapkey_iterate_or(self):

        predexps = [
            predexp.string_var('map_key'),
            predexp.string_value('Bob'),
            predexp.string_equal(),
            predexp.map_bin('map'),
            predexp.mapkey_iterate_or('map_key')
        ]
        query = self.as_connection.query('test', 'demo3')
        query.predexp(predexps)
        results = query.results()
        assert len(results) == 2
        assert_each_record_bins(
            results,
            lambda b: any([key == 'Bob' for key in b['map']]))

    def test_mapkey_iterate_and(self):
        predexps = [
            predexp.string_var('map_key'),
            predexp.string_value('Bob'),
            predexp.string_unequal(),
            predexp.map_bin('map'),
            predexp.mapkey_iterate_and('map_key')
        ]
        query = self.as_connection.query('test', 'demo3')
        query.predexp(predexps)
        results = query.results()
        assert len(results) == 1
        assert_each_record_bins(
            results,
            lambda b: all([key != 'Bob' for key in b['map']]))

    # MapValueTest
    def test_mapvalue_iterate_or(self):

        predexps = [
            predexp.integer_var('map_value'),
            predexp.integer_value(3),
            predexp.integer_equal(),
            predexp.map_bin('map'),
            predexp.mapval_iterate_or('map_value')
        ]
        query = self.as_connection.query('test', 'demo3')
        query.predexp(predexps)
        results = query.results()
        assert len(results) == 2
        assert_each_record_bins(
            results,
            lambda b: any([b['map'][key] == 3 for key in b['map']]))

    def test_mapvalue_iterate_and(self):
        predexps = [
            predexp.integer_var('map_value'),
            predexp.integer_value(3),
            predexp.integer_unequal(),
            predexp.map_bin('map'),
            predexp.mapval_iterate_and('map_value')
        ]
        query = self.as_connection.query('test', 'demo3')
        query.predexp(predexps)
        results = query.results()
        assert len(results) == 1
        assert_each_record_bins(
            results,
            lambda b: all([b['map'][key] != 3 for key in b['map']]))

    @pytest.mark.xfail(reason="This only works when not running data in memory")
    def test_rec_device_size(self):
        long_str_len = 65 * 1024
        long_str = long_str_len * 'a'  # A 65K string

        # Store 5 records with a string of 65K
        for i in range(5):
            key = 'test', 'dev_size', i
            self.as_connection.put(key, {'string': long_str})

        # Store 3 records with a size much less than 65K
        for i in range(5, 8):
            key = 'test', 'dev_size', i
            self.as_connection.put(key, {'string': "short"})

        query = self.as_connection.query('test', 'dev_size')
        predexps = [
            predexp.rec_device_size(),
            predexp.integer_value(64 * 1024),
            predexp.integer_greater()
        ]
        query.predexp(predexps)
        results = query.results()
        assert len(results) == 5
        assert_each_record_bins(
            results,
            lambda b: len(b['string']) == long_str_len) # This is faster than doing a string compare

    def test_rec_last_update(self):
        '''
        This could fail due to clock skew
        '''
        for i in range(7):
            key = 'test', 'lut', i
            self.as_connection.put(key, {'time': 'earlier'})

        cutoff_nanos = seconds_to_nanos(int(time.time() + 2))

        time.sleep(5) # Make sure that we wait long enough

        # Store 5 records after the cutoff
        for i in range(7, 12):
            key = 'test', 'lut', i
            self.as_connection.put(key, {'time': 'later'})

        query = self.as_connection.query('test', 'lut')
        predexps = [
            predexp.rec_last_update(),
            predexp.integer_value(cutoff_nanos),
            predexp.integer_less()
        ]
        query.predexp(predexps)
        results = query.results()
        assert len(results) == 7
        assert_each_record_bins(
            results,
            lambda b: b['time'] == 'earlier')

    def test_rec_void_time(self):
        '''
        This could fail due to clock skew
        '''
        for i in range(7):
            key = 'test', 'ttl', i
            self.as_connection.put(key, {'time': 'earlier'}, meta={'ttl': 100})

        # 150 second range for record TTLs should be enough, we are storing with
        # Current time + 100s and current time +5000s, so only one of the group should be found
        void_time_range_start = seconds_to_nanos(int(time.time() + 50))
        void_time_range_end = seconds_to_nanos(int(time.time() + 150))

        # Store 5 records after the cutoff
        for i in range(7, 12):
            key = 'test', 'ttl', i
            self.as_connection.put(key, {'time': 'later'}, meta={'ttl': 1000})

        query = self.as_connection.query('test', 'ttl')
        predexps = [
            predexp.rec_void_time(),
            predexp.integer_value(void_time_range_start),
            predexp.integer_greater(),

            predexp.rec_void_time(),
            predexp.integer_value(void_time_range_end),
            predexp.integer_less(),

            predexp.predexp_and(2)
        ]
        query.predexp(predexps)
        results = query.results()
        assert len(results) == 7
        assert_each_record_bins(
            results,
            lambda b: b['time'] == 'earlier')

    def test_with_or_nexpr_too_big(self):
        predexps = [predexp.predexp_or(1 << 65)] # This needs to be over 2 ^ 63 - 1
        query = self.as_connection.query('test', 'or')
        with pytest.raises(e.ParamError):
            query.predexp(predexps)

    def test_with_and_nexpr_too_big(self):
        predexps = [predexp.predexp_and(1 << 65)] # This needs to be over 2 ^ 63 - 1
        query = self.as_connection.query('test', 'or')
        with pytest.raises(e.ParamError):
            query.predexp(predexps)

    def test_with_invalid_predicate(self):
        '''
        This passes something which isn't a predicate
        '''
        predexps = [
            predexp.integer_var('map_value'),
            predexp.integer_value(3),
            predexp.integer_equal(),
            5
        ]
        with pytest.raises(e.ParamError):
            self.query.predexp(predexps)

    @pytest.mark.parametrize(
        "func",
        [
            predexp.integer_value,
            predexp.predexp_and,
            predexp.predexp_or,
            predexp.rec_digest_modulo,
        ])
    def test_with_wrong_predicate_argument_type_expecting_int(self, func):
        '''
        These functions all expect an integer argument, call with a string
        '''
        predexps = [
            func("five")
        ]
        with pytest.raises(e.ParamError):
            self.query.predexp(predexps)

    @pytest.mark.parametrize(
        "func",
        [
            predexp.integer_bin,
            predexp.string_bin,
            predexp.geojson_bin,
            predexp.map_bin,
            predexp.list_bin,
            predexp.string_value,
            predexp.geojson_value,
            predexp.integer_var,
            predexp.string_var,
            predexp.geojson_var,
            predexp.list_iterate_or,
            predexp.list_iterate_and,
            predexp.mapkey_iterate_or,
            predexp.mapkey_iterate_and,
            predexp.mapval_iterate_and,
            predexp.mapval_iterate_or
        ])
    def test_with_wrong_predicate_argument_type_expecting_str(self, func):
        '''
        These functions all expect an integer argument, call with a string
        '''
        predexps = [
            func(5)
        ]
        with pytest.raises(e.ParamError):
            self.query.predexp(predexps)

    def test_with_invalid_predicate_tuple(self):
        '''
        This passes something which isn't a predicate
        '''
        predexps = [
            (1234, "not real")
        ]
        with pytest.raises(e.ParamError):
            self.query.predexp(predexps)

    def test_with_empty_exp_list(self):
        '''
        Pass an empty list of predicates
        '''
        predexps = []
        with pytest.raises(e.ParamError):
            self.query.predexp(predexps)


    def test_with_non_list_predicate(self):
        '''
        This passes something which isn't a list
        '''
        predexps = "integer_bin(a) ,5, integer_equal"
        with pytest.raises(e.ParamError):
            self.query.predexp(predexps)