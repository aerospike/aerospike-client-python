# -*- coding: utf-8 -*-

import pytest
import sys
import cPickle as pickle
import time
from test_base_class import TestBaseClass

aerospike = pytest.importorskip("aerospike")

from aerospike import predicates as p


class TestQuery(object):

    def setup_class(cls):
        hostlist, user, password = TestBaseClass.get_hosts()
        config = {'hosts': hostlist}
        if user == None and password == None:
            TestQuery.client = aerospike.client(config).connect()
        else:
            TestQuery.client = aerospike.client(config).connect(user, password)

        TestQuery.client = aerospike.client(config).connect()

        TestQuery.client.index_integer_create('test', 'demo', 'test_age',
                                              'age_index')
        TestQuery.client.index_string_create('test', 'demo', 'addr',
                                             'addr_index')
        TestQuery.client.index_integer_create('test', 'demo', 'age1',
                                              'age_index1')
        TestQuery.client.index_list_create('test', 'demo', 'numeric_list',
                                           aerospike.INDEX_NUMERIC,
                                           'numeric_list_index')
        TestQuery.client.index_list_create('test', 'demo', 'string_list',
                                           aerospike.INDEX_STRING,
                                           'string_list_index')
        TestQuery.client.index_map_keys_create('test', 'demo', 'numeric_map',
                                               aerospike.INDEX_NUMERIC,
                                               'numeric_map_index')
        TestQuery.client.index_map_keys_create('test', 'demo', 'string_map',
                                               aerospike.INDEX_STRING,
                                               'string_map_index')
        TestQuery.client.index_map_values_create('test', 'demo', 'numeric_map',
                                                 aerospike.INDEX_NUMERIC,
                                                 'numeric_map_values_index')
        TestQuery.client.index_map_values_create('test', 'demo', 'string_map',
                                                 aerospike.INDEX_STRING,
                                                 'string_map_values_index')
        TestQuery.client.index_integer_create('test', None, 'test_age_none', 
                                                'age_index_none')

    def teardown_class(cls):
        policy = {}
        TestQuery.client.index_remove('test', 'age_index', policy)
        TestQuery.client.index_remove('test', 'age_index1', policy)
        TestQuery.client.index_remove('test', 'addr_index', policy)
        TestQuery.client.index_remove('test', 'numeric_list_index', policy)
        TestQuery.client.index_remove('test', 'string_list_index', policy)
        TestQuery.client.index_remove('test', 'numeric_map_index', policy)
        TestQuery.client.index_remove('test', 'string_map_index', policy)
        TestQuery.client.index_remove('test', 'numeric_map_values_index',
                                      policy)
        TestQuery.client.index_remove('test', 'string_map_values_index', policy)
        TestQuery.client.index_remove('test', 'age_index_none', policy);
        TestQuery.client.close()

    def setup_method(self, method):
        """
        Setup method.
        """
        for i in xrange(5):
            key = ('test', 'demo', i)
            rec = {
                'name': 'name%s' % (str(i)),
                'addr': 'name%s' % (str(i)),
                'numeric_list': [i, i + 1, i + 2],
                'string_list': ["str" + str(i), "str" + str(i + 1),
                                "str" + str(i + 2)],
                'numeric_map': {"a": i,
                                "b": i + 1,
                                "c": i + 2},
                'string_map': {
                    "a": "a" + str(i),
                    "b": "b" + str(i + 1),
                    "c": "c" + str(i + 2)
                },
                'test_age': i,
                'no': i
            }
            TestQuery.client.put(key, rec)
        for i in xrange(5, 10):
            key = ('test', 'demo', i)
            rec = {
                u'name': 'name%s' % (str(i)),
                u'addr': u'name%s' % (str(i)),
                u'test_age': i,
                u'no': i
            }
            TestQuery.client.put(key, rec)

    def teardown_method(self, method):
        """
        Teardown method.
        """
        for i in xrange(10):
            key = ('test', 'demo', i)
            #TestQuery.client.remove(key)

    def test_query_with_no_parameters(self):
        """
            Invoke query() without any mandatory parameters.
        """
        with pytest.raises(Exception) as exception:
            query = TestQuery.client.query()
            query.select()
            query.where()

        assert exception.value[0] == -2
        assert exception.value[1] == 'query() expects atleast 1 parameter'

    def test_query_with_correct_parameters(self):
        """
            Invoke query() with correct arguments
        """
        query = TestQuery.client.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.equals('test_age', 1))

        records = []

        def callback((key, metadata, record)):
            records.append(key)

        query.foreach(callback)
        assert records
        assert len(records) == 1

    def test_query_with_incorrect_ns_set(self):
        """
            Invoke query() with incorrect ns and set
        """
        with pytest.raises(Exception) as exception:
            query = TestQuery.client.query('test1', 'demo1')
            query.select('name', 'test_age')
            query.where(p.equals('test_age', 1))

            def callback((key, metadata, record)):
                assert metadata['gen'] != None

            query.foreach(callback)

        assert exception.value[0] == 4L
        assert exception.value[1] == 'AEROSPIKE_ERR_REQUEST_INVALID'

    def test_query_with_ns_not_string(self):
        """
            Invoke query() with incorrect ns and set
        """
        with pytest.raises(Exception) as exception:
            query = TestQuery.client.query(1, 'demo')
            query.select('name', 'test_age')
            query.where(p.equals('test_age', 1))
            def callback((key,metadata,record)):
                assert metadata['gen'] != None

            query.foreach(callback)

        assert exception.value[0] == -2L
        assert exception.value[1] == 'Namespace should be a string'

    def test_query_with_set_int(self):
        """
            Invoke query() with incorrect ns and set
        """
        with pytest.raises(Exception) as exception:
            query = TestQuery.client.query('test', 1)
            query.select('name', 'test_age')
            query.where(p.equals('test_age', 1))
            def callback((key,metadata,record)):
                assert metadata['gen'] != None

            query.foreach(callback)

        assert exception.value[0] == -2L
        assert exception.value[1] == 'Set should be string, unicode or None'

    def test_query_with_incorrect_bin_name(self):
        """
            Invoke query() with incorrect bin name
        """
        query = TestQuery.client.query('test', 'demo')
        query.select('name1', 'age1')
        query.where(p.equals('age1', 1))
        records = []

        def callback((key, metadata, record)):
            records.append(record)

        query.foreach(callback)
        assert len(records) == 0

    def test_query_without_callback_parameter(self):
        """
            Invoke query() with without callback
        """
        query = TestQuery.client.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.equals('test_age', 1))

        def callback((key, metadata, record)):
            assert metadata['gen'] != None

        with pytest.raises(TypeError) as typeError:
            query.foreach()

        assert "Required argument 'callback' (pos 1) not found" in typeError.value

    def test_query_with_nonindexed_bin(self):
        """
            Invoke query() with non-indexed bin
        """
        with pytest.raises(Exception) as exception:
            query = TestQuery.client.query('test', 'demo')
            query.select('name', 'no')
            query.where(p.equals('no', 1))

            def callback((key, metadata, record)):
                assert metadata['gen'] != None

            query.foreach(callback)

        assert exception.value[0] == 201L
        assert exception.value[1] == 'AEROSPIKE_ERR_INDEX_NOT_FOUND'

    def test_query_with_where_incorrect(self):
        """
            Invoke query() with where is incorrect
        """
        query = TestQuery.client.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.equals('test_age', 165))
        records = []

        def callback((key, metadata, record)):
            records.append(record)

        query.foreach(callback)
        assert len(records) == 0

    def test_query_with_where_none_value(self):
        """
            Invoke query() with where is null value
        """
        query = TestQuery.client.query('test', 'demo')
        query.select('name', 'test_age')
        with pytest.raises(Exception) as exception:
            query.where(p.equals('test_age', None))

        assert exception.value[0] == -2L
        assert exception.value[1] == 'predicate is invalid.'

    def test_query_with_policy(self):
        """
            Invoke query() with policy
        """
        policy = {'timeout': 1000}
        query = TestQuery.client.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.equals('test_age', 1))
        records = []

        def callback((key, metadata, record)):
            records.append(record)

        query.foreach(callback, policy)
        assert len(records) == 1

    def test_query_with_extra_argument(self):
        """
            Invoke query() with extra argument
        """
        policy = {'timeout': 1000}
        query = TestQuery.client.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.equals('test_age', 1))

        def callback((key, metadata, record)):
            assert metadata['gen'] != None

        with pytest.raises(TypeError) as typeError:
            query.foreach(callback, policy, "")

        assert "foreach() takes at most 2 arguments (3 given)" in typeError.value

    def test_query_with_incorrect_policy_value(self):
        """
            Invoke query() with incorrect policy
        """
        policy = {'timeout': ""}
        query = TestQuery.client.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.equals('test_age', 1))

        def callback((key, metadata, record)):
            assert metadata['gen'] != None

        with pytest.raises(Exception) as exception:
            query.foreach(callback, policy)

        assert exception.value[0] == -2L
        assert exception.value[1] == 'timeout is invalid'

    def test_query_with_put_in_callback(self):
        """
            Invoke query() with put in callback
        """
        policy = {'timeout': 1000}
        query = TestQuery.client.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.equals('test_age', 1))
        records = []

        def callback((key, metadata, record)):
            records.append(record)
            key = ('test', 'demo', 'put_in_callback')
            rec = {'name': 'name%s' % (str(8)), 'test_age': 8, }
            TestQuery.client.put(key, rec)

        query.foreach(callback, policy)

        key = ('test', 'demo', 'put_in_callback')
        key1, meta, bins = TestQuery.client.get(key)

        key = ('test', 'demo', 'put_in_callback')
        TestQuery.client.remove(key)
        assert bins == {'test_age': 8, 'name': 'name8'}

    def test_query_with_correct_parameters_between(self):
        """
            Invoke query() with correct arguments and using predicate between
        """
        query = TestQuery.client.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.between('test_age', 1, 4))

        records = []

        def callback((key, metadata, record)):
            records.append(record)

        query.foreach(callback)
        assert len(records) == 4

    def test_query_with_where_is_null(self):

        query = TestQuery.client.query('test', 'demo')
        query.select('name', 'test_age')
        with pytest.raises(Exception) as exception:
            query.where("")

        assert exception.value[0] == -2
        assert exception.value[1] == "predicate is invalid."

    def test_query_with_callback_contains_error(self):
        """
            Invoke query() with callback function contains an error
        """
        query = TestQuery.client.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.equals('test_age', 1))

        records = []

        def callback((key, metadata, record)):
            val += 1
            records.append(key)

        with pytest.raises(Exception) as exception:
            result = query.foreach(callback)
        assert exception.value[0] == -2L
        assert exception.value[1] == "Callback function contains an error"

    def test_query_with_callback_returning_false(self):
        """
            Invoke query() with callback function returns false
        """
        query = TestQuery.client.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.between('test_age', 1, 5))

        records = []

        def callback((key, metadata, record)):
            if len(records) == 2:
                return False
            records.append(key)

        result = query.foreach(callback)
        assert len(records) == 2

    def test_query_with_results_method(self):
        """
            Invoke query() with correct arguments
        """
        query = TestQuery.client.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.equals('test_age', 1))

        records = []
        records = query.results()
        assert len(records) == 1

    def test_query_with_unicode_binnames_in_select_and_where(self):
        """
            Invoke query() with unicode bin names in select
        """
        query = TestQuery.client.query('test', 'demo')
        query.select(u'name', u'test_age', 'addr')
        query.where(p.equals(u'test_age', 7))

        records = query.results()
        assert len(records) == 1
        assert records[0][2] == {
            'test_age': 7,
            'name': u'name7',
            'addr': u'name7'
        }

        query = TestQuery.client.query('test', 'demo')
        query.select(u'name', 'addr')
        query.where(p.equals(u'addr', u'name9'))

        records = query.results()
        assert records[0][2] == {'name': 'name9', 'addr': u'name9'}

    def test_query_with_select_bin_integer(self):
        """
            Invoke query() with select bin is of integer type.
        """
        query = TestQuery.client.query('test', 'demo')

        with pytest.raises(Exception) as exception:
            query.select(22, 'test_age')

        assert exception.value[0] == -2L
        assert exception.value[1] == 'Bin name should be of type string'

    def test_query_with_correct_parameters_contains(self):
        """
            Invoke query() with correct arguments and using predicate contains
        """
        query = TestQuery.client.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.contains('numeric_list', aerospike.INDEX_TYPE_LIST, 1))

        records = []

        def callback((key, metadata, record)):
            records.append(record)

        query.foreach(callback)
        assert len(records) == 2

    def test_query_with_correct_parameters_containsstring(self):
        """
            Invoke query() with correct arguments and using predicate contains
        """
        query = TestQuery.client.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.contains('string_list', aerospike.INDEX_TYPE_LIST,
                               "str3"))

        records = []

        def callback((key, metadata, record)):
            records.append(record)

        query.foreach(callback)
        assert len(records) == 3

    def test_query_with_correct_parameters_rangecontains(self):
        """
            Invoke query() with correct arguments and using predicate contains
        """
        query = TestQuery.client.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.range('numeric_list', aerospike.INDEX_TYPE_LIST, 1, 3))

        records = []

        def callback((key, metadata, record)):
            records.append(record)

        query.foreach(callback)
        assert len(records) == 2

    def test_query_with_correct_parameters_containsstring_mapkeys(self):
        """
            Invoke query() with correct arguments and using predicate contains
        """
        query = TestQuery.client.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.contains('string_map', aerospike.INDEX_TYPE_MAPKEYS, "a"))

        records = []

        def callback((key, metadata, record)):
            records.append(record)

        query.foreach(callback)
        assert len(records) == 5

    def test_query_with_correct_parameters_containsstring_mapvalues(self):
        """
            Invoke query() with correct arguments and using predicate contains
        """
        query = TestQuery.client.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.contains('string_map', aerospike.INDEX_TYPE_MAPVALUES,
                               "a1"))

        records = []

        def callback((key, metadata, record)):
            records.append(record)

    def test_query_with_multiple_foreach_on_same_query_object(self):
        """
            Invoke query() with multple foreach() call on same query object
        """
        query = TestQuery.client.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.equals('test_age', 1))

        records = []

        def callback((key, metadata, record)):
            records.append(key)

        query.foreach(callback)
        assert len(records) == 1

    def test_query_with_correct_parameters_containsnumeric_mapvalues(self):
        """
            Invoke query() with correct arguments and using predicate contains
        """
        query = TestQuery.client.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.contains('numeric_map', aerospike.INDEX_TYPE_MAPVALUES,
                               1))

        records = []

        def callback((key, metadata, record)):
            records.append(record)

        query.foreach(callback)
        assert len(records) == 2

    def test_query_with_correct_parameters_rangecontains(self):
        """
            Invoke query() with correct arguments and using predicate contains
        """
        query = TestQuery.client.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.range('numeric_map', aerospike.INDEX_TYPE_MAPVALUES, 1,
                            3))

        records = []

        def callback((key, metadata, record)):
            records.append(record)

        query.foreach(callback)
        assert len(records) == 8

    def test_query_with_correct_parameters_rangecontains_notuple(self):
        """
            Invoke query() with correct arguments and using predicate contains
        """
        query = TestQuery.client.query('test', 'demo')
        query.select('name', 'test_age')
        query.where('numeric_map', "range", aerospike.INDEX_TYPE_MAPVALUES,
                    aerospike.INDEX_NUMERIC, 1, 3)

        records = []

        def callback((key, metadata, record)):
            records.append(record)

        query.foreach(callback)
        assert len(records) == 8

    def test_query_with_correct_parameters_containsstring_mapvalues_notuple(
        self
    ):
        """
            Invoke query() with correct arguments and using predicate contains
        """
        query = TestQuery.client.query('test', 'demo')
        query.select('name', 'test_age')
        query.where('string_map', 'contains', aerospike.INDEX_TYPE_MAPVALUES,
                    aerospike.INDEX_STRING, "a1")

        records = []

        def callback((key, metadata, record)):
            records.append(record)

        query.foreach(callback)
        assert len(records) == 1

    def test_query_with_correct_parameters_containsstring_notuple(self):
        """
            Invoke query() with correct arguments and using predicate contains
        """
        query = TestQuery.client.query('test', 'demo')
        query.select('name', 'test_age')
        query.where('string_list', "contains", aerospike.INDEX_TYPE_LIST,
                    aerospike.INDEX_STRING, "str3")

        records = []

        def callback((key, metadata, record)):
            records.append(record)

        query.foreach(callback)
        assert len(records) == 3

    def test_query_with_correct_parameters_between_notuple(self):
        """
            Invoke query() with correct arguments and using predicate between
        """
        query = TestQuery.client.query('test', 'demo')
        query.select('name', 'test_age')
        query.where('test_age', 'between', 1, 4)

        records = []

        def callback((key, metadata, record)):
            records.append(record)

        query.foreach(callback)
        assert len(records) == 4

    def test_query_with_policy_notuple(self):
        """
            Invoke query() with policy
        """
        policy = {'timeout': 1000}
        query = TestQuery.client.query('test', 'demo')
        query.select('name', 'test_age')
        query.where('test_age', 'equals', 1)
        records = []

        def callback((key, metadata, record)):
            records.append(record)

        query.foreach(callback, policy)
        assert len(records) == 1
        records = []
        query.foreach(callback)
        assert len(records) == 1

    def test_query_with_multiple_results_call_on_same_query_object(self):
        """
            Invoke query() with multple results() call on same query object
        """
        query = TestQuery.client.query('test', 'demo')
        query.select(u'name', u'test_age', 'addr')
        query.where(p.equals(u'test_age', 7))

        records = query.results()
        assert len(records) == 1
        assert records[0][2] == {
            'test_age': 7,
            'name': u'name7',
            'addr': u'name7'
        }

        records = []
        records = query.results()
        assert len(records) == 1
        assert records[0][2] == {
            'test_age': 7,
            'name': u'name7',
            'addr': u'name7'
        }

    def test_query_with_correct_parameters_without_connection(self):
        """
            Invoke query() with correct arguments without connection
        """
        config = {'hosts': [('127.0.0.1', 3000)]}
        client1 = aerospike.client(config)

        with pytest.raises(Exception) as exception:
            query = client1.query('test', 'demo')
            query.select('name', 'test_age')
            query.where(p.equals('test_age', 1))

            records = []

            def callback((key, metadata, record)):
                records.append(key)

            query.foreach(callback)

        assert exception.value[0] == 11L
        assert exception.value[1] == 'No connection to aerospike cluster'

    def test_query_with_policy_on_none_set_index(self):
        """
            Invoke query() with policy on none set index
        """
        policy = {
            'timeout': 1000
        }
        query = TestQuery.client.query('test', None)
        query.select('name', 'test_age_none')
        query.where(p.equals('test_age_none', 1))
        records = []
        def callback((key,metadata,record)):
            records.append(record)

        query.foreach(callback, policy)
        assert len(records) == 0

    def test_query_with_only_ns(self):
        """
            Invoke query() with policy on none set index
        """
        policy = {
            'timeout': 1000
        }
        query = TestQuery.client.query('test')
        query.select('name', 'test_age_none')
        query.where(p.equals('test_age_none', 1))
        records = []
        def callback((key,metadata,record)):
            records.append(record)

        query.foreach(callback, policy)
        assert len(records) == 0
