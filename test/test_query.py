
# -*- coding: utf-8 -*-

import pytest
import sys
import cPickle as pickle
import time

try:
    import aerospike
except:
    print "Please install aerospike python client."
    sys.exit(1)

from aerospike import predicates as p

class TestQuery(object):

    def setup_class(cls):
        config = {
                'hosts': [('127.0.0.1', 3000)]
                }
        TestQuery.client = aerospike.client(config).connect()
        policy = {}
        TestQuery.client.index_integer_create(policy, 'test', 'demo', 'test_age', 'age_index')
        policy = {}
        TestQuery.client.index_integer_create(policy, 'test', 'demo',
'age1', 'age_index1')
        time.sleep(2)

    def teardown_class(cls):
        policy = {}
        TestQuery.client.index_remove(policy, 'test', 'age_index');
        TestQuery.client.index_remove(policy, 'test', 'age_index1');
        TestQuery.client.close()

    def setup_method(self, method):

        """
        Setup method.
        """

        for i in xrange(5):
            key = ('test', 'demo', i)
            rec = {
                    'name' : 'name%s' % (str(i)),
                    'addr' : 'name%s' % (str(i)),
                    'test_age'  : i,
                    'no'   : i
                    }
            TestQuery.client.put(key, rec)

    def teardown_method(self, method):
        """
        Teardown method.
        """
        for i in xrange(5):
            key = ('test', 'demo', i)
            TestQuery.client.remove(key)

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
        def callback((key,metadata,record)):
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
            def callback((key,metadata,record)):
                assert metadata['gen'] != None

            query.foreach(callback)

        assert exception.value[0] == 4L
        assert exception.value[1] == 'AEROSPIKE_ERR_REQUEST_INVALID'

    def test_query_with_incorrect_bin_name(self):
        """
            Invoke query() with incorrect bin name
        """
        query = TestQuery.client.query('test', 'demo')
        query.select('name1', 'age1')
        query.where(p.equals('age1', 1))
        records = []
        def callback((key,metadata,record)):
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
        def callback((key,metadata,record)):
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
            def callback((key,metadata,record)):
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
        def callback((key,metadata,record)):
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
        policy = {
            'timeout': 1000
        }
        query = TestQuery.client.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.equals('test_age', 1))
        records = []
        def callback((key,metadata,record)):
            records.append(record)

        query.foreach(callback, policy)
        assert len(records) == 1

    def test_query_with_extra_argument(self):
        """
            Invoke query() with extra argument
        """
        policy = {
            'timeout': 1000
        }
        query = TestQuery.client.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.equals('test_age', 1))
        def callback((key,metadata,record)):
            assert metadata['gen'] != None

        with pytest.raises(TypeError) as typeError:
            query.foreach(callback, policy, "")

        assert "foreach() takes at most 2 arguments (3 given)" in typeError.value

    def test_query_with_incorrect_policy_value(self):
        """
            Invoke query() with incorrect policy
        """
        policy = {
            'timeout': ""
        }
        query = TestQuery.client.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.equals('test_age', 1))
        def callback((key,metadata,record)):
            assert metadata['gen'] != None

        with pytest.raises(Exception) as exception:
            query.foreach(callback, policy)

        assert exception.value[0] == -2L
        assert exception.value[1] == 'timeout is invalid'

    def test_query_with_put_in_callback(self):
        """
            Invoke query() with put in callback
        """
        policy = {
            'timeout': 1000
        }
        query = TestQuery.client.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.equals('test_age', 1))
        records = []
        def callback((key,metadata,record)):
            records.append(record)
            key = ('test', 'demo', 'put_in_callback')
            rec = {
                    'name' : 'name%s' % (str(8)),
                    'test_age'  : 8,
                    }
            TestQuery.client.put(key, rec)

        query.foreach(callback, policy)

        key = ('test', 'demo', 'put_in_callback')
        key1, meta, bins = TestQuery.client.get( key )

        key = ('test', 'demo', 'put_in_callback')
        TestQuery.client.remove(key)
        assert bins == { 'test_age': 8, 'name': 'name8'}

    def test_query_with_correct_parameters_between(self):
        """
            Invoke query() with correct arguments and using predicate between
        """
        query = TestQuery.client.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.between('test_age', 1, 4))

        records = []
        def callback((key,metadata,record)):
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
        def callback((key,metadata,record)):
            val += 1
            records.append(key)

        result = query.foreach(callback)
        assert len(records) == 0

    def test_query_with_callback_returning_false(self):
        """
            Invoke query() with callback function returns false
        """
        query = TestQuery.client.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.between('test_age', 1, 5))

        records = []
        def callback((key,metadata,record)):
            if len(records) == 2:
                return False
            records.append(key)

        result = query.foreach(callback)
        assert len(records) == 2
