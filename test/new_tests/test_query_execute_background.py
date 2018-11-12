# -*- coding: utf-8 -*-
import pytest
import time

import aerospike
from aerospike import exception, predexp, predicates

TEST_NS = 'test'
TEST_SET = 'background'
TEST_UDF_MODULE = 'query_apply'
TEST_UDF_FUNCTION = 'mark_as_applied'
# Hack to get long to exist in Python 3
try:
    long
except NameError:
    long = int

def add_indexes_to_client(client):
    try:
        client.index_integer_create(TEST_NS, TEST_SET, 'number',
                                    'test_background_number_idx')
    except exception.IndexFoundError:
        pass

def add_test_udf(client):
    policy = {}
    client.udf_put(u"query_apply.lua", 0, policy)


def drop_test_udf(client):
    client.udf_remove("query_apply.lua")


def remove_indexes_from_client(client):
    client.index_remove(TEST_NS, 'test_background_number_idx')

def validate_records(client, keys, validator):
    for key in keys:
        _, _, rec = client.get(key)
        assert validator(rec)

# Add records around the test
@pytest.fixture(scope='function')
def clean_test_background(as_connection):
    keys = [(TEST_NS, TEST_SET, i) for i in range(50)]
    for i, key in enumerate(keys):
        as_connection.put(key, {'number': i})
    yield

class TestQueryApply(object):

    # These functions will run once for this test class, and do all of the
    # required setup and teardown
    connection_setup_functions = (add_test_udf, add_indexes_to_client)
    connection_teardown_functions = (drop_test_udf, remove_indexes_from_client)

    @pytest.fixture(autouse=True)
    def setup(self, connection_with_config_funcs):
        pass

    def test_background_execute_return_val(self, clean_test_background):
        """
        Ensure that Query.execute_background() returns an int like object
        """
        test_bin = 't1'
        query = self.as_connection.query(TEST_NS, TEST_SET)
        query.apply(TEST_UDF_MODULE, TEST_UDF_FUNCTION, [test_bin])
        res = query.execute_background()
        assert isinstance(res, (int, long))

    def test_background_execute_no_predicate(self, clean_test_background):
        """
        Ensure that Query.execute_background() gets applied to all records
        """
        test_bin = 't2'
        keys = [(TEST_NS, TEST_SET, i) for i in range(50)]

        query = self.as_connection.query(TEST_NS, TEST_SET)
        query.apply(TEST_UDF_MODULE, TEST_UDF_FUNCTION, [test_bin])
        query.execute_background()
        # Give time for the query to finish
        time.sleep(5)
        validate_records(
            self.as_connection, keys,
            lambda rec: rec[test_bin] == 'aerospike'
        )

    def test_background_execute_sindex_predicate(self, clean_test_background):
        """
        Ensure that Query.execute_background() only applies to records matched by
        the specified predicate
        """
        test_bin = 't3'
        keys = [(TEST_NS, TEST_SET, i) for i in range(50)]
        number_predicate = predicates.equals('number', 5)

        query = self.as_connection.query(TEST_NS, TEST_SET)
        query.where(number_predicate)
        query.apply(TEST_UDF_MODULE, TEST_UDF_FUNCTION, [test_bin])
        query.execute_background()
        # Give time for the query to finish
        time.sleep(5)
        keys = [(TEST_NS, TEST_SET, i) for i in range(50) if i != 5]
        validate_records(
            self.as_connection, keys,
            lambda rec: test_bin not in rec
        )
        _, _, num_5_record = self.as_connection.get((TEST_NS, TEST_SET, 5))
        assert num_5_record[test_bin] == 'aerospike'

    def test_background_execute_sindex_predexp(self, clean_test_background):
        """
        Ensure that Query.execute_background() only applies to records matched by
        the specified predicate
        """
        test_bin = 't4'
        keys = [(TEST_NS, TEST_SET, i) for i in range(50)]

        #  rec['number'] < 10
        predexps = [predexp.integer_bin('number'), predexp.integer_value(10), predexp.integer_less()]

        query = self.as_connection.query(TEST_NS, TEST_SET)
        query.predexp(predexps)
        query.apply(TEST_UDF_MODULE, TEST_UDF_FUNCTION, [test_bin])
        query.execute_background()
        # Give time for the query to finish
        time.sleep(5)

        # Records with number > 10 should not have had the UDF applied
        validate_records(
            self.as_connection, keys[10:],
            lambda rec: test_bin not in rec
        )
        #  Records with number < 10 should have had the udf applied
        validate_records(
            self.as_connection, keys[:10],
            lambda rec: rec[test_bin] == 'aerospike'
        )

    def test_background_execute_with_policy(self, clean_test_background):
        """
        Ensure that Query.execute_background() returns an int like object
        """
        test_bin = 't5'
        query = self.as_connection.query(TEST_NS, TEST_SET)
        query.apply(TEST_UDF_MODULE, TEST_UDF_FUNCTION, [test_bin])
        res = query.execute_background({'socket_timeout': 10000})
        assert isinstance(res, (int, long))

    def test_background_execute_with_policy_kwarg(self, clean_test_background):
        """
        Ensure that Query.execute_background() returns an int like object
        """
        test_bin = 't6'
        query = self.as_connection.query(TEST_NS, TEST_SET)
        query.apply(TEST_UDF_MODULE, TEST_UDF_FUNCTION, [test_bin])
        res = query.execute_background(policy={})
        assert isinstance(res, (int, long))

    def test_background_execute_with_invalid_policy_type(self, clean_test_background):
        """
        Ensure that Query.execute_background() returns an int like object
        """
        test_bin = 't6'
        query = self.as_connection.query(TEST_NS, TEST_SET)
        query.apply(TEST_UDF_MODULE, TEST_UDF_FUNCTION, [test_bin])
        # Policy needs to be a dict. Not a string
        with pytest.raises(exception.ParamError):
            res = query.execute_background("Honesty is the best Policy")
