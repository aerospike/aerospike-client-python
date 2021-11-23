# -*- coding: utf-8 -*-

import pytest
import sys
from .test_base_class import TestBaseClass
from .as_status_codes import AerospikeStatus
from aerospike import exception as e
from aerospike import predicates as p
from aerospike import predexp as as_predexp
from threading import Lock

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)


class TestQuery(TestBaseClass):

    def setup_class(cls):
        client = TestBaseClass.get_new_connection()

        try:
            client.index_integer_create('test', 'demo', 'test_age',
                                        'age_index')
        except e.IndexFoundError:
            pass

        try:
            client.index_string_create('test', 'demo', 'addr',
                                       'addr_index')
        except e.IndexFoundError:
            pass

        try:
            client.index_integer_create('test', 'demo', 'age1',
                                        'age_index1')
        except e.IndexFoundError:
            pass

        try:
            client.index_list_create('test', 'demo', 'numeric_list',
                                     aerospike.INDEX_NUMERIC,
                                     'numeric_list_index')
        except e.IndexFoundError:
            pass

        try:
            client.index_list_create('test', 'demo', 'string_list',
                                     aerospike.INDEX_STRING,
                                     'string_list_index')
        except e.IndexFoundError:
            pass

        try:
            client.index_map_keys_create('test', 'demo', 'numeric_map',
                                         aerospike.INDEX_NUMERIC,
                                         'numeric_map_index')
        except e.IndexFoundError:
            pass

        try:
            client.index_map_keys_create('test', 'demo', 'string_map',
                                         aerospike.INDEX_STRING,
                                         'string_map_index')
        except e.IndexFoundError:
            pass

        try:
            client.index_map_values_create('test', 'demo', 'numeric_map',
                                           aerospike.INDEX_NUMERIC,
                                           'numeric_map_values_index')
        except e.IndexFoundError:
            pass

        try:
            client.index_map_values_create('test', 'demo', 'string_map',
                                           aerospike.INDEX_STRING,
                                           'string_map_values_index')
        except e.IndexFoundError:
            pass

        try:
            client.index_integer_create('test', None, 'test_age_none',
                                        'age_index_none')
        except e.IndexFoundError:
            pass

        try:
            client.index_integer_create('test', 'demo',
                                        bytearray("sal\0kj", "utf-8"),
                                        'sal_index')
        except e.IndexFoundError:
            pass

        client.close()

    def teardown_class(cls):
        client = TestBaseClass.get_new_connection()

        policy = {}
        try:
            client.index_remove('test', 'age_index', policy)
        except e.IndexNotFound:
            pass

        try:
            client.index_remove('test', 'age_index1', policy)
        except e.IndexNotFound:
            pass

        try:
            client.index_remove('test', 'addr_index', policy)
        except e.IndexNotFound:
            pass

        try:
            client.index_remove('test', 'numeric_list_index', policy)
        except e.IndexNotFound:
            pass

        try:
            client.index_remove('test', 'string_list_index', policy)
        except e.IndexNotFound:
            pass

        try:
            client.index_remove('test', 'numeric_map_index', policy)
        except e.IndexNotFound:
            pass

        try:
            client.index_remove('test', 'string_map_index', policy)
        except e.IndexNotFound:
            pass

        try:
            client.index_remove('test', 'numeric_map_values_index', policy)
        except e.IndexNotFound:
            pass

        try:
            client.index_remove('test', 'string_map_values_index', policy)
        except e.IndexNotFound:
            pass

        try:
            client.index_remove('test', 'age_index_none', policy)
        except e.IndexNotFound:
            pass

        try:
            client.index_remove('test', 'sal_index')
        except e.IndexNotFound:
            pass
        client.close()

    @pytest.fixture(autouse=True)
    def setup_method(self, request, as_connection):
        """
        Setup method.
        """
        for i in range(5):
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
                'test_age_none': 1,
                'test_age': i,
                'no': i
            }
            as_connection.put(key, rec)
        for i in range(5, 10):
            key = ('test', 'demo', i)
            rec = {
                u'name': 'name%s' % (str(i)),
                u'addr': u'name%s' % (str(i)),
                u'test_age': i,
                u'no': i
            }
            as_connection.put(key, rec)

        key = ('test', 'demo', 122)
        llist = [{"op": aerospike.OPERATOR_WRITE,
                  "bin": bytearray("sal\0kj", "utf-8"),
                  "val": 80000}]
        as_connection.operate(key, llist)

        key = ('test', None, 145)
        rec = {'test_age_none': 1}
        as_connection.put(key, rec)

        def teardown():
            """
            Teardown method.
            """
            for i in range(10):
                key = ('test', 'demo', i)
                as_connection.remove(key)

            key = ('test', 'demo', 122)
            as_connection.remove(key)
            key = ('test', None, 145)
            as_connection.remove(key)

        request.addfinalizer(teardown)

    def test_query_with_results_method_and_predexp(self):
        """
            Invoke query() with correct arguments
        """
        from .test_base_class import TestBaseClass
        if TestBaseClass.major_ver >= 5 and TestBaseClass.minor_ver >=7:
            # print("TestBaseClass.major_ver:", TestBaseClass.major_ver, "TestBaseClass.minor_ver:", TestBaseClass.minor_ver)
            pytest.skip(
                'It deprecated and it only applies to < 5.7 earlier and enterprise edition')

        predexp = [
            as_predexp.integer_bin('test_age'),
            as_predexp.integer_value(1),
            as_predexp.integer_equal()
        ]

        policy = {
            'predexp': predexp
        }

        query = self.as_connection.query('test', 'demo')
        query.select('name', 'test_age')

        records = query.results(policy)
        assert len(records) == 1

    def test_query_with_results_method_and_invalid_predexp(self):
        """
            Invoke query() with correct arguments
        """
        predexp = [
            as_predexp.integer_bin('test_age'),
            as_predexp.integer_value('1'),
            as_predexp.integer_equal()
        ]

        policy = {
            'predexp': predexp
        }

        query = self.as_connection.query('test', 'demo')
        query.select('name', 'test_age')

        with pytest.raises(e.ParamError):
            query.results(policy)

    def test_query_with_correct_parameters_predexp(self):
        """
            Invoke query() with correct arguments and using predexp
        """

        from .test_base_class import TestBaseClass
        if TestBaseClass.major_ver >= 5 and TestBaseClass.minor_ver >=7:
            # print("TestBaseClass.major_ver:", TestBaseClass.major_ver, "TestBaseClass.minor_ver:", TestBaseClass.minor_ver)
            pytest.skip(
                'It deprecated and it only applies to < 5.7 earlier and enterprise edition')

        predexp = [
            as_predexp.integer_bin('test_age'),
            as_predexp.integer_value(4),
            as_predexp.integer_equal(),
        ]

        query = self.as_connection.query('test', 'demo')
        query.select('name', 'test_age')
        #query.where(predicate)

        records = []

        def callback(input_tuple):
            _, _, record = input_tuple
            records.append(record)

        query.foreach(callback, {'predexp': predexp})
        assert len(records) == 1
        assert records[0]['test_age'] == 4

    @pytest.mark.parametrize(
        "func",
        [
            as_predexp.integer_value,
            as_predexp.predexp_and,
            as_predexp.predexp_or,
            as_predexp.rec_digest_modulo,
        ])
    def test_with_wrong_predicate_argument_type_expecting_int(self, func):
        '''
        These functions all expect an integer argument, call with a string
        '''
        predexps = [
            func("five")
        ]

        def callback(input_tuple):
            _, _, record = input_tuple
            records.append(record)

        query = self.as_connection.query('test', 'demo')
        with pytest.raises(e.ParamError):
            query.foreach(callback, {'predexp': predexps})
