# -*- coding: utf-8 -*-

import pytest
import sys
from .test_base_class import TestBaseClass
from .as_status_codes import AerospikeStatus
from aerospike import exception as e
from aerospike import predicates as p
from aerospike_helpers import expressions as exp
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

    def test_query_with_correct_parameters(self):
        """
            Invoke query() with correct arguments
        """
        query = self.as_connection.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.equals('test_age', 1))

        records = []

        def callback(input_tuple):
            key, _, _ = input_tuple
            records.append(key)

        query.foreach(callback)
        assert records
        assert len(records) == 1

    def test_query_with_incorrect_bin_name(self):
        """
            Invoke query() with predicate comparing using non-extant bin name
        """
        query = self.as_connection.query('test', 'demo')
        query.select('name1', 'age1')
        query.where(p.equals('age1', 1))
        records = []

        def callback(input_tuple):
            _, _, record = input_tuple
            records.append(record)

        query.foreach(callback)
        assert len(records) == 0

    def test_query_without_callback_parameter(self):
        """
            Invoke query.foreach() with without callback
        """
        query = self.as_connection.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.equals('test_age', 1))

        def callback(input_tuple):
            _, metadata, _ = input_tuple
            assert metadata['gen'] is None

        with pytest.raises(TypeError) as typeError:
            query.foreach()

        assert "argument 'callback'" in str(
            typeError.value)

    def test_query_with_nonindexed_bin(self):
        """
            Invoke query() with non-indexed bin
        """
        # with pytest.raises(Exception) as exception:
        with pytest.raises(e.IndexNotFound) as err_info:
            query = self.as_connection.query('test', 'demo')
            query.select('name', 'no')
            query.where(p.equals('no', 1))

            def callback(input_tuple):
                pass
            query.foreach(callback)

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_INDEX_NOT_FOUND

    def test_query_with_where_incorrect(self):
        """
            Invoke query() with predicate which
            matches 0 records
        """
        query = self.as_connection.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.equals('test_age', 165))
        records = []

        def callback(input_tuple):
            _, _, record = input_tuple
            records.append(record)

        query.foreach(callback)
        assert len(records) == 0

    def test_query_with_where_none_value(self):
        """
            Invoke query() with equality
            predicate comparing to None
            This is more of a predicate test

        """
        query = self.as_connection.query('test', 'demo')
        query.select('name', 'test_age')
        try:
            query.where(p.equals('test_age', None))

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == 'predicate is invalid.'

    def test_query_with_policy(self):
        """
            Invoke query() with policy
        """
        policy = {'timeout': 1000}
        query = self.as_connection.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.equals('test_age', 1))
        records = []

        def callback(input_tuple):
            _, _, record = input_tuple
            records.append(record)

        query.foreach(callback, policy)
        assert len(records) == 1
    
    @pytest.mark.xfail(reason="Query policies do not support rps")
    def test_query_with_policy_records_per_second(self):
        """
            Invoke query() with the records_per_second policy set
        """
        policy = {'timeout': 1000, 'records_per_second': 10}
        query = self.as_connection.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.equals('test_age', 1))
        records = []

        def callback(input_tuple):
            _, _, record = input_tuple
            records.append(record)

        query.foreach(callback, policy)
        assert len(records) == 1

    @pytest.mark.xfail(reason="Only pass with server version >= 3.15")
    def test_query_with_no_bins_option(self):
        """
            Invoke query() with policy
        """
        policy = {'total_timeout': 1000}
        options = {'nobins': True}
        query = self.as_connection.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.equals('test_age', 1))
        records = []

        def callback(input_tuple):
            _, _, bins = input_tuple
            print(bins)
            assert len(bins) == 0

        query.foreach(callback, policy, options=options)

    def test_query_with_invalid_options_argument_type(self):
        """
            Invoke query() with incorrect options type, should be a dictionary
        """
        policy = {'timeout': 1000}
        query = self.as_connection.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.equals('test_age', 1))

        def callback(input_tuple):
            _, metadata, _ = input_tuple
            assert metadata['gen'] is None

        with pytest.raises(e.ParamError):
            query.foreach(callback, policy, "")

    def test_query_with_invalid_nobins_value(self):
        """
            Invoke query() with options['nobins'] type
        """
        policy = {'total_timeout': 1000}
        query = self.as_connection.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.equals('test_age', 1))

        def callback(input_tuple):
            pass

        with pytest.raises(e.ParamError):
            query.foreach(callback, policy, {'nobins': 'False'})

    def test_query_with_put_in_callback(self):
        """
            Invoke query() with client.put in foreach callback
        """
        policy = {'timeout': 1000}
        query = self.as_connection.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.equals('test_age', 1))
        records = []

        def callback(input_tuple):
            _, _, record = input_tuple
            records.append(record)
            key = ('test', 'demo', 'put_in_callback')
            rec = {'name': 'name%s' % (str(8)), 'test_age': 8, }
            self.as_connection.put(key, rec)

        query.foreach(callback, policy)

        key = ('test', 'demo', 'put_in_callback')
        _, _, bins = self.as_connection.get(key)
        assert bins == {'test_age': 8, 'name': 'name8'}

        self.as_connection.remove(key)

    def test_query_with_correct_parameters_between(self):
        """
            Invoke query() with correct arguments and using predicate between
        """
        query = self.as_connection.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.between('test_age', 1, 4))

        records = []

        def callback(input_tuple):
            _, _, record = input_tuple
            records.append(record)

        query.foreach(callback)
        assert len(records) == 4

    def test_query_with_callback_returning_false(self):
        """
            Invoke query() with callback function returns false
            This will stop iteration
        """
        query = self.as_connection.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.between('test_age', 1, 5))
        lock = Lock()
        records = []

        def callback(input_tuple):
            key, _, _ = input_tuple
            with lock:
                if len(records) == 2:
                    return False
                records.append(key)

        query.foreach(callback)
        assert len(records) == 2

    def test_query_with_results_method(self):
        """
            Invoke query() with correct arguments
        """
        query = self.as_connection.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.equals('test_age', 1))

        records = query.results()
        assert len(records) == 1

    def test_query_with_results_method_and_expressions(self):
        """
            Invoke query() with correct arguments
        """
        expr = exp.Eq(exp.IntBin('test_age'), 1)

        policy = {
            'expressions': expr.compile()
        }

        query = self.as_connection.query('test', 'demo')
        query.select('name', 'test_age')

        records = query.results(policy)
        assert len(records) == 1

    def test_query_with_results_method_and_invalid_predexp(self):
        """
            Invoke query() with correct arguments
        """
        expr = exp.Eq(exp.IntBin('test_age'), 'bad_arg')

        policy = {
            'expressions': expr.compile()
        }

        query = self.as_connection.query('test', 'demo')
        query.select('name', 'test_age')

        with pytest.raises(e.InvalidRequest):
            query.results(policy)

    def test_query_with_results_nobins_options(self):
        """
            Invoke query() with correct arguments
        """
        query = self.as_connection.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.equals('test_age', 1))

        records = query.results(options={'nobins': True})
        assert len(records) == 1

    def test_query_with_results_invalid_options_type(self):
        """
            Invoke query() with correct arguments
        """
        query = self.as_connection.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.equals('test_age', 1))
        with pytest.raises(e.ParamError):
            records = query.results(options=False)

    def test_query_with_results_invalid_nobins_options(self):
        """
            Invoke query() with correct arguments
        """
        query = self.as_connection.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.equals('test_age', 1))
        with pytest.raises(e.ParamError):
            records = query.results(options={'nobins': "false"})

    def test_query_with_unicode_binnames_in_select_and_where(self):
        """
            Invoke query() with unicode bin names in select
        """
        query = self.as_connection.query('test', 'demo')
        query.select(u'name', u'test_age', 'addr')
        query.where(p.equals(u'test_age', 7))

        records = query.results()
        assert len(records) == 1
        assert records[0][2] == {
            'test_age': 7,
            'name': u'name7',
            'addr': u'name7'
        }

    @pytest.mark.parametrize(
        'predicate, expected_length',
        (
            (
                p.contains('numeric_list', aerospike.INDEX_TYPE_LIST, 1),
                2
            ),
            (
                p.contains('string_list', aerospike.INDEX_TYPE_LIST,
                           "str3"),
                3
            ),
            (
                p.contains('string_map', aerospike.INDEX_TYPE_MAPKEYS, "a"),
                5
            ),
            (
                p.contains('string_map', aerospike.INDEX_TYPE_MAPVALUES,
                           "a1"),
                1
            ),
            (
                p.contains('numeric_map', aerospike.INDEX_TYPE_MAPVALUES,
                           1),
                2
            ),
            (
                p.range('numeric_map', aerospike.INDEX_TYPE_MAPVALUES, 1,
                        3), 8
            ),
            (
                p.range('numeric_list', aerospike.INDEX_TYPE_LIST, 1,
                        3),
                8
            )
        ),
        ids=(
            'numeric_list contains',
            'string list contains',
            'string mapkeys contains',
            'string mapvalue contains',
            'numeric mapvalue contains',
            'numeric mapvalue range',
            'numeric list range'
        )
    )
    def test_query_with_correct_parameters_predicates(self,
                                                      predicate,
                                                      expected_length):
        """
            Invoke query() with correct arguments and using predicate contains
        """
        query = self.as_connection.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(predicate)

        records = []

        def callback(input_tuple):
            _, _, record = input_tuple
            records.append(record)

        query.foreach(callback)
        assert len(records) == expected_length

    def test_query_with_correct_parameters_predexp(self):
        """
            Invoke query() with correct arguments and using predexp
        """

        from .test_base_class import TestBaseClass
        if TestBaseClass.major_ver >= 5 and TestBaseClass.minor_ver >=7:
            # print("TestBaseClass.major_ver:", TestBaseClass.major_ver, "TestBaseClass.minor_ver:", TestBaseClass.minor_ver)
            pytest.skip(
                'It deprecated and it only applies to < 5.7 earlier and enterprise edition')

        expr = exp.Eq(exp.IntBin('test_age'), 4)

        query = self.as_connection.query('test', 'demo')
        query.select('name', 'test_age')
        #query.where(predicate)

        records = []

        def callback(input_tuple):
            _, _, record = input_tuple
            records.append(record)

        query.foreach(callback, {'expressions': expr.compile()})
        assert len(records) == 1
        assert records[0]['test_age'] == 4

    def test_query_with_multiple_foreach_on_same_query_object(self):
        """
            Invoke query() with multple foreach() call on same query object
        """
        query = self.as_connection.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.equals('test_age', 1))

        records = []

        def callback(input_tuple):
            key, _, _ = input_tuple
            records.append(key)

        query.foreach(callback)
        assert len(records) == 1

        query.foreach(callback)
        assert len(records) == 2

    #  All of these removed tests feature undocumented fragile behavior removed in 3.0.0
    def test_removed_query_with_correct_parameters_rangecontains_notuple(self):
        """
        """
        query = self.as_connection.query('test', 'demo')
        query.select('name', 'test_age')
        with pytest.raises(e.ParamError):
            query.where('numeric_map', "range", aerospike.INDEX_TYPE_MAPVALUES,
                        aerospike.INDEX_NUMERIC, 1, 3)

    def test_removed_query_with_correct_parameters_containsstring_mapvalues_notuple(
        self
    ):
        """
            Invoke query() with mapvalue string index
            and using predicate contains
        """
        query = self.as_connection.query('test', 'demo')
        query.select('name', 'test_age')
        with pytest.raises(e.ParamError):
            query.where('string_map', 'contains', aerospike.INDEX_TYPE_MAPVALUES,
                        aerospike.INDEX_STRING, "a1")

    def test_test_removed_query_containsstring_notuple(self):
        """
            Invoke query() with correct a
            and a string list index arguments and using predicate contains
        """
        query = self.as_connection.query('test', 'demo')
        query.select('name', 'test_age')
        with pytest.raises(e.ParamError):
            query.where('string_list', "contains", aerospike.INDEX_TYPE_LIST,
                        aerospike.INDEX_STRING, "str3")

    def test_removed_query_with_correct_parameters_between_notuple(self):
        """
            Invoke query() with correct arguments and using predicate between
        """
        query = self.as_connection.query('test', 'demo')
        query.select('name', 'test_age')
        with pytest.raises(e.ParamError):
            query.where('test_age', 'between', 1, 4)

    def test_between_predicate_between_one_arg(self):
        """
            Invoke query and using predicate between with invalid predicate
            arguments
        """
        query = self.as_connection.query('test', 'demo')
        with pytest.raises(e.ParamError):
            query.where('test_age', 'between', 1)

    def test_between_predicate_between_no_args(self):
        """
            Invoke query and using predicate between with invalid predicate
            arguments
        """
        query = self.as_connection.query('test', 'demo')
        query.select('name', 'test_age')
        with pytest.raises(e.ParamError):
            query.where('test_age', 'between')

    def test_query_with_policy_notuple(self):
        """
            Invoke query() with policy
        """
        policy = {'timeout': 1000}
        query = self.as_connection.query('test', 'demo')
        query.select('name', 'test_age')
        with pytest.raises(e.ParamError):
            query.where('test_age', 'equals', 1)

    def test_query_with_multiple_results_call_on_same_query_object(self):
        """
            Invoke query() with multple results() call on same query object
        """
        query = self.as_connection.query('test', 'demo')
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

    def test_query_with_policy_on_none_set_index(self):
        """
            Invoke query() with set specified as None
        """
        policy = {
            'timeout': 1000
        }
        query = self.as_connection.query('test', None)
        query.select('name', 'test_age_none')
        query.where(p.equals('test_age_none', 1))
        records = []

        def callback(input_tuple):
            _, _, record = input_tuple
            records.append(record)

        # This should only select records in the ns with no set
        # that match the where clause, if it matched all in the ns,
        # this would return 6
        query.foreach(callback, policy)
        assert len(records) == 1

    def test_query_with_only_ns(self):
        """
            Invoke query() with only ns specified
        """
        policy = {
            'timeout': 1000
        }
        query = self.as_connection.query('test')
        query.select('name', 'test_age_none')
        query.where(p.equals('test_age_none', 1))
        records = []

        def callback(input_tuple):
            _, _, record = input_tuple
            records.append(record)

        # This should only select records in the ns with no set
        # that match the where clause, if it matched all in the ns,
        # this would return 6
        query.foreach(callback, policy)
        assert len(records) == 1

    def test_query_with_select_bytearray(self):
        """
            Invoke query() with correct arguments
            and select and equality predicate comparing a byte array
            named bin
        """
        query = self.as_connection.query('test', 'demo')
        query.select(bytearray("sal\0kj", "utf-8"))
        query.where(p.equals(bytearray("sal\0kj", "utf-8"), 80000))

        records = []

        def callback(input_tuple):
            key, _, _ = input_tuple
            records.append(key)

        query.foreach(callback)
        assert records
        assert len(records) == 1

    def test_query_with_no_parameters(self):
        """
            Invoke query() without any mandatory parameters.
        """
        with pytest.raises(e.ParamError) as err_info:
            query = self.as_connection.query()

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_PARAM

    def test_query_with_incorrect_policy_value(self):
        """
            Invoke query() with invalid policy passed to foreach
        """
        policy = {'total_timeout': ""}
        query = self.as_connection.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.equals('test_age', 1))

        def callback(input_tuple):
            _, metadata, _ = input_tuple
            print(metadata)
            assert metadata['gen'] is None

        with pytest.raises(e.ParamError) as err_info:
            query.foreach(callback, policy)

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_PARAM

    def test_query_with_select_bin_integer(self):
        """
            Invoke query() with select bin is of integer type.
            Try to select a bin with an integer name instead of a string
        """
        query = self.as_connection.query('test', 'demo')

        with pytest.raises(e.ParamError) as err_info:
            query.select(22, 'test_age')

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_PARAM

    def test_query_with_argument_to_where_is_empty_string(self):

        query = self.as_connection.query('test', 'demo')
        query.select('name', 'test_age')

        with pytest.raises(e.ParamError) as err_info:
            query.where("")

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_PARAM

    def test_query_with_callback_contains_error(self):
        """
            Invoke query() with callback function contains an error
        """
        query = self.as_connection.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.equals('test_age', 1))

        records = []

        def callback(input_tuple):
            raise Exception("error")
            pass

        with pytest.raises(e.ClientError) as err_info:
            query.foreach(callback)

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_CLIENT

    def test_query_with_incorrect_ns_set(self):
        """
            Invoke query() with incorrect ns and set
            foreach should raise an error since the ns
            is invalid
        """
        with pytest.raises(e.ClientError) as err_info:
            query = self.as_connection.query('fake_namespace', 'demo1')

            def callback(input_tuple):
                '''
                no-op callback
                '''
                pass

            query.foreach(callback)

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_CLIENT

    def test_query_with_ns_not_string(self):
        """
            Invoke query() with incorrect ns and set. It
            should raises a param error.
        """
        with pytest.raises(e.ParamError) as err_info:
            query = self.as_connection.query(1, 'demo')

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_PARAM

    def test_query_with_set_int(self):
        """
            Invoke query() with set argument not a string
        """
        with pytest.raises(e.ParamError) as err_info:
            query = self.as_connection.query('test', 1)

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_PARAM

    def test_query_with_correct_parameters_without_connection(self):
        """
            Invoke query() with correct arguments without connection
        """
        config = {'hosts': [('127.0.0.1', 3000)]}
        client1 = aerospike.client(config)

        with pytest.raises(e.ClusterError) as err_info:
            query = client1.query('test', 'demo')
            query.select('name', 'test_age')
            query.where(p.equals('test_age', 1))

            def callback(input_tuple):
                pass

            query.foreach(callback)

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_CLUSTER_ERROR

    @pytest.mark.skip(reason="segfault")
    def test_query_predicate_range_wrong_no_args(self):
        """
        """
        query = self.as_connection.query('test', 'demo')
        query.select('name', 'test_age')
        query.where('numeric_map', "range", aerospike.INDEX_TYPE_MAPVALUES,
                    aerospike.INDEX_NUMERIC)

    @pytest.mark.skip(reason="segfault")
    def test_query_predicate_range_wrong_one_end_args(self):
        """
        """
        query = self.as_connection.query('test', 'demo')
        query.select('name', 'test_age')
        query.where('numeric_map', "range", aerospike.INDEX_TYPE_MAPVALUES,
                    aerospike.INDEX_NUMERIC, 1)
