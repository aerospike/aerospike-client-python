# -*- coding: utf-8 -*-
import pytest
import time
import sys
import pickle
from .as_status_codes import AerospikeStatus
from aerospike import exception as e
from aerospike import predicates as p
from aerospike_helpers import expressions as exp

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)


def add_indexes_to_client(client):
    try:
        client.index_integer_create('test', 'demo', 'age',
                                    'test_demo_age_idx')
    except e.IndexFoundError:
        pass

    try:
        client.index_integer_create('test', None, 'age',
                                    'test_null_age_idx')
    except e.IndexFoundError:
        pass


def create_records(client):
    for i in range(1, 10):
        key = ('test', 'demo', i)
        rec = {'name': str(i), 'age': i, 'val': i}
        client.put(key, rec)

    key = ('test', None, "no_set")
    rec = {'name': 'no_set_name', 'age': 0}
    client.put(key, rec)


def drop_records(client):
    for i in range(1, 10):
        key = ('test', 'demo', i)
        try:
            client.remove(key)
        except e.RecordNotFound:
            pass

    try:
        client.remove(('test', None, "no_set"))
    except e.RecordNotFound:
        pass


def add_test_udf(client):
    policy = {}
    client.udf_put(u"query_apply.lua", 0, policy)


def drop_test_udf(client):
    client.udf_remove(u"query_apply.lua")


def add_test_parameter_udf(client):
    policy = {}
    client.udf_put(u"query_apply_parameters.lua", 0, policy)


def drop_test_parameter_udf(client):
    client.udf_remove(u"query_apply_parameters.lua")


def remove_indexes_from_client(client):
    client.index_remove('test', 'test_demo_age_idx')
    client.index_remove('test', 'test_null_age_idx')


class TestQueryApply(object):

    # These functions will run once for this test class, and do all of the
    # required setup and teardown
    connection_setup_functions = (add_test_udf, add_test_parameter_udf,
     add_indexes_to_client, create_records)
    connection_teardown_functions = (drop_test_udf, drop_test_parameter_udf,
     remove_indexes_from_client, drop_records)
    age_range_pred = p.between('age', 0, 4)  # Predicate for ages between [0,5)
    no_set_key = ('test', None, "no_set")  # Key for item stored in a namespace but not in a set

    @pytest.fixture(autouse=True)
    def setup(self, request, connection_with_config_funcs):
        client = connection_with_config_funcs
        create_records(client)

    def test_query_apply_with_no_parameters(self):
        """
        Invoke query_apply() without any mandatory parameters.
        It should raise a type error as the wrong parameters are passed
        """
        with pytest.raises(TypeError) as typeError:
            self.as_connection.query_apply()

    def test_query_apply_with_correct_parameters_no_policy(self):
        """
        Invoke query_apply() with correct parameters.
        It should apply the proper UDF, and
        """
        query_id = self.as_connection.query_apply(
            "test", "demo", self.age_range_pred, "query_apply",
            "mark_as_applied", ['name', 2])

        self._wait_for_query_complete(query_id)

        self._correct_items_have_been_applied()

    def test_query_apply_with_correct_policy(self):
        """
        Invoke query_apply() with correct policy
        """
        policy = {'total_timeout': 0}
        query_id = self.as_connection.query_apply(
            "test", "demo", self.age_range_pred, "query_apply",
            "mark_as_applied", ['name', 2], policy)

        self._wait_for_query_complete(query_id)
        self._correct_items_have_been_applied()

    def test_query_apply_with_new_expressions(self):
        """
        Invoke query_apply() with correct policy and expressions
        """

        expr = exp.Or(
            exp.Eq(exp.IntBin('age'), 2),
            exp.Eq(exp.IntBin('val'), 3)
        )

        policy = {'total_timeout': 0, 'expressions': expr.compile()}
        query_id = self.as_connection.query_apply(
            "test", "demo", self.age_range_pred, "query_apply",
            "mark_as_applied", ['name', 2], policy)

        self._wait_for_query_complete(query_id)

        recs = []

        for i in range(1, 10):
            key = ('test', 'demo', i)
            _, _, bins = self.as_connection.get(key)
            if bins['name'] == 'aerospike':
                recs.append(bins)
        
        assert len(recs) == 2
        for rec in recs:
            assert rec['age'] == 2 or rec['val'] == 3

    def test_query_apply_with_bad_new_expressions(self):
        """
        Invoke query_apply() with incorrect policy and expressions
        """

        expr = exp.Or(
            exp.Eq(exp.IntBin(5), 2),
            exp.Eq(exp.IntBin('val'), 3)
        )

        policy = {'total_timeout': 0, 'expressions': expr}
        with pytest.raises(e.ParamError):
            query_id = self.as_connection.query_apply(
                "test", "demo", self.age_range_pred, "query_apply",
                "mark_as_applied", ['name', 2], policy)

    def test_query_apply_with_set_argument_as_none(self):
        """
        Invoke query_apply() with correct policy,
        Should casuse no changes as the
        """
        policy = {'total_timeout': 0}
        query_id = self.as_connection.query_apply(
            "test", None, self.age_range_pred, "query_apply",
            "mark_as_applied", ['name', 2], policy)

        self._wait_for_query_complete(query_id)
        self._items_without_set_have_been_applied()

    def test_query_apply_with_incorrect_policy(self):
        """
        Invoke query_apply() with incorrect policy
        """
        policy = {
            'timeout': 0.5
        }

        with pytest.raises(e.ParamError):
            self.as_connection.query_apply(
                "test", "demo", self.age_range_pred, "query_apply",
                "mark_as_applied", ['name', 2], policy)

    def test_query_apply_with_nonexistent_set(self):
        """
        Invoke query_apply() with incorrect ns and set
        """
        with pytest.raises(e.NamespaceNotFound):
            self.as_connection.query_apply(
                "test1", "demo1", self.age_range_pred, "query_apply",
                "mark_as_applied", ['name', 2])

    def test_query_apply_with_incorrect_module_name(self):
        """
        Invoke query_apply() with incorrect module name
        Test that passing an incorrect lua_module does
        not call a function
        """
        query_id = self.as_connection.query_apply(
            "test", "demo", self.age_range_pred, "query_apply_incorrect",
            "mark_as_applied", ['name', 2])

        self._wait_for_query_complete(query_id)
        self._no_items_have_been_applied()

    def test_query_apply_with_incorrect_function_name(self):
        """
        Invoke query_apply() with incorrect function name,
        does not invoke a different function
        """
        query_id = self.as_connection.query_apply(
            "test", "demo", self.age_range_pred, "query_apply",
            "mytransform_incorrect", ['name', 2])

        self._wait_for_query_complete(query_id)
        self._no_items_have_been_applied()

    def test_query_apply_with_ns_set_none(self):
        """
        Invoke query_apply() with ns and set as None
        """
        with pytest.raises(TypeError):
            self.as_connection.query_apply(None, None, self.age_range_pred,
                                           "query_apply", "mark_as_applied",
                                           ['name', 2])

    def test_query_apply_with_module_argument_value_is_none(self):
        """
        Invoke query_apply() with None module function
        """

        with pytest.raises(e.ParamError):
            self.as_connection.query_apply(
                "test", "demo", self.age_range_pred, None, None, ['name', 2])

    def test_query_apply_with_too_many_arguments(self):
        """
        Invoke query_apply() with extra argument
        """
        policy = {'timeout': 1000}
        with pytest.raises(TypeError):
            self.as_connection.query_apply(
                "test", "demo", self.age_range_pred, "query_apply",
                "mytransform_incorrect", ['name', 2], policy, "")

    def test_query_apply_with_udf_arguments_as_string(self):
        """
        Invoke query_apply() with arguments as string
        """
        with pytest.raises(e.ParamError):
            self.as_connection.query_apply("test", "demo", self.age_range_pred,
                                           "query_apply", "mark_as_applied", "")

    def test_query_apply_with_udf_argument_as_none(self):
        """
        Invoke query_apply() with arguments as None
        """
        with pytest.raises(e.ParamError):
            self.as_connection.query_apply(
                "test", "demo", self.age_range_pred,
                "query_apply", "mark_as_applied", None)

    def test_query_apply_with_extra_parameter_to_lua_function(self):
        """
        Invoke query_apply() with extra call to lua
        test that passing an extra argument to a udf does
        not cause the function to fail
        """
        query_id = self.as_connection.query_apply(
            "test", "demo", self.age_range_pred,
            "query_apply", "mark_as_applied", ['name', 2, 3])

        # time.sleep(2)

        self._wait_for_query_complete(query_id)
        self._correct_items_have_been_applied()

    def test_query_apply_with_missing_parameter_to_function(self):
        """
        Invoke query_apply() with a missing argument
        to a lua function does not cause an error
        """
        query_id = self.as_connection.query_apply(
            "test", "demo", self.age_range_pred,
            "query_apply", "mark_as_applied_three_arg", ['name', 2])

        # time.sleep(2)

        self._wait_for_query_complete(query_id)
        self._correct_items_have_been_applied()

    def test_query_apply_unicode_literal_for_strings(self):
        """
        Invoke query_apply() with unicode udf
        """
        query_id = self.as_connection.query_apply(
            "test", "demo", self.age_range_pred, u"query_apply",
            u"mark_as_applied", ['name', 2])

        self._wait_for_query_complete(query_id)
        self._correct_items_have_been_applied()

    def test_query_apply_with_correct_parameters_without_connection(self):
        """
        Invoke query_apply() with correct parameters without connection
        """
        config = {'hosts': [('127.0.0.1', 3000)]}
        client1 = aerospike.client(config)

        with pytest.raises(e.ClusterError) as err_info:
            client1.query_apply("test", "demo", self.age_range_pred,
                                "query_apply", "mark_as_applied", ['name', 2])

        err_code = err_info.value.code

        assert err_code == AerospikeStatus.AEROSPIKE_CLUSTER_ERROR

    @pytest.mark.parametrize(
        "predicate",
        (
            (),  # Raises system error
            None,
            ('age'),
            ('age', 1),
            ('age', 1, 5),
            (1, 1, 'bin')  # start of a valid predicate
        )
    )
    def test_invalid_predicate_tuple(self, predicate):

        with pytest.raises(e.ParamError) as err_info:
            query_id = self.as_connection.query_apply(
                "test", "demo", predicate, "query_apply",
                "mark_as_applied", ['name', 2])

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_PARAM

    def test_stream_udf_parameters(self):
        """
        Invoke query.apply() with a stream udf. 
        that accepts additional arguments.
        """
        query_results = self.as_connection.query(
            "test", "demo",
        ).apply(
            'query_apply_parameters', 'query_params', [['age', 5]]
        ).results()
        
        query_results.sort()
        assert query_results == [6,7,8,9]

    def test_stream_udf_parameters_with_set(self):
        """
        Invoke query.apply() with a stream udf. 
        arguments contain an unsuported set. 
        """
        with pytest.raises(e.ClientError) as err_info:
            query_id = self.as_connection.query(
                "test", "demo",
            ).apply(
                'query_apply_parameters', 'query_params', [['job_type', 'job_type', 18],
                ['id', ['john', {'id', 'args', 'kwargs', 'john'}, ['john', {'mary' : 39}]]], []]
            )
        
        err_text = err_info.value.msg
        assert 'udf function argument type must be supported by Aerospike' in err_text
        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_CLIENT

        with pytest.raises(e.ClientError) as err_info_dict:
            query_id = self.as_connection.query(
                "test", "demo",
            ).apply(
                'query_apply_parameters', 'query_params', [['job_type', 'job_type', 18],
                ['id', ['john', ['john', {'mary' : 39, 'ken': {'lary', 'quinton', 'julie', 'mark'}}]]], []]
            )
        
        err_text = err_info_dict.value.msg
        assert 'udf function argument type must be supported by Aerospike' in err_text
        err_code = err_info_dict.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_CLIENT
    

    def test_stream_udf_parameters_with_tuple(self):
        """
        Invoke query.apply() with a stream udf. 
        arguments contain an unsuported tuple. 
        """
        with pytest.raises(e.ClientError) as err_info:
            query_id = self.as_connection.query(
                "test", "demo",
            ).apply(
                'query_apply_parameters', 'query_params', [['job_type', 'job_type', 18],
                ['id', ['john', ('id', 'args'), ['john', {'mary' : 39}]]], []]
            )
        
        err_text = err_info.value.msg
        assert 'udf function argument type must be supported by Aerospike' in err_text
        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_CLIENT


    def test_stream_udf_parameters_with_string(self):
        """
        Invoke query.apply() with a stream udf. 
        arguments contain a string not wrapped in a list.
        This should cause an exception.
        """
        with pytest.raises(e.ClientError) as err_info:
            query_id = self.as_connection.query(
                "test", "demo",
            ).apply(
                'query_apply_parameters', 'query_params', 'age'
            )
        
        err_text = err_info.value.msg
        assert 'udf function arguments must be enclosed in a list' in err_text
        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_CLIENT

    def test_stream_udf_parameters_with_serialized_set(self):
        """
        Invoke query.apply() with a stream udf. 
        arguments contain a serialized set.
        """
        query_results = self.as_connection.query(
            "test", "demo",
        ).apply(
            'query_apply_parameters', 'query_params', [['age', 5]
            ,pickle.dumps({'lary', 'quinton', 'julie', 'mark'})]
        ).results()
        
        query_results.sort()
        assert query_results == [6,7,8,9]

    def test_stream_udf_complicated_parameters(self):
        """
        Invoke query.apply() with a stream udf. 
        that accepts additional arguments.
        """
        query_results = self.as_connection.query(
            "test", "demo",
        ).apply(
            'query_apply_parameters', 'query_params', [['age', 2],
            ['id', ['john', ['hi']], ['john', {'mary' : 39}]], []]
        ).results()
        
        query_results.sort()
        assert query_results == [3,4,5,6,7,8,9]

    def _correct_items_have_been_applied(self):
        for i in range(1, 5):
            key = ('test', 'demo', i)
            _, _, bins = self.as_connection.get(key)
            assert bins['name'] == 'aerospike'

        for i in range(5, 10):
            key = ('test', 'demo', i)
            _, _, bins = self.as_connection.get(key)
            assert bins['name'] != 'aerospike'

        _, _, bins = self.as_connection.get(self.no_set_key)
        assert bins['name'] != 'aerospike'

    def _items_without_set_have_been_applied(self):
        for i in range(1, 10):
            key = ('test', 'demo', i)
            _, _, bins = self.as_connection.get(key)
            assert bins['name'] != 'aerospike'

        _, _, bins = self.as_connection.get(self.no_set_key)
        assert bins['name'] == 'aerospike'

    def _wait_for_query_complete(self, query_id):
        while True:
            response = self.as_connection.job_info(
                query_id, aerospike.JOB_QUERY)
            if response['status'] != aerospike.JOB_STATUS_INPROGRESS:
                return
            time.sleep(0.1)

    def _no_items_have_been_applied(self):

        for i in range(1, 10):
            key = ('test', 'demo', i)
            _, _, bins = self.as_connection.get(key)
            assert bins['name'] != 'aerospike'

        _, _, bins = self.as_connection.get(self.no_set_key)
        assert bins['name'] != 'aerospike'
