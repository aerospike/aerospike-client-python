# -*- coding: utf-8 -*-
import pytest
import time
import sys
from .as_status_codes import AerospikeStatus
from aerospike import exception as e
from aerospike import predicates as p

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
        client.remove(key)
    client.remove(('test', None, "no_set"))


def add_test_udf(client):
    policy = {}
    client.udf_put(u"query_apply.lua", 0, policy)


def drop_test_udf(client):
    client.udf_remove("query_apply.lua")


def remove_indexes_from_client(client):
    client.index_remove('test', 'test_demo_age_idx')
    client.index_remove('test', 'test_null_age_idx')


class TestQueryApply(object):

    # These functions will run once for this test class, and do all of the
    # required setup and teardown
    connection_setup_functions = (add_test_udf, add_indexes_to_client)
    connection_teardown_functions = (drop_test_udf, remove_indexes_from_client)
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
