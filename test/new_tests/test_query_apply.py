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
    client.index_integer_create('test', 'demo', 'age',
                                'test_demo_age_idx')
    client.index_integer_create('test', None, 'age',
                                'test_null_age_idx')


def add_test_udf(client):
    policy = {}
    client.udf_put(u"bin_lua.lua", 0, policy)


def drop_test_udf(client):
    client.udf_remove("bin_lua.lua")


def remove_indexes_from_client(client):
    client.index_remove('test', 'test_demo_age_idx')
    client.index_remove('test', 'test_null_age_idx')


class TestQueryApply(object):

    # These functions will run once for this test class, and do all of the
    # required setup and teardown
    connection_setup_functions = (add_test_udf, add_indexes_to_client)
    connection_teardown_functions = (drop_test_udf, remove_indexes_from_client)
    age_range_pred = p.between('age', 0, 5)  # Predicate for ages between [0,5)

    @pytest.fixture(autouse=True)
    def setup(self, request, connection_with_config_funcs):
        client = connection_with_config_funcs
        for i in range(1, 5):
            key = ('test', 'demo', i)
            rec = {'name': 'name%s' % (str(i)), 'age': i}
            client.put(key, rec)

        key = ('test', None, "no_set")
        self.no_set_key = key
        rec = {'name': 'no_set_name', 'age': 0}
        client.put(key, rec)

        def teardown():
            for i in range(1, 5):
                key = ('test', 'demo', i)
                client.remove(key)

            client.remove(self.no_set_key)

        request.addfinalizer(teardown)

    def test_query_apply_with_no_parameters(self):
        """
        Invoke query_apply() without any mandatory parameters.
        It should raise a type error as the wrong parameters are passed
        """
        with pytest.raises(TypeError) as typeError:
            self.as_connection.query_apply()

    def test_query_apply_with_correct_parameters(self):
        """
        Invoke query_apply() with correct parameters.
        It should apply the proper UDF, and
        """
        query_id = self.as_connection.query_apply(
            "test", "demo", self.age_range_pred, "bin_lua",
            "mytransform", ['age', 2])

        time.sleep(0.1)
        while True:
            response = self.as_connection.job_info(
                query_id, aerospike.JOB_QUERY)
            if response['status'] != aerospike.JOB_STATUS_INPROGRESS:
                break
            time.sleep(0.1)

        for i in range(1, 5):
            key = ('test', 'demo', i)
            _, _, bins = self.as_connection.get(key)

            assert bins['age'] == i + 2

        _, _, bins = self.as_connection.get(self.no_set_key)
        assert bins['age'] == 0

    def test_query_apply_with_correct_policy(self):
        """
        Invoke query_apply() with correct policy
        """
        policy = {'timeout': 1000}
        query_id = self.as_connection.query_apply(
            "test", "demo", self.age_range_pred, "bin_lua",
            "mytransform", ['age', 2], policy)

        time.sleep(0.1)
        while True:
            response = self.as_connection.job_info(
                query_id, aerospike.JOB_QUERY)
            if response['status'] != aerospike.JOB_STATUS_INPROGRESS:
                break
            time.sleep(0.1)

        for i in range(1, 5):
            key = ('test', 'demo', i)
            _, _, bins = self.as_connection.get(key)
            assert bins['age'] == i + 2

        _, _, bins = self.as_connection.get(self.no_set_key)
        assert bins['age'] == 0

    def test_query_apply_with_none_set(self):
        """
        Invoke query_apply() with correct policy,
        Should casuse no changes as the
        """
        policy = {'timeout': 1000}
        query_id = self.as_connection.query_apply(
            "test", None, self.age_range_pred, "bin_lua",
            "mytransform", ['age', 2], policy)

        time.sleep(0.1)
        while True:
            response = self.as_connection.job_info(
                query_id, aerospike.JOB_QUERY)
            if response['status'] != aerospike.JOB_STATUS_INPROGRESS:
                break
            time.sleep(0.1)

        # Since this query passes no set, the records stored in a set should
        # not be affected
        for i in range(1, 5):
            key = ('test', 'demo', i)
            _, _, bins = self.as_connection.get(key)
            assert bins['age'] == i

        # the setless record should have been changed
        _, _, bins = self.as_connection.get(self.no_set_key)
        assert bins['age'] == 2

    def test_query_apply_with_incorrect_policy(self):
        """
        Invoke query_apply() with incorrect policy
        """
        policy = {
            'timeout': 0.5
        }

        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.query_apply(
                "test", "demo", self.age_range_pred, "bin_lua",
                "mytransform", ['age', 2], policy)

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_PARAM

    def test_query_apply_with_incorrect_ns_set(self):
        """
        Invoke query_apply() with incorrect ns and set
        """
        with pytest.raises(e.NamespaceNotFound) as err_info:
            self.as_connection.query_apply(
                "test1", "demo1", self.age_range_pred, "bin_lua",
                "mytransform", ['age', 2])

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_NAMESPACE_NOT_FOUND

    def test_query_apply_with_incorrect_module_name(self):
        """
        Invoke query_apply() with incorrect module name
        Test that passing an incorrect lua_module does
        not call a function
        """
        query_id = self.as_connection.query_apply(
            "test", "demo", self.age_range_pred, "bin_lua_incorrect",
            "mytransform", ['age', 2])

        time.sleep(0.1)
        while True:
            response = self.as_connection.job_info(
                query_id, aerospike.JOB_QUERY)
            if response['status'] != aerospike.JOB_STATUS_INPROGRESS:
                break
            time.sleep(0.1)

        for i in range(1, 5):
            key = ('test', 'demo', i)
            _, _, bins = self.as_connection.get(key)
            assert bins['age'] == i

    def test_query_apply_with_incorrect_function_name(self):
        """
        Invoke query_apply() with incorrect function name,
        does not invoke a different function
        """
        query_id = self.as_connection.query_apply(
            "test", "demo", self.age_range_pred, "bin_lua",
            "mytransform_incorrect", ['age', 2])

        time.sleep(0.1)
        while True:
            response = self.as_connection.job_info(
                query_id, aerospike.JOB_QUERY)
            if response['status'] != aerospike.JOB_STATUS_INPROGRESS:
                break
            time.sleep(0.1)

        for i in range(1, 5):
            key = ('test', 'demo', i)
            _, _, bins = self.as_connection.get(key)
            assert bins['age'] == i

    def test_query_apply_with_ns_set_none(self):
        """
        Invoke query_apply() with ns and set as None
        """
        with pytest.raises(TypeError) as typeError:
            self.as_connection.query_apply(None, None, self.age_range_pred,
                                           "bin_lua", "mytransform",
                                           ['age', 2])

        assert "query_apply() argument 1 must be str" in str(typeError.value)

    def test_query_apply_with_module_function_none(self):
        """
        Invoke query_apply() with None module function
        """

        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.query_apply(
                "test", "demo", self.age_range_pred, None, None, ['age', 2])

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_PARAM

    def test_query_apply_with_extra_argument(self):
        """
        Invoke query_apply() with extra argument
        """
        policy = {'timeout': 1000}
        with pytest.raises(TypeError) as typeError:
            self.as_connection.query_apply(
                "test", "demo", self.age_range_pred, "bin_lua",
                "mytransform_incorrect", ['age', 2], policy, "")

        assert "query_apply() takes at most 7 arguments (8 given)" in str(
            typeError.value)

    def test_query_apply_with_argument_is_string(self):
        """
        Invoke query_apply() with arguments as string
        """
        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.query_apply("test", "demo", self.age_range_pred,
                                           "bin_lua", "mytransform", "")

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_PARAM

    def test_query_apply_with_argument_is_none(self):
        """
        Invoke query_apply() with arguments as None
        """
        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.query_apply(
                "test", "demo", self.age_range_pred,
                "bin_lua", "mytransform", None)

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_PARAM

    def test_query_apply_with_extra_call_to_lua(self):
        """
        Invoke query_apply() with extra call to lua
        test that passing an extra argument to a udf does
        not cause the function to fail
        """
        query_id = self.as_connection.query_apply(
            "test", "demo", self.age_range_pred,
            "bin_lua", "mytransform", ['age', 2, 3])

        # time.sleep(2)

        time.sleep(0.1)
        while True:
            response = self.as_connection.job_info(
                query_id, aerospike.JOB_QUERY)
            if response['status'] != aerospike.JOB_STATUS_INPROGRESS:
                break
            time.sleep(0.1)

        for i in range(1, 5):
            key = ('test', 'demo', i)
            _, _, bins = self.as_connection.get(key)
            assert bins['age'] == i + 2

    def test_query_apply_with_extra_parameter_in_lua(self):
        """
        Invoke query_apply() with a missing argument
        to a lua function does not cause an error
        """
        query_id = self.as_connection.query_apply(
            "test", "demo", self.age_range_pred,
            "bin_lua", "mytransformextra", ['age', 2])

        # time.sleep(2)

        time.sleep(0.1)
        while True:
            response = self.as_connection.job_info(
                query_id, aerospike.JOB_QUERY)
            if response['status'] != aerospike.JOB_STATUS_INPROGRESS:
                break
            time.sleep(0.1)

        for i in range(1, 5):
            key = ('test', 'demo', i)
            _, _, bins = self.as_connection.get(key)
            assert bins['age'] == i + 2

    def test_query_apply_with_less_parameter_in_lua(self):
        """
        Invoke query_apply() with less parameter in lua
        this verifies that passing 3 arguments to a lua
        function which expects 2, will cause an undefined int
        to be treated as 0, and not crash.
        """
        query_id = self.as_connection.query_apply(
            "test", "demo", self.age_range_pred,
            "bin_lua", "mytransformless", ['age', 2])

        # time.sleep(2)

        time.sleep(0.1)
        while True:
            response = self.as_connection.job_info(
                query_id, aerospike.JOB_QUERY)
            if response['status'] != aerospike.JOB_STATUS_INPROGRESS:
                break
            time.sleep(0.1)

        for i in range(1, 5):
            key = ('test', 'demo', i)
            _, _, bins = self.as_connection.get(key)
            if bins['age'] != i:
                assert True is False
            else:
                assert True is True

    def test_query_apply_unicode_input(self):
        """
        Invoke query_apply() with unicode udf
        """
        query_id = self.as_connection.query_apply(
            "test", "demo", self.age_range_pred, u"bin_lua",
            u"mytransform", ['age', 2])

        time.sleep(0.1)
        while True:
            response = self.as_connection.job_info(
                query_id, aerospike.JOB_QUERY)
            if response['status'] != aerospike.JOB_STATUS_INPROGRESS:
                break
            time.sleep(0.1)

        for i in range(1, 5):
            key = ('test', 'demo', i)
            _, _, bins = self.as_connection.get(key)
            assert bins['age'] == i + 2

    def test_query_apply_with_correct_parameters_without_connection(self):
        """
        Invoke query_apply() with correct parameters without connection
        """
        config = {'hosts': [('127.0.0.1', 3000)]}
        client1 = aerospike.client(config)

        with pytest.raises(e.ClusterError) as err_info:
            client1.query_apply("test", "demo", self.age_range_pred,
                                "bin_lua", "mytransform", ['age', 2])

        err_code = err_info.value.code

        assert err_code == AerospikeStatus.AEROSPIKE_CLUSTER_ERROR

    @pytest.mark.skip(reason="This isn't a test for query_apply," +
                             " but for job_info")
    def test_job_info_with_incorrect_module_type(self):
        """
        Invoke query_apply() with incorrect module type
        """
        query_id = self.as_connection.query_apply(
            "test", "demo", self.age_range_pred, "bin_lua",
            "mytransform", ['age', 2])

        with pytest.raises(e.ParamError) as err_info:
            time.sleep(0.1)
            self.as_connection.job_info(query_id, "aggregate")
        err_code = err_info.value.code

        assert err_code == AerospikeStatus.AEROSPIKE_ERR_PARAM

    @pytest.mark.xfail(reason="Passing an invalid predicate currently works " +
                              " or raises a System Error")
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
    def test_invalid_predicate(self, predicate):

        with pytest.raises(e.ParamError) as err_info:
            query_id = self.as_connection.query_apply(
                "test", "demo", predicate, "bin_lua",
                "mytransform", ['age', 2])

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_PARAM
