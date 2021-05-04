# -*- coding: utf-8 -*-
import pytest
import time
import sys
from .as_status_codes import AerospikeStatus
from aerospike_helpers import expressions as exp
from aerospike import exception as e

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)


def wait_for_job_completion(as_connection, job_id):
    '''
    Blocks until the job has completed
    '''
    time.sleep(0.1)
    while True:
        response = as_connection.job_info(job_id, aerospike.JOB_SCAN)
        if response['status'] != aerospike.JOB_STATUS_INPROGRESS:
            break
        time.sleep(0.1)


class TestScanApply(object):

    def setup_class(cls):
        cls.udf_to_load = u"bin_lua.lua"
        cls.loaded_udf_name = "bin_lua.lua"
        cls.udf_language = aerospike.UDF_TYPE_LUA

    @pytest.fixture(autouse=True)
    def setup(self, request, connection_with_udf):
        for i in range(5):
            key = ('test', 'demo', i)
            rec = {'name': 'name%s' % (str(i)), 'age': i}
            connection_with_udf.put(key, rec)

        no_set_key = ('test', None, 'no_set')
        rec = {'name': 'no_ns_key', 'age': 10}
        connection_with_udf.put(no_set_key, rec)

        def teardown():
            for i in range(5):
                key = ('test', 'demo', i)
                connection_with_udf.remove(key)
            connection_with_udf.remove(no_set_key)
        request.addfinalizer(teardown)

    def test_scan_apply_with_correct_parameters_with_set(self):
        """
        Invoke scan_apply() with correct parameters.
        The UDF should only apply to records in the ns and set.
        """
        scan_id = self.as_connection.scan_apply("test", "demo", "bin_lua",
                                                "mytransform", ['age', 2])

        wait_for_job_completion(self.as_connection, scan_id)

        for i in range(5):
            key = ('test', 'demo', i)
            _, _, bins = self.as_connection.get(key)
            assert bins['age'] == i + 2

        _, _, rec = self.as_connection.get(('test', None, 'no_set'))
        assert rec['age'] == 10

    def test_scan_apply_with_correct_policy(self):
        """
        Invoke scan_apply() with correct policy
        """
        policy = {'timeout': 1000, 'socket_timeout': 9876}
        scan_id = self.as_connection.scan_apply("test", "demo", "bin_lua",
                                                "mytransform", ['age', 2],
                                                policy)

        wait_for_job_completion(self.as_connection, scan_id)

        for i in range(5):
            key = ('test', 'demo', i)
            _, _, bins = self.as_connection.get(key)
            assert bins['age'] == i + 2

    def test_scan_apply_with_correct_policy_and_expressions(self):
        """
        Invoke scan_apply() with correct policy.
        It should invoke the function on all records in the set that match the expressions.
        """
        expr = exp.And(
            exp.Eq(exp.StrBin('name'), 'name4'),
            exp.NE(exp.IntBin('age'), 3),
        )

        policy = {'timeout': 1000, 'expressions': expr.compile()}
        scan_id = self.as_connection.scan_apply("test", None, "bin_lua",
                                                "mytransform", ['age', 2],
                                                policy)

        wait_for_job_completion(self.as_connection, scan_id)

        for i in range(5):
            key = ('test', 'demo', i)
            _, _, bins = self.as_connection.get(key)
            if bins['name'] == 'name4':
                assert bins['age'] == i + 2
            else :
                assert bins['age'] == i

    def test_scan_apply_with_correct_policy_and_invalid_expressions(self):
        """
        Invoke scan_apply() with invalid expressions.
        """
        expr = exp.Eq(exp.StrBin('name'), 4)

        policy = {'timeout': 1000, 'expressions': expr.compile()}
        with pytest.raises(e.InvalidRequest):
            scan_id = self.as_connection.scan_apply("test", None, "bin_lua",
                                                    "mytransform", ['age', 2],
                                                    policy)

    def test_scan_apply_with_none_set(self):
        """
        Invoke scan_apply() with set argument as None
        It should invoke the function on all records in the NS
        """
        policy = {'timeout': 1000}
        scan_id = self.as_connection.scan_apply("test", None, "bin_lua",
                                                "mytransform", ['age', 2],
                                                policy)

        wait_for_job_completion(self.as_connection, scan_id)

        for i in range(5):
            key = ('test', 'demo', i)
            _, _, bins = self.as_connection.get(key)
            assert bins['age'] == i + 2

        _, _, rec = self.as_connection.get(('test', None, 'no_set'))
        assert rec['age'] == 12

    def test_scan_apply_with_none_set_and_expressions(self):
        """
        Invoke scan_apply() with set argument as None
        It should invoke the function on all records in NS that match the expressions
        """
        expr = exp.And(
            exp.Eq(exp.StrBin('name'), 'name2'),
            exp.NE(exp.IntBin('age'), 3)
        )

        policy = {'timeout': 1000, 'expressions': expr.compile()}
        scan_id = self.as_connection.scan_apply("test", None, "bin_lua",
                                                "mytransform", ['age', 2],
                                                policy)

        wait_for_job_completion(self.as_connection, scan_id)

        for i in range(5):
            key = ('test', 'demo', i)
            _, _, bins = self.as_connection.get(key)
            if bins['name'] == 'name2':
                assert bins['age'] == i + 2
            else :
                assert bins['age'] == i

        _, _, rec = self.as_connection.get(('test', None, 'no_set'))
        assert rec['age'] == 10

    def test_scan_apply_with_extra_call_to_lua(self):
        """
        Invoke scan_apply() and pass 3 args to a function expecting 2
        """
        scan_id = self.as_connection.scan_apply("test", "demo", "bin_lua",
                                                "mytransform", ['age', 2, 3])

        wait_for_job_completion(self.as_connection, scan_id)

        for i in range(5):
            key = ('test', 'demo', i)
            _, _, bins = self.as_connection.get(key)
            assert bins['age'] == i + 2

    def test_scan_apply_with_extra_parameter_in_lua(self):
        """
        Invoke scan_apply() and pass 2 arguments to a function expecting 3
        """
        scan_id = self.as_connection.scan_apply("test", "demo", "bin_lua",
                                                "mytransformextra", ['age', 2])

        wait_for_job_completion(self.as_connection, scan_id)

        for i in range(5):
            key = ('test', 'demo', i)
            _, _, bins = self.as_connection.get(key)
            assert bins['age'] == i + 2

    def test_scan_apply_with_less_parameter_in_lua(self):
        """
        Invoke scan_apply() and pass 2 arguments to a function
        expecting 1
        """
        scan_id = self.as_connection.scan_apply("test", "demo", "bin_lua",
                                                "mytransformless", ['age', 2])

        wait_for_job_completion(self.as_connection, scan_id)

        for i in range(5):
            key = ('test', 'demo', i)
            _, _, bins = self.as_connection.get(key)
            assert bins['age'] == i

    def test_scan_apply_with_no_function_args(self):
        """
        Test that it is valid to call the function without
        a list of arguments for the UDF
        """
        scan_id = self.as_connection.scan_apply("test", "demo", "bin_lua",
                                                "mytransformless")

        wait_for_job_completion(self.as_connection, scan_id)

        for i in range(5):
            key = ('test', 'demo', i)
            _, _, bins = self.as_connection.get(key)
            assert bins['age'] == i

    def test_scan_apply_with_options_positive(self):
        """
        Invoke scan_apply() with options positive
        """
        policy = {'timeout': 1000}
        options = {
            "concurrent": False
        }
        scan_id = self.as_connection.scan_apply("test", "demo", "bin_lua",
                                                "mytransform",
                                                ['age', 2], policy, options)

        wait_for_job_completion(self.as_connection, scan_id)

        for i in range(5):
            key = ('test', 'demo', i)
            _, _, bins = self.as_connection.get(key)
            assert bins['age'] == i + 2

    def test_scan_apply_unicode_input(self):
        """
        Invoke scan_apply() with unicode udf
        """
        scan_id = self.as_connection.scan_apply("test", "demo", u"bin_lua",
                                                u"mytransform", ['age', 2])

        wait_for_job_completion(self.as_connection, scan_id)

        for i in range(5):
            key = ('test', 'demo', i)
            _, _, bins = self.as_connection.get(key)
            assert bins['age'] == i + 2

    def test_scan_apply_with_incorrect_module_name(self):
        """
        Invoke scan_apply() with incorrect module name
        """
        scan_id = self.as_connection.scan_apply(
            "test", "demo", "bin_lua_incorrect",
            "mytransform", ['age', 2])

        wait_for_job_completion(self.as_connection, scan_id)

        for i in range(5):
            key = ('test', 'demo', i)
            _, _, bins = self.as_connection.get(key)
            assert bins['age'] == i

    def test_scan_apply_with_incorrect_function_name(self):
        """
        Invoke scan_apply() with incorrect function name
        """
        scan_id = self.as_connection.scan_apply("test", "demo", "bin_lua",
                                                "mytransform_incorrect",
                                                ['age', 2])

        wait_for_job_completion(self.as_connection, scan_id)

        for i in range(5):
            key = ('test', 'demo', i)
            _, _, bins = self.as_connection.get(key)
            assert bins['age'] == i

    def test_scan_apply_with_argument_is_none(self):
        """
        Invoke scan_apply() with arguments as None
        """
        scan_id = self.as_connection.scan_apply(
            "test", "demo", "bin_lua", "mytransform", None)

        wait_for_job_completion(self.as_connection, scan_id)

        # The function application should have failed and not changed
        # any bin values
        for i in range(5):
            key = ('test', 'demo', i)
            _, _, bins = self.as_connection.get(key)
            assert bins['age'] == i

    def test_scan_apply_with_ns_set_none(self):
        """
        Invoke scan_apply() with ns and set as None
        """
        with pytest.raises(TypeError) as typeError:
            self.as_connection.scan_apply(None, None, "bin_lua",
                                          "mytransform", ['age', 2])

        assert "scan_apply() argument 1 must be str" in str(typeError.value)

    def test_scan_apply_with_set_is_int(self):
        """
        Invoke scan_apply() set as an int
        """
        with pytest.raises(e.ParamError) as typeError:
            self.as_connection.scan_apply('test', 5, "bin_lua",
                                          "mytransform", ['age', 2])

    def test_scan_apply_with_set_is_int(self):
        """
        Invoke scan_apply() set as an int
        """
        with pytest.raises(e.ParamError) as typeError:
            self.as_connection.scan_apply('test', 5, "bin_lua",
                                          "mytransform", ['age', 2])

    def test_scan_apply_with_module_function_none(self):
        """
        Invoke scan_apply() with None module function
        """

        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.scan_apply("test", "demo", None, None,
                                          ['age', 2])

        err_code = err_info.value.code
        assert err_code is AerospikeStatus.AEROSPIKE_ERR_PARAM

    def test_scan_apply_with_concurrent_int(self):
        """
        Invoke scan_apply() with concurrent int
        """
        policy = {'timeout': 1000}
        options = {
            "concurrent": 5
        }
        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.scan_apply("test", "demo", "bin_lua",
                                          "mytransform_incorrect",
                                          ['age', 2], policy, options)

        err_code = err_info.value.code
        assert err_code is AerospikeStatus.AEROSPIKE_ERR_PARAM

    def test_scan_apply_with_extra_argument(self):
        """
        Invoke scan_apply() with extra argument
        """
        policy = {'timeout': 1000}
        options = {
            "concurrent": False
        }
        with pytest.raises(TypeError) as typeError:
            self.as_connection.scan_apply("test", "demo", "bin_lua",
                                          "mytransform_incorrect",
                                          ['age', 2], policy, options, "")

        assert "scan_apply() takes at most 7 arguments (8 given)" in str(
            typeError.value)

    def test_scan_apply_with_argument_is_string(self):
        """
        Invoke scan_apply() with arguments as string
        """
        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.scan_apply(
                "test", "demo", "bin_lua", "mytransform", "")

        err_code = err_info.value.code
        assert err_code is AerospikeStatus.AEROSPIKE_ERR_PARAM

    def test_scan_apply_with_correct_parameters_without_connection(self):
        """
        Invoke scan_apply() with correct parameters without connection
        """
        config = {'hosts': [('127.0.0.1', 3000)]}
        client1 = aerospike.client(config)

        with pytest.raises(e.ClusterError) as err_info:
            client1.scan_apply(
                "test", "demo", "bin_lua", "mytransform", ['age', 2])

        err_code = err_info.value.code
        assert err_code is AerospikeStatus.AEROSPIKE_CLUSTER_ERROR

    def test_scan_apply_with_incorrect_policy(self):
        """
        Invoke scan_apply() with incorrect policy
        """
        policy = {
            'timeout': 0.5
        }

        with pytest.raises(e.ParamError):
            self.as_connection.scan_apply(
                "test", "demo", "bin_lua", "mytransform", ['age', 2], policy)

    def test_scan_apply_with_non_existent_ns(self):
        """
        Invoke scan_apply() with a namespace name which does not exist
        """
        with pytest.raises(e.NamespaceNotFound) as typeError:
            self.as_connection.scan_apply('fake_not_real_ns', None, "bin_lua",
                                          "mytransform", ['age', 2])

    def test_scan_apply_with_non_string_module(self):
        """
        Invoke scan_apply() with a namespace name which does not exist
        """
        with pytest.raises(e.ParamError) as typeError:
            self.as_connection.scan_apply('test', None, 15,
                                          "mytransform", ['age', 2])

    def test_scan_apply_with_non_string_function(self):
        """
        Invoke scan_apply() with a namespace name which does not exist
        """
        with pytest.raises(e.ParamError) as typeError:
            self.as_connection.scan_apply('test', None, 'bin_lua',
                                          14, ['age', 2])

    def test_scan_apply_with_non_existent_set(self):
        """
        Invoke scan_apply() with a set name which does not exist.
        This should not raise an error, and not apply the
        UDF to any records
        """
        self.as_connection.scan_apply('test', 'FakeNOTREALSET', 'bin_lua',
                                      'mytransform', ['age', 2])

        # Since a set was specified, and it does not exist,
        # The transformation should not have run on any records
        for i in range(5):
            key = ('test', 'demo', i)
            _, _, bins = self.as_connection.get(key)
            assert bins['age'] == i

        _, _, rec = self.as_connection.get(('test', None, 'no_set'))
        assert rec['age'] == 10

    def test_scan_apply_with_no_parameters(self):
        """
        Invoke scan_apply() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            self.as_connection.scan_apply()
        assert "argument 'ns' (pos 1)" in str(
            typeError.value)
