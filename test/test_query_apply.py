# -*- coding: utf-8 -*-
import pytest
import time
import sys
import cPickle as pickle
from test_base_class import TestBaseClass
from aerospike import predicates as p

aerospike = pytest.importorskip("aerospike")
try:
    from aerospike.exception import *
except:
    print "Please install aerospike python client."
    sys.exit(1)

class TestQueryApply(object):
    def setup_method(self, method):
        """
        Setup method.
        """
        hostlist, user, password = TestBaseClass.get_hosts()
        config = {'hosts': hostlist}
        if user == None and password == None:
            self.client = aerospike.client(config).connect()
        else:
            self.client = aerospike.client(config).connect(user, password)
        self.client.index_integer_create('test', 'demo', 'age',
                'test_demo_age_idx')
        self.client.index_integer_create('test', None, 'age',
                'test_null_age_idx')
        for i in xrange(1, 5):
            key = ('test', 'demo', i)
            rec = {'name': 'name%s' % (str(i)), 'age': i}
            self.client.put(key, rec)
        policy = {}
        self.client.udf_put(u"bin_lua.lua", 0, policy)

    def teardown_method(self, method):
        """
        Teardoen method.
        """
        for i in xrange(1, 5):
            key = ('test', 'demo', i)
            try:
                self.client.remove(key)
            except TimeoutError:
                pass
        self.client.close()

    def test_query_apply_with_no_parameters(self):
        """
        Invoke query_apply() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            self.client.query_apply()
        assert "Required argument 'ns' (pos 1) not found" in typeError.value

    def test_query_apply_with_correct_parameters(self):
        """
        Invoke query_apply() with correct parameters
        """
        query_id = self.client.query_apply("test", "demo", p.between("age", 1,
            5), "bin_lua", "mytransform", ['age', 2])

        while True:
            time.sleep(0.1)
            response = self.client.job_info(query_id, aerospike.JOB_QUERY)
            if response['status'] == aerospike.JOB_STATUS_COMPLETED:
                break
        for i in xrange(1, 5):
            key = ('test', 'demo', i)
            (key, meta, bins) = self.client.get(key)
            if bins['age'] != i + 2:
                assert True == False

        assert True == True

    def test_query_apply_with_correct_policy(self):
        """
        Invoke query_apply() with correct policy
        """
        policy = {'timeout': 1000}
        query_id = self.client.query_apply("test", "demo", p.between("age", 1,
            5), "bin_lua", "mytransform", ['age', 2], policy)

        while True:
            time.sleep(0.1)
            response = self.client.job_info(query_id, aerospike.JOB_QUERY)
            if response['status'] == aerospike.JOB_STATUS_COMPLETED:
                break
        for i in xrange(1, 5):
            key = ('test', 'demo', i)
            (key, meta, bins) = self.client.get(key)
            if bins['age'] != i + 2:
                assert True == False

        assert True == True

    def test_query_apply_with_none_set(self):
        """
        Invoke query_apply() with correct policy
        """
        policy = {'timeout': 1000}
        query_id = self.client.query_apply("test", None, p.between("age", 1, 5), "bin_lua",
                                         "mytransform", ['age', 2], policy)

        while True:
            time.sleep(0.1)
            response = self.client.job_info(query_id, aerospike.JOB_QUERY)
            if response['status'] == aerospike.JOB_STATUS_COMPLETED:
                break
        for i in xrange(1, 5):
            key = ('test', 'demo', i)
            (key, meta, bins) = self.client.get(key)
            if bins['age'] != i:
                assert True == False

        assert True == True

    def test_query_apply_with_incorrect_policy(self):
        """
        Invoke query_apply() with incorrect policy
        """
        policy = {
            'timeout': 0.5
        }
        try:
            query_id = self.client.query_apply("test", "demo", ["age", 1, 5], "bin_lua", "mytransform", ['age', 2], policy)

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "timeout is invalid"

    def test_query_apply_with_incorrect_ns_set(self):
        """
        Invoke query_apply() with incorrect ns and set
        """
        try:
            query_id = self.client.query_apply("test1", "demo1", ["age", 1, 5], "bin_lua", "mytransform", ['age', 2])

        except NamespaceNotFound as exception:
            assert exception.code == 20L
        except ServerError as exception:
            assert exception.code == 1L

    def test_query_apply_with_incorrect_module_name(self):
        """
        Invoke query_apply() with incorrect module name
        """
        query_id = self.client.query_apply("test", "demo", p.between("age", 1,
            5), "bin_lua_incorrect", "mytransform", ['age', 2])

        while True:
            time.sleep(0.1)
            response = self.client.job_info(query_id, aerospike.JOB_QUERY)
            if response['status'] == aerospike.JOB_STATUS_COMPLETED:
                break
        for i in xrange(1, 5):
            key = ('test', 'demo', i)
            (key, meta, bins) = self.client.get(key)
            if bins['age'] != i:
                assert True == False
            else:
                assert True == True

    def test_query_apply_with_incorrect_function_name(self):
        """
        Invoke query_apply() with incorrect function name
        """
        query_id = self.client.query_apply("test", "demo", p.between("age", 1,
            5), "bin_lua", "mytransform_incorrect", ['age', 2])

        while True:
            time.sleep(0.1)
            response = self.client.job_info(query_id, aerospike.JOB_QUERY)
            if response['status'] == aerospike.JOB_STATUS_COMPLETED:
                break
        for i in xrange(1, 5):
            key = ('test', 'demo', i)
            (key, meta, bins) = self.client.get(key)
            if bins['age'] != i:
                assert True == False
            else:
                assert True == True

    def test_query_apply_with_ns_set_none(self):
        """
        Invoke query_apply() with ns and set as None
        """
        with pytest.raises(TypeError) as typeError:
            query_id = self.client.query_apply(None, None, p.between("age", 1, 5), "bin_lua",
                                             "mytransform", ['age', 2])

        assert "query_apply() argument 1 must be string, not None" in typeError.value

    def test_query_apply_with_module_function_none(self):
        """
        Invoke query_apply() with None module function
        """

        try:
            query_id = self.client.query_apply("test", "demo", p.between("age",
                1, 5), None, None, ['age', 2])

        except ParamError as exception:
            assert exception.code == -2L
            assert exception.msg == "Module name should be string"

    def test_query_apply_with_extra_argument(self):
        """
        Invoke query_apply() with extra argument
        """
        policy = {'timeout': 1000}
        with pytest.raises(TypeError) as typeError:
            query_id = self.client.query_apply("test", "demo", p.between("age",
                1, 5), "bin_lua", "mytransform_incorrect", ['age', 2], policy, "")

        assert "query_apply() takes at most 7 arguments (8 given)" in typeError.value

    def test_query_apply_with_argument_is_string(self):
        """
        Invoke query_apply() with arguments as string
        """
        try:
            query_id = self.client.query_apply("test", "demo", p.between("age",
                1, 5), "bin_lua", "mytransform", "")

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Arguments should be a list"

    def test_query_apply_with_argument_is_none(self):
        """
        Invoke query_apply() with arguments as None
        """
        try:
            query_id = self.client.query_apply("test", "demo", p.between("age",
                1, 5), "bin_lua", "mytransform", None)

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Arguments should be a list"

    def test_query_apply_with_extra_call_to_lua(self):
        """
        Invoke query_apply() with extra call to lua
        """
        query_id = self.client.query_apply("test", "demo", p.between("age", 1,
            5), "bin_lua","mytransform", ['age', 2, 3])

        #time.sleep(2)

        while True:
            time.sleep(0.1)
            response = self.client.job_info(query_id, aerospike.JOB_QUERY)
            if response['status'] == aerospike.JOB_STATUS_COMPLETED:
                break
        for i in xrange(1, 5):
            key = ('test', 'demo', i)
            (key, meta, bins) = self.client.get(key)
            if bins['age'] != i + 2:
                assert True == False

        assert True == True

    def test_query_apply_with_extra_parameter_in_lua(self):
        """
        Invoke query_apply() with extra parameter in lua
        """
        query_id = self.client.query_apply("test", "demo", p.between("age", 1,
            5), "bin_lua", "mytransformextra", ['age', 2])

        #time.sleep(2)

        while True:
            time.sleep(0.1)
            response = self.client.job_info(query_id, aerospike.JOB_QUERY)
            if response['status'] == aerospike.JOB_STATUS_COMPLETED:
                break
        for i in xrange(1, 5):
            key = ('test', 'demo', i)
            (key, meta, bins) = self.client.get(key)
            if bins['age'] != i + 2:
                assert True == False

        assert True == True

    def test_query_apply_with_less_parameter_in_lua(self):
        """
        Invoke query_apply() with less parameter in lua
        """
        query_id = self.client.query_apply("test", "demo", p.between("age", 1,
            5), "bin_lua", "mytransformless", ['age', 2])

        #time.sleep(2)

        while True:
            time.sleep(0.1)
            response = self.client.job_info(query_id, aerospike.JOB_QUERY)
            if response['status'] == aerospike.JOB_STATUS_COMPLETED:
                break
        for i in xrange(1, 5):
            key = ('test', 'demo', i)
            (key, meta, bins) = self.client.get(key)
            if bins['age'] != i:
                assert True == False
            else:
                assert True == True

    def test_query_apply_unicode_input(self):
        """
        Invoke query_apply() with unicode udf
        """
        query_id = self.client.query_apply("test", "demo", p.between("age", 1,
            5), u"bin_lua", u"mytransform", ['age', 2])

        while True:
            time.sleep(0.1)
            response = self.client.job_info(query_id, aerospike.JOB_QUERY)
            if response['status'] == aerospike.JOB_STATUS_COMPLETED:
                break
        for i in xrange(1, 5):
            key = ('test', 'demo', i)
            (key, meta, bins) = self.client.get(key)
            if bins['age'] != i + 2:
                assert True == False

        assert True == True

    def test_query_apply_with_correct_parameters_without_connection(self):
        """
        Invoke query_apply() with correct parameters without connection
        """
        config = {'hosts': [('127.0.0.1', 3000)]}
        client1 = aerospike.client(config)

        try:
            query_id = client1.query_apply("test", "demo", p.between("age", 1,
                5), "bin_lua", "mytransform", ['age', 2])

        except ClusterError as exception:
            assert exception.code == 11L
            assert exception.msg == 'No connection to aerospike cluster'

    def test_query_apply_with_incorrect_module_type(self):
        """
        Invoke query_apply() with incorrect module type
        """
        query_id = self.client.query_apply("test", "demo", p.between("age", 1,
            5), "bin_lua", "mytransform", ['age', 2])

        try:
            time.sleep(0.1)
            response = self.client.job_info(query_id, "aggregate")
        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Module can have only two values: aerospike.JOB_SCAN or aerospike.JOB_QUERY"
