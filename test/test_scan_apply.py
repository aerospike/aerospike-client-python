# -*- coding: utf-8 -*-
import pytest
import time
import sys
import cPickle as pickle
from test_base_class import TestBaseClass

aerospike = pytest.importorskip("aerospike")
try:
    from aerospike.exception import *
except:
    print "Please install aerospike python client."
    sys.exit(1)

class TestScanApply(object):
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
        for i in xrange(5):
            key = ('test', 'demo', i)
            rec = {'name': 'name%s' % (str(i)), 'age': i}
            self.client.put(key, rec)
        policy = {}
        self.client.udf_put(u"bin_lua.lua", 0, policy)

    def teardown_method(self, method):
        """
        Teardoen method.
        """
        for i in xrange(5):
            key = ('test', 'demo', i)
            try:
                self.client.remove(key)
            except TimeoutError:
                pass
        self.client.close()

    def test_scan_apply_with_no_parameters(self):
        """
        Invoke scan_apply() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            self.client.scan_apply()
        assert "Required argument 'ns' (pos 1) not found" in typeError.value

    def test_scan_apply_with_correct_parameters(self):
        """
        Invoke scan_apply() with correct parameters
        """
        scan_id = self.client.scan_apply("test", "demo", "bin_lua",
                                         "mytransform", ['age', 2])

        while True:
            response = self.client.job_info(scan_id, aerospike.JOB_SCAN)
            if response['status'] == aerospike.JOB_STATUS_COMPLETED:
                break
        for i in xrange(5):
            key = ('test', 'demo', i)
            (key, meta, bins) = self.client.get(key)
            if bins['age'] != i + 2:
                assert True == False

        assert True == True

    def test_scan_apply_with_correct_policy(self):
        """
        Invoke scan_apply() with correct policy
        """
        policy = {'timeout': 1000}
        scan_id = self.client.scan_apply("test", "demo", "bin_lua",
                                         "mytransform", ['age', 2], policy)

        while True:
            response = self.client.job_info(scan_id, aerospike.JOB_SCAN)
            if response['status'] == aerospike.JOB_STATUS_COMPLETED:
                break
        for i in xrange(5):
            key = ('test', 'demo', i)
            (key, meta, bins) = self.client.get(key)
            if bins['age'] != i + 2:
                assert True == False

        assert True == True

    def test_scan_apply_with_none_set(self):
        """
        Invoke scan_apply() with correct policy
        """
        policy = {'timeout': 1000}
        scan_id = self.client.scan_apply("test", None, "bin_lua",
                                         "mytransform", ['age', 2], policy)

        while True:
            response = self.client.job_info(scan_id, aerospike.JOB_SCAN)
            if response['status'] == aerospike.JOB_STATUS_COMPLETED:
                break
        for i in xrange(5):
            key = ('test', 'demo', i)
            (key, meta, bins) = self.client.get(key)
            if bins['age'] != i + 2:
                assert True == False

        assert True == True

    def test_scan_apply_with_incorrect_policy(self):
        """
        Invoke scan_apply() with incorrect policy
        """
        policy = {
            'timeout': 0.5
        }
        try:
            scan_id = self.client.scan_apply("test", "demo", "bin_lua", "mytransform", ['age', 2], policy)

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "timeout is invalid"

    def test_scan_apply_with_incorrect_ns_set(self):
        """
        Invoke scan_apply() with incorrect ns and set
        """
        try:
            scan_id = self.client.scan_apply("test1", "demo1", "bin_lua", "mytransform", ['age', 2])

        except NamespaceNotFound as exception:
            assert exception.code == 20L
        except ServerError as exception:
            assert exception.code == 1L

    def test_scan_apply_with_incorrect_module_name(self):
        """
        Invoke scan_apply() with incorrect module name
        """
        scan_id = self.client.scan_apply("test", "demo", "bin_lua_incorrect",
                                         "mytransform", ['age', 2])

        while True:
            response = self.client.job_info(scan_id, aerospike.JOB_SCAN)
            if response['status'] == aerospike.JOB_STATUS_COMPLETED:
                break
        for i in xrange(5):
            key = ('test', 'demo', i)
            (key, meta, bins) = self.client.get(key)
            if bins['age'] != i:
                assert True == False
            else:
                assert True == True

    def test_scan_apply_with_incorrect_function_name(self):
        """
        Invoke scan_apply() with incorrect function name
        """
        scan_id = self.client.scan_apply("test", "demo", "bin_lua",
                                         "mytransform_incorrect", ['age', 2])

        while True:
            response = self.client.job_info(scan_id, aerospike.JOB_SCAN)
            if response['status'] == aerospike.JOB_STATUS_COMPLETED:
                break
        for i in xrange(5):
            key = ('test', 'demo', i)
            (key, meta, bins) = self.client.get(key)
            if bins['age'] != i:
                assert True == False
            else:
                assert True == True

    def test_scan_apply_with_ns_set_none(self):
        """
        Invoke scan_apply() with ns and set as None
        """
        with pytest.raises(TypeError) as typeError:
            scan_id = self.client.scan_apply(None, None, "bin_lua",
                                             "mytransform", ['age', 2])

        assert "scan_apply() argument 1 must be string, not None" in typeError.value

    def test_scan_apply_with_module_function_none(self):
        """
        Invoke scan_apply() with None module function
        """

        try:
            scan_id = self.client.scan_apply("test", "demo", None, None, ['age', 2])

        except ParamError as exception:
            assert exception.code == -2L
            assert exception.msg == "Module name should be string"

    def test_scan_apply_with_percent_string(self):
        """
        Invoke scan_apply() with percent string
        """
        policy = {'timeout': 1000}
        options = {
            "percent": "80",
            "concurrent": False,
            "priority": aerospike.SCAN_PRIORITY_HIGH
        }
        try:
            scan_id = self.client.scan_apply("test", "demo", "bin_lua",
                                             "mytransform_incorrect",
                                             ['age', 2], policy, options)

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Invalid value(type) for percent"

    def test_scan_apply_with_priority_string(self):
        """
        Invoke scan_apply() with priority string
        """
        policy = {'timeout': 1000}
        options = {
            "percent": 80,
            "concurrent": False,
            "priority": "aerospike.SCAN_PRIORITY_HIGH"
        }
        try:
            scan_id = self.client.scan_apply("test", "demo", "bin_lua",
                                             "mytransform_incorrect",
                                             ['age', 2], policy, options)

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Invalid value(type) for priority"

    def test_scan_apply_with_concurrent_int(self):
        """
        Invoke scan_apply() with concurrent int
        """
        policy = {'timeout': 1000}
        options = {
            "percent": 80,
            "concurrent": 5,
            "priority": aerospike.SCAN_PRIORITY_HIGH
        }
        try:
            scan_id = self.client.scan_apply("test", "demo", "bin_lua",
                                             "mytransform_incorrect",
                                             ['age', 2], policy, options)

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Invalid value(type) for concurrent"

    def test_scan_apply_with_extra_argument(self):
        """
        Invoke scan_apply() with extra argument
        """
        policy = {'timeout': 1000}
        options = {
            "percent": 80,
            "concurrent": False,
            "priority": aerospike.SCAN_PRIORITY_HIGH
        }
        with pytest.raises(TypeError) as typeError:
            scan_id = self.client.scan_apply("test", "demo", "bin_lua",
                                             "mytransform_incorrect",
                                             ['age', 2], policy, options, "")

        assert "scan_apply() takes at most 7 arguments (8 given)" in typeError.value

    def test_scan_apply_with_argument_is_string(self):
        """
        Invoke scan_apply() with arguments as string
        """
        try:
            scan_id = self.client.scan_apply("test", "demo", "bin_lua", "mytransform", "")

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Arguments should be a list"

    def test_scan_apply_with_argument_is_none(self):
        """
        Invoke scan_apply() with arguments as None
        """
        try:
            scan_id = self.client.scan_apply("test", "demo", "bin_lua", "mytransform", None)

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Arguments should be a list"

    def test_scan_apply_with_extra_call_to_lua(self):
        """
        Invoke scan_apply() with extra call to lua
        """
        scan_id = self.client.scan_apply("test", "demo", "bin_lua",
                                         "mytransform", ['age', 2, 3])

        #time.sleep(2)

        while True:
            response = self.client.job_info(scan_id, aerospike.JOB_SCAN)
            if response['status'] == aerospike.JOB_STATUS_COMPLETED:
                break
        for i in xrange(5):
            key = ('test', 'demo', i)
            (key, meta, bins) = self.client.get(key)
            if bins['age'] != i + 2:
                assert True == False

        assert True == True

    def test_scan_apply_with_extra_parameter_in_lua(self):
        """
        Invoke scan_apply() with extra parameter in lua
        """
        scan_id = self.client.scan_apply("test", "demo", "bin_lua",
                                         "mytransformextra", ['age', 2])

        #time.sleep(2)

        while True:
            response = self.client.job_info(scan_id, aerospike.JOB_SCAN)
            if response['status'] == aerospike.JOB_STATUS_COMPLETED:
                break
        for i in xrange(5):
            key = ('test', 'demo', i)
            (key, meta, bins) = self.client.get(key)
            if bins['age'] != i + 2:
                assert True == False

        assert True == True

    def test_scan_apply_with_less_parameter_in_lua(self):
        """
        Invoke scan_apply() with less parameter in lua
        """
        scan_id = self.client.scan_apply("test", "demo", "bin_lua",
                                         "mytransformless", ['age', 2])

        #time.sleep(2)

        while True:
            response = self.client.job_info(scan_id, aerospike.JOB_SCAN)
            if response['status'] == aerospike.JOB_STATUS_COMPLETED:
                break
        for i in xrange(5):
            key = ('test', 'demo', i)
            (key, meta, bins) = self.client.get(key)
            if bins['age'] != i:
                assert True == False
            else:
                assert True == True

    def test_scan_apply_with_options_positive(self):
        """
        Invoke scan_apply() with options positive
        """
        policy = {'timeout': 1000}
        options = {
            "percent": 100,
            "concurrent": False,
            "priority": aerospike.SCAN_PRIORITY_HIGH
        }
        scan_id = self.client.scan_apply("test", "demo", "bin_lua",
                                         "mytransform", ['age',
                                                         2], policy, options)

        while True:
            response = self.client.job_info(scan_id, aerospike.JOB_SCAN)
            if response['status'] == aerospike.JOB_STATUS_COMPLETED:
                break
        for i in xrange(5):
            key = ('test', 'demo', i)
            (key, meta, bins) = self.client.get(key)
            if bins['age'] != i + 2:
                assert True == False

        assert True == True

    def test_scan_apply_unicode_input(self):
        """
        Invoke scan_apply() with unicode udf
        """
        scan_id = self.client.scan_apply("test", "demo", u"bin_lua",
                                         u"mytransform", ['age', 2])

        while True:
            response = self.client.job_info(scan_id, aerospike.JOB_SCAN)
            if response['status'] == aerospike.JOB_STATUS_COMPLETED:
                break
        for i in xrange(5):
            key = ('test', 'demo', i)
            (key, meta, bins) = self.client.get(key)
            if bins['age'] != i + 2:
                assert True == False

        assert True == True

    def test_scan_apply_with_correct_parameters_without_connection(self):
        """
        Invoke scan_apply() with correct parameters without connection
        """
        config = {'hosts': [('127.0.0.1', 3000)]}
        client1 = aerospike.client(config)

        try:
            scan_id = client1.scan_apply("test", "demo", "bin_lua", "mytransform", ['age', 2])

        except ClusterError as exception:
            assert exception.code == 11L
            assert exception.msg == 'No connection to aerospike cluster'
