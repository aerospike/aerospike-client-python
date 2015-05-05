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

class TestGetRegistered(object):
    def setup_class(cls):
        """
        Setup class.
        """
        hostlist, user, password = TestBaseClass.get_hosts()
        config = {'hosts': hostlist}
        if user == None and password == None:
            TestGetRegistered.client = aerospike.client(config).connect()
        else:
            TestGetRegistered.client = aerospike.client(config).connect(
                user, password)
        policy = {'timeout': 5000}
        TestGetRegistered.client.udf_put(u"bin_lua.lua", 0, policy)

    def teardown_class(cls):
        """
        Teardoen class.
        """
        policy = {'timeout': 5000}
        TestGetRegistered.client.udf_remove("bin_lua.lua", policy)
        TestGetRegistered.client.close()

    def test_udf_get_with_no_parameters(self):
        """
        Invoke udf_get() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            TestGetRegistered.client.udf_get()

        assert "Required argument 'module' (pos 1) not found" in typeError.value

    def test_udf_get_with_correct_paramters(self):
        """
        Invoke udf_get() with correct parameters
        """
        module = "bin_lua.lua"
        language = aerospike.UDF_TYPE_LUA
        policy = {'timeout': 5000}

        udf_contents = TestGetRegistered.client.udf_get(module, language,
                                                        policy)

        #Check for udf file contents
        fo = open("bin_lua.lua", "r")
        contents = fo.read()
        assert contents == udf_contents
        fo.close()

    def test_udf_get_with_unicode_string_module_name(self):
        """
        Invoke udf_get() with correct parameters
        """
        module = u"bin_lua.lua"
        language = aerospike.UDF_TYPE_LUA
        policy = {'timeout': 5000}

        udf_contents = TestGetRegistered.client.udf_get(module, language,
                                                        policy)

        #Check for udf file contents
        fo = open("bin_lua.lua", "r")
        contents = fo.read()
        assert contents == udf_contents
        fo.close()

    def test_udf_get_with_correct_policy(self):
        """
        Invoke udf_get() with correct policy
        """
        module = "bin_lua.lua"
        language = aerospike.UDF_TYPE_LUA
        policy = {'timeout': 5000}
        udf_contents = TestGetRegistered.client.udf_get(module, language,
                                                        policy)

        #Check for udf file contents
        fo = open("bin_lua.lua", "r")
        contents = fo.read()
        assert contents == udf_contents
        fo.close()

    def test_udf_get_with_incorrect_policy(self):
        """
        Invoke udf_get() with incorrect policy
        """
        module = "bin_lua.lua"
        language = aerospike.UDF_TYPE_LUA
        policy = {'timeout': 0.5}

        try:
            TestGetRegistered.client.udf_get(module, language, policy)

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "timeout is invalid"

    def test_udf_get_with_nonexistent_module(self):
        """
        Invoke udf_get() with non-existent module
        """
        module = "bin_transform_random"
        language = aerospike.UDF_TYPE_LUA
        policy = {'timeout': 1000}

        try:
            TestGetRegistered.client.udf_get(module, language, policy)

        except UDFError as exception:
            assert exception.code == 100
            assert exception.msg == "error=not_found\n"

    def test_udf_get_with_random_language(self):
        """
        Invoke udf_get() with random language
        """
        module = "bin_lua.lua"
        language = 85
        policy = {'timeout': 1000}

        try:
            TestGetRegistered.client.udf_get(module, language, policy)

        except ClientError as exception:
            assert exception.code == -1
            assert exception.msg == "Invalid language"

    def test_udf_get_with_extra_parameter(self):
        """
        Invoke udf_get() with extra parameter.
        """
        module = "bin_lua.lua"
        language = aerospike.UDF_TYPE_LUA
        policy = {'timeout': 1000}

        #Check for status or empty udf contents
        with pytest.raises(TypeError) as typeError:
            TestGetRegistered.client.udf_get(module, language, policy, "")

        assert "udf_get() takes at most 3 arguments (4 given)" in typeError.value

    def test_udf_get_policy_is_string(self):
        """
        Invoke udf_get() with policy is string
        """
        module = "bin_lua.lua"
        language = aerospike.UDF_TYPE_LUA

        #with pytest.raises(Exception) as exception:
        try:
            TestGetRegistered.client.udf_get(module, language, "")

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "policy must be a dict"

    def test_udf_get_module_is_none(self):
        """
        Invoke udf_get() with module is none
        """
        language = aerospike.UDF_TYPE_LUA

        try:
            TestGetRegistered.client.udf_get(None, language)

        except ClientError as exception:
            assert exception.code == -1
            assert exception.msg == "Module name should be a string or unicode string."
    
    def test_udf_get_with_unicode_module(self):
        """
        Invoke udf_get() with module name is unicode string
        """
        module = u"bin_lua.lua"
        language = aerospike.UDF_TYPE_LUA
        policy = {'timeout': 5000}

        udf_contents = TestGetRegistered.client.udf_get(module, language,
                                                        policy)

        #Check for udf file contents
        fo = open("bin_lua.lua", "r")
        contents = fo.read()
        assert contents == udf_contents
        fo.close()

    def test_udf_get_with_correct_paramters_without_connection(self):
        """
        Invoke udf_get() with correct parameters without connection
        """
        module = "bin_lua.lua"
        language = aerospike.UDF_TYPE_LUA
        policy = {'timeout': 5000}

        config = {'hosts': [('127.0.0.1', 3000)]}
        client1 = aerospike.client(config)

        try:
            udf_contents = client1.udf_get(module, language, policy)

        except ClusterError as exception:
            assert exception.code == 11L
            assert exception.msg == 'No connection to aerospike cluster'
