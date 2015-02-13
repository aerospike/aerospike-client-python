# -*- coding: utf-8 -*-
import pytest
import time
import sys
import cPickle as pickle
try:
    import aerospike
except:
    print "Please install aerospike python client."
    sys.exit(1)

class TestGetRegistered(object):

    def setup_class(cls):
        """
        Setup class.
        """
        config = {
            'hosts': [('127.0.0.1', 3000)]
        }
        policy = {
                'timeout' : 5000
                }
        TestGetRegistered.client = aerospike.client(config).connect()
        TestGetRegistered.client.udf_put(policy, "bin_lua.lua", 0)

    def teardown_class(cls):
        """
        Teardoen class.
        """
        policy = {
                'timeout' : 5000
                }
        TestGetRegistered.client.udf_remove(policy, "bin_lua.lua")
        TestGetRegistered.client.close()

    def test_getRegistered_with_no_parameters(self):
        """
        Invoke getRegistered() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            TestGetRegistered.client.udf_getRegistered()

        assert "Required argument 'module' (pos 1) not found" in typeError.value

    def test_getRegistered_with_correct_paramters(self):
        """
        Invoke getRegistered() with correct parameters
        """
        module = "bin_lua.lua"
        language = aerospike.UDF_TYPE_LUA
        policy = {
                'timeout' : 5000
                }

        udf_contents = TestGetRegistered.client.udf_getRegistered(module, language, policy)


        #Check for udf file contents
        fo = open("bin_lua.lua","r")
        contents = fo.read()
        assert contents == udf_contents
        fo.close()

    def test_getRegistered_with_unicode_string_module_name(self):
        """
        Invoke getRegistered() with correct parameters
        """
        module = u"bin_lua.lua"
        language = aerospike.UDF_TYPE_LUA
        policy = {
                'timeout' : 5000
                }

        udf_contents = TestGetRegistered.client.udf_getRegistered(module, language, policy)


        #Check for udf file contents
        fo = open("bin_lua.lua","r")
        contents = fo.read()
        assert contents == udf_contents
        fo.close()

    def test_getRegistered_with_correct_policy(self):
        """
        Invoke getRegistered() with correct policy
        """
        module = "bin_lua.lua"
        language = aerospike.UDF_TYPE_LUA
        policy = {
                'timeout' : 5000
                }
        udf_contents = TestGetRegistered.client.udf_getRegistered(module, language, policy)

        #Check for udf file contents
        fo = open("bin_lua.lua","r")
        contents = fo.read()
        assert contents == udf_contents
        fo.close()

    def test_getRegistered_with_incorrect_policy(self):
        """
        Invoke getRegistered() with incorrect policy
        """
        module = "bin_lua.lua"
        language = aerospike.UDF_TYPE_LUA
        policy = {
                'timeout' : 0.5
                }

        with pytest.raises(Exception) as exception:
            TestGetRegistered.client.udf_getRegistered(module, language, policy)

        assert exception.value[0] == -2
        assert exception.value[1] == "timeout is invalid"

    def test_getRegistered_with_nonexistent_module(self):
        """
        Invoke getRegistered() with non-existent module
        """
        module = "bin_transform_random"
        language = aerospike.UDF_TYPE_LUA
        policy = {
                'timeout' : 1000
                }

        with pytest.raises(Exception) as exception:
            TestGetRegistered.client.udf_getRegistered(module, language, policy)

        assert exception.value[0] == 100
        assert exception.value[1] == "error=not_found\n"

    def test_getRegistered_with_random_language(self):
        """
        Invoke getRegistered() with random language
        """
        module = "bin_lua.lua"
        language = 85
        policy = {
                'timeout' : 1000
                }

        with pytest.raises(Exception) as exception:
            TestGetRegistered.client.udf_getRegistered(module, language, policy)

        assert exception.value[0] == -1
        assert exception.value[1] == "Invalid language"

    def test_getRegistered_with_extra_parameter(self):
        """
        Invoke getRegistered() with extra parameter.
        """
        module = "bin_lua.lua"
        language = aerospike.UDF_TYPE_LUA
        policy = {
                'timeout' : 1000
                }

        #Check for status or empty udf contents
        with pytest.raises(TypeError) as typeError:
            TestGetRegistered.client.udf_getRegistered(module, language, policy, "")

        assert "udf_getRegistered() takes at most 3 arguments (4 given)" in typeError.value

    def test_getRegistered_policy_is_string(self):
        """
        Invoke getRegistered() with policy is string
        """
        module = "bin_lua.lua"
        language = aerospike.UDF_TYPE_LUA

        with pytest.raises(Exception) as exception:
            TestGetRegistered.client.udf_getRegistered(module, language, "")

        assert exception.value[0] == -2
        assert exception.value[1] == "policy must be a dict"

    def test_getRegistered_module_is_none(self):
        """
        Invoke getRegistered() with module is none
        """
        language = aerospike.UDF_TYPE_LUA

        with pytest.raises(Exception) as exception:
            TestGetRegistered.client.udf_getRegistered(None, language)

        assert exception.value[0] == -1
        assert exception.value[1] == "Module name should be a string or unicode string."
