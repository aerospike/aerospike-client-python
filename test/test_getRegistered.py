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

    def setup_method(self, method):
	"""
        Setup method.
        """
        config = {
            'hosts': [('127.0.0.1', 3000)]
        }
        policy = {
                'timeout' : 5000
                }
        self.client = aerospike.client(config).connect()
	self.client.udf_put(policy, "bin_lua.lua", 0)

    def teardown_method(self, method):
        """
        Teardoen method.
        """
        policy = {
                'timeout' : 5000
                }
        self.client.udf_remove(policy, "bin_lua.lua")

    def test_getRegistered_with_no_parameters(self):
        """
        Invoke getRegistered() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            self.client.udf_getRegistered()

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
        
        udf_contents = self.client.udf_getRegistered(module, language, policy)

        time.sleep(2)
        #Check for udf file contents


    def test_getRegistered_with_correct_policy(self):
        """
        Invoke getRegistered() with correct policy
        """
        module = "bin_lua.lua"
        language = aerospike.UDF_TYPE_LUA
        policy = {
                'timeout' : 5000
                }
        
        udf_contents = self.client.udf_getRegistered(module, language, policy)

    def test_append_with_incorrect_policy(self):
        """
        Invoke append() with incorrect policy
        """
        module = "bin_lua.lua"
        language = aerospike.UDF_TYPE_LUA
        policy = {
                'timeout' : 0.5
                }
        
        with pytest.raises(Exception) as exception:
            self.client.udf_getRegistered(module, language, policy)

        assert exception.value[0] == -1
        assert exception.value[1] == "Invalid value(type) for policy key"

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
            self.client.udf_getRegistered(module, language, policy)

        assert exception.value[0] == 100
        assert exception.value[1] == "AEROSPIKE_ERR_UDF"

    def test_getRegistered_with_random_language(self):
        """
        Invoke getRegistered() with random language
        """
        module = "bin_lua.lua"
        language = aerospike.UDF_TYPE_LUAa
        policy = {
                'timeout' : 1000
                }
       
        with pytest.raises(Exception) as exception:
            self.client.udf_getRegistered(module, language, policy)

        assert exception.value[0] == -1
        assert exception.value[1] == "Invalid language(type)"

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
            self.client.udf_getRegistered(module, language, policy, "")

        assert "udf_getRegistered() takes at most 3 arguments (4 given)" in typeError.value

    def test_append_policy_is_string(self):
        """
        Invoke getRegistered() with policy is string
        """
        module = "bin_lua.lua"
        language = aerospike.UDF_TYPE_LUA

        with pytest.raises(Exception) as exception:
            self.client.udf_getRegistered(module, language, "")

        assert exception.value[0] == -2
        assert exception.value[1] == "policy must be a dict"

    def test_getRegistered_module_is_none(self):
        """
        Invoke getRegistered() with module is none
        """
        language = aerospike.UDF_TYPE_LUA

        with pytest.raises(Exception) as exception:
            self.client.udf_getRegistered(None, language)

        assert exception.value[0] == -1
        assert exception.value[1] == "Module name should be a string"
