# -*- coding: utf-8 -*-

import pytest
import sys
import time

try:
    import aerospike
except:
    print "Please install aerospike python client."
    sys.exit(1)

class TestUdfRemove(object):

    def setup_class(cls):

        """
        Setup class
        """
        config = { 'hosts' : [ ('127.0.0.1', 3000) ] }

        TestUdfRemove.client = aerospike.client(config).connect()

    def teardown_class(cls):

        """
        Teardown class
        """

        TestUdfRemove.client.close()

    def setup_method(self, method):
        """
        Setup method
        """
        TestUdfRemove.client.udf_put( {}, 'example.lua', 0 )
        time.sleep(2)

    def teardown_method(self,method):
        """
        Teardown method
        """
        udf_list = TestUdfRemove.client.udf_list( { 'timeout' : 0 } )
        time.sleep(2)
        for udf in udf_list:
            if udf['name'] == 'example.lua':
                TestUdfRemove.client.udf_remove({}, "example.lua")

    def test_udf_remove_without_parameters(self):

        with pytest.raises(TypeError) as typeError:
            status = TestUdfRemove.client.udf_remove()
        assert "Required argument 'policy' (pos 1) not found" in typeError.value

    def test_udf_remove_with_none_as_parameters(self):

        with pytest.raises(Exception) as exception:
            status = TestUdfRemove.client.udf_remove(None, None)

        assert exception.value[0] == -2
        assert exception.value[1] == "Filename should be a string"

    def test_udf_remove_with_proper_parameters(self):

        policy = { 'timeout' : 0 }
        module = "example.lua"
        status = TestUdfRemove.client.udf_remove( policy, module )

        assert status == 0

        time.sleep(4)
        udf_list = TestUdfRemove.client.udf_list( {'timeout' : 0} )

        present = False
        for udf in udf_list:
            if 'example.lua' == udf['name']:
                present = True

        assert False if present else True

    def test_udf_remove_with_invalid_timeout_policy_value(self):

        policy = { 'timeout' : 0.1 }
        module = "example.lua"

        status = TestUdfRemove.client.udf_remove( policy, module )

        assert status == 0

    def test_udf_remove_with_proper_timeout_policy_value(self):

        policy = { 'timeout' : 1000 }
        module = "example.lua"

        status = TestUdfRemove.client.udf_remove( policy, module )

        assert status == 0
        time.sleep(3)

        udf_list = TestUdfRemove.client.udf_list( {'timeout' : 0} )
       
        present = False
        for udf in udf_list:
            if 'example.lua' == udf['name']:
                present = True

        assert False if present else True

    def test_udf_remove_with_non_existent_module(self):

        policy = {}
        module = "some_module"

        with pytest.raises(Exception) as exception:
            status = TestUdfRemove.client.udf_remove( policy, module )

        assert exception.value[0] == 100
        assert exception.value[1] == "AEROSPIKE_ERR_UDF"
