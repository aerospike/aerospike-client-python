# -*- coding: utf-8 -*-

import pytest
import sys
import time

try:
    import aerospike
except:
    print "Please install aerospike python client."
    sys.exit(1)

class TestUdfPut(object):
    def setup_class(cls):
        """
        Setup class
        """
        config = { 'hosts' : [ ('127.0.0.1', 3000) ] }

        TestUdfPut.client = aerospike.client(config).connect()

    def teardown_class(cls):
        TestUdfPut.client.close()

    def setup_method(self, method):

        """
        Setup method
        """
        time.sleep(2)

    def teardown_method(self, method):

        """
        Teardown method
        """
        udf_list = TestUdfPut.client.udf_list( { 'timeout' : 0 } )
        #time.sleep(2)
        for udf in udf_list:
            if udf['name'] == 'example.lua':
                TestUdfPut.client.udf_remove({}, "example.lua")

    def test_udf_put_without_parameters(self):

        with pytest.raises(TypeError) as typeError:
            TestUdfPut.client.udf_put()

        assert "Required argument 'policy' (pos 1) not found" in typeError.value

    def test_udf_put_with_proper_parameters(self):

        policy = {}
        filename = "example.lua"
        udf_type = 0

        status = TestUdfPut.client.udf_put( policy, filename, udf_type )

        assert status == 0
        time.sleep(2)
        udf_list = TestUdfPut.client.udf_list( {} )

        present = False
        for udf in udf_list:
            if 'example.lua' == udf['name']:
                present = True

        assert True if present else False

    def test_udf_put_with_invalid_timeout_policy_value(self):

        policy = { 'timeout' : 0.1 }
        filename = "example.lua"
        udf_type = 0

        with pytest.raises(Exception) as exception:
            status = TestUdfPut.client.udf_put( policy, filename, udf_type )

        assert exception.value[0] == -2
        assert exception.value[1] == "timeout is invalid"

    def test_udf_put_with_proper_timeout_policy_value(self):

        policy = { 'timeout' : 1000 }
        filename = "example.lua"
        udf_type = 0

        status = TestUdfPut.client.udf_put( policy, filename, udf_type )

        assert status == 0

        udf_list = TestUdfPut.client.udf_list( {} )

        present = False
        for udf in udf_list:
            if 'example.lua' == udf['name']:
                present = True


    def test_udf_put_with_non_existent_filename(self):

        policy = {}
        filename = "somefile"
        udf_type = 0

        with pytest.raises(Exception) as exception:
            status = TestUdfPut.client.udf_put( policy, filename, udf_type )

        assert exception.value[0] == 2
        assert exception.value[1] == "cannot open script file"

    def test_udf_put_with_non_lua_udf_type_and_lua_script_file(self):

        policy = { 'timeout' : 0 }
        filename = "example.lua"
        udf_type = 1

        with pytest.raises(Exception) as exception:
            status = TestUdfPut.client.udf_put( policy, filename, udf_type )

        assert exception.value[0] == 4
        assert exception.value[1] == "AEROSPIKE_ERR_REQUEST_INVALID"

    def test_udf_put_with_all_none_parameters(self):

        with pytest.raises(Exception) as exception:
            status = TestUdfPut.client.udf_put( None, None, None )

        assert exception.value[0] == -2
        assert exception.value[1] == "Filename should be a string"
