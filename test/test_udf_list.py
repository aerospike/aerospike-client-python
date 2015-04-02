# -*- coding: utf-8 -*-

import pytest
import sys
import time
from test_base_class import TestBaseClass

try:
    import aerospike
except:
    print "Please install aerospike python client."
    sys.exit(1)

class TestUdfList(TestBaseClass):

    def setup_class(cls):

        """
        Setup class
        """
        hostlist, user, password = TestBaseClass.get_hosts()
        config = { 'hosts' : hostlist }

        if user == None and password == None:
            TestUdfList.client = aerospike.client(config).connect()
        else:
            TestUdfList.client = aerospike.client(config).connect(user, password)

        TestUdfList.client.udf_put('example.lua', 0, {})

    def teardown_class(cls):

        """
        Teardown class
        """
        TestUdfList.client.udf_remove("example.lua")

        TestUdfList.client.close()

    def test_udf_list_without_any_paramter(self):

        with pytest.raises(TypeError) as typeError:
            TestUdfList.client.udf_list()

        assert "Required argument 'policy' (pos 1) not found" in typeError.value

    def test_udf_list_with_proper_parameters(self):

        policy = { 'timeout' : 0 }
        udf_list = TestUdfList.client.udf_list( policy )

        present = False
        for udf in udf_list:
            if 'example.lua' == udf['name']:
                present = True

        assert True if present else False

    def test_udf_list_with_timeout_policy(self):

        policy = { 'timeout' : 1000 }

        udf_list = TestUdfList.client.udf_list( policy )

        present = False
        for udf in udf_list:
            if 'example.lua' == udf['name']:
                present = True

        assert True if present else False

    def test_udf_list_with_invalid_timeout_policy_value(self):

        policy = { 'timeout' : 0.1 }

        with pytest.raises(Exception) as exception:
            udf_list = TestUdfList.client.udf_list( policy )

        assert exception.value[0] == -2

        assert exception.value[1] == 'timeout is invalid'
