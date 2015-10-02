# -*- coding: utf-8 -*-

import pytest
import sys
import time
from test_base_class import TestBaseClass

aerospike = pytest.importorskip("aerospike")
try:
    from aerospike.exception import *
except:
    print "Please install aerospike python client."
    sys.exit(1)

class TestUdfPut(TestBaseClass):
    def setup_class(cls):
        """
        Setup class
        """
        hostlist, user, password = TestBaseClass.get_hosts()
        config = {'hosts': hostlist}

        if user == None and password == None:
            TestUdfPut.client = aerospike.client(config).connect()
        else:
            TestUdfPut.client = aerospike.client(config).connect(user, password)

    def teardown_class(cls):
        TestUdfPut.client.close()

    def setup_method(self, method):
        """
        Setup method
        """

    def teardown_method(self, method):
        """
        Teardown method
        """
        time.sleep(1)
        udf_list = TestUdfPut.client.udf_list({'timeout': 100})
        for udf in udf_list:
            if udf['name'] == 'example.lua':
                TestUdfPut.client.udf_remove("example.lua")

    def test_udf_put_without_parameters(self):

        with pytest.raises(TypeError) as typeError:
            TestUdfPut.client.udf_put()

        assert "Required argument 'filename' (pos 1) not found" in typeError.value

    def test_udf_put_with_proper_parameters(self):

        policy = {}
        filename = "example.lua"
        udf_type = 0

        status = TestUdfPut.client.udf_put(filename, udf_type, policy)

        assert status == 0
        udf_list = TestUdfPut.client.udf_list({})

        present = False
        for udf in udf_list:
            if 'example.lua' == udf['name']:
                present = True

        assert True if present else False

    def test_udf_put_with_invalid_timeout_policy_value(self):

        policy = {'timeout': 0.1}
        filename = "example.lua"
        udf_type = 0

        try:
            status = TestUdfPut.client.udf_put( filename, udf_type, policy )

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "timeout is invalid"

    def test_udf_put_with_proper_timeout_policy_value(self):

        policy = {'timeout': 1000}
        filename = "example.lua"
        udf_type = 0

        status = TestUdfPut.client.udf_put(filename, udf_type, policy)

        assert status == 0

        udf_list = TestUdfPut.client.udf_list({})

        present = False
        for udf in udf_list:
            if 'example.lua' == udf['name']:
                present = True

    def test_udf_put_with_non_existent_filename(self):

        policy = {}
        filename = "somefile"
        udf_type = 0

        try:
            TestUdfPut.client.udf_put( filename, udf_type, policy )
        except LuaFileNotFound as e:
            assert e.code == 1302

    def test_udf_put_with_non_lua_udf_type_and_lua_script_file(self):

        policy = {'timeout': 0}
        filename = "example.lua"
        udf_type = 1

        try:
            status = TestUdfPut.client.udf_put( filename, udf_type, policy )

        except ClientError as exception:
            assert exception.code == -1
            assert exception.msg == "Invalid UDF language"

    def test_udf_put_with_all_none_parameters(self):

        with pytest.raises(TypeError) as exception:
            status = TestUdfPut.client.udf_put(None, None, None)

        assert "an integer is required" in exception.value

    def test_udf_put_with_filename_unicode(self):

        policy = {}
        filename = u"example.lua"
        udf_type = 0

        status = TestUdfPut.client.udf_put(filename, udf_type, policy)

        assert status == 0
        time.sleep(2)
        udf_list = TestUdfPut.client.udf_list({})

        present = False
        for udf in udf_list:
            if 'example.lua' == udf['name']:
                present = True

        assert True if present else False

    def test_udf_put_with_proper_parameters_without_connection(self):

        policy = {}
        filename = "example.lua"
        udf_type = 0

        config = {'hosts': [('127.0.0.1', 3000)]}

        client1 = aerospike.client(config)

        try:
            status = client1.udf_put( filename, udf_type, policy )

        except ClusterError as exception:
            assert exception.code == 11L
            assert exception.msg == 'No connection to aerospike cluster'
