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


class TestUdfRemove(TestBaseClass):
    def setup_class(cls):
        """
        Setup class
        """
        hostlist, user, password = TestBaseClass.get_hosts()
        config = {'hosts': hostlist}

        if user == None and password == None:
            TestUdfRemove.client = aerospike.client(config).connect()
        else:
            TestUdfRemove.client = aerospike.client(config).connect(user, password)

    def teardown_class(cls):
        """
        Teardown class
        """

        TestUdfRemove.client.close()

    def setup_method(self, method):
        """
        Setup method
        """
        TestUdfRemove.client.udf_put(u'example.lua', 0, {})
        time.sleep(2)

    def teardown_method(self, method):
        """
        Teardown method
        """
        time.sleep(2)
        udf_list = TestUdfRemove.client.udf_list({'timeout': 100})
        for udf in udf_list:
            if udf['name'] == 'example.lua':
                TestUdfRemove.client.udf_remove("example.lua", {})

    def test_udf_remove_without_parameters(self):

        with pytest.raises(TypeError) as typeError:
            status = TestUdfRemove.client.udf_remove()
        assert "Required argument 'filename' (pos 1) not found" in typeError.value

    def test_udf_remove_with_none_as_parameters(self):

        try:
            status = TestUdfRemove.client.udf_remove(None, None)

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Filename should be a string"

    def test_udf_remove_with_proper_parameters(self):

        policy = {'timeout': 50}
        module = "example.lua"
        status = TestUdfRemove.client.udf_remove(module, policy)

        assert status == 0

        time.sleep(4)
        udf_list = TestUdfRemove.client.udf_list({'timeout': 100})

        present = False
        for udf in udf_list:
            if 'example.lua' == udf['name']:
                present = True

        assert False if present else True

    def test_udf_remove_with_invalid_timeout_policy_value(self):

        policy = {'timeout': 0.1}
        module = "example.lua"

        status = TestUdfRemove.client.udf_remove(module, policy)

        assert status == 0

    def test_udf_remove_with_proper_timeout_policy_value(self):

        policy = {'timeout': 1000}
        module = "example.lua"

        status = TestUdfRemove.client.udf_remove(module, policy)

        assert status == 0
        time.sleep(3)

        udf_list = TestUdfRemove.client.udf_list({'timeout': 0})

        present = False
        for udf in udf_list:
            if 'example.lua' == udf['name']:
                present = True

        assert False if present else True

    def test_udf_remove_with_non_existent_module(self):

        policy = {}
        module = "some_module"

        try:
            status = TestUdfRemove.client.udf_remove( module, policy )

        except UDFError as exception:
            assert exception.code == 100
            assert exception.msg == "error=file_not_found\n"
            assert exception.module == "some_module"

    def test_udf_remove_with_unicode_filename(self):

        policy = {'timeout': 100}
        module = u"example.lua"
        status = TestUdfRemove.client.udf_remove(module, policy)

        assert status == 0

        time.sleep(4)
        udf_list = TestUdfRemove.client.udf_list({'timeout': 100})

        present = False
        for udf in udf_list:
            if 'example.lua' == udf['name']:
                present = True

        assert False if present else True

    def test_udf_remove_with_proper_parameters_without_connection(self):

        config = {'hosts': [('127.0.0.1', 3000)]}

        client1 = aerospike.client(config)
        policy = {'timeout': 100}
        module = "example.lua"

        try:
            status = client1.udf_remove( module, policy )

        except ClusterError as exception:
            assert exception.code == 11L
            assert exception.msg == 'No connection to aerospike cluster'
