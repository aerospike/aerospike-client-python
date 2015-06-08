# -*- coding: utf-8 -*-

import pytest
import sys
import time
from test_base_class import TestBaseClass
from types import *

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
    from aerospike.exception import *
except:
    print "Please install aerospike python client."
    sys.exit(1)

class TestUdfList(TestBaseClass):
    def setup_class(cls):
        """
        Setup class
        """
        hostlist, user, password = TestBaseClass.get_hosts()
        config = {'hosts': hostlist}

        if user == None and password == None:
            TestUdfList.client = aerospike.client(config).connect()
        else:
            TestUdfList.client = aerospike.client(config).connect(user,
                                                                  password)

        TestUdfList.client.udf_put('example.lua', 0, {})

    def teardown_class(cls):
        """
        Teardown class
        """
        TestUdfList.client.udf_remove("example.lua")

        TestUdfList.client.close()

    def test_udf_list_without_any_paramter(self):

        ulist = TestUdfList.client.udf_list()
        assert type(ulist) is ListType

    def test_udf_list_with_proper_parameters(self):

        policy = {'timeout': 0}
        udf_list = TestUdfList.client.udf_list(policy)

        present = False
        for udf in udf_list:
            if 'example.lua' == udf['name']:
                present = True

        assert True if present else False

    def test_udf_list_with_timeout_policy(self):

        policy = {'timeout': 1000}

        udf_list = TestUdfList.client.udf_list(policy)

        present = False
        for udf in udf_list:
            if 'example.lua' == udf['name']:
                present = True

        assert True if present else False

    def test_udf_list_with_invalid_timeout_policy_value(self):

        policy = {'timeout': 0.1}

        try:
            udf_list = TestUdfList.client.udf_list( policy )

        except ParamError as exception:
            assert exception.code == -2

            assert exception.msg == 'timeout is invalid'

    def test_udf_list_with_proper_parameters_without_connection(self):

        config = {'hosts': [('127.0.0.1', 3000)]}

        client1 = aerospike.client(config)

        policy = {'timeout': 0}

        try:
            udf_list = client1.udf_list( policy )

        except ClusterError as exception:
            assert exception.code == 11L
            assert exception.msg == 'No connection to aerospike cluster'
