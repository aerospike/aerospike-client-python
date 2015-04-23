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

class TestGetNodes(object):
    def setup_class(cls):
        """
        Setup class.
        """
        hostlist, user, password = TestBaseClass.get_hosts()
        config = {'hosts': hostlist}
        if user == None and password == None:
            TestGetNodes.client = aerospike.client(config).connect()
        else:
            TestGetNodes.client = aerospike.client(config).connect(user,
                                                                   password)

    def teardown_class(cls):
        """
        Teardown class.
        """
        TestGetNodes.client.close()

    def test_get_nodes_positive(self):

        response = TestGetNodes.client.get_nodes()
        assert response != None

    def test_get_nodes_with_parameter(self):

        response = TestGetNodes.client.get_nodes("parameter")
        assert response != None

    def test_get_nodes_positive_without_connection(self):
        config = {'hosts': [('127.0.0.1', 3000)]}
        client1 = aerospike.client(config)

        try:
            response = client1.get_nodes()

        except ClusterError as exception:
            assert exception.code == 11L
            assert exception.msg == 'No connection to aerospike cluster'
