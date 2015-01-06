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

class TestGetNodes(object):

    def setup_class(cls):
        """
        Setup class.
        """
        config = {
            'hosts': [('127.0.0.1', 3000)]
        }
        TestGetNodes.client = aerospike.client(config).connect()

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
