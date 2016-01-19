# -*- coding: utf-8 -*-
import pytest
import sys

from .test_base_class import TestBaseClass
from aerospike import exception as e

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)


class TestGetNodes(object):

    def setup_class(cls):
        """
        Setup class.
        """
        hostlist, user, password = TestBaseClass.get_hosts()
        config = {'hosts': hostlist}
        if user is None and password is None:
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
        assert response is not None

    def test_get_nodes_with_parameter(self):

        response = TestGetNodes.client.get_nodes("parameter")
        assert response is not None

    def test_get_nodes_positive_without_connection(self):
        config = {'hosts': [('127.0.0.1', 3000)]}
        client1 = aerospike.client(config)

        try:
            client1.get_nodes()

        except e.ClusterError as exception:
            assert exception.code == 11
            assert exception.msg == 'No connection to aerospike cluster'
