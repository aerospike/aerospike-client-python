# -*- coding: utf-8 -*-
import pytest
from .test_base_class import TestBaseClass
from aerospike import exception as e

import aerospike


@pytest.mark.xfail(TestBaseClass.tls_in_use(), reason="get_nodes may fail when using TLS")
@pytest.mark.usefixtures("as_connection")
class TestGetNodes(object):
    """
    Test Cases for the use of aerospike.Client.get_nodes
    method
    """

    def test_pos_get_nodes(self):
        """
        Test successful to aerospike.Client.get_nodes
        without parameters.

        It should return something like [('1.1.1.1', 3000)]
        """
        response = self.as_connection.get_nodes()
        assert response is not None
        assert isinstance(response, list)
        assert isinstance(response[0], tuple)
        assert len(response[0]) == 2

    def test_get_nodes_with_parameter(self):
        """
        Test that a call to aerospike.Client.get_nodes with
        a string parameter does not raise an error
        """
        response = self.as_connection.get_nodes("parameter")
        assert response is not None

    # Tests for behaviors that raise errors
    def test_pos_get_nodes_without_connection(self):
        """
        Test that an attempt to call get_nodes before a connection
        is established will raise the expected error
        """
        config = TestBaseClass.get_connection_config()
        unconnected_client = aerospike.client(config)
        unconnected_client.close()

        try:
            unconnected_client.get_nodes()

        except e.ClusterError as exception:
            assert exception.code == 11
            assert exception.msg == "No connection to aerospike cluster"
