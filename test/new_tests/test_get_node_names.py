# -*- coding: utf-8 -*-
import pytest
from .test_base_class import TestBaseClass
from aerospike import exception as e

import aerospike


@pytest.mark.usefixtures("as_connection")
class TestGetNodeNames(object):
    """
    Test Cases for the use of aerospike.Client.get_node_names
    method
    """

    def test_pos_get_node_names(self):
        """
        Test successful to aerospike.Client.get_node_names
        without parameters.

        It should return something like [{'address': '1.1.1.1', 'port': 3000, 'node_name': 'BCER199932C'}]
        """
        response = self.as_connection.get_node_names()
        assert response is not None
        assert isinstance(response, list)
        assert isinstance(response[0], dict)
        assert isinstance(response[0]["address"], str)
        assert isinstance(response[0]["port"], int)
        assert isinstance(response[0]["node_name"], str)
        assert len(response[0]) == 3

    # Tests for behaviors that raise errors
    def test_pos_get_node_names_without_connection(self):
        """
        Test that an attempt to call get_node_names before a connection
        is established will raise the expected error
        """
        config = TestBaseClass.get_connection_config()
        unconnected_client = aerospike.client(config)
        unconnected_client.close()

        try:
            unconnected_client.get_node_names()

        except e.ClusterError as exception:
            assert exception.code == 11
            assert exception.msg == "No connection to aerospike cluster."
