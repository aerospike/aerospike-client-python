# -*- coding: utf-8 -*-
import pytest

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
