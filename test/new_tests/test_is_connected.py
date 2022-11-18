# -*- coding: utf-8 -*-

from .test_base_class import TestBaseClass

import aerospike


class TestIsConnected(object):
    def setup_class(cls):
        """
        Setup the config which is used in the tests
        """
        cls.config = TestBaseClass.get_connection_config()

    def _connect(self):
        """
        Private method to connect self.client to the server
        with or without username and password
        """
        # if self.user is None and self.password is None:
        #     self.client = aerospike.client(self.config).connect()
        # else:
        #     self.client = aerospike.client(self.config).connect(self.user,
        #                                                         self.password)
        self.client = TestBaseClass.get_new_connection()

    def test_is_connected_before_connect(self):
        """
        Client call itself establishes connection.
        """
        client = aerospike.client(self.config)
        assert client.is_connected() is True

    def test_pos_is_connected(self):
        """
        Test that is_connected returns True after a connection is established
        """
        self._connect()  # Establish a connection
        assert self.client.is_connected() is True
        self.client.close()  # Close the connection

    def test_pos_is_connected_after_multiple_connects(self):
        """
        Test that is_connected returns True after a connection is established
        """
        self._connect()  # Establish a connection
        self.client.connect()
        self.client.connect()
        assert self.client.is_connected() is True
        self.client.close()  # Close the connection

    def test_is_connected_after_close(self):
        """
        Client call itself establishes connection.
        Connect/Close are deprecated and it is no-op to client
        """
        self._connect()
        assert self.client.is_connected() is True
        self.client.close()
        assert self.client.is_connected() is False
