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

class TestIsconnected(object):

    def setup_method(self, method):
        """
        Setup method.
        """

    def teardown_method(self, method):
        """
        Teardoen method.
        """

    def test_isconnected_positive(self):
        """
        Invoke isconnected() positive.
        """
        config = {
            'hosts': [('127.0.0.1', 3000)]
        }
        self.client = aerospike.client(config).connect()
        if self.client.isConnected():
            assert True == True
        else:
            assert True == False

        self.client.close()

    def test_isconnected_beforeconnect(self):
        """
        Invoke isconnected() before connect
        """
        config = {
            'hosts': [('127.0.0.1', 3000)]
        }
        self.client = aerospike.client(config)
        if self.client.isConnected():
            assert True == False
        else:
            assert True == True

        self.client.connect()

        if self.client.isConnected():
            assert True == True
        else:
            assert True == False
        self.client.close()

    def test_isconnected_afterclose(self):
        """
        Invoke isconnected() positive.
        """
        config = {
            'hosts': [('127.0.0.1', 3000)]
        }
        self.client = aerospike.client(config).connect()

        self.client.close()
        if self.client.isConnected():
            assert True == False
        else:
            assert True == True

