# -*- coding: utf-8 -*-
import pytest
import time
import sys
import cPickle as pickle
from test_base_class import TestBaseClass
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
        hostlist, user, password = TestBaseClass.get_hosts()
        config = {
                'hosts': hostlist
                }
        if user == None and password == None:
            self.client = aerospike.client(config).connect()
        else:
            self.client = aerospike.client(config).connect(user, password)
        if self.client.isConnected():
            assert True == True
        else:
            assert True == False

        self.client.close()

    def test_isconnected_beforeconnect(self):
        """
        Invoke isconnected() before connect
        """
        hostlist, user, password = TestBaseClass.get_hosts()
        config = {
                'hosts': hostlist
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
        hostlist, user, password = TestBaseClass.get_hosts()
        config = {
                'hosts': hostlist
                }
        if user == None and password == None:
            self.client = aerospike.client(config).connect()
        else:
            self.client = aerospike.client(config).connect(user, password)

        self.client.close()
        if self.client.isConnected():
            assert True == False
        else:
            assert True == True

