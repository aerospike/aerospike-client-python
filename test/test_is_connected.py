# -*- coding: utf-8 -*-
import pytest
import time
import sys
import cPickle as pickle
from test_base_class import TestBaseClass

aerospike = pytest.importorskip("aerospike")


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
        Invoke is_connected() positive.
        """
        hostlist, user, password = TestBaseClass.get_hosts()
        config = {'hosts': hostlist}
        if user == None and password == None:
            self.client = aerospike.client(config).connect()
        else:
            self.client = aerospike.client(config).connect(user, password)
        assert self.client.is_connected() == True
        self.client.close()

    def test_isconnected_beforeconnect(self):
        """
        Invoke is_connected() before connect
        """
        hostlist, user, password = TestBaseClass.get_hosts()
        config = {'hosts': hostlist}
        self.client = aerospike.client(config)
        assert self.client.is_connected() == False
        if user == None and password == None:
            self.client.connect()
        else:
            self.client.connect(user, password)
        assert self.client.is_connected() == True
        self.client.close()

    def test_isconnected_afterclose(self):
        """
        Invoke is_connected() after close.
        """
        hostlist, user, password = TestBaseClass.get_hosts()
        config = {'hosts': hostlist}
        if user == None and password == None:
            self.client = aerospike.client(config).connect()
        else:
            self.client = aerospike.client(config).connect(user, password)

        self.client.close()
        assert self.client.is_connected() == False
