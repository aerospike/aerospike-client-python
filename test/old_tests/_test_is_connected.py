# -*- coding: utf-8 -*-
import pytest
import sys
from .test_base_class import TestBaseClass

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
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
        Invoke is_connected() positive.
        """
        hostlist, user, password = TestBaseClass.get_hosts()
        config = {'hosts': hostlist}
        if user is None and password is None:
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
        if user is None and password is None:
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
        if user is None and password is None:
            self.client = aerospike.client(config).connect()
        else:
            self.client = aerospike.client(config).connect(user, password)

        self.client.close()
        assert self.client.is_connected() == False
