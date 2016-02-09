# -*- coding: utf-8 -*-

import pytest
import sys
import cPickle as pickle
from test_base_class import TestBaseClass

aerospike = pytest.importorskip("aerospike")
try:
    from aerospike.exception import *
except:
    print "Please install aerospike python client."
    sys.exit(1)

class TestClose():
    def setup_class(cls):
        TestClose.hostlist, TestClose.user, TestClose.password = TestBaseClass.get_hosts()

    def test_pos_close(self):
        """
            Invoke close() after positive connect
        """
        config = {'hosts': TestBaseClass.hostlist}
        if TestClose.user == None and TestClose.password == None:
            self.client = aerospike.client(config).connect()
        else:
            self.client = aerospike.client(config).connect(TestClose.user,
                                                           TestClose.password)
        self.closeobject = self.client.close()
        assert self.closeobject == None

    def test_pos_close_without_connection(self):
        """
            Invoke close() without connection
        """
        config = {'hosts': [('127.0.0.1', 3000)]}
        self.client = aerospike.client(config)

        try:
            self.closeobject = self.client.close()

        except ClusterError as exception:
            assert exception.code == 11L

    def test_neg_close(self):
        """
            Invoke close() after negative connect
        """
        config = {'hosts': [('127.0.0.1', 2000)]}

        with pytest.raises(Exception) as exception:
            self.client = aerospike.client(config).connect()
        with pytest.raises(AttributeError) as attributeError:
            self.closeobject = self.client.close()
        assert "TestClose instance has no attribute 'client'" in attributeError.value


