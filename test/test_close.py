
# -*- coding: utf-8 -*-

import pytest
import sys
import cPickle as pickle
from test_base_class import TestBaseClass

try:
    import aerospike
except:
    print "Please install aerospike python client."
    sys.exit(1)

class TestClose(TestBaseClass):

    def setup_class(cls):
        hostlist, user, password = TestBaseClass.get_hosts()

    def test_close_positive(self):
        """
            Invoke close() after positive connect
        """
        config = {
                'hosts': TestClose.hostlist
                }
        if TestClose.user == None and TestClose.password == None:
            self.client = aerospike.client(config).connect()
        else:
            self.client = aerospike.client(config).connect(TestClose.user, TestClose.password)

        self.closeobject = self.client.close()
        assert self.closeobject == None

    def test_close_negative(self):
        """
            Invoke close() after negative connect
        """
        config = {
                'hosts': [('127.0.0.1', 2000)]
                }

        with pytest.raises(Exception) as exception:
            self.client = aerospike.client(config).connect()
        with pytest.raises(AttributeError) as attributeError:
            self.closeobject = self.client.close()
        assert "'TestClose' object has no attribute 'client'" in attributeError.value
