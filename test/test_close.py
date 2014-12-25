
# -*- coding: utf-8 -*-

import pytest
import sys
import cPickle as pickle
try:
    import aerospike
except:
    print "Please install aerospike python client."
    sys.exit(1)

class TestClose(object):

    def test_close_positive(self):
        """
            Invoke close() after positive connect
        """
        config = {
                'hosts': [('127.0.0.1', 3000)]
                }
        self.client = aerospike.client(config).connect()
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
