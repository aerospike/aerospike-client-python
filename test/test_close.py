# -*- coding: utf-8 -*-

import pytest
import sys
import cPickle as pickle
from test_base_class import TestBaseClass

aerospike = pytest.importorskip("aerospike")


class TestClose(TestBaseClass):
    def setup_class(cls):
        hostlist, user, password = TestBaseClass.get_hosts()

    def test_close_positive(self):
        """
            Invoke close() after positive connect
        """
        config = {'hosts': TestClose.hostlist}
        if TestClose.user == None and TestClose.password == None:
            self.client = aerospike.client(config).connect()
        else:
            self.client = aerospike.client(config).connect(TestClose.user,
                                                           TestClose.password)

        self.closeobject = self.client.close()
        assert self.closeobject == None

    def test_close_negative(self):
        """
            Invoke close() after negative connect
        """
        config = {'hosts': [('127.0.0.1', 2000)]}

        with pytest.raises(Exception) as exception:
            self.client = aerospike.client(config).connect()
        with pytest.raises(AttributeError) as attributeError:
            self.closeobject = self.client.close()
        assert "'TestClose' object has no attribute 'client'" in attributeError.value

    def test_close_positive_without_connection(self):
        """
            Invoke close() without connection
        """
        config = {'hosts': [('127.0.0.1', 3000)]}
        self.client = aerospike.client(config)

        with pytest.raises(Exception) as exception:
            self.closeobject = self.client.close()

        assert exception.value[0] == 11L
        assert exception.value[1] == 'No connection to aerospike cluster'
