# -*- coding: utf-8 -*-

import pytest
import sys
from .test_base_class import TestBaseClass

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
    from aerospike import exception as e
except:
    print("Please install aerospike python client.")
    sys.exit(1)


class TestClose():

    def setup_class(cls):
        TestClose.hostlist, TestClose.user, TestClose.password = TestBaseClass.get_hosts()

    def test_pos_close(self):
        """
            Invoke close() after positive connect
        """
        config = {'hosts': TestBaseClass.hostlist}
        if TestClose.user is None and TestClose.password is None:
            self.client = aerospike.client(config).connect()
        else:
            self.client = aerospike.client(config).connect(TestClose.user,
                                                           TestClose.password)
        self.closeobject = self.client.close()
        assert self.closeobject is None

    def test_pos_close_without_connection(self):
        """
            Invoke close() without connection
        """
        config = {'hosts': [('127.0.0.1', 3000)]}
        self.client = aerospike.client(config)

        try:
            self.closeobject = self.client.close()

        except e.ClusterError as exception:
            assert exception.code == 11

    def test_neg_close(self):
        """
            Invoke close() after negative connect
        """
        config = {'hosts': [('127.0.0.1', 2000)]}

        with pytest.raises(Exception):
            self.client = aerospike.client(config).connect()
        with pytest.raises(AttributeError) as attributeError:
            self.closeobject = self.client.close()
        assert "has no attribute" in str(attributeError.value)

    def test_close_twice_in_a_row(self):
        config = {'hosts': TestBaseClass.hostlist}
        if TestClose.user is None and TestClose.password is None:
            self.client = aerospike.client(config).connect()
        else:
            self.client = aerospike.client(config).connect(TestClose.user,
                                                           TestClose.password)

        assert self.client.is_connected()
        self.client.close()
        assert self.client.is_connected() is False

        # This second call should not raise any errors
        self.client.close()
        assert self.client.is_connected() is False
