# -*- coding: utf-8 -*-

import pytest
from .test_base_class import TestBaseClass
import aerospike
from aerospike import exception as e

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
    from aerospike import exception as e
except:
    print("Please install aerospike python client.")
    sys.exit(1)


class TestClose():

class TestClose:
    def setup_class(cls):
        config = TestBaseClass.get_connection_config()
        TestClose.hostlist = config["hosts"]
        TestClose.user = config["user"]
        TestClose.password = config["password"]
        TestClose.auth_mode = config["policies"]["auth_mode"]

    def test_pos_close(self):
        """
            Invoke close() after positive connect
        """
        self.client = TestBaseClass.get_new_connection()
        self.closeobject = self.client.close()
        assert self.closeobject is None

    def test_neg_close(self):
        """
            Invoke close() after negative connect
        """
        config = {"hosts": [("127.0.0.1", 2000)]}

        with pytest.raises(Exception):
            self.client = aerospike.client(config).connect()
        with pytest.raises(AttributeError) as attributeError:
            self.closeobject = self.client.close()
        assert "has no attribute" in str(attributeError.value)

    def test_close_twice_in_a_row(self):
        """
         Client call itself establishes connection.
         Connect/Close are deprecated and it is no-op to client
         """
        config = TestBaseClass.get_connection_config()
        if TestClose.user is None and TestClose.password is None:
            self.client = aerospike.client(config).connect()
        else:
            self.client = aerospike.client(config).connect(TestClose.user, TestClose.password)

        assert self.client.is_connected()
        self.client.close()
        assert self.client.is_connected() is True

        # This second call should not raise any errors
        self.client.close()
        assert self.client.is_connected() is True
