# -*- coding: utf-8 -*-

import pytest
import sys
import json
import time
import os

from contextlib import contextmanager
from .test_base_class import TestBaseClass
from aerospike import exception as e
aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)

test_memleak = int(os.environ.get("TEST_MEMLEAK", 0))
if test_memleak != 1:
    if pytest.__version__ < "3.0.0":
        pytest.skip('Skip memleak tests')
    else:
        pytestmark = pytest.mark.skip

test_memleak_loop = int(os.environ.get("TEST_MEMLEAK_LOOP", 1))

@contextmanager
def open_as_connection(config):
    """
    Context manager to let us open aerospike connections with
    specified config
    """    
    as_connection = TestBaseClass.get_new_connection(config)

    # Connection is setup, so yield it
    yield as_connection

    # close the connection
    as_connection.close()


# adds cls.connection_config to this class
@pytest.mark.usefixtures("connection_config")
class TestConnectLeak(object):
    
    def test_connect_leak(self):
        """
            Invoke connect() with positive parameters.
        """
        config = self.connection_config.copy()
        i = 0
        while i < test_memleak_loop:
            with open_as_connection(config) as client:
                assert client is not None
                assert client.is_connected()
                time.sleep(.1)
            i = i + 1
