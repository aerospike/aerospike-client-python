# -*- coding: utf-8 -*-
import pytest
import sys
import random
from datetime import datetime
from aerospike import exception as e
from aerospike_helpers.operations import operations as operation
from .test_base_class import TestBaseClass

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)

random.seed(datetime.now())

class TestBitwiseOperations(object):
    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        """
        Setup Method
        """
        self.keys = []
        self.test_key = 'test', 'demo', 'bitwise_op'
        self.true_bin = 'true_bin'
        self.false_bin = 'false_bin'

        self.as_connection.put(
            self.test_key,
            {
                self.true_bin: True,
                self.false_bin: False
            }
        )
        self.keys.append(self.test_key)

        yield

        for key in self.keys:
            try:
                self.as_connection.remove(key)
            except e.AerospikeError:
                pass

    @pytest.mark.parametrize("send_bool_as, expected_true, expected_false", [
        (aerospike.PY_BYTES, True, False),
        (aerospike.INTEGER, 1, 0),
        (aerospike.AS_BOOL, True, False),
        (100, True, False),
        (0, True, False),
        (-1, True, False)
    ])
    def test_bool_read_write_pos(self, send_bool_as, expected_true, expected_false):
        """
        Write Python bools with different client configurations.
        """
        config = TestBaseClass.get_connection_config()
        config["send_bool_as"] = send_bool_as
        test_client = aerospike.client(config).connect(config['user'], config['password'])
        ops = [
            operation.write("cfg_true", True),
            operation.write("cfg_false", False),
        ]

        try:
            test_client.operate(self.test_key, ops)
        except e.InvalidRequest:
            if self.server_version < [5, 6] and send_bool_as == aerospike.AS_BOOL:
                pytest.mark.xfail(reason="Servers older than 5.6 do not support as_bool")
                pytest.xfail()


        _, _, bins = self.as_connection.get(self.test_key)
        test_client.close()
        #  We should not have changed the zeroes bin
        assert bins["cfg_true"] is expected_true
        assert bins["cfg_false"] is expected_false
        assert bins[self.true_bin] is True
        assert bins[self.false_bin] is False