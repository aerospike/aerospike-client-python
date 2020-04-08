# -*- coding: utf-8 -*-
import pytest
import sys
import random
from .test_base_class import TestBaseClass
from aerospike import exception as e
from aerospike_helpers.operations import hll_operations 

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)


class TestListAppend(object):

    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        key = ('test', 'demo', 1)
        rec = {'list_bin': [1, 2, 3]}
        as_connection.put(key, rec)
        self.test_key = key

        def teardown():
            """
            Teardown method.
            """
            try:
                as_connection.remove(self.test_key)
            except e.RecordNotFound:
                pass

        request.addfinalizer(teardown)


    def test_pos_hll_add(self):
        """
        Invoke list_append() append value to a list
        """
        ops = [
            hll_operations.hll_add()
        ]

        _, _, res = self.as_connection.operate(self.test_key)

        (key, _, bins) = self.as_connection.get(self.test_key)
        print(bins)