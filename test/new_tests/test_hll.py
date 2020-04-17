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


class TestHLL(object):

    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        key = ('test', 'demo', 1)
        # rec = {'list_bin': [1, 2, 3]}
        # as_connection.put(key, rec)

        ops = [
            hll_operations.hll_init('hll_bin', 10),
            hll_operations.hll_add('hll_bin', ['key1', 'key2', 'key3', 'key4'], 8),
            hll_operations.hll_add_mh('mh_bin', ['key1', 'key2', 'key3'], 8, 10)
        ]
        self.test_key = key
        _, _, _ = as_connection.operate(self.test_key, ops)

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
        Invoke hll_add() creating a new HLL.
        """
        ops = [
            hll_operations.hll_add('new_bin', ['key1', 'key2', 'key3'], 8)
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)

        assert res['new_bin'] == 3
        #TODO use get_count to actually check the new hll
        #(key, _, bins) = self.as_connection.get(self.test_key)

    def test_pos_hll_add_mh(self):
        """
        Invoke hll_add_mh() creating a new min hash HLL.
        """
        ops = [
            hll_operations.hll_add_mh('new_bin', ['key1', 'key2', 'key3'], 6, 8)
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)

        assert res['new_bin'] == 3
        #TODO use get_count to actually check the new hll
        #(key, _, bins) = self.as_connection.get(self.test_key)

    def test_pos_hll_get_count(self):
        """
        Invoke hll_get_count() to check an HLL's count.
        """
        ops = [
            hll_operations.hll_get_count('hll_bin')
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        assert res['hll_bin'] == 4

    def test_pos_hll_describe(self):
        """
        Invoke hll_describe() and check index and min hash bits.
        """
        ops = [
            hll_operations.hll_describe('mh_bin')
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        assert res['mh_bin'] == [8, 10]

    def test_pos_hll_fold(self):
        """
        Invoke hll_fold().
        """
        ops = [
            hll_operations.hll_fold('hll_bin', 4),
            hll_operations.hll_get_count('hll_bin')
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        assert res['hll_bin'] == 4

    def test_pos_hll_fold(self):
        """
        Invoke hll_fold().
        """
        ops = [
            hll_operations.hll_fold('hll_bin', 4),
            hll_operations.hll_get_count('hll_bin')
        ]

        _, _, res = self.as_connection.operate(self.test_key, ops)
        assert res['hll_bin'] == 4