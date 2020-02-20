# -*- coding: utf-8 -*-

import pytest
import sys
from .test_base_class import TestBaseClass
try:
    from collections import Counter
except ImportError:
    from counter26 import Counter

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
    from aerospike import exception as e
except:
    print("Please install aerospike python client.")
    sys.exit(1)


class TestGetMany():

    pytestmark = pytest.mark.skipif(
        not TestBaseClass.enterprise_in_use(),
        reason="No user specified, may be not secured cluster.")

    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        self.keys = []

        for i in range(5):
            key = ('test', 'demo', i)
            rec = {'name': 'name%s' % (str(i)), 'age': i}
            as_connection.put(key, rec)
            self.keys.append(key)

        key = ('test', 'demo', 'float_value')
        as_connection.put(key, {"float_value": 4.3})
        self.keys.append(key)

        def teardown():
            """
            Teardown method.
            """
            for i in range(5):
                key = ('test', 'demo', i)
                as_connection.remove(key)
            
            key = ('test', 'demo', 'float_value')
            as_connection.remove(key)

        request.addfinalizer(teardown)

    def test_batch_with_compress_policy(self):

        policy = {'compress': True}
        records = self.as_connection.get_many(self.keys, policy)

        assert isinstance(records, list)
        assert len(records) == 6
        assert Counter([x[0][2] for x in records]) == Counter([0, 1, 2, 3,
                                                               4, 'float_value'])
        assert records[5][2] == {'float_value': 4.3}

    def test_operate_with_compress_policy(self):
        """
        Invoke operate() with correct policy
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key': aerospike.POLICY_KEY_SEND,
            'commit_level': aerospike.POLICY_COMMIT_LEVEL_MASTER,
            'compress': True
        }

        llist = [{"op": aerospike.OPERATOR_APPEND,
                  "bin": "name",
                  "val": "aa"},
                 {"op": aerospike.OPERATOR_INCR,
                  "bin": "age",
                  "val": 3}, {"op": aerospike.OPERATOR_READ,
                              "bin": "name"}]

        key, _, bins = self.as_connection.operate(key, llist, {}, policy)

        assert bins == {'name': 'name1aa'}
        assert key == ('test', 'demo', 1, bytearray(
            b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8')
        )

    def test_read_with_compress_policy(self, put_data):
        """
            Invoke get() for a record having string data.
        """

        key = ('test', 'demo', 1)

        rec = {'name': 'john', 'age': 1}

        policy = {
            'key': aerospike.POLICY_KEY_SEND,
            'compress': True
        }

        self.as_connection.put(key, rec, policy)

        key, _, bins = self.as_connection.get(key, policy)

        assert bins == {'name': 'john', 'age': 1}
        assert key == ('test', 'demo', 1, bytearray(
            b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8')
        )

    def test_scan_with_compress_policy(self):

        ns = 'test'
        st = 'demo'

        records = []

        def callback(input_tuple):
            _, _, bins = input_tuple
            records.append(bins)

        scan_obj = self.as_connection.scan('test', 'demo')

        scan_obj.execute_background({'compress': True})