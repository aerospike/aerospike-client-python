# -*- coding: utf-8 -*-

import pytest
import sys
try:
    import cPickle as pickle
except:
    import pickle
from .test_base_class import TestBaseClass
import time

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
    from aerospike import exception as e
except:
    print("Please install aerospike python client.")
    sys.exit(1)

class SomeClass(object):
    pass

@pytest.mark.usefixtures("as_connection")
class TestExists():

    def test_pos_exists_with_key_and_policy(self, put_data):
        """
            Invoke exists() with a key and policy.
        """
        key = ('test', 'demo', 1)
        record = {"Name": "Jeff"}
        policy = {
            'timeout': 1000,
            'replica': aerospike.POLICY_REPLICA_MASTER,
            'consistency': aerospike.POLICY_CONSISTENCY_ONE
        }

        put_data(self.as_connection, key, record)

        key, meta = self.as_connection.exists(key, policy)

        assert meta['gen'] != None
        assert meta['ttl'] != None

    @pytest.mark.parametrize("key, record", [
        (('test', 'demo', 'key'), {"Name": "Jeff"}),
        (('test', 'demo', 'list_key'), {'names': ['John', 'Marlen', 'Steve']}),
        (('test', 'demo', 'map_key'), {'names': {'name': 'John', 'age': 24}}),
        (('test', 'demo', 'bytes_key'), {'bytes': bytearray('John', 'utf-8')}),
        # list of objects.
        (('test', 'demo', 'objects'),
            {'objects': [pickle.dumps(SomeClass()),
            pickle.dumps(SomeClass())]}),
        (('test', 'demo', 'list_map_key'), {
            'names': ['John', 'Marlen', 'Steve'],
            'names_and_age': [{'name': 'John',
                               'age': 24}, {'name': 'Marlen',
                                            'age': 25}]
        }),
        ])
    def test_pos_exists_with_diff_datatype(self, key, record, put_data):
        """
            Invoke exists() for diffrent record data.
        """
        put_data(self.as_connection, key, record)
        key, meta = self.as_connection.exists(key)

        assert meta['gen'] != None
        assert meta['ttl'] != None

    @pytest.mark.parametrize("key, record, policy", [
    (('test', 'demo', 'p_None'),  {"name": "John"}, None),
    (('test', 'demo', 'p_Replica'),  {"name": "Michel"}, {
        'timeout': 1000,
        'replica': aerospike.POLICY_REPLICA_ANY,
        'consistency': aerospike.POLICY_CONSISTENCY_ONE}),
    (('test', 'demo', "p_consistency_level"),{"name": "Michel"}, {
        'timeout': 1000,
        'replica': aerospike.POLICY_REPLICA_MASTER,
        'consistency': aerospike.POLICY_CONSISTENCY_ALL}),
    ])
    def test_pos_exists_with_key_and_policy(self, key, record, policy, put_data):
        """
            Invoke exists() with key and policy.
        """
        put_data(self.as_connection, key, record, _policy=policy)
        key, meta = self.as_connection.exists(key, policy)

        assert meta['gen'] != None
        assert meta['ttl'] != None

    # Negative Tests

    def test_neg_exists_with_record_expiry(self, put_data):
        key = ('test', 'demo', 30)
        rec = {"name": "John"}
        meta = {'gen': 3, 'ttl': 1}
        policy = {'timeout': 1000}
        put_data(self.as_connection, key, rec, meta, policy)
        time.sleep(2)
        try:
            key, meta = self.as_connection.exists(key)
            assert meta is None
        except e.RecordNotFound as exception:
            assert exception.code == 2

    @pytest.mark.parametrize("key, ex, ex_code", [
        # reason for xfail CLIENT-533
        pytest.mark.xfail((('test', 'demo', 'non-existent'), e.RecordNotFound, 2)),     # non-existent key
        pytest.mark.xfail((('test', 'set', 1), e.RecordNotFound, 2)),                    # non-existent set
        (('namespace', 'demo', 1), e.NamespaceNotFound, 20),           # non-existent Namespace
        pytest.mark.xfail((('test', None, 2), e.RecordNotFound, 2)),                    #None set in key tuple.
        pytest.mark.xfail((('test', 'demo', 'Non_existing_key'), e.RecordNotFound, 2)),  # Non_existing_key
        ])
    def test_neg_exists_with_non_existent_data(self, key, ex, ex_code):
        """
            Invoke exists() for non-existent data.
        """
        try:
            key, _ = self.as_connection.exists(key)

            """
            We are making the api backward compatible. In case of RecordNotFound an
            exception will not be raised. Instead Ok response is returned withe the
            meta as None. This might change with further releases.
            """
        except ex as exception:
            assert exception.code == ex_code

    def test_neg_exists_with_only_key_without_connection(self):
        """
            Invoke exists() with a key and not policy's dict and no connection
        """
        key = ('test', 'demo', 1)
        config = {'hosts': [('127.0.0.1', 3000)]}
        client1 = aerospike.client(config)

        try:
            key, _ = client1.exists(key)

        except e.ClusterError as exception:
            assert exception.code == 11

    @pytest.mark.parametrize("key, record, meta, policy", [
        (('test', 'demo', 20), {"name": "John"}, {'gen': 3, 'ttl': 1}, {'timeout': 2}),
        ])
    def test_neg_exists_with_low_timeout(self, key, record, meta, policy, put_data):
        try:
            put_data(self.as_connection, key, record, meta, policy)
        except e.TimeoutError as exception:
            assert exception.code == 9
        key, meta = self.as_connection.exists(key)
        assert meta['gen'] is not None

    def test_neg_exists_with_no_paramters(self):
        """
            Invoke self() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            self.as_connection.exists()

        assert "Required argument 'key' (pos 1) not found" in str(typeError.value)

    @pytest.mark.parametrize("key, record, policy, ex_code, ex_msg",[
        # timeout_is_string
        (('test', 'demo', "timeout_is_string"),
            {'names': ['John', 'Marlen', 'Steve']},
            {'timeout': "1000"}, -2, 'timeout is invalid'),
        (('test', 'demo', "policy_is_string"),
            {"Name": "Jeff"}, "policy_str", -2, 'policy must be a dict'),
        ])
    def test_neg_exists_with_invalid_meta(self, key, record, policy, ex_code, ex_msg, put_data):
        """
            Invoke exists() with a key and timeout as string.
        """
        put_data(self.as_connection, key, record)

        try:
            key, _ = self.as_connection.exists(key, policy)

        except e.ParamError as exception:
            assert exception.code == ex_code
            assert exception.msg == ex_msg

    @pytest.mark.parametrize("key, ex_code, ex_msg", [
    (['test', 'demo', 'key_as_list'], -2, "key is invalid"),
    (None, -2, "key is invalid"),
    (('test', 'demo', None), -2, 'either key or digest is required'),
    ((None, 'demo', 2), -2, 'namespace must be a string')
    ])
    def test_neg_exists_key_invalid_data(self, key, ex_code, ex_msg):
        """
            Invoke exists() with invalid key
        """
        try:
            key, _ = self.as_connection.exists(key)
        except e.ParamError as exception:
            assert exception.code == ex_code
            assert exception.msg == ex_msg
