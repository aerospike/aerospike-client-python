# -*- coding: utf-8 -*-

import pytest
import sys
import cPickle as pickle
from test_base_class import TestBaseClass

aerospike = pytest.importorskip("aerospike")
try:
    from aerospike.exception import *
except:
    print "Please install aerospike python client."
    sys.exit(1)

class TestRemove(TestBaseClass):
    def setup_class(cls):
        """
        Setup class
        """
        hostlist, user, password = TestBaseClass.get_hosts()
        config = {'hosts': hostlist}
        if user == None and password == None:
            TestRemove.client = aerospike.client(config).connect()
        else:
            TestRemove.client = aerospike.client(config).connect(user, password)

    def teardown_class(cls):
        TestRemove.client.close()

    def setup_method(self, method):
        """
        Setup method.
        """
        for i in xrange(5):
            key = ('test', 'demo', i)
            rec = {
                'name': 'name%s' % (str(i)),
                'addr': 'name%s' % (str(i)),
                'age': i,
                'no': i
            }
            TestRemove.client.put(key, rec)

    def teardown_method(self, method):
        """
        Teardoen method.
        """
        for i in xrange(5):
            try:
                key = ('test', 'demo', i)
                TestRemove.client.remove(key)
            except RecordError:
                pass

    def test_remove_with_no_parameters(self):
        """
            Invoke remove() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            TestRemove.client.remove()

        assert "Required argument 'key' (pos 1) not found" in typeError.value

    def test_remove_with_correct_parameters(self):
        """
            Invoke remove() with correct arguments
        """
        key = ('test', 'demo', 1)
        retobj = TestRemove.client.remove(key)

        assert retobj == 0L

        try:
            (key, meta, bins) = TestRemove.client.get(key)

        except RecordNotFound as exception:
            assert exception.code == 2
        
        key = ('test', 'demo', 1)
        rec = {
            'name': 'name%s' % (str(1)),
            'addr': 'name%s' % (str(1)),
            'age': 1,
            'no': 1
        }
        TestRemove.client.put(key, rec)

    def test_remove_with_policy(self):
        """
            Invoke remove() with policy
        """
        key = ('test', 'demo', 1)
        meta = {'gen': 0}
        policy = {'timeout': 1000}

        retobj = TestRemove.client.remove(key, meta, policy)

        assert retobj == 0L

        try:
            (key, meta, bins) = TestRemove.client.get(key)

        except RecordNotFound as exception:
            assert exception.code == 2
    
        key = ('test', 'demo', 1)
        rec = {
            'name': 'name%s' % (str(1)),
            'addr': 'name%s' % (str(1)),
            'age': 1,
            'no': 1
        }
        TestRemove.client.put(key, rec)

    def test_remove_with_policy_all(self):
        """
            Invoke remove() with policy
        """
        key = ('test', 'demo', 1)
        meta = {'gen': 0}
        policy = {
            'timeout': 1000,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND
        }
        retobj = TestRemove.client.remove(key, meta, policy)

        assert retobj == 0L

        try:
            (key, meta, bins) = TestRemove.client.get(key)

        except RecordNotFound as exception:
            assert exception.code == 2

        key = ('test', 'demo', 1)
        rec = {
            'name': 'name%s' % (str(1)),
            'addr': 'name%s' % (str(1)),
            'age': 1,
            'no': 1
        }
        TestRemove.client.put(key, rec)

    def test_remove_with_policy_key_digest(self):
        """
            Invoke remove() with policy_key_digest
        """

        key = ('test', 'demo', None, bytearray("asd;as[d'as;djk;uyfl",
                                               "utf-8"))
        meta = {'gen': 0}
        policy = {
            'timeout': 1000,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_DIGEST
        }
        retobj = TestRemove.client.put(key, policy)

        assert retobj == 0L

        retobj = TestRemove.client.remove(key, meta, policy)

        assert retobj == 0L

        try:
            (key, meta, bins) = TestRemove.client.get(key)

        except RecordNotFound as exception:
            assert exception.code == 2
        
    def test_remove_with_policy_gen_ignore(self):
        """
            Invoke remove() with policy gen ignore
        """
        key = ('test', 'demo', 1)
        meta = {'gen': 0}
        policy = {
            'timeout': 1000,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND,
            'gen': aerospike.POLICY_GEN_IGNORE
        }

        retobj = TestRemove.client.remove(key, meta, policy)

        assert retobj == 0L

        try:
            (key, meta, bins) = TestRemove.client.get(key)

        except RecordNotFound as exception:
            assert exception.code == 2

        key = ('test', 'demo', 1)
        rec = {
            'name': 'name%s' % (str(1)),
            'addr': 'name%s' % (str(1)),
            'age': 1,
            'no': 1
        }
        TestRemove.client.put(key, rec)

    def test_remove_with_policy_gen_eq_positive(self):
        """
            Invoke remove() with policy gen positive
        """
        key = ('test', 'demo', 1)
        meta = {'gen': 0}
        policy = {
            'timeout': 1000,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND,
            'gen': aerospike.POLICY_GEN_EQ
        }

        (key, meta) = TestRemove.client.exists(key)
        gen = meta['gen']
        retobj = TestRemove.client.remove(key, meta, policy)

        assert retobj == 0L

        try:
            (key, meta, bins) = TestRemove.client.get(key)

        except RecordNotFound as exception:
            assert exception.code == 2

        key = ('test', 'demo', 1)
        rec = {
            'name': 'name%s' % (str(1)),
            'addr': 'name%s' % (str(1)),
            'age': 1,
            'no': 1
        }
        TestRemove.client.put(key, rec)

    def test_remove_with_policy_gen_eq_not_equal(self):
        """
            Invoke remove() with policy gen not equal
        """
        key = ('test', 'demo', 1)

        (key, meta) = TestRemove.client.exists(key)
        gen = meta['gen'] + 5
        metadata = {'gen': gen}
        policy = {
            'timeout': 1000,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND,
            'gen': aerospike.POLICY_GEN_EQ
        }

        (key, meta) = TestRemove.client.exists(key)
        gen = meta['gen'] + 5

        try:
            retobj = TestRemove.client.remove(key, gen, policy)

        except RecordGenerationError as exception:
            assert exception.code == 3
            assert exception.msg == 'AEROSPIKE_ERR_RECORD_GENERATION'

        (key, meta, bins) = TestRemove.client.get(key)

        assert key == ('test', 'demo', None, bytearray(
            b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8')
                      )
        assert meta != None
        assert bins == {'addr': 'name1', 'age': 1, 'name': 'name1', 'no': 1}

    def test_remove_with_policy_gen_GT_lesser(self):
        """
            Invoke remove() with policy gen GT lesser
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND,
            'gen': aerospike.POLICY_GEN_GT
        }

        (key, meta) = TestRemove.client.exists(key)
        gen = meta['gen']
        try:
            retobj = TestRemove.client.remove(key, gen, policy)

        except RecordGenerationError as exception:
            assert exception.code == 3
            assert exception.msg == 'AEROSPIKE_ERR_RECORD_GENERATION'

        (key, meta, bins) = TestRemove.client.get(key)

        assert key == ('test', 'demo', None, bytearray(
            b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8')
                      )
        assert meta != None
        assert bins == {'addr': 'name1', 'age': 1, 'name': 'name1', 'no': 1}

    def test_remove_with_policy_gen_GT_positive(self):
        """
            Invoke remove() with policy gen GT positive
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND,
            'gen': aerospike.POLICY_GEN_GT
        }

        (key, meta) = TestRemove.client.exists(key)
        gen = meta['gen'] + 5
        metadata = {'gen': gen}
        retobj = TestRemove.client.remove(key, metadata, policy)

        assert retobj == 0L

        try:
            (key, meta, bins) = TestRemove.client.get(key)

        except RecordNotFound as exception:
            assert exception.code == 2

        key = ('test', 'demo', 1)
        rec = {
            'name': 'name%s' % (str(1)),
            'addr': 'name%s' % (str(1)),
            'age': 1,
            'no': 1
        }
        TestRemove.client.put(key, rec)

    def test_remove_with_policy_as_string(self):
        """
            Invoke remove() with policy as string
        """
        key = ('test', 'demo', 1)
        meta = {
                'gen' : 0
                }
        try:
            retobj = TestRemove.client.remove(key, meta, "")

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == 'policy must be a dict'

    def test_remove_with_extra_parameter(self):
        """
            Invoke remove() with extra parameter
        """
        key = ('test', 'demo', 1)
        meta = {'gen': 0}
        policy = {'timeout': 1000}
        with pytest.raises(TypeError) as typeError:
            retobj = TestRemove.client.remove(key, meta, policy, "")

        assert "remove() takes at most 3 arguments (4 given)" in typeError.value

    def test_remove_with_key_none(self):
        """
            Invoke remove() with key as None
        """
        try:
            retobj = TestRemove.client.remove(None)

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == 'key is invalid'

    def test_remove_with_key_incorrect(self):
        """
            Invoke remove() with key incorrect
        """
        key = ('test', 'demo', 15)
        try:
            retobj = TestRemove.client.remove(key)

        except RecordNotFound as exception:
            assert exception.code == 2
            assert exception.msg == 'AEROSPIKE_ERR_RECORD_NOT_FOUND'

    def test_remove_with_namespace_none(self):
        """
            Invoke remove() with namespace as None
        """
        key = (None, 'demo', 1)
        try:
            retobj = TestRemove.client.remove(key)

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == 'namespace must be a string'

    def test_remove_with_set_none(self):
        """
            Invoke remove() with set as None
        """
        key = ('test', None, 1)
        try:
            retobj = TestRemove.client.remove(key)

        except RecordNotFound as exception:
            assert exception.code == 2
            assert exception.msg == 'AEROSPIKE_ERR_RECORD_NOT_FOUND'

    def test_remove_with_correct_parameters_without_connection(self):
        """
            Invoke remove() with correct arguments without connection
        """
        config = {'hosts': [('127.0.0.1', 3000)]}
        client1 = aerospike.client(config)
        key = ('test', 'demo', 1)

        try:
            retobj = client1.remove(key)

        except ClusterError as exception:
            assert exception.code == 11L
            assert exception.msg == 'No connection to aerospike cluster'
