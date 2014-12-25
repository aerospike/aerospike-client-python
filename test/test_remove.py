# -*- coding: utf-8 -*-

import pytest
import sys
import cPickle as pickle
try:
    import aerospike
except:
    print "Please install aerospike python client."
    sys.exit(1)

class TestRemove(object):

    def setup_class(cls):
        """
        Setup class
        """
        config = {
                'hosts': [('127.0.0.1', 3000)]
                }
        TestRemove.client = aerospike.client(config).connect()

    def teardown_class(cls):
        TestRemove.client.close()

    def setup_method(self, method):
        """
        Setup method.
        """
        for i in xrange(5):
            key = ('test', 'demo', i)
            rec = {
                    'name' : 'name%s' % (str(i)),
                    'addr' : 'name%s' % (str(i)),
                    'age'  : i,
                    'no'   : i
                    }
            TestRemove.client.put(key, rec)

    def teardown_method(self, method):
        """
        Teardoen method.
        """
        for i in xrange(5):
            key = ('test', 'demo', i)
            TestRemove.client.remove(key)

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

        (key, meta, bins) = TestRemove.client.get(key)
        
        assert meta == None
        assert bins == None
        
        key = ('test', 'demo', 1)
        rec = {
                'name' : 'name%s' % (str(1)),
                'addr' : 'name%s' % (str(1)),
                'age'  : 1,
                'no'   : 1
              }
        TestRemove.client.put(key, rec)
        
    def test_remove_with_policy(self):
        """
            Invoke remove() with policy
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000
        }
        retobj = TestRemove.client.remove(key, 0, policy)

        assert retobj == 0L

        (key, meta, bins) = TestRemove.client.get(key)

        assert meta == None
        assert bins == None
    
        key = ('test', 'demo', 1)
        rec = {
                'name' : 'name%s' % (str(1)),
                'addr' : 'name%s' % (str(1)),
                'age'  : 1,
                'no'   : 1
              }
        TestRemove.client.put(key, rec)
        
    def test_remove_with_policy_all(self):
        """
            Invoke remove() with policy
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND
        }
        retobj = TestRemove.client.remove(key, 0, policy)

        assert retobj == 0L

        (key, meta, bins) = TestRemove.client.get(key)

        assert key == ('test', 'demo', 1,
                bytearray(b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8'))
        assert meta == None
        assert bins == None
        
        key = ('test', 'demo', 1)
        rec = {
                'name' : 'name%s' % (str(1)),
                'addr' : 'name%s' % (str(1)),
                'age'  : 1,
                'no'   : 1
              }
        TestRemove.client.put(key, rec)
        
    def test_remove_with_policy_key_digest(self):
        """
            Invoke remove() with policy_key_digest
        """

        key = ( 'test', 'demo', None, bytearray("asd;as[d'as;djk;uyfl",
               "utf-8"))
        policy = {
            'timeout': 1000,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_DIGEST
        }
        retobj = TestRemove.client.put(key, policy)

        assert retobj == 0L

        retobj = TestRemove.client.remove(key, 0, policy)

        assert retobj == 0L

        (key, meta, bins) = TestRemove.client.get(key)
        
        assert key == ('test', 'demo', None,
                bytearray(b"asd;as[d\'as;djk;uyfl"))
        assert meta == None
        assert bins == None
        
    def test_remove_with_policy_gen_ignore(self):
        """
            Invoke remove() with policy gen ignore
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND,
            'gen': aerospike.POLICY_GEN_IGNORE
        }
        gen = 5
        retobj = TestRemove.client.remove(key, gen, policy)

        assert retobj == 0L

        (key, meta, bins) = TestRemove.client.get(key)

        assert key == ('test', 'demo', 1,
                bytearray(b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8'))
        assert meta == None
        assert bins == None

        key = ('test', 'demo', 1)
        rec = {
                'name' : 'name%s' % (str(1)),
                'addr' : 'name%s' % (str(1)),
                'age'  : 1,
                'no'   : 1
              }
        TestRemove.client.put(key, rec)


    def test_remove_with_policy_gen_eq_positive(self):
        """
            Invoke remove() with policy gen positive
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND,
            'gen': aerospike.POLICY_GEN_EQ
        }

        (key, meta) = TestRemove.client.exists(key)
        gen = meta['gen']
        retobj = TestRemove.client.remove(key, gen, policy)

        assert retobj == 0L

        (key, meta, bins) = TestRemove.client.get(key)

        assert key == ('test', 'demo', 1,
                bytearray(b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8'))
        assert meta == None
        assert bins == None

        key = ('test', 'demo', 1)
        rec = {
                'name' : 'name%s' % (str(1)),
                'addr' : 'name%s' % (str(1)),
                'age'  : 1,
                'no'   : 1
              }
        TestRemove.client.put(key, rec)

    def test_remove_with_policy_gen_eq_not_equal(self):
        """
            Invoke remove() with policy gen not equal
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND,
            'gen': aerospike.POLICY_GEN_EQ
        }

        (key, meta) = TestRemove.client.exists(key)
        gen = meta['gen'] + 5
        retobj = TestRemove.client.remove(key, gen, policy)

        (key, meta, bins) = TestRemove.client.get(key)

        assert key == ('test', 'demo', 1,
                bytearray(b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8'))
        assert meta == None
        assert bins == None

        key = ('test', 'demo', 1)
        rec = {
                'name' : 'name%s' % (str(1)),
                'addr' : 'name%s' % (str(1)),
                'age'  : 1,
                'no'   : 1
              }
        TestRemove.client.put(key, rec)

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
        retobj = TestRemove.client.remove(key, gen, policy)

        assert retobj == 0L

        (key, meta, bins) = TestRemove.client.get(key)

        assert key == ('test', 'demo', 1,
                bytearray(b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8'))
        assert meta == None
        assert bins == None

        key = ('test', 'demo', 1)
        rec = {
                'name' : 'name%s' % (str(1)),
                'addr' : 'name%s' % (str(1)),
                'age'  : 1,
                'no'   : 1
              }
        TestRemove.client.put(key, rec)

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
        retobj = TestRemove.client.remove(key, gen, policy)

        assert retobj == 0L

        (key, meta, bins) = TestRemove.client.get(key)

        assert key == ('test', 'demo', 1,
                bytearray(b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8'))
        assert meta == None
        assert bins == None

        key = ('test', 'demo', 1)
        rec = {
                'name' : 'name%s' % (str(1)),
                'addr' : 'name%s' % (str(1)),
                'age'  : 1,
                'no'   : 1
              }
        TestRemove.client.put(key, rec)

    def test_remove_with_policy_as_string(self):
        """
            Invoke remove() with policy as string
        """
        key = ('test', 'demo', 1)
        with pytest.raises(Exception) as exception:
            retobj = TestRemove.client.remove(key, 0, "")

        assert exception.value[0] == -2
        assert exception.value[1] == 'policy must be a dict'

    def test_remove_with_extra_parameter(self):
        """
            Invoke remove() with extra parameter
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000
        }
        with pytest.raises(TypeError) as typeError:
            retobj = TestRemove.client.remove(key, 0, policy, "")

        assert "remove() takes at most 3 arguments (4 given)" in typeError.value

    def test_remove_with_key_none(self):
        """
            Invoke remove() with key as None
        """
        with pytest.raises(Exception) as exception:
            retobj = TestRemove.client.remove(None)

        assert exception.value[0] == -2
        assert exception.value[1] == 'key is invalid'

    def test_remove_with_key_incorrect(self):
        """
            Invoke remove() with key incorrect
        """
        key = ('test', 'demo', 15)
        with pytest.raises(Exception) as exception:
            retobj = TestRemove.client.remove(key)

        assert exception.value[0] == 2
        assert exception.value[1] == 'AEROSPIKE_ERR_RECORD_NOT_FOUND'

    def test_remove_with_namespace_none(self):
        """
            Invoke remove() with namespace as None
        """
        key = (None, 'demo', 1)
        with pytest.raises(Exception) as exception:
            retobj = TestRemove.client.remove(key)

        assert exception.value[0] == -2
        assert exception.value[1] == 'namespace must be a string'

    def test_remove_with_set_none(self):
        """
            Invoke remove() with set as None
        """
        key = ('test', None, 1)
        with pytest.raises(Exception) as exception:
            retobj = TestRemove.client.remove(key)

        assert exception.value[0] == 2
        assert exception.value[1] == 'AEROSPIKE_ERR_RECORD_NOT_FOUND'
