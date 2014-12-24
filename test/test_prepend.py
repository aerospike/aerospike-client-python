# -*- coding: utf-8 -*-
import pytest
import time
import sys
import cPickle as pickle
try:
    import aerospike
except:
    print "Please install aerospike python client."
    sys.exit(1)

class TestPrepend(object):
    def setup_class(cls):
        """
        Setup method.
        """
        config = {
            'hosts': [('127.0.0.1', 3000)]
        }
        TestPrepend.client = aerospike.client(config).connect()

    def teardown_class(cls):
        TestPrepend.client.close()

    def setup_method(self, method):
        for i in xrange(5):
            key = ('test', 'demo', i)
            rec = {
                'name' : 'name%s' % (str(i)),
                'age' : i,
                'nolist': [1, 2, 3]
            }
            TestPrepend.client.put(key, rec)

    def teardown_method(self, method):
        """
        Teardoen method.
        """
        for i in xrange(5):
            key = ('test', 'demo', i)
            TestPrepend.client.remove(key)

    def test_prepend_with_no_parameters(self):
        """
        Invoke prepend() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            TestPrepend.client.prepend()
        assert "Required argument 'key' (pos 1) not found" in typeError.value

    def test_prepend_with_correct_paramters(self):
        """
        Invoke prepend() with correct parameters
        """
        key = ('test', 'demo', 1)
        TestPrepend.client.prepend(key, "name", "str")


        (key , meta, bins) = TestPrepend.client.get(key)

        assert bins == { 'age': 1, 'name': 'strname1', 'nolist': [1, 2, 3]}

    def test_prepend_with_correct_policy(self):
        """
        Invoke prepend() with correct policy
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key' : aerospike.POLICY_KEY_SEND,
            'commit_level': aerospike.POLICY_COMMIT_LEVEL_ALL
        }
        
        TestPrepend.client.prepend(key, "name", "str", {}, policy)


        (key , meta, bins) = TestPrepend.client.get(key)

        assert bins == { 'age': 1, 'name': 'strname1', 'nolist': [1, 2, 3]}

    def test_prepend_with_policy_key_send(self):
        """
        Invoke prepend() with policy key send
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key' : aerospike.POLICY_KEY_SEND,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'commit_level': aerospike.POLICY_COMMIT_LEVEL_MASTER
        }
        TestPrepend.client.prepend(key, "name", "str", {}, policy)


        (key , meta, bins) = TestPrepend.client.get(key)

        assert bins == { 'age': 1, 'name': 'strname1', 'nolist': [1, 2, 3]}
        assert key == ('test', 'demo', 1,
                bytearray(b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8'))

    def test_prepend_with_policy_key_gen_EQ_ignore(self):
        """
        Invoke prepend() with gen eq positive
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key' : aerospike.POLICY_KEY_SEND,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'gen': aerospike.POLICY_GEN_IGNORE
        }

        meta = {
            'gen': 10,
            'ttl': 1200
        }
        TestPrepend.client.prepend(key, "name", "str", meta, policy)


        (key , meta, bins) = TestPrepend.client.get(key)

        assert bins == { 'age': 1, 'name': 'strname1', 'nolist': [1, 2, 3]}
        assert key == ('test', 'demo', 1,
                bytearray(b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8'))

    def test_prepend_with_policy_key_gen_EQ_positive(self):
        """
        Invoke prepend() with gen eq positive
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key' : aerospike.POLICY_KEY_SEND,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'gen': aerospike.POLICY_GEN_EQ
        }
        (key, meta) = TestPrepend.client.exists(key) 

        gen = meta['gen']
        meta = {
            'gen': gen,
            'ttl': 1200
        }
        TestPrepend.client.prepend(key, "name", "str", meta, policy)


        (key , meta, bins) = TestPrepend.client.get(key)

        assert bins == { 'age': 1, 'name': 'strname1', 'nolist': [1, 2, 3]}
        assert key == ('test', 'demo', 1,
                bytearray(b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8'))

    def test_prepend_with_policy_key_gen_EQ_not_equal(self):
        """
        Invoke prepend() with policy key GEN_EQ not equal
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key' : aerospike.POLICY_KEY_SEND,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'gen': aerospike.POLICY_GEN_EQ
        }
        (key, meta) = TestPrepend.client.exists(key) 
        gen = meta['gen']
        
        meta = {
            'gen': gen + 5,
            'ttl': 1200
        }
        with pytest.raises(Exception) as exception:
            TestPrepend.client.prepend(key, "name", "str", meta, policy)

        assert exception.value[0] == 3
        assert exception.value[1] == "AEROSPIKE_ERR_RECORD_GENERATION"


        (key , meta, bins) = TestPrepend.client.get(key)

        assert bins == { 'age': 1, 'name': 'name1', 'nolist': [1, 2, 3]}
        assert key == ('test', 'demo', 1,
                bytearray(b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8'))

    def test_prepend_with_policy_key_gen_GT_lesser(self):
        """
        Invoke prepend() with gen GT positive lesser
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key' : aerospike.POLICY_KEY_SEND,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'gen': aerospike.POLICY_GEN_GT
        }
        (key, meta) = TestPrepend.client.exists(key) 

        gen = meta['gen']
        meta = {
            'gen': gen,
            'ttl': 1200
        }
        with pytest.raises(Exception) as exception:
            TestPrepend.client.prepend(key, "name", "str", meta, policy)

        assert exception.value[0] == 3
        assert exception.value[1] == "AEROSPIKE_ERR_RECORD_GENERATION"


        (key , meta, bins) = TestPrepend.client.get(key)

        assert bins == { 'age': 1, 'name': 'name1', 'nolist': [1, 2, 3]}
        assert key == ('test', 'demo', 1,
                bytearray(b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8'))

    def test_prepend_with_policy_key_gen_GT_positive(self):
        """
        Invoke prepend() with gen GT positive
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key' : aerospike.POLICY_KEY_SEND,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'gen': aerospike.POLICY_GEN_GT
        }
        (key, meta) = TestPrepend.client.exists(key) 

        gen = meta['gen']
        meta = {
            'gen': gen+2,
            'ttl': 1200
        }
        TestPrepend.client.prepend(key, "name", "str", meta, policy)


        (key , meta, bins) = TestPrepend.client.get(key)

        assert bins == { 'age': 1, 'name': 'strname1', 'nolist': [1, 2, 3]}
        assert key == ('test', 'demo', 1,
                bytearray(b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8'))

    def test_prepend_with_policy_key_digest(self):
        """
        Invoke prepend() with policy key digest
        """
        key = ( 'test', 'demo', None, bytearray("asd;as[d'as;djk;uyfl",
               "utf-8"))
        rec = {
            'name' : 'name%s' % (str(1)),
            'age' : 1,
            'nolist': [1, 2, 3]
        }
        TestPrepend.client.put(key, rec)

        policy = {
            'timeout': 1000,
            'key' : aerospike.POLICY_KEY_DIGEST,
            'retry' : aerospike.POLICY_RETRY_NONE
        }
        TestPrepend.client.prepend(key, "name", "str", {}, policy)


        (key , meta, bins) = TestPrepend.client.get(key)

        assert bins == { 'age': 1, 'name': 'strname1', 'nolist': [1, 2, 3]}
        assert key == ('test', 'demo', None,
                bytearray(b"asd;as[d\'as;djk;uyfl"))

        TestPrepend.client.remove(key)
    """
    def test_prepend_with_correct_policyandlist(self):
        #Invoke prepend() with correct policy
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key' : aerospike.POLICY_KEY_SEND
        }
        TestPrepend.client.prepend(key, "age", "str", policy)


        (key , meta, bins) = TestPrepend.client.get(key)

        assert bins == { 'age': 1, 'name': 'strname1', 'nolist': [1, 2, 3]}
    """
    def test_prepend_with_incorrect_policy(self):
        """
        Invoke prepend() with incorrect policy
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 0.5
        }
        with pytest.raises(Exception) as exception:
            TestPrepend.client.prepend(key, "name", "str", {}, policy)

        assert exception.value[0] == -2
        assert exception.value[1] == "timeout is invalid"

    def test_prepend_with_nonexistent_key(self):
        """
        Invoke prepend() with non-existent key
        """
        key = ('test', 'demo', 1000)
        status = TestPrepend.client.prepend(key, "name", "str")

        assert status == 0L
        TestPrepend.client.remove(key)

    def test_prepend_with_nonexistent_bin(self):
        """
        Invoke prepend() with non-existent bin
        """
        key = ('test', 'demo', 1)
        status = TestPrepend.client.prepend(key, "name1", "str")

        assert status == 0L

    def test_prepend_value_not_string(self):
        """
        Invoke prepend() not a string
        """
        key = ('test', 'demo', 1)
        with pytest.raises(TypeError) as typeError:
            TestPrepend.client.prepend(key, "name", 2)

        assert "prepend() argument 3 must be string, not int" in typeError.value

    def test_prepend_with_extra_parameter(self):
        """
        Invoke prepend() with extra parameter.
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000
        }
        with pytest.raises(TypeError) as typeError:
            TestPrepend.client.prepend(key, "name", "str", {}, policy, "")
        
        assert "prepend() takes at most 5 arguments (6 given)" in typeError.value

    def test_prepend_policy_is_string(self):
        """
        Invoke prepend() with policy is string
        """
        key = ('test', 'demo', 1)
        with pytest.raises(Exception) as exception:
            TestPrepend.client.prepend(key, "name", "abc", {}, "")

        assert exception.value[0] == -2
        assert exception.value[1] == "policy must be a dict"

    def test_prepend_key_is_none(self):
        """
        Invoke prepend() with key is none
        """
        with pytest.raises(Exception) as exception:
            TestPrepend.client.prepend(None, "name", "str")

        assert exception.value[0] == -2
        assert exception.value[1] == "key is invalid"

    def test_prepend_bin_is_none(self):
        """
        Invoke prepend() with bin is none
        """
        key = ('test', 'demo', 1)
        with pytest.raises(Exception) as exception:
            TestPrepend.client.prepend(key, None, "str")

        assert exception.value[0] == -2
        assert exception.value[1] == "Bin should be a string"
