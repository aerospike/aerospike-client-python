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

class TestIncrement(object):
    def setup_class(cls):
        """
        Setup method.
        """
        config = {
            'hosts': [('127.0.0.1', 3000)]
        }
        TestIncrement.client = aerospike.client(config).connect()

    def teardown_class(cls):
        TestIncrement.client.close()

    def setup_method(self, method):
        for i in xrange(5):
            key = ('test', 'demo', i)
            rec = {
                'name' : 'name%s' % (str(i)),
                'age' : i
            }
            TestIncrement.client.put(key, rec)

    def teardown_method(self, method):
        """
        Teardoen method.
        """
        for i in xrange(5):
            key = ('test', 'demo', i)
            TestIncrement.client.remove(key)

    def test_increment_with_no_parameters(self):
        """
        Invoke increment() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            TestIncrement.client.increment()
        assert "Required argument 'key' (pos 1) not found" in typeError.value

    def test_increment_with_correct_parameters(self):
        """
        Invoke increment() with correct parameters
        """
        key = ('test', 'demo', 1)
        TestIncrement.client.increment(key, "age", 5)


        (key , meta, bins) = TestIncrement.client.get(key)

        assert bins == { 'age': 6, 'name': 'name1'}

    def test_increment_with_initial_value_positive(self):
        """
        Invoke increment() with initiali value positive
        """
        key = ('test', 'demo', 1)
        TestIncrement.client.increment(key, "no", 5, 0)


        (key , meta, bins) = TestIncrement.client.get(key)

        assert bins == { 'age': 1, 'name': 'name1', 'no': 0}

    def test_increment_with_policy_key_send(self):
        """
        Invoke increment() with policy key send
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key' : aerospike.POLICY_KEY_SEND,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'commit_level': aerospike.POLICY_COMMIT_LEVEL_MASTER
        }
        TestIncrement.client.increment(key, "age", 5, 0, {}, policy)


        (key , meta, bins) = TestIncrement.client.get(key)

        assert bins == { 'age': 6, 'name': 'name1'}
        assert key == ('test', 'demo', 1,
                bytearray(b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8'))

    def test_increment_with_policy_key_digest(self):
        """
        Invoke increment() with policy key digest
        """
        key = ( 'test', 'demo', None, bytearray("asd;as[d'as;djk;uyfl",
               "utf-8"))
        rec = {
            'name' : 'name%s' % (str(1)),
            'age' : 1,
            'nolist': [1, 2, 3]
        }
        TestIncrement.client.put(key, rec)

        policy = {
            'timeout': 1000,
            'key' : aerospike.POLICY_KEY_DIGEST,
            'retry' : aerospike.POLICY_RETRY_NONE
        }
        TestIncrement.client.increment(key, "age", 5, 0, {}, policy)


        (key , meta, bins) = TestIncrement.client.get(key)

        assert bins == { 'age': 6, 'name': 'name1', 'nolist': [1, 2, 3]}
        assert key == ('test', 'demo', None,
                bytearray(b"asd;as[d\'as;djk;uyfl"))
        TestIncrement.client.remove(key)

    def test_increment_with_correct_policy(self):
        """
        Invoke increment() with correct policy
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key' : aerospike.POLICY_KEY_SEND
        }
        TestIncrement.client.increment(key, "age", 5, 0, {}, policy)


        (key , meta, bins) = TestIncrement.client.get(key)

        assert bins == { 'age': 6, 'name': 'name1'}

    def test_increment_with_policy_key_gen_EQ_ignore(self):
        """
        Invoke increment() with gen eq positive ignore
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
        TestIncrement.client.increment(key, "age", 5, 0, meta, policy)


        (key , meta, bins) = TestIncrement.client.get(key)

        assert bins == { 'age': 6, 'name': 'name1'}
        assert key == ('test', 'demo', 1,
                bytearray(b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8'))

    def test_increment_with_policy_key_gen_EQ_positive(self):
        """
        Invoke increment() with gen eq positive
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key' : aerospike.POLICY_KEY_SEND,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'gen': aerospike.POLICY_GEN_EQ
        }
        (key, meta) = TestIncrement.client.exists(key) 

        gen = meta['gen']
        meta = {
            'gen': gen,
            'ttl': 1200
        }
        TestIncrement.client.increment(key, "age", 5, 0, meta, policy)


        (key , meta, bins) = TestIncrement.client.get(key)

        assert bins == { 'age': 6, 'name': 'name1'}
        assert key == ('test', 'demo', 1,
                bytearray(b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8'))

    def test_increment_with_policy_key_gen_EQ_not_equal(self):
        """
        Invoke increment() with policy key EQ not equal
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key' : aerospike.POLICY_KEY_SEND,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'gen': aerospike.POLICY_GEN_EQ
        }
        (key, meta) = TestIncrement.client.exists(key) 
        gen = meta['gen']

        meta = {
            'gen': gen + 5,
            'ttl': 1200
        }
        with pytest.raises(Exception) as exception:
            TestIncrement.client.increment(key, "age", 5, 0, meta, policy)

        assert exception.value[0] == 3
        assert exception.value[1] == "AEROSPIKE_ERR_RECORD_GENERATION"


        (key , meta, bins) = TestIncrement.client.get(key)

        assert bins == { 'age': 1, 'name': 'name1'}
        assert key == ('test', 'demo', 1,
                bytearray(b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8'))

    def test_increment_with_policy_key_gen_GT_lesser(self):
        """
        Invoke increment() with gen GT lesser
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key' : aerospike.POLICY_KEY_SEND,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'gen': aerospike.POLICY_GEN_GT
        }
        (key, meta) = TestIncrement.client.exists(key) 

        gen = meta['gen']
        meta = {
            'gen': gen,
            'ttl': 1200
        }
        with pytest.raises(Exception) as exception:
            TestIncrement.client.increment(key, "age", 5, 0, meta, policy)

        assert exception.value[0] == 3
        assert exception.value[1] == "AEROSPIKE_ERR_RECORD_GENERATION"

        (key , meta, bins) = TestIncrement.client.get(key)

        assert bins == { 'age': 1, 'name': 'name1'}
        assert key == ('test', 'demo', 1,
                bytearray(b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8'))

    def test_increment_with_policy_key_gen_GT_positive(self):
        """
        Invoke increment() with gen GT positive
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key' : aerospike.POLICY_KEY_SEND,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'gen': aerospike.POLICY_GEN_GT
        }
        (key, meta) = TestIncrement.client.exists(key) 

        gen = meta['gen']
        meta = {
            'gen': gen+5,
            'ttl': 1200
        }
        TestIncrement.client.increment(key, "age", 5, 0, meta, policy)


        (key , meta, bins) = TestIncrement.client.get(key)

        assert bins == { 'age': 6, 'name': 'name1'}
        assert key == ('test', 'demo', 1,
                bytearray(b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8'))

    def test_increment_with_incorrect_policy(self):
        """
        Invoke increment() with incorrect policy
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 0.5
        }
        with pytest.raises(Exception) as exception:
            TestIncrement.client.increment(key, "age", 5, 0, {}, policy)

        assert exception.value[0] == -2
        assert exception.value[1] == "timeout is invalid"

    def test_increment_with_nonexistent_key(self):
        """
        Invoke increment() with non-existent key
        """
        key = ('test', 'demo', 1000)
        with pytest.raises(Exception) as exception:
            status = TestIncrement.client.increment(key, "age", 5)

        assert exception.value[0] == 2
        assert exception.value[1] == "AEROSPIKE_ERR_RECORD_NOT_FOUND"


    def test_increment_with_nonexistent_bin(self):
        """
        Invoke increment() with non-existent bin
        """
        key = ('test', 'demo', 1)
        status = TestIncrement.client.increment(key, "age1", 5, 2)

        assert status == 0L

    def test_increment_value_is_string(self):
        """
        Invoke increment() value is string
        """
        key = ('test', 'demo', 1)
        with pytest.raises(TypeError) as typeError:
            TestIncrement.client.increment(key, "age", "str")

        assert "an integer is required" in typeError.value

    def test_increment_with_extra_parameter(self):
        """
        Invoke increment() with extra parameter.
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000
        }
        with pytest.raises(TypeError) as typeError:
            TestIncrement.client.increment(key, "age", 2, 0, {}, policy, "")

        assert "increment() takes at most 6 arguments (7 given)" in typeError.value

    def test_increment_policy_is_string(self):
        """
        Invoke increment() with policy is string
        """
        key = ('test', 'demo', 1)
        with pytest.raises(Exception) as exception:
            TestIncrement.client.increment(key, "age", 2, 0, {}, "")

        assert exception.value[0] == -2
        assert exception.value[1] == "policy must be a dict"

    def test_increment_key_is_none(self):
        """
        Invoke increment() with key is none
        """
        with pytest.raises(Exception) as exception:
            TestIncrement.client.increment(None, "age", 2)

        assert exception.value[0] == -2
        assert exception.value[1] == "key is invalid"

    def test_increment_bin_is_none(self):
        """
        Invoke increment() with bin is none
        """
        key = ('test', 'demo', 1)
        with pytest.raises(Exception) as exception:
            TestIncrement.client.increment(key, None, 2)

        assert exception.value[0] == -2
        assert exception.value[1] == "Bin should be a string"
