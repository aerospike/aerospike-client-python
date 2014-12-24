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

class TestTouch(object):
    def setup_class(cls):
        """
        Setup method.
        """
        config = {
            'hosts': [('127.0.0.1', 3000)]
        }
        TestTouch.client = aerospike.client(config).connect()

    def teardown_class(cls):
        TestTouch.client.close()

    def setup_method(self, method):
        for i in xrange(5):
            key = ('test', 'demo', i)
            rec = {
                'name' : 'name%s' % (str(i)),
                'age' : i
            }
            TestTouch.client.put(key, rec)

    def teardown_method(self, method):
        """
        Teardoen method.
        """
        for i in xrange(5):
            key = ('test', 'demo', i)
            TestTouch.client.remove(key)

    def test_touch_with_no_parameters(self):
        """
        Invoke touch() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            TestTouch.client.touch()
        assert "Required argument 'key' (pos 1) not found" in typeError.value

    def test_touch_with_correct_paramters(self):
        """
        Invoke touch() with correct parameters
        """
        key = ('test', 'demo', 1)
        response = TestTouch.client.touch(key, 120)

        assert response == 0

    def test_touch_with_correct_policy(self):
        """
        Invoke touch() with correct policy
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'retry' : aerospike.POLICY_RETRY_ONCE 
        }
        response = TestTouch.client.touch(key, 120, {}, policy)
        assert response == 0

    def test_touch_with_policy_key_send(self):
        """
        Invoke touch() with policy key send
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key' : aerospike.POLICY_KEY_SEND,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'commit_level': aerospike.POLICY_COMMIT_LEVEL_MASTER
        }
        TestTouch.client.touch(key, 120, {}, policy)


        (key , meta, bins) = TestTouch.client.get(key)

        assert bins == { 'age': 1, 'name': 'name1'}
        assert key == ('test', 'demo', 1,
                bytearray(b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8'))

    def test_touch_with_policy_key_digest(self):
        """
        Invoke touch() with policy key digest
        """
        key = ( 'test', 'demo', None, bytearray("asd;as[d'as;djk;uyfl",
               "utf-8"))
        rec = {
            'name' : 'name%s' % (str(1)),
            'age' : 1,
            'nolist': [1, 2, 3]
        }
        TestTouch.client.put(key, rec)

        policy = {
            'timeout': 1000,
            'key' : aerospike.POLICY_KEY_DIGEST,
            'retry' : aerospike.POLICY_RETRY_NONE
        }
        TestTouch.client.touch(key, 120, {}, policy)


        (key , meta, bins) = TestTouch.client.get(key)

        assert bins == { 'age': 1, 'name': 'name1', 'nolist': [1, 2, 3]}
        assert key == ('test', 'demo', None,
                bytearray(b"asd;as[d\'as;djk;uyfl"))
        TestTouch.client.remove(key)

    def test_touch_with_policy_key_gen_EQ_ignore(self):
        """
        Invoke touch() with gen eq positive ignore
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
        TestTouch.client.touch(key, 120, meta, policy)


        (key , meta, bins) = TestTouch.client.get(key)

        assert bins == { 'age': 1, 'name': 'name1'}
        assert key == ('test', 'demo', 1,
                bytearray(b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8'))

    def test_touch_with_policy_key_gen_EQ_positive(self):
        """
        Invoke touch() with gen eq positive
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key' : aerospike.POLICY_KEY_SEND,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'gen': aerospike.POLICY_GEN_EQ
        }
        (key, meta) = TestTouch.client.exists(key) 

        gen = meta['gen']
        meta = {
            'gen': gen,
            'ttl': 1200
        }
        TestTouch.client.touch(key, 120, meta, policy)


        (key , meta, bins) = TestTouch.client.get(key)

        assert bins == { 'age': 1, 'name': 'name1'}
        assert key == ('test', 'demo', 1,
                bytearray(b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8'))

    def test_touch_with_policy_key_gen_EQ_not_equal(self):
        """
        Invoke touch() with policy key EQ not equal
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key' : aerospike.POLICY_KEY_SEND,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'gen': aerospike.POLICY_GEN_EQ
        }
        meta = {
            'gen': 10,
            'ttl': 1200
        }
        with pytest.raises(Exception) as exception:
            TestTouch.client.touch(key, 120, meta, policy)

        assert exception.value[0] == 3
        assert exception.value[1] == "AEROSPIKE_ERR_RECORD_GENERATION"


        (key , meta, bins) = TestTouch.client.get(key)

        assert bins == { 'age': 1, 'name': 'name1'}
        assert key == ('test', 'demo', 1,
                bytearray(b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8'))

    def test_touch_with_policy_key_gen_GT_lesser(self):
        """
        Invoke touch() with gen GT lesser
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key' : aerospike.POLICY_KEY_SEND,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'gen': aerospike.POLICY_GEN_GT
        }
        (key, meta) = TestTouch.client.exists(key) 

        gen = meta['gen']
        meta = {
            'gen': gen,
            'ttl': 1200
        }
        with pytest.raises(Exception) as exception:
            TestTouch.client.touch(key, 120, meta, policy)

        assert exception.value[0] == 3
        assert exception.value[1] == "AEROSPIKE_ERR_RECORD_GENERATION"

        (key , meta, bins) = TestTouch.client.get(key)

        assert bins == { 'age': 1, 'name': 'name1'}
        assert key == ('test', 'demo', 1,
                bytearray(b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8'))

    def test_touch_with_policy_key_gen_GT_positive(self):
        """
        Invoke touch() with gen GT positive
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key' : aerospike.POLICY_KEY_SEND,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'gen': aerospike.POLICY_GEN_GT
        }
        (key, meta) = TestTouch.client.exists(key) 

        gen = meta['gen']
        meta = {
            'gen': gen + 5,
            'ttl': 1200
        }
        TestTouch.client.touch(key, 120, meta, policy)


        (key , meta, bins) = TestTouch.client.get(key)

        assert bins == { 'age': 1, 'name': 'name1'}
        assert key == ('test', 'demo', 1,
                bytearray(b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8'))

    def test_touch_with_incorrect_policy(self):
        """
        Invoke touch() with incorrect policy
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 0.5
        }
        with pytest.raises(Exception) as exception:
            TestTouch.client.touch(key, 120, {}, policy)

        assert exception.value[0] == -2
        assert exception.value[1] == "timeout is invalid"

    def test_touch_with_nonexistent_key(self):
        """
        Invoke touch() with non-existent key
        """
        key = ('test', 'demo', 1000)

        with pytest.raises(Exception) as exception:
            status = TestTouch.client.touch(key, 120)

        assert exception.value[0] == 2
        assert exception.value[1] == "AEROSPIKE_ERR_RECORD_NOT_FOUND"


    def test_touch_value_string(self):
        """
        Invoke touch() not a string
        """
        key = ('test', 'demo', 1)
        with pytest.raises(TypeError) as typeError:
            TestTouch.client.touch(key, "name")

        assert "an integer is required" in typeError.value

    def test_touch_with_extra_parameter(self):
        """
        Invoke touch() with extra parameter.
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000
        }
        with pytest.raises(TypeError) as typeError:
            TestTouch.client.touch(key, 120, {}, policy, "")

        assert "touch() takes at most 4 arguments (5 given)" in typeError.value

    def test_touch_policy_is_string(self):
        """
        Invoke touch() with policy is string
        """
        key = ('test', 'demo', 1)
        with pytest.raises(Exception) as exception:
            TestTouch.client.touch(key, 120, {}, "")

        assert exception.value[0] == -2
        assert exception.value[1] == "policy must be a dict"
