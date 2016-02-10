# -*- coding: utf-8 -*-
import pytest
import sys
from .test_base_class import TestBaseClass
from aerospike import exception as e

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)


class TestTouch(object):

    def setup_class(cls):
        """
        Setup method.
        """
        hostlist, user, password = TestBaseClass.get_hosts()
        config = {'hosts': hostlist}
        if user is None and password is None:
            TestTouch.client = aerospike.client(config).connect()
        else:
            TestTouch.client = aerospike.client(config).connect(user, password)

    def teardown_class(cls):
        TestTouch.client.close()

    def setup_method(self, method):
        for i in range(5):
            key = ('test', 'demo', i)
            rec = {'name': 'name%s' % (str(i)), 'age': i}
            TestTouch.client.put(key, rec)

    def teardown_method(self, method):
        """
        Teardoen method.
        """
        for i in range(5):
            key = ('test', 'demo', i)
            TestTouch.client.remove(key)

    def test_touch_with_no_parameters(self):
        """
        Invoke touch() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            TestTouch.client.touch()
        assert "Required argument 'key' (pos 1) not found" in str(
            typeError.value)

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
        policy = {'timeout': 1000, 'retry': aerospike.POLICY_RETRY_ONCE}
        response = TestTouch.client.touch(key, 120, {}, policy)
        assert response == 0

    def test_touch_with_policy_key_send(self):
        """
        Invoke touch() with policy key send
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key': aerospike.POLICY_KEY_SEND,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'commit_level': aerospike.POLICY_COMMIT_LEVEL_MASTER
        }
        TestTouch.client.touch(key, 120, {}, policy)

        (key, _, bins) = TestTouch.client.get(key)

        assert bins == {'age': 1, 'name': 'name1'}
        assert key == ('test', 'demo', None, bytearray(
            b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8')
        )

    def test_touch_with_policy_key_digest(self):
        """
        Invoke touch() with policy key digest
        """
        key = ('test', 'demo', None, bytearray("asd;as[d'as;djk;uyfl",
                                               "utf-8"))
        rec = {'name': 'name%s' % (str(1)), 'age': 1, 'nolist': [1, 2, 3]}
        TestTouch.client.put(key, rec)

        policy = {
            'timeout': 1000,
            'key': aerospike.POLICY_KEY_DIGEST,
            'retry': aerospike.POLICY_RETRY_NONE
        }
        TestTouch.client.touch(key, 120, {}, policy)

        (key, _, bins) = TestTouch.client.get(key)

        assert bins == {'age': 1, 'name': 'name1', 'nolist': [1, 2, 3]}
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
            'key': aerospike.POLICY_KEY_SEND,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'gen': aerospike.POLICY_GEN_IGNORE
        }

        meta = {'gen': 10, 'ttl': 1200}
        TestTouch.client.touch(key, 120, meta, policy)

        (key, meta, bins) = TestTouch.client.get(key)

        assert bins == {'age': 1, 'name': 'name1'}
        assert key == ('test', 'demo', None, bytearray(
            b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8')
        )

    def test_touch_with_policy_key_gen_EQ_positive(self):
        """
        Invoke touch() with gen eq positive
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key': aerospike.POLICY_KEY_SEND,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'gen': aerospike.POLICY_GEN_EQ
        }
        (key, meta) = TestTouch.client.exists(key)

        gen = meta['gen']
        meta = {'gen': gen, 'ttl': 1200}
        TestTouch.client.touch(key, 120, meta, policy)

        (key, meta, bins) = TestTouch.client.get(key)

        assert bins == {'age': 1, 'name': 'name1'}
        assert key == ('test', 'demo', None, bytearray(
            b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8')
        )

    def test_touch_with_policy_key_gen_EQ_not_equal(self):
        """
        Invoke touch() with policy key EQ not equal
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key': aerospike.POLICY_KEY_SEND,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'gen': aerospike.POLICY_GEN_EQ
        }
        meta = {
            'gen': 10,
            'ttl': 1200
        }
        try:
            TestTouch.client.touch(key, 120, meta, policy)

        except e.RecordGenerationError as exception:
            assert exception.code == 3
            assert exception.msg == "AEROSPIKE_ERR_RECORD_GENERATION"

        (key, meta, bins) = TestTouch.client.get(key)

        assert bins == {'age': 1, 'name': 'name1'}
        assert key == ('test', 'demo', None, bytearray(
            b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8')
        )

    def test_touch_with_policy_key_gen_GT_lesser(self):
        """
        Invoke touch() with gen GT lesser
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key': aerospike.POLICY_KEY_SEND,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'gen': aerospike.POLICY_GEN_GT
        }
        (key, meta) = TestTouch.client.exists(key)

        gen = meta['gen']
        meta = {
            'gen': gen,
            'ttl': 1200
        }
        try:
            TestTouch.client.touch(key, 120, meta, policy)

        except e.RecordGenerationError as exception:
            assert exception.code == 3
            assert exception.msg == "AEROSPIKE_ERR_RECORD_GENERATION"

        (key, meta, bins) = TestTouch.client.get(key)

        assert bins == {'age': 1, 'name': 'name1'}
        assert key == ('test', 'demo', None, bytearray(
            b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8')
        )

    def test_touch_with_policy_key_gen_GT_positive(self):
        """
        Invoke touch() with gen GT positive
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key': aerospike.POLICY_KEY_SEND,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'gen': aerospike.POLICY_GEN_GT
        }
        (key, meta) = TestTouch.client.exists(key)

        gen = meta['gen']
        meta = {'gen': gen + 5, 'ttl': 1200}
        TestTouch.client.touch(key, 120, meta, policy)

        (key, meta, bins) = TestTouch.client.get(key)

        assert bins == {'age': 1, 'name': 'name1'}
        assert key == ('test', 'demo', None, bytearray(
            b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8')
        )

    def test_touch_with_incorrect_policy(self):
        """
        Invoke touch() with incorrect policy
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 0.5
        }
        try:
            TestTouch.client.touch(key, 120, {}, policy)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "timeout is invalid"

    def test_touch_with_nonexistent_key(self):
        """
        Invoke touch() with non-existent key
        """
        key = ('test', 'demo', 1000)

        try:
            TestTouch.client.touch(key, 120)

        except e.RecordNotFound as exception:
            assert exception.code == 2
            assert exception.msg == "AEROSPIKE_ERR_RECORD_NOT_FOUND"

    def test_touch_value_string(self):
        """
        Invoke touch() not a string
        """
        key = ('test', 'demo', 1)
        try:
            TestTouch.client.touch(key, "name")

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Unsupported operand type(s) for touch : only int or long allowed"

    def test_touch_with_extra_parameter(self):
        """
        Invoke touch() with extra parameter.
        """
        key = ('test', 'demo', 1)
        policy = {'timeout': 1000}
        with pytest.raises(TypeError) as typeError:
            TestTouch.client.touch(key, 120, {}, policy, "")

        assert "touch() takes at most 4 arguments (5 given)" in str(
            typeError.value)

    def test_touch_policy_is_string(self):
        """
        Invoke touch() with policy is string
        """
        key = ('test', 'demo', 1)
        try:
            TestTouch.client.touch(key, 120, {}, "")

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "policy must be a dict"

    def test_touch_with_correct_paramters_without_connection(self):
        """
        Invoke touch() with correct parameters without connection
        """
        config = {'hosts': [('127.0.0.1', 3000)]}
        client1 = aerospike.client(config)
        key = ('test', 'demo', 1)

        try:
            client1.touch(key, 120)

        except e.ClusterError as exception:
            assert exception.code == 11
            assert exception.msg == 'No connection to aerospike cluster'

    def test_touch_withttlvalue_greaterthan_maxsize(self):
        """
        Invoke touch() with ttl value greater than (2^63-1)
        """
        key = ('test', 'demo', 1)
        meta = {'gen': 10, 'ttl': 12005678901234567890}
        try:
            TestTouch.client.touch(key, 120, meta, None)
        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == 'integer value for ttl exceeds sys.maxsize'
