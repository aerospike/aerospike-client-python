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


class TestPrepend(object):

    def setup_class(cls):
        """
        Setup method.
        """
        hostlist, user, password = TestBaseClass.get_hosts()
        config = {'hosts': hostlist}
        if user is None and password is None:
            TestPrepend.client = aerospike.client(config).connect()
        else:
            TestPrepend.client = aerospike.client(config).connect(user,
                                                                  password)

    def teardown_class(cls):
        TestPrepend.client.close()

    def setup_method(self, method):
        for i in range(5):
            key = ('test', 'demo', i)
            rec = {'name': 'name%s' % (str(i)), 'age': i, 'nolist': [1, 2, 3]}
            TestPrepend.client.put(key, rec)
        key = ('test', 'demo', 'bytearray_key')
        TestPrepend.client.put(
            key, {"bytearray_bin": bytearray("asd;as[d'as;d", "utf-8")})

    def teardown_method(self, method):
        """
        Teardoen method.
        """
        for i in range(5):
            key = ('test', 'demo', i)
            TestPrepend.client.remove(key)
        key = ('test', 'demo', 'bytearray_key')
        TestPrepend.client.remove(key)

    def test_prepend_with_no_parameters(self):
        """
        Invoke prepend() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            TestPrepend.client.prepend()
        assert "Required argument 'key' (pos 1) not found" in str(
            typeError.value)

    def test_prepend_with_correct_paramters(self):
        """
        Invoke prepend() with correct parameters
        """
        key = ('test', 'demo', 1)
        TestPrepend.client.prepend(key, "name", "str")

        (key, _, bins) = TestPrepend.client.get(key)

        assert bins == {'age': 1, 'name': 'strname1', 'nolist': [1, 2, 3]}

    def test_prepend_with_correct_policy(self):
        """
        Invoke prepend() with correct policy
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key': aerospike.POLICY_KEY_SEND,
            'commit_level': aerospike.POLICY_COMMIT_LEVEL_ALL
        }

        TestPrepend.client.prepend(key, "name", "str", {}, policy)

        (key, _, bins) = TestPrepend.client.get(key)

        assert bins == {'age': 1, 'name': 'strname1', 'nolist': [1, 2, 3]}

    def test_prepend_with_policy_key_send(self):
        """
        Invoke prepend() with policy key send
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key': aerospike.POLICY_KEY_SEND,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'commit_level': aerospike.POLICY_COMMIT_LEVEL_MASTER
        }
        TestPrepend.client.prepend(key, "name", "str", {}, policy)

        (key, _, bins) = TestPrepend.client.get(key)

        assert bins == {'age': 1, 'name': 'strname1', 'nolist': [1, 2, 3]}
        assert key == ('test', 'demo', None, bytearray(
            b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8')
        )

    def test_prepend_with_policy_key_gen_EQ_ignore(self):
        """
        Invoke prepend() with gen eq positive
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key': aerospike.POLICY_KEY_SEND,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'gen': aerospike.POLICY_GEN_IGNORE
        }

        meta = {'gen': 10, 'ttl': 1200}
        TestPrepend.client.prepend(key, "name", "str", meta, policy)

        (key, meta, bins) = TestPrepend.client.get(key)

        assert bins == {'age': 1, 'name': 'strname1', 'nolist': [1, 2, 3]}
        assert key == ('test', 'demo', None, bytearray(
            b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8')
        )

    def test_prepend_with_policy_key_gen_EQ_positive(self):
        """
        Invoke prepend() with gen eq positive
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key': aerospike.POLICY_KEY_SEND,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'gen': aerospike.POLICY_GEN_EQ
        }
        (key, meta) = TestPrepend.client.exists(key)

        gen = meta['gen']
        meta = {'gen': gen, 'ttl': 1200}
        TestPrepend.client.prepend(key, "name", "str", meta, policy)

        (key, meta, bins) = TestPrepend.client.get(key)

        assert bins == {'age': 1, 'name': 'strname1', 'nolist': [1, 2, 3]}
        assert key == ('test', 'demo', None, bytearray(
            b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8')
        )

    def test_prepend_with_policy_key_gen_EQ_not_equal(self):
        """
        Invoke prepend() with policy key GEN_EQ not equal
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key': aerospike.POLICY_KEY_SEND,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'gen': aerospike.POLICY_GEN_EQ
        }
        (key, meta) = TestPrepend.client.exists(key)
        gen = meta['gen']

        meta = {
            'gen': gen + 5,
            'ttl': 1200
        }
        try:
            TestPrepend.client.prepend(key, "name", "str", meta, policy)

        except e.RecordGenerationError as exception:
            assert exception.code == 3
            assert exception.msg == "AEROSPIKE_ERR_RECORD_GENERATION"
            assert exception.bin == 'name'

        (key, meta, bins) = TestPrepend.client.get(key)

        assert bins == {'age': 1, 'name': 'name1', 'nolist': [1, 2, 3]}
        assert key == ('test', 'demo', None, bytearray(
            b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8')
        )

    def test_prepend_with_policy_key_gen_GT_lesser(self):
        """
        Invoke prepend() with gen GT positive lesser
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key': aerospike.POLICY_KEY_SEND,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'gen': aerospike.POLICY_GEN_GT
        }
        (key, meta) = TestPrepend.client.exists(key)

        gen = meta['gen']
        meta = {
            'gen': gen,
            'ttl': 1200
        }
        try:
            TestPrepend.client.prepend(key, "name", "str", meta, policy)

        except e.RecordGenerationError as exception:
            assert exception.code == 3
            assert exception.msg == "AEROSPIKE_ERR_RECORD_GENERATION"
            assert exception.bin == "name"

        (key, meta, bins) = TestPrepend.client.get(key)

        assert bins == {'age': 1, 'name': 'name1', 'nolist': [1, 2, 3]}
        assert key == ('test', 'demo', None, bytearray(
            b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8')
        )

    def test_prepend_with_policy_key_gen_GT_positive(self):
        """
        Invoke prepend() with gen GT positive
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key': aerospike.POLICY_KEY_SEND,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'gen': aerospike.POLICY_GEN_GT
        }
        (key, meta) = TestPrepend.client.exists(key)

        gen = meta['gen']
        meta = {'gen': gen + 2, 'ttl': 1200}
        TestPrepend.client.prepend(key, "name", "str", meta, policy)

        (key, meta, bins) = TestPrepend.client.get(key)

        assert bins == {'age': 1, 'name': 'strname1', 'nolist': [1, 2, 3]}
        assert key == ('test', 'demo', None, bytearray(
            b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8')
        )

    def test_prepend_with_policy_key_digest(self):
        """
        Invoke prepend() with policy key digest
        """
        key = ('test', 'demo', None, bytearray("asd;as[d'as;djk;uyfl",
                                               "utf-8"))
        rec = {'name': 'name%s' % (str(1)), 'age': 1, 'nolist': [1, 2, 3]}
        TestPrepend.client.put(key, rec)

        policy = {
            'timeout': 1000,
            'key': aerospike.POLICY_KEY_DIGEST,
            'retry': aerospike.POLICY_RETRY_NONE
        }
        TestPrepend.client.prepend(key, "name", "str", {}, policy)

        (key, _, bins) = TestPrepend.client.get(key)

        assert bins == {'age': 1, 'name': 'strname1', 'nolist': [1, 2, 3]}
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
        try:
            TestPrepend.client.prepend(key, "name", "str", {}, policy)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "timeout is invalid"

    def test_prepend_with_nonexistent_key(self):
        """
        Invoke prepend() with non-existent key
        """
        key = ('test', 'demo', 1000)
        status = TestPrepend.client.prepend(key, "name", "str")

        assert status == 0
        TestPrepend.client.remove(key)

    def test_prepend_with_nonexistent_bin(self):
        """
        Invoke prepend() with non-existent bin
        """
        key = ('test', 'demo', 1)
        status = TestPrepend.client.prepend(key, "name1", "str")

        assert status == 0

    def test_prepend_value_not_string(self):
        """
        Invoke prepend() not a string
        """
        key = ('test', 'demo', 1)
        try:
            TestPrepend.client.prepend(key, "name", 2)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Cannot concatenate 'str' and 'non-str' objects"

    def test_prepend_with_extra_parameter(self):
        """
        Invoke prepend() with extra parameter.
        """
        key = ('test', 'demo', 1)
        policy = {'timeout': 1000}
        with pytest.raises(TypeError) as typeError:
            TestPrepend.client.prepend(key, "name", "str", {}, policy, "")

        assert "prepend() takes at most 5 arguments (6 given)" in str(
            typeError.value)

    def test_prepend_policy_is_string(self):
        """
        Invoke prepend() with policy is string
        """
        key = ('test', 'demo', 1)
        try:
            TestPrepend.client.prepend(key, "name", "abc", {}, "")

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "policy must be a dict"

    def test_prepend_key_is_none(self):
        """
        Invoke prepend() with key is none
        """
        try:
            TestPrepend.client.prepend(None, "name", "str")

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "key is invalid"

    def test_prepend_bin_is_none(self):
        """
        Invoke prepend() with bin is none
        """
        key = ('test', 'demo', 1)
        try:
            TestPrepend.client.prepend(key, None, "str")

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Bin name should be of type string"

    def test_prepend_unicode_string(self):
        """
        Invoke prepend() with unicode string
        """
        key = ('test', 'demo', 1)
        TestPrepend.client.prepend(key, "name", u"age")

        key, _, bins = TestPrepend.client.get(key)
        assert bins['name'] == 'agename1'

    def test_prepend_unicode_bin_name(self):
        """
        Invoke prepend() with unicode string
        """
        key = ('test', 'demo', 1)
        TestPrepend.client.prepend(key, u"add", u"address")

        key, _, bins = TestPrepend.client.get(key)
        assert bins['add'] == 'address'

    def test_prepend_with_correct_parameters_without_connection(self):
        """
        Invoke prepend() with correct parameters without connection
        """
        config = {'hosts': [('127.0.0.1', 3000)]}
        client1 = aerospike.client(config)
        key = ('test', 'demo', 1)

        try:
            client1.prepend(key, "name", "str")

        except e.ClusterError as exception:
            assert exception.code == 11
            assert exception.msg == 'No connection to aerospike cluster'

    def test_prepend_with_bytearray(self):
        """
        Invoke prepend() with bytearray value
        """
        key = ('test', 'demo', 'bytearray_key')
        TestPrepend.client.prepend(
            key, "bytearray_bin", bytearray("abc", "utf-8"))

        (key, _, bins) = TestPrepend.client.get(key)

        assert bins == {
            'bytearray_bin': bytearray("abcasd;as[d'as;d", "utf-8")}

    def test_prepend_with_bytearray_new_key(self):
        """
        Invoke prepend() with bytearray value with a new record(non-existing)
        """
        key = ('test', 'demo', 'bytearray_new')
        TestPrepend.client.prepend(
            key, "bytearray_bin", bytearray("asd;as[d'as;d", "utf-8"))

        (key, _, bins) = TestPrepend.client.get(key)

        assert bins == {'bytearray_bin': bytearray("asd;as[d'as;d", "utf-8")}

        TestPrepend.client.remove(key)
