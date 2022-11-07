# -*- coding: utf-8 -*-
import pytest
import sys
from .test_base_class import TestBaseClass

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
    from aerospike import exception as e
except:
    print("Please install aerospike python client.")
    sys.exit(1)


class TestPrepend():

    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        """
        Setup Method
        """
        for i in range(5):
            key = ('test', 'demo', i)
            rec = {'name': 'name%s' % (str(i)), 'age': i, 'nolist': [1, 2, 3]}
            as_connection.put(key, rec)

        key = ('test', 'demo', 'bytearray_key')
        as_connection.put(
            key, {
                "bytearray_bin": bytearray(
                    "asd;as[d'as;d", "utf-8")})

        key = ("test", "demo", "bytes_key")
        as_connection.put(key, {"bytes_bin": b"x"})

        def teardown():
            """
            Teardown Method
            """
            for i in range(5):
                key = ('test', 'demo', i)
                as_connection.remove(key)

            key = ('test', 'demo', 'bytearray_key')
            as_connection.remove(key)

            key = ('test', 'demo', 'bytes_key')
            as_connection.remove(key)

        request.addfinalizer(teardown)

    def test_pos_prepend_with_correct_parameters(self):
        """
        Invoke prepend() with correct parameters
        """
        key = ('test', 'demo', 1)
        self.as_connection.prepend(key, "name", "str")

        (key, _, bins) = self.as_connection.get(key)

        assert bins == {'age': 1, 'name': 'strname1', 'nolist': [1, 2, 3]}

    def test_pos_prepend_with_correct_policy(self):
        """
        Invoke prepend() with correct policy
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key': aerospike.POLICY_KEY_SEND,
            'commit_level': aerospike.POLICY_COMMIT_LEVEL_ALL
        }

        self.as_connection.prepend(key, "name", "str", {}, policy)

        (key, _, bins) = self.as_connection.get(key)

        assert bins == {'age': 1, 'name': 'strname1', 'nolist': [1, 2, 3]}

    def test_pos_prepend_with_policy_key_send(self):
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
        self.as_connection.prepend(key, "name", "str", {}, policy)

        (key, _, bins) = self.as_connection.get(key)

        assert bins == {'age': 1, 'name': 'strname1', 'nolist': [1, 2, 3]}
        assert key == ('test', 'demo', None, bytearray(
            b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8')
        )

    def test_pos_prepend_with_policy_key_gen_EQ_ignore(self):
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
        self.as_connection.prepend(key, "name", "str", meta, policy)

        (key, meta, bins) = self.as_connection.get(key)

        assert bins == {'age': 1, 'name': 'strname1', 'nolist': [1, 2, 3]}
        assert key == ('test', 'demo', None, bytearray(
            b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8')
        )

    def test_pos_prepend_with_policy_key_gen_EQ_positive(self):
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
        (key, meta) = self.as_connection.exists(key)

        gen = meta['gen']
        meta = {'gen': gen, 'ttl': 1200}
        self.as_connection.prepend(key, "name", "str", meta, policy)

        (key, meta, bins) = self.as_connection.get(key)

        assert bins == {'age': 1, 'name': 'strname1', 'nolist': [1, 2, 3]}
        assert key == ('test', 'demo', None, bytearray(
            b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8')
        )

    def test_pos_prepend_with_policy_key_gen_GT_positive(self):
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
        (key, meta) = self.as_connection.exists(key)

        gen = meta['gen']
        meta = {'gen': gen + 2, 'ttl': 1200}
        self.as_connection.prepend(key, "name", "str", meta, policy)

        (key, meta, bins) = self.as_connection.get(key)

        assert bins == {'age': 1, 'name': 'strname1', 'nolist': [1, 2, 3]}
        assert key == ('test', 'demo', None, bytearray(
            b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8')
        )

    def test_pos_prepend_with_policy_key_digest(self):
        """
        Invoke prepend() with policy key digest
        """
        key = ('test', 'demo', None, bytearray("asd;as[d'as;djk;uyfl",
                                               "utf-8"))
        rec = {'name': 'name%s' % (str(1)), 'age': 1, 'nolist': [1, 2, 3]}
        self.as_connection.put(key, rec)

        policy = {
            'timeout': 1000,
            'key': aerospike.POLICY_KEY_DIGEST,
            'retry': aerospike.POLICY_RETRY_NONE
        }
        self.as_connection.prepend(key, "name", "str", {}, policy)

        (key, _, bins) = self.as_connection.get(key)

        assert bins == {'age': 1, 'name': 'strname1', 'nolist': [1, 2, 3]}
        assert key == ('test', 'demo', None,
                       bytearray(b"asd;as[d\'as;djk;uyfl"))

        self.as_connection.remove(key)

    @pytest.mark.parametrize("key, bin, value, expected", [
        (('test', 'demo', 1), "name", u"age", 'agename1'),
        (('test', 'demo', 1), u"add", u"address", 'address')
    ])
    def test_pos_prepend_unicode_parameters(self, key, bin, value, expected):
        """
        Invoke prepend() with unicode parameters
        """
        self.as_connection.prepend(key, bin, value)

        key, _, bins = self.as_connection.get(key)
        assert bins[bin] == expected

    def test_pos_prepend_key_with_none_set_name(self):
        """
        Invoke get_many with none set name
        """
        key = ('test', None, 1)
        policy = {'timeout': 1000}
        self.as_connection.prepend(key, "name", "ABC", {}, policy)
        key, _, bins = self.as_connection.get(key)
        assert bins == {'name': 'ABC'}
        self.as_connection.remove(key)

    def test_pos_prepend_with_nonexistent_key(self):
        """
        Invoke prepend() with non-existent key
        """
        key = ('test', 'demo', 1000)
        status = self.as_connection.prepend(key, "name", "str")

        assert status == 0
        key, _, bins = self.as_connection.get(key)
        assert bins['name'] == 'str'

        self.as_connection.remove(key)

    def test_pos_prepend_with_nonexistent_bin(self):
        """
        Invoke prepend() with non-existent bin
        """
        key = ('test', 'demo', 1)
        status = self.as_connection.prepend(key, "name1", "str")

        assert status == 0
        key, _, bins = self.as_connection.get(key)
        assert bins['name1'] == 'str'

    def test_pos_prepend_with_bytearray(self):
        """
        Invoke prepend() with bytearray value
        """
        key = ('test', 'demo', 'bytearray_key')
        self.as_connection.prepend(
            key, "bytearray_bin", bytearray("abc", "utf-8"))

        (key, _, bins) = self.as_connection.get(key)

        assert bins == {
            'bytearray_bin': bytearray("abcasd;as[d'as;d", "utf-8")}

    def test_pos_prepend_with_bytearray_new_key(self):
        """
        Invoke prepend() with bytearray value with a new record(non-existing)
        """
        key = ('test', 'demo', 'bytearray_new')
        self.as_connection.prepend(
            key, "bytearray_bin", bytearray("asd;as[d'as;d", "utf-8"))

        (key, _, bins) = self.as_connection.get(key)

        assert bins == {'bytearray_bin': bytearray("asd;as[d'as;d", "utf-8")}

        self.as_connection.remove(key)

    def test_pos_prepend_with_bytes(self):
        """
        Invoke prepend() with bytes value
        """
        key = ('test', 'demo', 'bytes_key')
        self.as_connection.prepend(key, "bytes_bin", b'a')

        (key, _, bins) = self.as_connection.get(key)

        assert bins == {
            'bytes_bin': b'ax'}

    def test_pos_prepend_with_bytes_new_key(self):
        """
        Invoke prepend() with bytes value with a new record(non-existing)
        """
        key = ('test', 'demo', 'bytes_new')
        self.as_connection.prepend(
            key, "bytes_bin", b'a')

        (key, _, bins) = self.as_connection.get(key)

        assert bins == {'bytes_bin': b'a'}

        self.as_connection.remove(key)

    # Negative tests
    def test_neg_prepend_with_no_parameters(self):
        """
        Invoke prepend() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            self.as_connection.prepend()
        assert "argument 'key' (pos 1)" in str(
            typeError.value)

    def test_neg_prepend_with_policy_key_gen_EQ_not_equal(self):
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
        (key, meta) = self.as_connection.exists(key)
        gen = meta['gen']

        meta = {
            'gen': gen + 5,
            'ttl': 1200
        }
        try:
            self.as_connection.prepend(key, "name", "str", meta, policy)

        except e.RecordGenerationError as exception:
            assert exception.code == 3
            assert exception.bin == 'name'

        (key, meta, bins) = self.as_connection.get(key)

        assert bins == {'age': 1, 'name': 'name1', 'nolist': [1, 2, 3]}
        assert key == ('test', 'demo', None, bytearray(
            b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8')
        )

    def test_neg_prepend_with_policy_key_gen_GT_lesser(self):
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
        (key, meta) = self.as_connection.exists(key)

        gen = meta['gen']
        meta = {
            'gen': gen,
            'ttl': 1200
        }
        try:
            self.as_connection.prepend(key, "name", "str", meta, policy)

        except e.RecordGenerationError as exception:
            assert exception.code == 3
            assert exception.bin == "name"

        (key, meta, bins) = self.as_connection.get(key)

        assert bins == {'age': 1, 'name': 'name1', 'nolist': [1, 2, 3]}
        assert key == ('test', 'demo', None, bytearray(
            b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8')
        )

    def test_neg_prepend_with_incorrect_policy(self):
        """
        Invoke prepend() with incorrect policy
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 0.5
        }
        try:
            self.as_connection.prepend(key, "name", "str", {}, policy)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "timeout is invalid"

    @pytest.mark.parametrize("key, bin, value, meta, policy, ex_code, ex_msg", [
        (('test', 'demo', 1), "name", 2, {}, {}, -
         2, "Cannot concatenate 'str' and 'non-str' objects"),
        (('test', 'demo', 1), "name", "abc", {}, "", -2, "policy must be a dict"),
        (('test', 'demo', 1), 1, "ABC", {}, {"timeout": 1000}, -
         2, "Bin name should be of type string")
    ])
    def test_neg_prepend_parameters_incorrect_datatypes(self, key, bin, value,
                                                        meta, policy, ex_code, ex_msg):
        """
        Invoke prepend() with parameters of incorrect datatypes
        """
        try:
            self.as_connection.prepend(key, bin, value, meta, policy)

        except e.ParamError as exception:
            assert exception.code == ex_code
            assert exception.msg == ex_msg

    def test_neg_prepend_with_extra_parameter(self):
        """
        Invoke prepend() with extra parameter.
        """
        key = ('test', 'demo', 1)
        policy = {'timeout': 1000}
        with pytest.raises(TypeError) as typeError:
            self.as_connection.prepend(key, "name", "str", {}, policy, "")

        assert "prepend() takes at most 5 arguments (6 given)" in str(typeError.value)

    @pytest.mark.parametrize("key, bin, value, ex_code, ex_msg", [
        (None, "name", "str", -2, "key is invalid"),
        (("test", "demo", 1), None, "str", -2, "Bin name should be of type string")
    ])
    def test_neg_prepend_parameters_as_none(
            self, key, bin, value, ex_code, ex_msg):
        """
        Invoke prepend() with parameters as None
        """
        try:
            self.as_connection.prepend(key, bin, value)

        except e.ParamError as exception:
            assert exception.code == ex_code
            assert exception.msg == ex_msg

    def test_neg_prepend_invalid_key_invalid_ns(self):
        """
        Invoke prepend() invalid namespace
        """
        key = ('test1', 'demo', 1)
        policy = {'timeout': 1000}
        with pytest.raises(e.ClientError):
            self.as_connection.prepend(key, "name", "ABC", {}, policy)

    @pytest.mark.parametrize("key, bin, value, meta, policy, ex_code, ex_msg", [
        (('test', 1), "name", "ABC", {}, {'timeout': 1000}, -2,
         'key tuple must be (Namespace, Set, Key) or (Namespace, Set, None, Digest)'),
        (('test', 'set'), "name", "ABC", {}, {'timeout': 1000}, -2,
         'key tuple must be (Namespace, Set, Key) or (Namespace, Set, None, Digest)'),
        (('test', 'demo', 1, 1, 1), "name", "ABC", {}, {'timeout': 1000}, -2,
         'key tuple must be (Namespace, Set, Key) or (Namespace, Set, None, Digest)')
    ])
    def test_neg_prepend_invalid_key_combinations(self, key, bin, value,
                                                  meta, policy, ex_code, ex_msg):
        """
        Invoke prepend() with invalid key combinations
        """
        try:
            self.as_connection.prepend(key, bin, value, meta, policy)
            key, meta, _ = self.as_connection.get(key)

        except e.ParamError as exception:
            assert exception.code == ex_code
            assert exception.msg == ex_msg

    def test_neg_prepend_without_bin_name(self):
        """
        Invoke prepend without bin name
        """
        key = ('test', 'demo', 1)
        policy = {'timeout': 1000}
        try:
            self.as_connection.prepend(key, "ABC", {}, policy)
            key, _, _ = self.as_connection.get(key)
        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Cannot concatenate 'str' and 'non-str' objects"

    def test_neg_prepend_with_correct_parameters_without_connection(self):
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
