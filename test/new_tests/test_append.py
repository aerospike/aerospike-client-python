# -*- coding: utf-8 -*-
import pytest
import sys
from aerospike import exception as e

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)

# @pytest.mark.usefixtures("as_connection")


class TestAppend(object):

    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        """
        Setup Method
        """
        for i in range(5):
            key = ('test', 'demo', i)
            rec = {'name': 'name%s' % (str(i)), 'age': i}
            as_connection.put(key, rec)

        key = ("test", "demo", "bytearray_key")
        as_connection.put(key, {"bytearray_bin": bytearray("asd;as[d'as;d",
                                                           "utf-8")})

        key = ("test", "demo", "bytes_key")
        as_connection.put(key, {"bytes_bin": b""})

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

    def test_pos_append_with_correct_paramters(self):
        """
        Invoke append() with correct parameters
        """
        key = ('test', 'demo', 1)
        self.as_connection.append(key, "name", "str")

        (key, _, bins) = self.as_connection.get(key)

        assert bins == {'age': 1, 'name': 'name1str'}

    def test_pos_append_with_correct_policies(self):
        """
        Invoke append() with correct policies
        """
        key = ('test', 'demo', 1)
        policy = {'total_timeout': 1000,
                  'retry': aerospike.POLICY_RETRY_ONCE,
                  'commit_level': aerospike.POLICY_COMMIT_LEVEL_MASTER}
        self.as_connection.append(key, "name", "str", {}, policy)

        (key, _, bins) = self.as_connection.get(key)

        assert bins == {'age': 1, 'name': 'name1str'}

    def test_pos_append_with_policy_key_send(self):
        """
        Invoke append() with policy key send
        """
        key = ('test', 'demo', 1)
        policy = {
            'total_timeout': 1000,
            'key': aerospike.POLICY_KEY_SEND,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'commit_level': aerospike.POLICY_COMMIT_LEVEL_ALL
        }
        self.as_connection.append(key, "name", "str", {}, policy)

        (key, _, bins) = self.as_connection.get(key)

        assert bins == {'age': 1, 'name': 'name1str'}
        assert key == ('test', 'demo', None, bytearray(
            b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8')
        )

    def test_pos_append_with_policy_key_digest(self):
        """
        Invoke append() with policy key digest
        """
        key = ('test', 'demo', None, bytearray("asd;as[d'as;djk;uyfl",
                                               "utf-8"))
        rec = {'name': 'name%s' % (str(1)), 'age': 1, 'nolist': [1, 2, 3]}
        self.as_connection.put(key, rec)

        policy = {
            'total_timeout': 1000,
            'key': aerospike.POLICY_KEY_DIGEST,
            'retry': aerospike.POLICY_RETRY_NONE
        }
        self.as_connection.append(key, "name", "str", {}, policy)

        (key, _, bins) = self.as_connection.get(key)

        assert bins == {'age': 1, 'name': 'name1str', 'nolist': [1, 2, 3]}
        assert key == ('test', 'demo', None,
                       bytearray(b"asd;as[d\'as;djk;uyfl"))

        self.as_connection.remove(key)

    def test_pos_append_with_policy_key_gen_EQ_ignore(self):
        """
        Invoke append() with gen eq positive ignore
        """
        key = ('test', 'demo', 1)
        policy = {
            'total_timeout': 1000,
            'key': aerospike.POLICY_KEY_SEND,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'gen': aerospike.POLICY_GEN_IGNORE
        }

        meta = {'gen': 10, 'ttl': 1200}
        self.as_connection.append(key, "name", "str", meta, policy)

        (key, meta, bins) = self.as_connection.get(key)

        assert bins == {'age': 1, 'name': 'name1str'}
        assert key == ('test', 'demo', None, bytearray(
            b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8')
        )

    def test_pos_append_with_policy_key_gen_EQ_positive(self):
        """
        Invoke append() with gen eq positive
        """
        key = ('test', 'demo', 1)
        policy = {
            'total_timeout': 1000,
            'key': aerospike.POLICY_KEY_SEND,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'gen': aerospike.POLICY_GEN_EQ
        }
        (key, meta) = self.as_connection.exists(key)

        gen = meta['gen']

        meta = {'gen': gen, 'ttl': 1200}
        self.as_connection.append(key, "name", "str", meta, policy)

        (key, meta, bins) = self.as_connection.get(key)

        assert bins == {'age': 1, 'name': 'name1str'}
        assert key == ('test', 'demo', None, bytearray(
            b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8')
        )

    def test_pos_append_with_policy_key_gen_GT_positive(self):
        """
        Invoke append() with gen GT positive
        """
        key = ('test', 'demo', 1)
        policy = {
            'total_timeout': 1000,
            'key': aerospike.POLICY_KEY_SEND,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'gen': aerospike.POLICY_GEN_GT
        }
        (key, meta) = self.as_connection.exists(key)
        gen = meta['gen']

        meta = {'gen': gen + 2, 'ttl': 1200}
        self.as_connection.append(key, "name", "str", meta, policy)

        (key, meta, bins) = self.as_connection.get(key)

        assert bins == {'age': 1, 'name': 'name1str'}
        assert key == ('test', 'demo', None, bytearray(
            b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8')
        )

    def test_pos_append_with_nonexistent_key(self):
        """
        Invoke append() with non-existent key
        """
        key = ('test', 'demo', 1000)
        status = self.as_connection.append(key, "name", "str")

        assert status == 0
        self.as_connection.remove(key)

    def test_pos_append_with_nonexistent_bin(self):
        """
        Invoke append() with non-existent bin
        """
        key = ('test', 'demo', 1)
        status = self.as_connection.append(key, "name1", "str")

        assert status == 0

    def test_pos_append_unicode_value(self):
        """
        Invoke append() with unicode string
        """
        key = ('test', 'demo', 1)
        self.as_connection.append(key, "name", u"address")

        key, _, bins = self.as_connection.get(key)
        assert bins['name'] == 'name1address'

    def test_pos_append_unicode_bin_name(self):
        """
        Invoke append() with unicode string
        """
        key = ('test', 'demo', 1)
        self.as_connection.append(key, u"add", u"address")

        key, _, bins = self.as_connection.get(key)
        assert bins['add'] == 'address'

    def test_pos_append_with_correct_timeout_policy(self):
        """
        Invoke append() with correct policy
        """
        key = ('test', 'demo', 1)
        policy = {'gen': 5,
                  'total_timeout': 300,
                  'retry': aerospike.POLICY_RETRY_ONCE,
                  'commit_level': aerospike.POLICY_COMMIT_LEVEL_MASTER
                  }
        self.as_connection.append(key, "name", "str", {}, policy)

        (key, _, bins) = self.as_connection.get(key)

        assert bins == {'age': 1, 'name': 'name1str'}

    def test_pos_append_with_bytearray(self):
        """
        Invoke append() with bytearray value
        """
        key = ('test', 'demo', 'bytearray_key')
        self.as_connection.append(
            key, "bytearray_bin", bytearray(
                "abc", "utf-8"))

        (key, _, bins) = self.as_connection.get(key)

        assert bins == {
            'bytearray_bin': bytearray("asd;as[d'as;dabc", "utf-8")}

    def test_pos_append_with_bytearray_new_key(self):
        """
        Invoke append() with bytearray value with a new record(non-existing)
        """
        key = ('test', 'demo', 'bytearray_new')
        self.as_connection.append(
            key, "bytearray_bin", bytearray("asd;as[d'as;d", "utf-8"))

        (key, _, bins) = self.as_connection.get(key)

        assert bins == {'bytearray_bin': bytearray("asd;as[d'as;d", "utf-8")}

        self.as_connection.remove(key)

    def test_pos_append_with_bytes(self):
        """
        Invoke append() with bytes value
        """
        key = ('test', 'demo', 'bytes_key')
        self.as_connection.append(key, "bytes_bin", b'a')

        (key, _, bins) = self.as_connection.get(key)

        assert bins == {
            'bytes_bin': b'a'}

    def test_pos_append_with_bytes_new_key(self):
        """
        Invoke append() with bytes value with a new record(non-existing)
        """
        key = ('test', 'demo', 'bytes_new')
        self.as_connection.append(
            key, "bytes_bin", b'a')

        (key, _, bins) = self.as_connection.get(key)

        assert bins == {'bytes_bin': b'a'}

        self.as_connection.remove(key)

    # Negative append tests
    def test_neg_append_with_no_parameters(self):
        """
        Invoke append() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            self.as_connection.append()
        assert "argument 'key' (pos 1)" in str(
            typeError.value)

    def test_neg_append_with_policy_key_gen_GT_lesser(self):
        """
        Invoke append() with gen GT lesser
        """
        key = ('test', 'demo', 1)
        policy = {
            'total_timeout': 1000,
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
            self.as_connection.append(key, "name", "str", meta, policy)
        except e.RecordGenerationError as exception:
            assert exception.code == 3
            assert exception.bin == "name"
        (key, meta, bins) = self.as_connection.get(key)
        assert bins == {'age': 1, 'name': 'name1'}
        assert key == ('test', 'demo', None, bytearray(
            b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8')
        )

    def test_neg_append_with_policy_key_gen_EQ_not_equal(self):
        """
        Invoke append() with policy key EQ not equal
        """
        key = ('test', 'demo', 1)
        policy = {
            'total_timeout': 1000,
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
            self.as_connection.append(key, "name", "str", meta, policy)

        except e.RecordGenerationError as exception:
            assert exception.code == 3
            assert exception.bin == "name"

        (key, meta, bins) = self.as_connection.get(key)

        assert bins == {'age': 1, 'name': 'name1'}
        assert key == ('test', 'demo', None, bytearray(
            b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8')
        )

    @pytest.mark.parametrize("key, bin, value, meta, policy, ex_code, ex_msg", [
        (('test', 'demo', 1), "name", 2, {}, {}, -2,
            "Cannot concatenate 'str' and 'non-str' objects"),
        (('test', 'demo', 1), "name", "pqr", {}, "", -2,
            "policy must be a dict"),
        (('test', 'demo', 1), 3, "str", {},
            {'gen': 5, 'total_timeout': 3000,
             'retry': aerospike.POLICY_RETRY_ONCE,
             'commit_level': aerospike.POLICY_COMMIT_LEVEL_MASTER}, -2,
            'Bin name should be of type string'),
        (('test', 'name'), "name", "str", {}, {"gen": 5}, -2,
            'key tuple must be (Namespace, Set, Key) or (Namespace, Set, None, Digest)'),
        (('test', 'demo', 1), "name", "str", {}, {
         'total_timeout': 0.5}, -2, "total_timeout is invalid")
    ])
    def test_neg_append_params_of_incorrect_types(self, key, bin, value, meta,
                                                  policy, ex_code, ex_msg):
        """
        Invoke append() parameters of incorrect datatypes
        """
        with pytest.raises(e.ParamError):
            self.as_connection.append(key, bin, value, meta, policy)

    def test_neg_append_with_extra_parameter(self):
        """
        Invoke append() with extra parameter.
        """
        key = ('test', 'demo', 1)
        policy = {'total_timeout': 1000}
        with pytest.raises(TypeError) as typeError:
            self.as_connection.append(key, "name", "str", {}, policy, "")
        msg = "append() takes at most 5 arguments (6 given)"
        assert msg in str(typeError.value)

    @pytest.mark.parametrize("key, bin, ex_code, ex_msg", [
        (None, "name", -2, "key is invalid"),
        (('test', 'demo', 1), None, -2, "Bin name should be of type string")
    ])
    def test_neg_append_parameters_as_none(self, key, bin, ex_code, ex_msg):
        """
        Invoke append() with parametes as None
        """
        with pytest.raises(e.ParamError):
            self.as_connection.append(key, bin, "str")

    def test_neg_append_with_correct_parameters_without_connection(self):
        """
        Invoke append() with correct parameters without connection
        """
        config = {'hosts': [('127.0.0.1', 3000)]}
        client1 = aerospike.client(config)
        key = ('test', 'demo', 1)

        try:
            client1.append(key, "name", "str")

        except e.ClusterError as exception:
            assert exception.code == 11

    def test_neg_append_with_low_timeout(self):
        """
        Invoke append() with low timeout in policy
        """
        key = ('test', 'demo', 1)
        policy = {'gen': 5,
                  'total_timeout': 1,
                  # 'retry': aerospike.POLICY_RETRY_ONCE,
                  'commit_level': aerospike.POLICY_COMMIT_LEVEL_MASTER
                  }
        try:
            self.as_connection.append(key, "name", "str", {}, policy)
        except e.TimeoutError as exception:
            assert exception.code == 9

    def test_neg_append_with_non_existent_ns(self):
        """
        Invoke append() with non existent ns
        """
        key = ('test1', 'demo', 'name')
        policy = {'gen': 5,
                  'total_timeout': 300,
                  }
        with pytest.raises(e.ClientError):
            self.as_connection.append(key, "name", "str", {}, policy)
