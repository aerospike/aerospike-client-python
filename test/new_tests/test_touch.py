# -*- coding: utf-8 -*-
import pytest
import sys
from .test_base_class import TestBaseClass
from aerospike import exception as e
from .as_status_codes import AerospikeStatus
aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)


class TestTouch(object):
    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        self.test_demo_1_digest = aerospike.calc_digest(
            'test', 'demo', 1)
        self.added_keys = []

        for i in range(5):
            key = ('test', 'demo', i)
            rec = {'name': 'name%s' % (str(i)), 'age': i}
            as_connection.put(key, rec)
            self.added_keys.append(key)

        def teardown():
            for key in self.added_keys:
                as_connection.remove(key)
            self.added_keys = []

        request.addfinalizer(teardown)

    def test_touch_with_no_parameters(self):
        """
        Invoke touch() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            self.as_connection.touch()
        assert "argument 'key' (pos 1)" in str(
            typeError.value)

    def test_touch_with_correct_paramters(self):
        """
        Invoke touch() with correct parameters
        """
        key = ('test', 'demo', 1)
        response = self.as_connection.touch(key, 120)

        assert response == AerospikeStatus.AEROSPIKE_OK

    def test_touch_with_correct_policy(self):
        """
        Invoke touch() with correct policy
        """
        key = ('test', 'demo', 1)
        policy = {'timeout': 1000, 'retry': aerospike.POLICY_RETRY_ONCE}
        response = self.as_connection.touch(key, 120, {}, policy)
        assert response == AerospikeStatus.AEROSPIKE_OK

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
        self.as_connection.touch(key, 120, {}, policy)

        (key, _, bins) = self.as_connection.get(key)

        assert bins == {'age': 1, 'name': 'name1'}
        assert key == ('test', 'demo', None, self.test_demo_1_digest)

    def test_touch_with_policy_key_digest(self):
        """
        Invoke touch() with policy key digest
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
        self.as_connection.touch(key, 120, {}, policy)

        (key, _, bins) = self.as_connection.get(key)

        assert bins == {'age': 1, 'name': 'name1', 'nolist': [1, 2, 3]}
        assert key == ('test', 'demo', None,
                       bytearray(b"asd;as[d\'as;djk;uyfl"))
        self.as_connection.remove(key)

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
        self.as_connection.touch(key, 120, meta, policy)

        (key, meta, bins) = self.as_connection.get(key)

        assert bins == {'age': 1, 'name': 'name1'}
        assert key == ('test', 'demo', None, self.test_demo_1_digest)

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
        (key, meta) = self.as_connection.exists(key)

        gen = meta['gen']
        meta = {'gen': gen, 'ttl': 1200}
        self.as_connection.touch(key, 120, meta, policy)

        (key, meta, bins) = self.as_connection.get(key)

        assert bins == {'age': 1, 'name': 'name1'}
        assert key == ('test', 'demo', None, self.test_demo_1_digest)

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

        with pytest.raises(e.RecordGenerationError) as err_info:
            self.as_connection.touch(key, 120, meta, policy)

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_RECORD_GENERATION

        (key, meta, bins) = self.as_connection.get(key)

        assert bins == {'age': 1, 'name': 'name1'}
        assert key == ('test', 'demo', None, self.test_demo_1_digest)

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
        (key, meta) = self.as_connection.exists(key)

        gen = meta['gen']
        meta = {
            'gen': gen,
            'ttl': 1200
        }
        with pytest.raises(e.RecordGenerationError) as err_info:
            self.as_connection.touch(key, 120, meta, policy)

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_RECORD_GENERATION

        (key, meta, bins) = self.as_connection.get(key)

        assert bins == {'age': 1, 'name': 'name1'}
        assert key == ('test', 'demo', None, self.test_demo_1_digest)

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
        (key, meta) = self.as_connection.exists(key)

        gen = meta['gen']
        meta = {'gen': gen + 5, 'ttl': 1200}
        self.as_connection.touch(key, 120, meta, policy)

        (key, meta, bins) = self.as_connection.get(key)

        assert bins == {'age': 1, 'name': 'name1'}
        assert key == ('test', 'demo', None, self.test_demo_1_digest)

    def test_touch_with_incorrect_policy(self):
        """
        Invoke touch() with incorrect policy
        """
        key = ('test', 'demo', 1)
        policy = {'total_timeout': 0.5}

        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.touch(key, 120, {}, policy)

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_PARAM

    def test_touch_with_nonexistent_key(self):
        """
        Invoke touch() with non-existent key
        """
        key = ('test', 'demo', 1000)

        with pytest.raises(e.RecordNotFound) as err_info:
            self.as_connection.touch(key, 120)

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_RECORD_NOT_FOUND

    def test_touch_value_string(self):
        """
        Invoke touch() not a string
        """
        key = ('test', 'demo', 1)
        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.touch(key, "name")

        assert err_info.value.code == AerospikeStatus.AEROSPIKE_ERR_PARAM

    def test_touch_with_extra_parameter(self):
        """
        Invoke touch() with extra parameter.
        """
        key = ('test', 'demo', 1)
        policy = {'timeout': 1000}
        with pytest.raises(TypeError) as typeError:
            self.as_connection.touch(key, 120, {}, policy, "")

        assert "touch() takes at most 4 arguments (5 given)" in str(
            typeError.value)

    def test_touch_policy_is_string(self):
        """
        Invoke touch() with policy is string
        """
        key = ('test', 'demo', 1)
        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.touch(key, 120, {}, "")

        assert err_info.value.code == AerospikeStatus.AEROSPIKE_ERR_PARAM

    def test_touch_with_correct_paramters_without_connection(self):
        """
        Invoke touch() with correct parameters without connection
        """
        config = {'hosts': [('127.0.0.1', 3000)]}
        client1 = aerospike.client(config)
        key = ('test', 'demo', 1)

        with pytest.raises(e.ClusterError) as err_info:
            client1.touch(key, 120)

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_CLUSTER_ERROR

    def test_touch_withttlvalue_greaterthan_maxsize(self):
        """
        Invoke touch() with ttl value greater than (2^63-1)
        """
        key = ('test', 'demo', 1)
        meta = {'gen': 10, 'ttl': 2 ** 64}
        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.touch(key, 120, meta, None)

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_PARAM
