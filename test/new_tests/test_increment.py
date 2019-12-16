# -*- coding: utf-8 -*-
import pytest
import sys

from .test_base_class import TestBaseClass
from .as_status_codes import AerospikeStatus
from aerospike import exception as e

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)


class TestIncrement(object):

    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        for i in range(5):
            key = ('test', 'demo', i)
            rec = {'name': 'name%s' % (str(i)), 'age': i}
            as_connection.put(key, rec)
        key = ('test', 'demo', 6)
        rec = {'age': 6.5}
        as_connection.put(key, rec)

        def teardown():
            """
            Teardown method.
            """
            for i in range(5):
                key = ('test', 'demo', i)
                as_connection.remove(key)
            key = ('test', 'demo', 6)
            as_connection.remove(key)

        request.addfinalizer(teardown)

    def test_increment_with_no_parameters(self):
        """
        Invoke increment() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            self.as_connection.increment()
        assert "argument 'key' (pos 1)" in str(
            typeError.value)

    def test_increment_with_correct_parameters(self):
        """
        Invoke increment() with correct parameters
        """
        key = ('test', 'demo', 1)
        self.as_connection.increment(key, "age", 5)

        (key, _, bins) = self.as_connection.get(key)

        assert bins == {'age': 6, 'name': 'name1'}

    def test_increment_with_correct_parameters_float_value(self):
        """
        Invoke increment() with correct parameters and a float value
        """
        if TestIncrement.skip_old_server is True:
            pytest.skip("Server does not support increment on float type")
        key = ('test', 'demo', 6)
        self.as_connection.increment(key, "age", 6.4)

        (key, _, bins) = self.as_connection.get(key)

        assert bins == {'age': 12.9}

    def test_increment_with_policy_key_send(self):
        """
        Invoke increment() with policy key send
        """
        key = ('test', 'demo', 1)
        policy = {
            'total_timeout': 1000,
            'key': aerospike.POLICY_KEY_SEND,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'commit_level': aerospike.POLICY_COMMIT_LEVEL_MASTER
        }
        self.as_connection.increment(key, "age", 5, {}, policy)

        (key, _, bins) = self.as_connection.get(key)

        assert bins == {'age': 6, 'name': 'name1'}
        assert key == ('test', 'demo', None, bytearray(
            b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8')
        )

    def test_increment_with_policy_key_digest(self):
        """
        Invoke increment() with policy key digest
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
        self.as_connection.increment(key, "age", 5, {}, policy)

        (key, _, bins) = self.as_connection.get(key)

        assert bins == {'age': 6, 'name': 'name1', 'nolist': [1, 2, 3]}
        assert key == ('test', 'demo', None,
                       bytearray(b"asd;as[d\'as;djk;uyfl"))
        self.as_connection.remove(key)

    def test_increment_with_correct_policy(self):
        """
        Invoke increment() with correct policy
        """
        key = ('test', 'demo', 1)
        policy = {'total_timeout': 1000, 'key': aerospike.POLICY_KEY_SEND}
        self.as_connection.increment(key, "age", 5, {}, policy)

        (key, _, bins) = self.as_connection.get(key)

        assert bins == {'age': 6, 'name': 'name1'}

    def test_increment_with_policy_key_gen_EQ_ignore(self):
        """
        Invoke increment() with gen eq positive ignore
        """
        key = ('test', 'demo', 1)
        policy = {
            'total_timeout': 1000,
            'key': aerospike.POLICY_KEY_SEND,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'gen': aerospike.POLICY_GEN_IGNORE
        }

        meta = {'gen': 10, 'ttl': 1200}
        self.as_connection.increment(key, "age", 5, meta, policy)

        (key, meta, bins) = self.as_connection.get(key)

        assert bins == {'age': 6, 'name': 'name1'}
        assert key == ('test', 'demo', None, bytearray(
            b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8')
        )

    def test_increment_with_policy_key_gen_EQ_positive(self):
        """
        Invoke increment() with gen eq positive
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
        self.as_connection.increment(key, "age", 5, meta, policy)

        (key, meta, bins) = self.as_connection.get(key)

        assert bins == {'age': 6, 'name': 'name1'}
        assert key == ('test', 'demo', None, bytearray(
            b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8')
        )

    def test_increment_with_policy_key_gen_EQ_not_equal(self):
        """
        Invoke increment() with policy key EQ not equal
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

        #  Since the generations are not equal, this should raise an error
        #  And not increment.
        with pytest.raises(e.RecordGenerationError) as err_info:
            self.as_connection.increment(key, "age", 5, meta, policy)

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_RECORD_GENERATION

        # The age bin should not have been incremented
        _, _, bins = self.as_connection.get(key)

        assert bins['age'] == 1

    def test_increment_with_policy_key_gen_GT_lesser(self):
        """
        Invoke increment() with gen GT lesser
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
        #  since gen is equal to the server version, this should raise an error
        with pytest.raises(e.RecordGenerationError) as err_info:
            self.as_connection.increment(key, "age", 5, meta, policy)

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_RECORD_GENERATION

        # The age bin should not have been incremented
        _, _, bins = self.as_connection.get(key)

        assert bins['age'] == 1

    def test_increment_with_policy_key_gen_GT_positive(self):
        """
        Invoke increment() with gen GT positive
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
        meta = {'gen': gen + 5, 'ttl': 1200}
        self.as_connection.increment(key, "age", 5, meta, policy)

        (key, meta, bins) = self.as_connection.get(key)

        assert bins == {'age': 6, 'name': 'name1'}
        assert key == ('test', 'demo', None, bytearray(
            b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8')
        )

    def test_increment_with_incorrect_policy(self):
        """
        Invoke increment() with incorrect policy
        """
        key = ('test', 'demo', 1)
        policy = {
            'total_timeout': 0.5
        }
        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.increment(key, "age", 5, {}, policy)

    def test_increment_with_nonexistent_key(self):
        """
        Invoke increment() with non-existent key
        """
        key = ('test', 'demo', 'non-existentkey')
        self.as_connection.increment(key, "age", 5)

        (key, _, bins) = self.as_connection.get(key)

        assert bins == {'age': 5}

        self.as_connection.remove(key)

    def test_increment_with_nonexistent_bin(self):
        """
        Invoke increment() with non-existent bin
        """
        key = ('test', 'demo', 1)
        self.as_connection.increment(key, "age1", 5)

        (key, _, bins) = self.as_connection.get(key)

        assert bins == {'age1': 5, 'name': u'name1', 'age': 1}

    def test_increment_with_extra_parameter(self):
        """
        Invoke increment() with extra parameter.
        """
        key = ('test', 'demo', 1)
        policy = {'total_timeout': 1000}
        with pytest.raises(TypeError) as typeError:
            self.as_connection.increment(key, "age", 2, {}, policy, "")

        assert "increment() takes at most 5 arguments (6 given)" in str(
            typeError.value)

    def test_increment_policy_is_string(self):
        """
        Invoke increment() with policy is string
        """
        key = ('test', 'demo', 1)
        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.increment(key, "age", 2, {}, "")

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_PARAM

    def test_increment_key_is_none(self):
        """
        Invoke increment() with key is none
        """
        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.increment(None, "age", 2)

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_PARAM

    def test_increment_bin_is_none(self):
        """
        Invoke increment() with bin is none
        """
        key = ('test', 'demo', 1)
        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.increment(key, None, 2)

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_PARAM

    def test_increment_with_unicode_bin(self):
        """
        Invoke increment() with bin is unicode string
        """
        key = ('test', 'demo', 1)
        self.as_connection.increment(key, u"age", 10)

        (key, _, bins) = self.as_connection.get(key)

        assert bins == {'age': 11, 'name': 'name1'}

    def test_increment_with_correct_parameters_without_connection(self):
        """
        Invoke increment() with correct parameters without connection
        """
        key = ('test', 'demo', 1)
        config = {'hosts': [('127.0.0.1', 3000)]}
        client1 = aerospike.client(config)

        with pytest.raises(e.ClusterError) as err_info:
            client1.increment(key, "age", 5)

        err_code = err_info.value.code

        assert err_code == AerospikeStatus.AEROSPIKE_CLUSTER_ERROR

    @pytest.mark.skip(reason="This raises a system error." +
                             " Something else should be raised")
    def test_increment_with_integer_greaterthan_maxsize(self):
        """
        Invoke increment() with integer greater then(2^63 - 1)
        """
        key = ('test', 'demo', 1)
        bins = {"age": 10}
        self.as_connection.put(key, bins)
        try:
            self.as_connection.increment(key, 'age', 68786586756785785745)
        # except SystemError:
        #       pass
        except Exception as exception:
            assert exception.code == AerospikeStatus.AEROSPIKE_ERR_PARAM
            assert exception.msg == 'integer value exceeds sys.maxsize'

    def test_increment_with_string_value(self):
        """
        Invoke increment() with string value
        """
        with pytest.raises(e.ParamError) as err_info:
            key = ('test', 'demo', 1)
            self.as_connection.increment(key, "age", "5")

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_PARAM

    def test_increment_non_numeric_value(self):
        '''
        Try to increment a string by an int
        '''
        key = ('test', 'demo', 1)
        with pytest.raises(e.BinIncompatibleType) as err_info:
            self.as_connection.increment(key, 'name', 1)

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_BIN_INCOMPATIBLE_TYPE

    def test_increment_float_value_by_int(self):
        """
        Invoke increment() with correct parameters and a float value
        """
        if TestIncrement.skip_old_server is True:
            pytest.skip("Server does not support increment on float type")
        key = ('test', 'demo', 6)
        with pytest.raises(e.BinIncompatibleType):
            self.as_connection.increment(key, "age", 1)

    def test_increment_by_float_val_causing_greater_than_max(self):
        if TestIncrement.skip_old_server is True:
            pytest.skip("Server does not support increment on float type")
        key = ('test', 'demo', 6)
        big_number = sys.float_info.max

        self.as_connection.increment(key, "age", big_number)
        self.as_connection.increment(key, "age", big_number)

        _, _, record = self.as_connection.get(key)
        assert record['age'] == float('inf')

    def test_increment_float_value_by_infinity(self):
        """
        Invoke increment() with correct parameters and a float value
        """
        if TestIncrement.skip_old_server is True:
            pytest.skip("Server does not support increment on float type")
        key = ('test', 'demo', 6)

        self.as_connection.increment(key, "age", float('inf'))

        _, _, record = self.as_connection.get(key)
        assert record['age'] == float('inf')

    def test_increment_float_value_by_negative_infinity(self):
        """
        Invoke increment() with correct parameters and a float value
        """
        if TestIncrement.skip_old_server is True:
            pytest.skip("Server does not support increment on float type")
        key = ('test', 'demo', 6)

        self.as_connection.increment(key, "age", -float('inf'))

        _, _, record = self.as_connection.get(key)
        assert record['age'] == -float('inf')

    def test_increment_int_value_by_float(self):
        """
        Invoke increment() with correct parameters and a float value
        """
        if TestIncrement.skip_old_server is True:
            pytest.skip("Server does not support increment on float type")
        key = ('test', 'demo', 1)

        with pytest.raises(e.BinIncompatibleType):
            self.as_connection.increment(key, "age", 1.5)

    @pytest.mark.parametrize(
        "add_value",
        (
            bytearray('abc', 'utf-8'),
            None,
            "a"
        ),
        ids=[
            'bytes',
            'None',
            'string'
        ]
    )
    def test_increment_by_non_numeric_value(self, add_value):
        key = ('test', 'demo', 1)
        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.increment(key, 'age', add_value)

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_PARAM

    @pytest.mark.skip(reason="Not sure correct behavior is for overflow")
    def test_increment_past_max_size(self):
        key = ('test', 'demo', 'overflow')
        record = {'age': 2 ** 63 - 1}
        self.as_connection.put(key, record)

        self.as_connection.increment(key, 'age', 2)

        _, _, data = self.as_connection.get(key)
        self.as_connection.remove(key)
        assert data['age'] > 2 ** 63 - 1

    @pytest.mark.skip(reason="Not sure what happens with negative overflow")
    def test_increment_beyond_min_size(self):
        key = ('test', 'demo', 'overflow')
        record = {'age': -2 ** 63}
        self.as_connection.put(key, record)

        self.as_connection.increment(key, 'age', -2)

        _, _, data = self.as_connection.get(key)
        self.as_connection.remove(key)
        assert data['age'] < -2 ** 63

    def test_increment_with_no_inc_value(self):
        key = ('test', 'demo', 1)
        with pytest.raises(TypeError) as err_info:
            self.as_connection.increment(key, 'age')
