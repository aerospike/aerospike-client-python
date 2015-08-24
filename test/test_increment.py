# -*- coding: utf-8 -*-
import pytest
import time
import sys
import cPickle as pickle
from test_base_class import TestBaseClass

aerospike = pytest.importorskip("aerospike")
try:
    from aerospike.exception import *
except:
    print "Please install aerospike python client."
    sys.exit(1)

class TestIncrement(object):
    def setup_class(cls):
        """
        Setup method.
        """
        hostlist, user, password = TestBaseClass.get_hosts()
        config = {'hosts': hostlist}
        if user == None and password == None:
            TestIncrement.client = aerospike.client(config).connect()
        else:
            TestIncrement.client = aerospike.client(config).connect(user,
                                                                    password)
        TestIncrement.skip_old_server = True
        versioninfo = TestIncrement.client.info('version')
        for keys in versioninfo:
            for value in versioninfo[keys]:
                if value != None:
                    versionlist = value[value.find("build") + 6:value.find("\n")].split(".")
                    if int(versionlist[0]) >= 3 and int(versionlist[1]) >= 6:
                        TestIncrement.skip_old_server = False

    def teardown_class(cls):
        TestIncrement.client.close()

    def setup_method(self, method):
        for i in xrange(5):
            key = ('test', 'demo', i)
            rec = {'name': 'name%s' % (str(i)), 'age': i}
            TestIncrement.client.put(key, rec)
        key = ('test', 'demo', 6)
        rec = {'age': 6.5}
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

        (key, meta, bins) = TestIncrement.client.get(key)

        assert bins == {'age': 6, 'name': 'name1'}

    def test_increment_with_correct_parameters_float_value(self):
        """
        Invoke increment() with correct parameters and a float value
        """
        if TestIncrement.skip_old_server == True:
            pytest.skip("Server does not support increment on float type")
        key = ('test', 'demo', 6)
        TestIncrement.client.increment(key, "age", 6.4)

        (key, meta, bins) = TestIncrement.client.get(key)

        assert bins == {'age': 12.9}

    def test_increment_with_policy_key_send(self):
        """
        Invoke increment() with policy key send
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key': aerospike.POLICY_KEY_SEND,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'commit_level': aerospike.POLICY_COMMIT_LEVEL_MASTER
        }
        TestIncrement.client.increment(key, "age", 5, {}, policy)

        (key, meta, bins) = TestIncrement.client.get(key)

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
        TestIncrement.client.put(key, rec)

        policy = {
            'timeout': 1000,
            'key': aerospike.POLICY_KEY_DIGEST,
            'retry': aerospike.POLICY_RETRY_NONE
        }
        TestIncrement.client.increment(key, "age", 5, {}, policy)

        (key, meta, bins) = TestIncrement.client.get(key)

        assert bins == {'age': 6, 'name': 'name1', 'nolist': [1, 2, 3]}
        assert key == ('test', 'demo', None,
                       bytearray(b"asd;as[d\'as;djk;uyfl"))
        TestIncrement.client.remove(key)

    def test_increment_with_correct_policy(self):
        """
        Invoke increment() with correct policy
        """
        key = ('test', 'demo', 1)
        policy = {'timeout': 1000, 'key': aerospike.POLICY_KEY_SEND}
        TestIncrement.client.increment(key, "age", 5, {}, policy)

        (key, meta, bins) = TestIncrement.client.get(key)

        assert bins == {'age': 6, 'name': 'name1'}

    def test_increment_with_policy_key_gen_EQ_ignore(self):
        """
        Invoke increment() with gen eq positive ignore
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key': aerospike.POLICY_KEY_SEND,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'gen': aerospike.POLICY_GEN_IGNORE
        }

        meta = {'gen': 10, 'ttl': 1200}
        TestIncrement.client.increment(key, "age", 5, meta, policy)

        (key, meta, bins) = TestIncrement.client.get(key)

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
            'timeout': 1000,
            'key': aerospike.POLICY_KEY_SEND,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'gen': aerospike.POLICY_GEN_EQ
        }
        (key, meta) = TestIncrement.client.exists(key)

        gen = meta['gen']
        meta = {'gen': gen, 'ttl': 1200}
        TestIncrement.client.increment(key, "age", 5, meta, policy)

        (key, meta, bins) = TestIncrement.client.get(key)

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
            'timeout': 1000,
            'key': aerospike.POLICY_KEY_SEND,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'gen': aerospike.POLICY_GEN_EQ
        }
        (key, meta) = TestIncrement.client.exists(key)
        gen = meta['gen']

        meta = {
            'gen': gen + 5,
            'ttl': 1200
        }
        try:
            TestIncrement.client.increment(key, "age", 5, meta, policy)

        except RecordGenerationError as exception:
            assert exception.code == 3
            assert exception.msg == "AEROSPIKE_ERR_RECORD_GENERATION"
            assert exception.bin == "age"

        (key , meta, bins) = TestIncrement.client.get(key)

        assert bins == {'age': 1, 'name': 'name1'}
        assert key == ('test', 'demo', None, bytearray(
            b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8')
                      )

    def test_increment_with_policy_key_gen_GT_lesser(self):
        """
        Invoke increment() with gen GT lesser
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key': aerospike.POLICY_KEY_SEND,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'gen': aerospike.POLICY_GEN_GT
        }
        (key, meta) = TestIncrement.client.exists(key)

        gen = meta['gen']
        meta = {
            'gen': gen,
            'ttl': 1200
        }
        try:
            TestIncrement.client.increment(key, "age", 5, meta, policy)

        except RecordGenerationError as exception:
            assert exception.code == 3
            assert exception.msg == "AEROSPIKE_ERR_RECORD_GENERATION"

        (key, meta, bins) = TestIncrement.client.get(key)

        assert bins == {'age': 1, 'name': 'name1'}
        assert key == ('test', 'demo', None, bytearray(
            b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8')
                      )

    def test_increment_with_policy_key_gen_GT_positive(self):
        """
        Invoke increment() with gen GT positive
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key': aerospike.POLICY_KEY_SEND,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'gen': aerospike.POLICY_GEN_GT
        }
        (key, meta) = TestIncrement.client.exists(key)

        gen = meta['gen']
        meta = {'gen': gen + 5, 'ttl': 1200}
        TestIncrement.client.increment(key, "age", 5, meta, policy)

        (key, meta, bins) = TestIncrement.client.get(key)

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
            'timeout': 0.5
        }
        try:
            TestIncrement.client.increment(key, "age", 5, {}, policy)

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "timeout is invalid"

    def test_increment_with_nonexistent_key(self):
        """
        Invoke increment() with non-existent key
        """
        key = ('test', 'demo', 'non-existentkey')
        status = TestIncrement.client.increment(key, "age", 5)

        (key, meta, bins) = TestIncrement.client.get(key)

        assert bins == {'age': 5}

        TestIncrement.client.remove(key)

    def test_increment_with_nonexistent_bin(self):
        """
        Invoke increment() with non-existent bin
        """
        key = ('test', 'demo', 1)
        status = TestIncrement.client.increment(key, "age1", 5)

        (key, meta, bins) = TestIncrement.client.get(key)

        assert bins == {'age1': 5, 'name': u'name1', 'age': 1}

    def test_increment_value_is_string(self):
        """
        Invoke increment() value is string
        """
        key = ('test', 'demo', 1)
        try:
            TestIncrement.client.increment(key, "age", "str")

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Unsupported operand type(s) for +: 'int' and 'str'"

    def test_increment_with_extra_parameter(self):
        """
        Invoke increment() with extra parameter.
        """
        key = ('test', 'demo', 1)
        policy = {'timeout': 1000}
        with pytest.raises(TypeError) as typeError:
            TestIncrement.client.increment(key, "age", 2, {}, policy, "")

        assert "increment() takes at most 5 arguments (6 given)" in typeError.value

    def test_increment_policy_is_string(self):
        """
        Invoke increment() with policy is string
        """
        key = ('test', 'demo', 1)
        try:
            TestIncrement.client.increment(key, "age", 2, {}, "")

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "policy must be a dict"

    def test_increment_key_is_none(self):
        """
        Invoke increment() with key is none
        """
        try:
            TestIncrement.client.increment(None, "age", 2)

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "key is invalid"

    def test_increment_bin_is_none(self):
        """
        Invoke increment() with bin is none
        """
        key = ('test', 'demo', 1)
        try:
            TestIncrement.client.increment(key, None, 2)

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Bin name should be of type string"

    def test_increment_with_unicode_bin(self):
        """
        Invoke increment() with bin is unicode string
        """
        key = ('test', 'demo', 1)
        TestIncrement.client.increment(key, u"age", 10)

        (key, meta, bins) = TestIncrement.client.get(key)

        assert bins == {'age': 11, 'name': 'name1'}

    def test_increment_with_correct_parameters_without_connection(self):
        """
        Invoke increment() with correct parameters without connection
        """
        key = ('test', 'demo', 1)
        config = {'hosts': [('127.0.0.1', 3000)]}
        client1 = aerospike.client(config)

        try:
            client1.increment(key, "age", 5)

        except ClusterError as exception:
            assert exception.code == 11L
            assert exception.msg == 'No connection to aerospike cluster'

    def test_increment_with_integer_greaterthan_maxsize(self):
        """
        Invoke increment() with integer greater then(2^63 - 1)
        """
        key = ('test', 'demo', 1)
        bins = {"age": 10}
        TestIncrement.client.put(key, bins)
        try:
            TestIncrement.client.increment(key, 'age', 68786586756785785745)
        except Exception as exception:
            assert exception.code == -2
            assert exception.msg == 'integer value exceeds sys.maxsize'
    def test_increment_with_string_value(self):
        """
        Invoke increment() with string value
        """
        key = ('test', 'demo', 1)
        TestIncrement.client.increment(key, "age", "5")

        (key, meta, bins) = TestIncrement.client.get(key)

        assert bins == {'age': 6, 'name': 'name1'}

