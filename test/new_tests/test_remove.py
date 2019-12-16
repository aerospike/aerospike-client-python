# -*- coding: utf-8 -*-

import pytest
import sys
from .test_base_class import TestBaseClass
from .test_data import *

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
    from aerospike import exception as e
except:
    print("Please install aerospike python client.")
    sys.exit(1)


@pytest.mark.usefixtures("as_connection")
class TestRemove():

    @pytest.mark.xfail(reason="open bug #client-533")
    def test_pos_remove_with_existing_record(self):
        """
            Invoke remove() when records are present
        """
        key = ('test', 'demo', 1)
        retobj = self.as_connection.remove(key)

        assert retobj == 0

        with pytest.raises(e.RecordNotFound) as exception:
            (key, _, _) = self.as_connection.get(key)

        (code, msg, _, _) = exception.value
        assert msg == "AEROSPIKE_ERR_RECORD_NOT_FOUND"
        assert code == 2

    @pytest.mark.xfail(reason="open bug #client-533")
    def test_pos_remove_with_policy(self):
        """
            Invoke remove() with policy
        """
        key = ('test', 'demo', 1)
        meta = {'gen': 0}
        policy = {'timeout': 1000}

        retobj = self.as_connection.remove(key, meta, policy)

        assert retobj == 0

        with pytest.raises(e.RecordNotFound) as exception:
            (key, meta, _) = self.as_connection.get(key)

        (code, msg, _, _) = exception.value
        assert msg == "AEROSPIKE_ERR_RECORD_NOT_FOUND"
        assert code == 2

        key = ('test', 'demo', 1)
        rec = {
            'name': 'name%s' % (str(1)),
            'addr': 'name%s' % (str(1)),
            'age': 1,
            'no': 1
        }
        self.as_connection.put(key, rec)

    @pytest.mark.xfail(reason="open bug #client-533")
    def test_pos_remove_with_policy_all(self):
        """
            Invoke remove() with policy
        """
        key = ('test', 'demo', 1)
        meta = {'gen': 0}
        policy = {
            'timeout': 1000,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND,
            'gen': aerospike.POLICY_GEN_IGNORE
        }
        retobj = self.as_connection.remove(key, meta, policy)

        assert retobj == 0

        with pytest.raises(e.RecordNotFound) as exception:
            (key, meta, _) = self.as_connection.get(key)

        (code, msg, _, _) = exception.value
        assert msg == "AEROSPIKE_ERR_RECORD_NOT_FOUND"
        assert code == 2

        key = ('test', 'demo', 1)
        rec = {
            'name': 'name%s' % (str(1)),
            'addr': 'name%s' % (str(1)),
            'age': 1,
            'no': 1
        }
        self.as_connection.put(key, rec)

    @pytest.mark.xfail(reason="open bug #client-533")
    def test_pos_remove_with_policy_key_digest(self):
        """
            Invoke remove() with policy_key_digest
        """

        key = ('test', 'demo', None, bytearray("asd;as[d'as;djk;uyfl",
                                               "utf-8"))
        meta = {'gen': 0}
        policy = {
            'timeout': 1000,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_DIGEST
        }
        retobj = self.as_connection.put(key, policy)

        assert retobj == 0

        retobj = self.as_connection.remove(key, meta, policy)

        assert retobj == 0

        with pytest.raises(e.RecordNotFound) as exception:
            (key, meta, _) = self.as_connection.get(key)

        (code, msg, _, _) = exception.value
        assert msg == "AEROSPIKE_ERR_RECORD_NOT_FOUND"
        assert code == 2

    @pytest.mark.xfail(reason="open bug #client-533")
    def test_pos_remove_with_policy_gen_ignore(self):
        """
            Invoke remove() with policy gen ignore
        """
        key = ('test', 'demo', 1)
        meta = {'gen': 0}
        policy = {
            'timeout': 1000,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND,
            'gen': aerospike.POLICY_GEN_IGNORE
        }

        retobj = self.as_connection.remove(key, meta, policy)

        assert retobj == 0

        with pytest.raises(e.RecordNotFound) as exception:
            (key, meta, _) = self.as_connection.get(key)

        (code, msg, _, _) = exception.value
        assert msg == "AEROSPIKE_ERR_RECORD_NOT_FOUND"
        assert code == 2

        key = ('test', 'demo', 1)
        rec = {
            'name': 'name%s' % (str(1)),
            'addr': 'name%s' % (str(1)),
            'age': 1,
            'no': 1
        }
        self.as_connection.put(key, rec)

    @pytest.mark.xfail(reason="Issue1 : open bug #client-533")
    def test_pos_remove_with_policy_gen_eq_positive(self):
        """
            Invoke remove() with policy gen positive
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND,
            'gen': aerospike.POLICY_GEN_EQ
        }

        (key, meta) = self.as_connection.exists(key)
        gen = meta['gen']
        meta = {'gen': gen}
        retobj = self.as_connection.remove(key, meta, policy)

        assert retobj == 0

        with pytest.raises(e.RecordNotFound) as exception:
            (key, meta, _) = self.as_connection.get(key)
        (code, msg, _, _) = exception.value
        assert msg == "AEROSPIKE_ERR_RECORD_NOT_FOUND"
        assert code == 2

        key = ('test', 'demo', 1)
        rec = {
            'name': 'name%s' % (str(1)),
            'addr': 'name%s' % (str(1)),
            'age': 1,
            'no': 1
        }
        self.as_connection.put(key, rec)

    @pytest.mark.xfail(reason="open bug #client-533")
    def test_pos_remove_with_policy_gen_GT_positive(self):
        """
            Invoke remove() with policy gen GT positive
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND,
            'gen': aerospike.POLICY_GEN_GT
        }

        (key, meta) = self.as_connection.exists(key)
        gen = meta['gen'] + 5
        metadata = {'gen': gen}

        retobj = self.as_connection.remove(key, metadata, policy)

        assert retobj == 0

        with pytest.raises(e.RecordNotFound) as exception:
            (key, meta, _) = self.as_connection.get(key)
        (code, msg, _, _) = exception.value
        assert msg == "AEROSPIKE_ERR_RECORD_NOT_FOUND"
        assert code == 2

        key = ('test', 'demo', 1)
        rec = {
            'name': 'name%s' % (str(1)),
            'addr': 'name%s' % (str(1)),
            'age': 1,
            'no': 1
        }
        self.as_connection.put(key, rec)

    # Negative Tests
    def test_neg_remove_with_policy_gen_eq_not_equal(self, put_data):
        """
            Invoke remove() with policy gen not equal
        """
        key = ('test', 'demo', 1)
        record = {"name": "Michal", 'age': 34}
        put_data(self.as_connection, key, record)

        (key, meta) = self.as_connection.exists(key)
        gen = meta['gen'] + 5
        policy = {
            'timeout': 1000,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND,
            'gen': aerospike.POLICY_GEN_EQ
        }

        (key, meta) = self.as_connection.exists(key)
        gen = meta['gen'] + 5  # Increment Generation by 5

        with pytest.raises(e.RecordGenerationError) as exception:
            self.as_connection.remove(key, {"gen": gen}, policy)
            (code, msg, _, _) = exception.value
            assert msg == "AEROSPIKE_ERR_RECORD_GENERATION"
            assert code == 3

    def test_neg_remove_with_policy_gen_GT_lesser(self, put_data):
        """
            Invoke remove() with policy gen GT lesser
        """
        key = ('test', 'demo', "gen_GT_lesser")
        record = {"Company": "Microsoft", 'Place': "US"}
        put_data(self.as_connection, key, record)
        policy = {
            'timeout': 1000,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND,
            'gen': aerospike.POLICY_GEN_GT
        }

        (key, meta) = self.as_connection.exists(key)
        lesserGen = meta['gen'] - 1

        with pytest.raises(e.RecordGenerationError) as exception:
            self.as_connection.remove(key, ("gen", lesserGen), policy)
            (code, msg, _, _) = exception.value
            assert msg == "AEROSPIKE_ERR_RECORD_GENERATION"
            assert code == 3

        (key, meta, bins) = self.as_connection.get(key)

        assert key == ('test', 'demo', None, bytearray(
            b'\xdd)Fs9\xa151\x91{\x1c\n\xc0\xac zV\x10Z+'))
        assert meta is not None
        assert bins == record

    def test_neg_remove_with_policy_as_string(self):
        """
            Invoke remove() with policy as string
        """
        key = ('test', 'demo', 1)
        meta = {
            'gen': 0
        }
        with pytest.raises(e.ParamError) as exception:
            self.as_connection.remove(key, meta, "String_policy")
            (code, msg, _, _) = exception.value
            assert msg == "policy must be a dict"
            assert code == -2

    def test_neg_remove_with_extra_parameter(self):
        """
            Invoke remove() with extra parameter
        """
        key = ('test', 'demo', 1)
        meta = {'gen': 0}
        policy = {'timeout': 1000}
        with pytest.raises(TypeError) as typeError:
            self.as_connection.remove(key, meta, policy, "Extra Param")
        assert "remove() takes at most 3 arguments (4 given)" in str(typeError.value)

    @pytest.mark.parametrize("key, ex_code, ex_msg", key_neg)
    def test_neg_remove_with_incorrect_data(self, key, ex_code, ex_msg):
        """
            Invoke remove() with namespace as None
        """

        with pytest.raises(e.ParamError) as exception:
            self.as_connection.remove(key)
            (code, msg, _, _) = exception.value
            assert msg == ex_msg
            assert code == ex_code

    @pytest.mark.parametrize("key, ex_name, ex_code", [
        (('test', None, 1), e.RecordNotFound, 2),
        (('test', 'demo', "incorrect_key"), e.RecordNotFound, 2),
        (('test', 'demo', "missing_record"), e.RecordNotFound, 2)

    ])
    def test_neg_remove_with_missing_record(self, key, ex_name, ex_code):
        """
            Invoke remove() with set as None
        """
        with pytest.raises(ex_name) as exception:
            self.as_connection.remove(key)
            (code, _, _, _) = exception.value
            assert code == ex_code

    def test_neg_remove_with_correct_parameters_without_connection(self):
        """
            Invoke remove() with correct arguments without connection
        """
        config = {'hosts': [('127.0.0.1', 3000)]}
        client1 = aerospike.client(config)
        key = ('test', 'demo', 1)

        with pytest.raises(e.ClusterError) as exception:
            client1.remove(key)
            (code, _, _, _) = exception.value
            assert code == 11

    def test_neg_remove_with_no_parameters(self):
        """
            Invoke remove() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            self.as_connection.remove()

        assert "argument 'key' (pos 1)" in str(
            typeError.value)
