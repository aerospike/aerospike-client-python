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


@pytest.mark.usefixtures("as_connection")
class TestRemovebin(object):

    @pytest.mark.parametrize("key, record, bin_for_removal", [
        (('test', 'demo', 1), {"Name": "Steve", 'age': 60}, ["age"]),
        (('test', 'demo', 2), {'name': "jeff", 'age': 45}, [u"name"]),

    ])
    def test_pos_remove_bin_with_correct_parameters(
            self, key, record, bin_for_removal, put_data):
        """
        Invoke remove_bin() with correct parameters
        """
        put_data(self.as_connection, key, record)
        self.as_connection.remove_bin(key, bin_for_removal)

        (key, _, bins) = self.as_connection.get(key)

        del record[''.join(bin_for_removal)]
        assert bins == record

    def test_pos_remove_bin_with_correct_policy(self, put_data):
        """
        Invoke remove_bin() with correct policy
        """
        key = ('test', 'demo', 1)
        record = {"Name": "Herry", 'age': 60}
        policy = {'timeout': 1000}
        put_data(self.as_connection, key, record)
        self.as_connection.remove_bin(key, ["age"], {}, policy)

        (key, _, bins) = self.as_connection.get(key)
        del record["age"]
        assert bins == record

    def test_pos_remove_bin_with_policy_send_gen_ignore(self, put_data):
        """
        Invoke remove_bin() with policy send
        """
        key = ('test', 'demo', "policy_send_gen_ingnore")
        record = {"Name": "Herry", 'age': 60, 'address': '202, washingtoon'}
        put_data(self.as_connection, key, record)

        policy = {
            'timeout': 1000,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND,
            'gen': aerospike.POLICY_GEN_IGNORE
        }
        meta = {'gen': 2, 'ttl': 1000}
        self.as_connection.remove_bin(key, ["age"], meta, policy)

        (key, meta, bins) = self.as_connection.get(key)
        del record["age"]
        assert bins == record
        assert key == ('test', 'demo', None,
                       bytearray(b"\xbd\x87-\x84\xae99|\x06z\x12\xf3\xef\x12\xb9\x1a\xa2\x1a;\'"))

    def test_pos_remove_bin_with_policy_send_gen_eq_positive(self, put_data):
        """
        Invoke remove_bin() with policy gen eq less
        """
        key = ('test', 'demo', 1)
        record = {
            "Company": "Apple",
            'years': 30,
            'address': '202, sillicon Vally'}
        put_data(self.as_connection, key, record)
        policy = {
            'timeout': 1000,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND,
            'gen': aerospike.POLICY_GEN_EQ
        }

        (key, meta) = self.as_connection.exists(key)
        gen = meta['gen']
        meta = {'gen': gen, 'ttl': 1000}

        self.as_connection.remove_bin(key, ["years"], meta, policy)

        (key, meta, bins) = self.as_connection.get(key)

        del record["years"]
        assert bins == record
        assert key == ('test', 'demo', None, bytearray(
            b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8'))

    def test_pos_remove_bin_with_policy_key_digest(self, put_data):
        """
        Invoke remove_bin() with policy key digest
        """
        key = ('test', 'demo', None, bytearray("asd;as[d'as;djk;uyfl",
                                               "utf-8"))
        record = {'age': 1, 'name': 'name1'}

        put_data(self.as_connection, key, record)
        policy = {'timeout': 1000, 'key': aerospike.POLICY_KEY_DIGEST}
        self.as_connection.remove_bin(key, ["age"], {}, policy)

        (key, _, bins) = self.as_connection.get(key)

        del record['age']
        assert bins == record
        assert key == ('test', 'demo', None,
                       bytearray(b"asd;as[d\'as;djk;uyfl"))

    def test_pos_remove_bin_with_single_bin_in_a_record(self):
        """
        Invoke remove_bin() with policy key digest
        """
        key = ('test', 'demo', "single-bin")
        try:
            self.as_connection.remove(key)
        except:
            pass

        rec = {'name': 'single'}
        self.as_connection.put(key, rec)

        policy = {'timeout': 1000}
        self.as_connection.remove_bin(key, ["name"], {}, policy)

        try:
            _, _, bins = self.as_connection.get(key)
            assert bins is None
        except e.RecordNotFound as exception:
            assert exception.code == 2

    def test_pos_remove_bin_no_bin(self, put_data):
        """
        Invoke remove_bin() no bin
        """
        key = ('test', 'demo', 1)
        record = {'name': "jeff", 'age': 45}
        put_data(self.as_connection, key, record)
        try:
            self.as_connection.remove_bin(key, [])
            (key, _, bins) = self.as_connection.get(key)
            assert bins == record
        except e.InvalidRequest:
            pass

    @pytest.mark.parametrize("key, record, bins_for_removal", [
        (('test', 'demo', 1), {'name': "Devid", 'age': 30}, ["name", "age"]),
        (('test', 'demo', 3), {'name': "jeff", 'age': 45}, [u"name", "age"]),
        (('test', 'demo', 4), {'name': "jeff", 'age': 45}, ["name", u"age"]),
    ])
    def test_pos_remove_bin_with_unicode_all(
            self, key, record, bins_for_removal, put_data):
        """
        Invoke remove_bin() with unicode bin name
        """
        put_data(self.as_connection, key, record)
        self.as_connection.remove_bin(key, bins_for_removal)

        try:
            (key, _, _) = self.as_connection.get(key)

        except e.RecordNotFound as exception:
            assert exception.code == 2

    @pytest.mark.parametrize("key, record, policy, bin_for_removal", [
        (('test', 'demo', "p_commit_level_all"),
            {"Name": "John", 'age': 30, 'address': '202, washingtoon'},
            {'timeout': 1000,
             'retry': aerospike.POLICY_RETRY_ONCE,
             'key': aerospike.POLICY_KEY_SEND,
             'commit': aerospike.POLICY_COMMIT_LEVEL_ALL
             },
            'age'),
        (('test', 'demo', "p_commit_level_master"),
            {"Name": "John", 'age': 30, 'address': '202, washingtoon'},
            {'timeout': 1000,
             'retry': aerospike.POLICY_RETRY_ONCE,
             'key': aerospike.POLICY_KEY_SEND,
             'commit': aerospike.POLICY_COMMIT_LEVEL_MASTER
             },
            'age'),
        (('test', 'demo', "p_gen_GT"),
            {"Name": "John", 'age': 30, 'address': '202, washingtoon'},
            {'timeout': 1000,
             'retry': aerospike.POLICY_RETRY_ONCE,
             'key': aerospike.POLICY_KEY_SEND,
             'gen': aerospike.POLICY_GEN_GT
             },
            'age'),
    ])
    def test_pos_remove_bin_with_policy(
            self, key, record, policy, bin_for_removal, put_data):
        """
        Invoke remove_bin() with policy
        """
        key_digest = self.as_connection.get_key_digest(key[0], key[1], key[2])

        put_data(self.as_connection, key, record)
        (key, meta) = self.as_connection.exists(key)
        gen = meta['gen']
        meta = {'gen': gen + 5, 'ttl': 1000}

        self.as_connection.remove_bin(key, [bin_for_removal], meta, policy)

        (key, meta, bins) = self.as_connection.get(key)

        del record[bin_for_removal]

        assert bins == record
        assert key == ('test', 'demo', None, key_digest)

    # Negative Tests

    @pytest.mark.parametrize("key, bin_for_removal, ex_code, ex_msg", [
        # key_is_none
        (None, ["age"], -2, "key is invalid"),
        (('test', 'demo', 1), None, -2, "Bins should be a list"),  # bin_is_none
    ])
    def test_neg_remove_bin_with_none(
            self, key, bin_for_removal, ex_code, ex_msg):
        """
        Invoke remove_bin() with none
        """
        try:
            self.as_connection.remove_bin(None, bin_for_removal)

        except e.ParamError as exception:
            assert exception.code == ex_code
            assert exception.msg == ex_msg

    def test_neg_remove_bin_with_correct_parameters_without_connection(self):
        """
        Invoke remove_bin() with correct parameters without connection
        """
        config = {'hosts': [('127.0.0.1', 3000)]}
        client1 = aerospike.client(config)

        key = ('test', 'demo', 1)

        try:
            client1.remove_bin(key, ["age"])

        except e.ClusterError as exception:
            assert exception.code == 11

    def test_neg_remove_bin_with_incorrect_meta(self):
        """
        Invoke remove_bin() with incorrect meta
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND,
            'gen': aerospike.POLICY_GEN_IGNORE
        }
        try:
            self.as_connection.remove_bin(key, ["age"], policy)

        except (e.ClusterError, e.RecordNotFound):
            pass

    def test_neg_remove_bin_with_incorrect_policy(self):
        """
        Invoke remove_bin() with incorrect policy
        """
        key = ('test', 'demo', 1)

        policy = {
            'time': 1001
        }
        try:
            self.as_connection.remove_bin(key, ["age"], {}, policy)

        except (e.ClientError, e.RecordNotFound):
            pass

    def test_neg_remove_bin_with_no_parameters(self):
        """
        Invoke remove_bin() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            self.as_connection.remove_bin()
        assert "argument 'key' (pos 1)" in str(
            typeError.value)

    def test_neg_remove_bin_with_policy_send_gen_eq_not_equal(self, put_data):
        """
        Invoke remove_bin() with policy gen eq not equal
        """
        key = ('test', 'demo', 1)
        record = {"Name": "John", 'age': 30, 'address': '202, washingtoon'}
        put_data(self.as_connection, key, record)

        policy = {
            'timeout': 1000,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND,
            'gen': aerospike.POLICY_GEN_EQ
        }
        (key, meta) = self.as_connection.exists(key)
        gen = meta['gen']
        meta = {'gen': gen + 5, 'ttl': 1000}

        try:
            self.as_connection.remove_bin(key, ["age"], meta, policy)

        except e.RecordGenerationError as exception:
            assert exception.code == 3

        (key, meta, bins) = self.as_connection.get(key)

        assert bins == record
        assert key == ('test', 'demo', None, bytearray(
            b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8')
        )

    def test_neg_remove_bin_with_policy_send_gen_GT_lesser(self, put_data):
        """
        Invoke remove_bin() with policy gen GT lesser
        """
        key = ('test', 'demo', "send_gen_GT_lesser")
        record = {"Name": "John", 'age': 30, 'address': '202, washingtoon'}
        put_data(self.as_connection, key, record)
        policy = {
            'timeout': 1000,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND,
            'gen': aerospike.POLICY_GEN_GT
        }

        (key, meta) = self.as_connection.exists(key)
        gen = meta['gen']
        meta = {'gen': gen, 'ttl': 1000}

        try:
            self.as_connection.remove_bin(key, ["age"], meta, policy)

        except e.RecordGenerationError as exception:
            assert exception.code == 3

        (key, meta, bins) = self.as_connection.get(key)

        assert bins == record
        assert key == ('test', 'demo', None, bytearray(
            b'\x1b\xb9\xda`\xa7\xbd\xe0\xc1\xdet1\xe4\x82\x94\xc7\xb3\xd8\xd5\x7f.')
        )

    def test_neg_remove_bin_with_incorrect_policy_value(self):
        """
        Invoke remove_bin() with incorrect policy value
        """
        key = ('test', 'demo', 1)

        policy = {
            'total_timeout': 0.5
        }
        try:
            self.as_connection.remove_bin(key, ["age"], {}, policy)

        except e.ClientError as exception:
            assert exception.code == -1

    @pytest.mark.parametrize("key, bin_for_removal, ex_code", [
        (('test', 'demo', 1), ["non-existent"],
         0),               # non-existent bin
        (('test', 'demo', "non-existent"),
         ["age"], 0),        # non-existent key
    ])
    def test_neg_remove_bin_with_nonexistent_data(
            self, key, bin_for_removal, ex_code):
        """
        Invoke remove_bin() with non-existent data
        """
        try:
            self.as_connection.remove_bin(key, bin_for_removal)

        except e.RecordNotFound:
            pass

    def test_neg_remove_bin_with_extra_parameter(self):
        """
        Invoke remove_bin() with extra parameter.
        """
        key = ('test', 'demo', 1)
        policy = {'timeout': 1000}
        with pytest.raises(TypeError) as typeError:
            self.as_connection.remove_bin(key, ["age"], {}, policy, "")

        assert "remove_bin() takes at most 4 arguments (5 given)" in str(typeError.value)

    def test_remove_bin_with_non_string_bin_nane(self):
        """
        Invoke remove_bin() with extra parameter.
        """
        key = ('test', 'demo', 1)
        with pytest.raises(e.ClientError) as typeError:
            self.as_connection.remove_bin(key, [1.5])

    @pytest.mark.skip(reason="system error")
    def test_remove_bin_with_ttl_too_large(self, put_data):
        key = ('test', 'demo', 1)
        record = {"Name": "Herry", 'age': 60}
        put_data(self.as_connection, key, record)
        meta = {'gen': 2, 'ttl': 2 ** 65}
        with pytest.raises(e.ClientError) as typeError:
            self.as_connection.remove_bin(key, ["age"], meta=meta)

    @pytest.mark.skip(reason="system error")
    def test_remove_bin_with_gen_too_large(self, put_data):
        key = ('test', 'demo', 1)
        record = {"Name": "Herry", 'age': 60}
        put_data(self.as_connection, key, record)
        meta = {'gen': 2 ** 65, 'ttl': 2}
        with pytest.raises(e.ClientError) as typeError:
            self.as_connection.remove_bin(key, ["age"], meta=meta)
