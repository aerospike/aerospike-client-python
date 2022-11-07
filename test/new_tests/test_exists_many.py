# -*- coding: utf-8 -*-

import pytest
import time
import sys
from .test_base_class import TestBaseClass

try:
    from collections import Counter
except ImportError:
    from counter26 import Counter

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
    from aerospike import exception as e
except:
    print("Please install aerospike python client.")
    sys.exit(1)


@pytest.mark.usefixtures("as_connection")
class TestExistsMany():

    def test_pos_exists_many_without_policy(self, put_data):
        self.keys = []
        rec_length = 5
        for i in range(rec_length):
            key = ('test', 'demo', i)
            record = {'name': 'name%s' % (str(i)), 'age': i}
            put_data(self.as_connection, key, record)
            self.keys.append(key)
        records = self.as_connection.exists_many(self.keys)
        assert isinstance(records, list)
        assert len(records) == rec_length

    def test_pos_exists_many_with_proper_parameters_without_connection(
            self, put_data):
        self.keys = []
        rec_length = 5
        for i in range(rec_length):
            key = ('test', 'demo', i)
            record = {'name': 'name%s' % (str(i)), 'age': i}
            put_data(self.as_connection, key, record)
            self.keys.append(key)
        records = self.as_connection.exists_many(self.keys, {'total_timeout': 1200})

        assert isinstance(records, list)
        assert len(records) == rec_length
        assert Counter([x[0][2] for x in records]) == Counter([0, 1, 2, 3,
                                                               4])

    def test_pos_exists_many_with_none_policy(self, put_data):
        self.keys = []
        rec_length = 5
        for i in range(rec_length):
            key = ('test', 'demo', i)
            record = {'name': 'name%s' % (str(i)), 'age': i}
            put_data(self.as_connection, key, record)
            self.keys.append(key)

        records = self.as_connection.exists_many(self.keys, None)

        assert isinstance(records, list)
        assert len(records) == rec_length
        assert Counter([x[0][2] for x in records]) == Counter([0, 1, 2, 3,
                                                               4])

    def test_pos_exists_many_with_non_existent_keys(self, put_data):
        self.keys = []
        rec_length = 5
        for i in range(rec_length):
            key = ('test', 'demo', i)
            record = {'name': 'name%s' % (str(i)), 'age': i}
            put_data(self.as_connection, key, record)
            self.keys.append(key)

        self.keys.append(('test', 'demo', 'some_key'))

        records = self.as_connection.exists_many(self.keys)

        assert isinstance(records, list)
        assert len(records) == rec_length + 1
        assert Counter([x[0][2] for x in records]) == Counter([0, 1, 2, 3,
                                                               4, 'some_key'])
        for x in records:
            if x[0][2] == 'some_key':
                assert x[1] is None

    def test_pos_exists_many_with_all_non_existent_keys(self):

        keys = [('test', 'demo', 'NonExistingKey')]

        records = self.as_connection.exists_many(keys)

        assert len(records) == 1
        for x in records:
            if x[0][2] == 'key':
                assert x[1] is None

    def test_pos_exists_many_with_initkey_as_digest(self, put_data):

        keys = []
        key = (
            "test",
            "demo",
            None,
            bytearray(
                "asd;as[d'as;djk;uyfl",
                "utf-8"))
        rec = {'name': 'name1', 'age': 1}
        put_data(self.as_connection, key, rec)
        keys.append(key)

        key = (
            "test",
            "demo",
            None,
            bytearray(
                "ase;as[d'as;djk;uyfl",
                "utf-8"))
        rec = {'name': 'name2', 'age': 2}
        put_data(self.as_connection, key, rec)
        keys.append(key)

        records = self.as_connection.exists_many(keys)

        assert isinstance(records, list)
        assert len(records) == 2
        i = 0
        for x in records:
            if i:
                assert x[0][3] == bytearray(b"ase;as[d'as;djk;uyfl")
            else:
                assert x[0][3] == bytearray(b"asd;as[d'as;djk;uyfl")
            i += 1

    def test_pos_exists_many_with_non_existent_keys_in_middle(self, put_data):
        self.keys = []
        rec_length = 5
        for i in range(rec_length):
            key = ('test', 'demo', i)
            record = {'name': 'name%s' % (str(i)), 'age': i}
            put_data(self.as_connection, key, record)
            self.keys.append(key)

        self.keys.append(('test', 'demo', 'some_key'))

        for i in range(15, 20):
            key = ('test', 'demo', i)
            rec = {'name': 'name%s' % (str(i)), 'age': i}
            put_data(self.as_connection, key, rec)
            self.keys.append(key)

        records = self.as_connection.exists_many(self.keys)

        assert isinstance(records, list)
        assert len(records) == 11
        assert Counter([x[0][2] for x in records]) == Counter([0, 1, 2, 3,
                                                               4, 'some_key', 15, 16, 17, 18, 19])
        for x in records:
            if x[0][2] == 'some_key':
                assert x[1] is None

    def test_pos_exists_many_with_record_expiry(self, put_data):
        keys = []
        key = ('test', 'demo', 20)
        rec = {"name": "John"}
        meta = {'gen': 3, 'ttl': 1}
        policy = {'total_timeout': 1000}
        put_data(self.as_connection, key, rec, meta, policy)
        keys.append(key)
        time.sleep(2)
        records = self.as_connection.exists_many(keys)
        assert isinstance(records, list)
        assert len(records) == 1
        for record in records:
            assert record[1] is None

    def test_exists_many_with_bytearray_key(self, put_data):
        self.keys = [('test', 'demo', bytearray([1, 2, 3]))]
        for key in self.keys:
            put_data(self.as_connection, key, {'byte': 'array'})

        records = self.as_connection.exists_many(self.keys)

        bytearray_key = records[0][0]
        assert len(bytearray_key) == 4

        bytearray_pk = bytearray_key[2]
        assert bytearray_pk == bytearray([1, 2, 3])

    # Negative Tests

    def test_neg_exists_many_with_none_keys(self):

        with pytest.raises(e.ParamError):
            self.as_connection.exists_many(None, {})

    def test_neg_exists_many_with_an_invalid_key_in_list(self):

        with pytest.raises(e.ParamError):
            self.as_connection.exists_many([('test', 'demo', 1), ('test', 'demo', 2), 5])

    def test_neg_exists_many_with_invalid_key(self):

        with pytest.raises(e.ParamError):
            self.as_connection.exists_many("key")

    def test_neg_exists_many_with_invalid_timeout(self, put_data):
        self.keys = []
        rec_length = 5
        for i in range(rec_length):
            key = ('test', 'demo', i)
            record = {'name': 'name%s' % (str(i)), 'age': i}
            put_data(self.as_connection, key, record)
            self.keys.append(key)
        policies = {'total_timeout': 0.2}
        with pytest.raises(e.ParamError):
            self.as_connection.exists_many(self.keys, policies)

    def test_neg_exists_many_with_proper_parameters_without_connection(
            self, put_data):
        self.keys = []
        rec_length = 5
        for i in range(rec_length):
            key = ('test', 'demo', i)
            record = {'name': 'name%s' % (str(i)), 'age': i}
            put_data(self.as_connection, key, record)
            self.keys.append(key)

        config = {'hosts': [('127.0.0.1', 3000)]}
        client1 = aerospike.client(config)

        try:
            client1.exists_many(self.keys, {'total_timeout': 20})

        except e.ClusterError as exception:
            assert exception.code == 11

    def test_neg_exists_many_with_extra_parameter_in_key(self, put_data):
        keys = []
        key = (
            "test",
            "demo",
            None,
            bytearray(
                "asd;as[d'as;djk;uyfl",
                "utf-8"))
        rec = {'name': 'name1', 'age': 1}
        put_data(self.as_connection, key, rec)
        keys.append(key)

        key = (
            "test",
            "demo",
            None,
            bytearray(
                "ase;as[d'as;djk;uyfl",
                "utf-8"))
        rec = {'name': 'name2', 'age': 2}
        put_data(self.as_connection, key, rec)
        keys.append(key)

        keys_get = []
        key = (
            "test",
            "demo",
            None,
            bytearray(
                "asd;as[d'as;djk;uyfl",
                "utf-8"),
            None)
        keys_get.append(key)

        key = (
            "test",
            "demo",
            None,
            bytearray(
                "ase;as[d'as;djk;uyfl",
                "utf-8"),
            None)
        keys_get.append(key)

        try:
            self.as_connection.exists_many(keys_get)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "key tuple must be (Namespace, Set, Key) or (Namespace, Set, None, Digest)"

        for key in keys:
            self.as_connection.remove(key)

    def test_neg_exists_many_with_low_timeout(self, put_data):
        keys = []
        key = ('test', 'demo', 20)
        rec = {"name": "John"}
        meta = {'gen': 3, 'ttl': 1}
        policy = {'timeout': 2}

        try:
            put_data(self.as_connection, key, rec, meta, policy)
        except e.TimeoutError as exception:
            assert exception.code == 9

        keys.append(key)
        records = self.as_connection.exists_many(keys)
        assert isinstance(records, list)
        assert len(records) == 1

    def test_neg_exists_many_with_invalid_ns(self):
        # ToDo: not sure about put operation
        keys = []
        key = ('test2', 'demo', 20)
        # self.as_connection.put(key, rec, meta, policy)
        keys.append(key)
        with pytest.raises(e.ClientError):
            self.as_connection.exists_many(keys)

    def test_neg_exists_many_without_any_parameter(self):

        with pytest.raises(TypeError) as typeError:
            self.as_connection.exists_many()

        assert "argument 'keys' (pos 1)" in str(
            typeError.value)
