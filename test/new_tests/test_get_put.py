# -*- coding: utf-8 -*-

import pytest
import sys
try:
    import cPickle as pickle
except:
    import pickle

# from collections import OrderedDict
from .test_base_class import TestBaseClass
aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
    from aerospike import exception as e
except:
    print("Please install aerospike python client.")
    sys.exit(1)


class SomeClass(object):
    pass

@pytest.mark.usefixtures("as_connection")
class TestGetPut():
    @pytest.mark.parametrize("_input, _expected", [
        (('test', 'demo', 1),   {'age': 1, 'name': 'name1'}),
        (('test', 'demo', 2),   {'age': 2, 'name': 'Mr John', 'bmi': 3.55}),
        (('test', 'demo', 'boolean_key'), {'is_present': True}),
        (('test', 'demo', 'string'),   {'place': "New York", 'name': 'John'}),
        (('test', 'demo', u"bb"), {'a': [u'aa', 2, u'aa', 4, u'cc', 3, 2, 1]}),
        (('test', u'demo', 1), {'age': 1, 'name': 'name1'}),
        (('test', 'demo', 1), {"is_present": None}),
        (('test', 'unknown_set', 1), {'a': {'k': [bytearray("askluy3oijs", "utf-8")]}}),

        # Bytearray
        (("test", "demo", bytearray("asd;as[d'as;d", "utf-8")), {"name": "John"}),
        (('test', 'demo', 'bytes_key'), {'bytes': bytearray('John', 'utf-8')}),

        # List Data
        (('test', 'demo', 'list_key'), {'names': ['John', 'Marlen', 'Steve']}),
        (('test', 'demo', 'list_key'), {'names': [1, 2, 3, 4, 5]}),
        (('test', 'demo', 'list_key'), {'names': [1.5, 2.565, 3.676, 4, 5.89]}),
        (('test', 'demo', 'list_key'), {'names': ['John', 'Marlen', 1024]}),
        (('test', 'demo', 'list_key_unicode'), {'a': [u'aa', u'bb', 1, u'bb', u'aa']}),
        (('test', 'demo', 'objects'), {'objects': [pickle.dumps(SomeClass()), pickle.dumps(SomeClass())]}),

        # Map Data
        (('test', 'demo', 'map_key'), {'names': {'name': 'John', 'age': 24}}),
        (('test', 'demo', 'map_key_float'), {"double_map": {"1": 3.141,"2": 4.123,"3": 6.285}}),
        (('test', 'demo', 'map_key_unicode'), {'a': {u'aa': u'11'}, 'b': {u'bb': u'22'}}),
        #        (('test', 'demo', 1),
        #            {'odict': OrderedDict(sorted({'banana': 3, 'apple': 4, 'pear': 1, 'orange': 2}.items(),
        #                key=lambda t: t[0]))}),

        # Tuple Data
        (('test', 'demo', 'tuple_key'), {'tuple_seq': tuple('abc')}),

        # Set Data
        (('test', 'demo', 'set_key'), {"set_data": set([1, 2])}),
        (('test', 'demo', 'fset_key'),{"fset_data": frozenset(["Frankfurt", "Basel", "Freiburg"])}),

        #Hybrid
        (('test', 'demo', 'multiple_bins'), {
            'i': ["nanslkdl", 1, bytearray("asd;as[d'as;d", "utf-8")],
            's': {"key": "asd';q;'1';"},
            'b': 1234,
            'l': '!@#@#$QSDAsd;as'
        }),
        (('test', 'demo', 'list_map_key'), {
            'names': ['John', 'Marlen', 'Steve'],
            'names_and_age': [{'name': 'John',
                               'age': 24}, {'name': 'Marlen',
                                            'age': 25}]
        }),
        (('test', 'demo', 'map_tuple_key'), {
            'seq': {'bb': tuple('abc')}
        })
        ])
    def test_pos_get_put_with_key(self, _input, _expected, put_data):
        """
            Invoke get() with a key and not policy's dict.
        """
        put_data(self.as_connection, _input, _expected)
        _, _, bins = self.as_connection.get(_input)
        assert bins == _expected

    @pytest.mark.parametrize("_input, _expected", [
        (('test', 'demo', '1'), None),
        (('test', None, 2), None),                  # None is valid entry for set
        (('test', 'some_random_set', 1), None),
        (('test', 'demo', 'non-existent'), None),
        ])
    def test_pos_get_with_data_missing(self, _input, _expected):
        """
            Invoke get() with different combinations of None in key
        """
        _, meta, bins = self.as_connection.get(_input)
        assert bins == _expected
        assert meta == _expected

    def test_pos_get_initkey_with_digest(self, put_data):
        """
            Invoke get() for a record having bytestring data.
        """
        key = ('test', 'demo', None, bytearray("asd;as[d'as;djk;uyfl",
                                               "utf-8"))
        rec = {'name': 'john'}

        policy = {'key': aerospike.POLICY_KEY_DIGEST}

        put_data(self.as_connection, key, rec, policy)

        key, _, bins = self.as_connection.get(key, policy)

        assert bins == {'name': 'john'}
        assert key == ('test', 'demo', None,
                       bytearray(b"asd;as[d\'as;djk;uyfl"))

    @pytest.mark.parametrize("_input, _record, _policy, _expected", [
        (('test', 'demo', 3),
            {'name': 'name%s' % (str(3)), 'age': 3},
            aerospike.POLICY_KEY_DIGEST,
            None),
        (('test', 'demo', 3),
            {'name': 'name%s' % (str(3)), 'age': 3},
            aerospike.POLICY_KEY_SEND,
            3),
        ])
    def test_pos_get_with_policy_key_digest(self, _input, _record, _policy,_expected, put_data):
        """
            Invoke get() for a record with POLICY_KEY_DIGEST
        """
        put_data(self.as_connection, _input, _record)

        (key, _, _) = self.as_connection.get(
            _input,
            policy={'key': _policy})
        assert key[2] == _expected

    def test_pos_get_initkey_with_policy_send(self, put_data):
        """
            Invoke get() for a record having string data.
        """

        key = ('test', 'demo', 1)

        rec = {'name': 'john', 'age': 1}

        policy = {
            'key': aerospike.POLICY_KEY_SEND,
            'replica': aerospike.POLICY_REPLICA_ANY,
            'consistency': aerospike.POLICY_CONSISTENCY_ONE
        }

        put_data(self.as_connection, key, rec, policy)

        key, _, bins = self.as_connection.get(key, policy)

        assert bins == {'name': 'john', 'age': 1}
        assert key == ('test', 'demo', 1, bytearray(
            b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8')
                      )

    # Negative get tests
    def test_neg_get_with_no_parameter(self):
        """
            Invoke get() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            self.as_connection.get()

        assert "Required argument 'key' (pos 1) not found" in str(typeError.value)

    def test_neg_get_with_extra_parameter_in_key(self, put_data):
        """
            Invoke get() with extra parameter
        """
        key = ('test', 'demo', None, bytearray("asd;as[d'as;djk;uyfl",
                                               "utf-8"), 1)
        rec = {'name': 'john'}

        policy = {'key': aerospike.POLICY_KEY_DIGEST}
        try:
            put_data(self.as_connection, key, rec, policy)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == 'key tuple must be (Namespace, Set, Key) or (Namespace, Set, None, Digest)'

    def test_neg_get_with_key_digest(self):
        """
            Invoke get() with a key digest.
        """
        key = ('test', 'demo', 1)
        key, _ = self.as_connection.exists(key)
        try:
            key, _, _ = self.as_connection.get((key[0], key[1], None, key[2]))
        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == 'digest is invalid. expected a bytearray'

    @pytest.mark.parametrize("_input, _expected", [
        ((None, 'demo', 2),
            (-2, 'namespace must be a string')),
        (('test', 'demo', None),
            (-2, 'either key or digest is required')),
        (None,
            (-2, 'key is invalid')),
        (('test', 'demo'),
            (-2, 'key tuple must be (Namespace, Set, Key) or (Namespace, Set, None, Digest)')),
        (('test', 'demo', '', ''),
            (-2, 'key tuple must be (Namespace, Set, Key) or (Namespace, Set, None, Digest)')),
        ])
    def test_neg_get_with_none(self, _input, _expected):
        """
            Invoke get() with None namespace/key in key tuple.
        """
        try:
            self.as_connection.get(_input)

        except e.ParamError as exception:
            assert exception.code == _expected[0]
            assert exception.msg == _expected[1]

    def test_neg_get_with_non_existent_namespace(self):
        """
            Invoke get() for non-existent namespace.
        """
        key = ('namespace', 'demo', 1)

        try:
            key, _, _ = self.as_connection.get(key)

        except e.NamespaceNotFound as exception:
            assert exception.code == 20

    @pytest.mark.parametrize("_input, _expected", [
        (('test', 'demo', 1), {'name': 'john', 'age': 1}),
        ])
    def test_neg_get_remove_key_and_check_get(self, _input, _expected, put_data):
        """
            Invoke get() for a record having string data.
        """
        put_data(self.as_connection, _input, _expected)
        self.as_connection.remove(_input)
        _, _, bins = self.as_connection.get(_input)
        assert bins is None

    def test_neg_get_with_only_key_no_connection(self):
        """
            Invoke get() with a key and not policy's dict no connection
        """
        key = ('test', 'demo', 1)
        config = {'hosts': [('127.0.0.1', 3000)]}
        client1 = aerospike.client(config)

        try:
            key, _, _ = client1.get(key)

        except e.ClusterError as exception:
            assert exception.code == 11

    # Put Tests
    def test_pos_put_with_policy_exists_create_or_replace(self):
        """
            Invoke put() for a record with create or replace policy positive
            If record exists and will be replaced
        """
        key = ('test', 'demo', 1)

        rec = {"name": "Smith"}
        meta = {'gen': 2, 'ttl': 25000}
        policy = {
            'timeout': 1000,
            'exists': aerospike.POLICY_EXISTS_CREATE_OR_REPLACE,
            'gen': aerospike.POLICY_GEN_IGNORE,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND
        }
        assert 0 == self.as_connection.put(key, rec, meta, policy)

        (key, meta, bins) = self.as_connection.get(key)
        assert rec == bins

        rec = {"name": "John"}
        meta = {'gen': 2, 'ttl': 25000}
        policy = {
            'timeout': 1000,
            'exists': aerospike.POLICY_EXISTS_CREATE_OR_REPLACE,
            'gen': aerospike.POLICY_GEN_IGNORE,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND
        }
        assert 0 == self.as_connection.put(key, rec, meta, policy)

        (key, meta, bins) = self.as_connection.get(key)
        assert rec == bins
        self.as_connection.remove(key)

    def test_pos_put_with_policy_exists_ignore_create(self):
        """
            Invoke put() for a record with ignore.
        """
        key = ('test', 'demo', 1)

        rec = {"name": "Smith"}
        meta = {'gen': 2, 'ttl': 25000}
        policy = {
            'timeout': 1000,
            'exists': aerospike.POLICY_EXISTS_IGNORE,
            'gen': aerospike.POLICY_GEN_IGNORE,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND
        }
        assert 0 == self.as_connection.put(key, rec, meta, policy)

        (key, meta, bins) = self.as_connection.get(key)
        assert rec == bins
        self.as_connection.remove(key)

    def test_pos_put_with_policy_exists_ignore_update(self):
        """
            Invoke put() for a record with ignore.
            Ignore for an existing records should update without any error
        """
        key = ('test', 'demo', 1)

        rec = {"name": "Smith"}
        meta = {'gen': 2, 'ttl': 25000}
        policy = {
            'timeout': 1000,
            'exists': aerospike.POLICY_EXISTS_IGNORE,
            'gen': aerospike.POLICY_GEN_IGNORE,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND
        }
        assert 0 == self.as_connection.put(key, rec, meta, policy)

        (key, meta, bins) = self.as_connection.get(key)
        assert rec == bins

        key = ('test', 'demo', 1)

        rec = {"name": "John"}
        meta = {'gen': 2, 'ttl': 25000}
        policy = {
            'timeout': 1000,
            'exists': aerospike.POLICY_EXISTS_IGNORE,
            'gen': aerospike.POLICY_GEN_IGNORE,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND
        }
        assert 0 == self.as_connection.put(key, rec, meta, policy)

        (key, meta, bins) = self.as_connection.get(key)
        assert rec == bins
        self.as_connection.remove(key)

    def test_pos_put_with_policy_replace(self):
        """
            Invoke put() for a record with replace positive.
        """
        key = ('test', 'demo', 1)

        rec = {"name": "John"}
        meta = {'gen': 2, 'ttl': 25000}
        policy = {
            'timeout': 1000,
            'gen': aerospike.POLICY_GEN_IGNORE,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND,
        }
        assert 0 == self.as_connection.put(key, rec, meta, policy)

        (key, meta, bins) = self.as_connection.get(key)

        assert rec == bins

        rec = {"name": "Smith"}
        meta = {'gen': 2, 'ttl': 25000}
        policy = {'timeout': 1000, 'exists': aerospike.POLICY_EXISTS_REPLACE}
        assert 0 == self.as_connection.put(key, rec, meta, policy)

        (key, meta, bins) = self.as_connection.get(key)

        assert rec == bins
        self.as_connection.remove(key)

    def test_pos_put_with_policy_exists_update_positive(self):
        """
            Invoke put() for a record with all policies.
        """
        key = ('test', 'demo', 1)

        rec = {"name": "John"}
        meta = {'gen': 2, 'ttl': 25000}
        policy = {
            'timeout': 1000,
            'gen': aerospike.POLICY_GEN_IGNORE,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND,
        }
        assert 0 == self.as_connection.put(key, rec, meta, policy)

        (key, meta, bins) = self.as_connection.get(key)

        assert rec == bins

        rec = {"name": "Smith"}
        meta = {'gen': 2, 'ttl': 25000}
        policy = {'timeout': 1000, 'exists': aerospike.POLICY_EXISTS_UPDATE}
        assert 0 == self.as_connection.put(key, rec, meta, policy)
        (key, meta, bins) = self.as_connection.get(key)

        assert rec == bins
        self.as_connection.remove(key)

    def test_pos_put_with_policy_gen_GT(self):
        """
            Invoke put() for a record with generation as GT positive
        """
        key = ('test', 'demo', 1)

        rec = {"name": "John"}
        meta = {'gen': 2, 'ttl': 25000}
        policy = {'timeout': 1000}
        assert 0 == self.as_connection.put(key, rec, meta, policy)

        (key, meta, bins) = self.as_connection.get(key)

        assert rec == bins
        gen = meta['gen']
        assert gen == 1
        rec = {"name": "Smith"}
        policy = {'timeout': 1000, 'gen': aerospike.POLICY_GEN_GT}
        meta = {'gen': gen + 5}

        self.as_connection.put(key, rec, meta, policy)

        (key, meta, bins) = self.as_connection.get(key)
        assert rec == bins
        self.as_connection.remove(key)

    def test_pos_put_with_policy_gen_ignore(self):
        """
            Invoke put() for a record with generation as gen_ignore
        """
        key = ('test', 'demo', 1)

        rec = {"name": "John"}
        meta = {'gen': 2, 'ttl': 25000}
        policy = {'timeout': 1000}
        assert 0 == self.as_connection.put(key, rec, meta, policy)

        (key, meta, bins) = self.as_connection.get(key)

        assert rec == bins

        gen = meta['gen']
        rec = {"name": "Smith"}
        policy = {'timeout': 1000, 'gen': aerospike.POLICY_GEN_IGNORE}
        meta = {'gen': gen}

        assert 0 == self.as_connection.put(key, rec, meta, policy)

        (key, meta, bins) = self.as_connection.get(key)
        assert rec == bins
        self.as_connection.remove(key)

    @pytest.mark.parametrize("key, record, meta, policy", [
        (('test', 'demo', 1), {'name': 'john'},
            {'gen': True, 'ttl': 25000}, {'timeout': 1000}),
        (('test', 'demo', 1), {'name': 'john'},
            {'gen': 3, 'ttl': True}, {'timeout': 1000}),
        (('test', 'demo', 1), {'name': 'john'},
            {'gen': True, 'ttl': True}, {'timeout': 1000}),
        ])
    def test_pos_put_with_metadata_bool(self, key, record, meta, policy, put_data):
        """
            Invoke put() for a record with generation as boolean.
        """
        put_data(self.as_connection, key, record, meta, policy)

        (key, meta, bins) = self.as_connection.get(key)
        assert bins == record
        assert meta['gen'] != None
        assert meta['ttl'] != None

    def test_pos_put_user_serializer_no_deserializer(self):
        """
            Invoke put() for float data record with user serializer is
            registered, but deserializer is not registered.
        """
        key = ('test', 'demo', 'put_user_serializer')

        rec = {"pi": 3.14}

        def serialize_function(val):
            return pickle.dumps(val)

        aerospike.set_serializer(serialize_function)

        res = self.as_connection.put(key, rec, {}, {}, aerospike.SERIALIZER_USER)

        assert res == 0

        _, _, bins = self.as_connection.get(key)

        if self.skip_old_server is False:
            assert bins == {'pi': 3.14}
        else:
            assert bins == {'pi': bytearray(b'F3.1400000000000001\n.')}

        self.as_connection.remove(key)

    # put negative
    def test_neg_put_with_no_parameters(self):
        """
            Invoke put() without any parameters.
        """
        with pytest.raises(TypeError) as typeError:
            self.as_connection.put()

        assert "Required argument 'key' (pos 1) not found" in str(typeError.value)

    def test_neg_put_without_record(self):
        """
            Invoke put() without any record data.
        """
        key = ('test', 'demo', 1)

        with pytest.raises(TypeError) as typeError:
            self.as_connection.put(key)

        assert "Required argument 'bins' (pos 2) not found" in str(typeError.value)

    @pytest.mark.parametrize("key, record, ex_code, ex_msg", [
        (None, {"name": "John"}, -2, 'key is invalid'),
        # Invalid Namespace
        ((None, "demo", 1), {"name": "Steve"}, -2, "namespace must be a string" ),
        # Invalid Key
        (("test", "demo", None), {"name": "John"}, -2, "either key or digest is required"),
        # Invalid bin 
        (('test', 'demo', 15), "Name : John", -2, "Record should be passed as bin-value pair"),
        # Invalid set name
        (('test', 123, 1), {'a': ['!@#!#$%#', bytearray('ASD@#$AR#$@#ERQ#', 'utf-8')]}, -2, 'set must be a string'),
        # Invalid Namespace
        ((123, 'demo', 1), {'i': 'asdadasd'}, -2, 'namespace must be a string')

        ])
    def test_neg_put_with_none_key(self, key, record, ex_code, ex_msg):
        """
            Invoke put() with invalid data
        """

        try:
            self.as_connection.put(key, record)

        except e.ParamError as exception:
            assert exception.code == ex_code
            assert exception.msg == ex_msg

    @pytest.mark.parametrize("key, record, exception_code", [
        # Non-existing NS & Set
        (('demo', 'test', 1), {'a': ['!@#!#$%#', bytearray('ASD@#$AR#$@#ERQ#', 'utf-8')]}, 20),
        (('test1', 'demo', 1), {'i': 'asdadasd'}, 20),    # Non-existing Namespace

        ])
    def test_neg_put_with_wrong_ns_and_set(self, key, record, exception_code):
        """
            Invoke put() with non-existent data
        """
        try:
            self.as_connection.put(key, record)

        except e.NamespaceNotFound as exception:
            assert exception.code == exception_code

    def test_neg_put_with_policy_gen_EQ_less(self):
        """
            Invoke put() for a record with generation as EQ less
        """
        key = ('test', 'demo', 'policy_gen_EQ_key')

        rec = {"name": "John"}
        meta = {'gen': 2, 'ttl': 25000}
        policy = {'timeout': 1000}
        assert 0 == self.as_connection.put(key, rec, meta, policy)

        (key, meta, bins) = self.as_connection.get(key)

        assert {"name": "John"} == bins
        rec = {"name": "Smith"}
        policy = {'timeout': 1000, 'gen': aerospike.POLICY_GEN_EQ}
        meta = {'gen': 10}

        try:
            self.as_connection.put(key, rec, meta, policy)

        except e.RecordGenerationError as exception:
            assert exception.code == 3
            assert exception.msg == 'AEROSPIKE_ERR_RECORD_GENERATION'

        (key, meta, bins) = self.as_connection.get(key)
        assert {"name": "John"} == bins

        self.as_connection.remove(key)

    def test_neg_put_with_policy_gen_EQ_more(self):
        """
            Invoke put() for a record with generation as EQ less
        """
        key = ('test', 'demo', 'policy_gen_EQ_more_key')

        rec = {"name": "John"}
        meta = {'gen': 10, 'ttl': 25000}
        policy = {'timeout': 1000}
        assert 0 == self.as_connection.put(key, rec, meta, policy)

        (key, meta, bins) = self.as_connection.get(key)

        assert {"name": "John"} == bins
        rec = {"name": "Smith"}
        policy = {'timeout': 1000, 'gen': aerospike.POLICY_GEN_EQ}
        meta = {'gen': 4}

        try:
            self.as_connection.put(key, rec, meta, policy)

        except e.RecordGenerationError as exception:
            assert exception.code == 3
            assert exception.msg == 'AEROSPIKE_ERR_RECORD_GENERATION'

        (key, meta, bins) = self.as_connection.get(key)
        assert {"name": "John"} == bins

        self.as_connection.remove(key)

    def test_neg_put_with_policy_exists_create(self):
        """
            Invoke put() for a record with all policies.
        """
        key = ('test', 'demo', 1)

        rec = {"name": "John"}
        meta = {'gen': 2, 'ttl': 25000}
        policy = {
            'timeout': 1000,
            'gen': aerospike.POLICY_GEN_IGNORE,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND,
        }
        assert 0 == self.as_connection.put(key, rec, meta, policy)

        (key, meta, bins) = self.as_connection.get(key)

        assert {"name": "John"} == bins

        rec = {"name": "Smith"}
        policy = {'timeout': 1000, 'exists': aerospike.POLICY_EXISTS_CREATE}
        meta = {'gen': 2}

        try:
            self.as_connection.put(key, rec, meta, policy)

        except e.RecordExistsError as exception:
            assert exception.code == 5
            assert exception.msg == 'AEROSPIKE_ERR_RECORD_EXISTS'
            assert exception.bin == {'name': 'Smith'}

        (key, meta, bins) = self.as_connection.get(key)
        assert {"name": "John"} == bins

        self.as_connection.remove(key)

    def test_neg_put_with_policy_exists_replace_negative(self):
        """
            Invoke put() for a record with replace policy negative.
        """
        key = ('test', 'demo', 1)

        rec = {"name": "John"}
        meta = {'gen': 2, 'ttl': 25000}
        policy = {
            'timeout': 1000,
            'exists': aerospike.POLICY_EXISTS_REPLACE,
            'gen': aerospike.POLICY_GEN_IGNORE,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND
        }
        try:
            assert 0 == self.as_connection.put(key, rec, meta, policy)

        except e.RecordNotFound as exception:
            assert exception.code == 2
            assert exception.msg == 'AEROSPIKE_ERR_RECORD_NOT_FOUND'

    def test_neg_put_with_policy_replace(self):
        """
            Invoke put() for a record with replace positive.
        """
        key = ('test', 'demo', 1)

        rec = {"name": "Smith"}
        meta = {'gen': 2, 'ttl': 25000}
        policy = {'timeout': 1000, 'exists': aerospike.POLICY_EXISTS_REPLACE}

        try:
            self.as_connection.put(key, rec, meta, policy)
        except e.RecordNotFound as exception:
            assert exception.code == 2
            assert exception.msg == 'AEROSPIKE_ERR_RECORD_NOT_FOUND'

    def test_neg_put_with_policy_exists_update_negative(self):
        """
            Invoke put() for a record with update policy negative.
        """
        key = ('test', 'demo', 1)

        rec = {"name": "John"}
        meta = {'gen': 2, 'ttl': 25000}
        policy = {
            'timeout': 1000,
            'exists': aerospike.POLICY_EXISTS_UPDATE,
            'gen': aerospike.POLICY_GEN_IGNORE,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND
        }
        try:
            assert 0 == self.as_connection.put(key, rec, meta, policy)

        except e.RecordNotFound as exception:
            assert exception.code == 2
            assert exception.msg == 'AEROSPIKE_ERR_RECORD_NOT_FOUND'

    def test_neg_put_with_policy_gen_GT_lesser(self):
        """
            Invoke put() for a record with generation as GT lesser
        """
        key = ('test', 'demo', 1)

        rec = {"name": "John"}
        meta = {'gen': 2, 'ttl': 25000}
        policy = {'timeout': 1000}
        assert 0 == self.as_connection.put(key, rec, meta, policy)

        (key, meta, bins) = self.as_connection.get(key)

        assert {"name": "John"} == bins
        gen = meta['gen']
        rec = {"name": "Smith"}
        policy = {'timeout': 1000, 'gen': aerospike.POLICY_GEN_GT}
        meta = {'gen': gen}

        try:
            self.as_connection.put(key, rec, meta, policy)

        except e.RecordGenerationError as exception:
            assert exception.code == 3
            assert exception.msg == 'AEROSPIKE_ERR_RECORD_GENERATION'

        (key, meta, bins) = self.as_connection.get(key)
        assert {"name": "John"} == bins
        self.as_connection.remove(key)

    def test_neg_put_with_string_record_without_connection(self):
        """
            Invoke put() for a record with string data without connection
        """
        config = {"hosts": [("127.0.0.1", 3000)]}
        client1 = aerospike.client(config)

        key = ('test', 'demo', 1)

        bins = {"name": "John"}

        try:
            client1.put(key, bins)
        except e.ClusterError as exception:
            assert exception.code == 11

    @pytest.mark.parametrize("key, record, meta, policy, ex_code, ex_msg", [
        (('test', 'demo', 1), {'name': 'john'},
            {'gen': "wrong", 'ttl': 25000}, {'timeout': 1000},  #Gen as string
            -2, "Generation should be an int or long"),
        (('test', 'demo', 1), {'name': 'john'},
            {'gen': 3, 'ttl': "25000"}, {'timeout': 1000},      # ttl as string
            -2, "TTL should be an int or long"),
        (('test', 'demo', 1), {'name': 'john'},
            {'gen': 3, 'ttl': 25000}, {'timeout': "1000"},      # Timeout as string
            -2, "timeout is invalid"),
        (('test', 'demo', 1), {'name': 'john'},             #Policy as string
            {'gen': 3, 'ttl': 25000}, "Policy",
            -2, "policy must be a dict"),
        pytest.mark.xfail((('test', 'demo', 1), {'i': 13},             #Meta as string
            "OK", {'timeout': 1000},
            -2, "meta must be a dict")),
        pytest.mark.xfail((('test', 'demo', 1), {'i': 13},             #Meta as string
            1234, {'timeout': 1000},
            -2, "meta must be a dict")),
        ])
    def test_neg_put_with_invalid_metadata(self, key, record, meta, policy, ex_code, ex_msg, put_data):
        """
            Invoke put() for a record with generation as string
        """
        try:
            put_data(self.as_connection, key, record, meta, policy)
            # self.as_connection.remove(key)
        except e.ParamError as exception:
            assert exception.code == ex_code
            assert exception.msg == ex_msg

    # put edge cases
    def test_edge_put_record_with_bin_name_exceeding_max_limit(self):
        """
            Invoke put() with bin name exceeding the max limit of bin name.
        """
        key = ('test', 'demo', 'put_rec')
        put_record = {
            'containers_free': [],
            'containers_used': [
                {'cluster_id': 'bob',
                 'container_id': 1,
                 'port': 4000}
            ],
            'list_of_map': [{'test': 'bar'}],
            'map_of_list': {'fizz': ['b', 'u', 'z', 'z']},
            'ports_free': [],
            'ports_unused': [4100, 4200, 4300],
            'provider_id': u'i-f01fc206'
        }

        try:
            self.as_connection.put(key, put_record)

        except e.BinNameError as exception:
            assert exception.code == 21

    def test_edge_put_with_integer_greater_than_maxisze(self):
        """
            Invoke put() for a record with integer greater than max size
        """
        key = ('test', 'demo', 1)

        bins = {"no": 111111111111111111111111111111111111111111111}

        try:
            assert 0 == self.as_connection.put(key, bins)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == 'integer value exceeds sys.maxsize'
        except SystemError as exception:
            pass

    def test_edge_put_with_key_as_an_integer_greater_than_maxsize(self):
        """
            Invoke put() for a record with integer greater than max size
        """
        key = ('test', 'demo', 1111111111111111111111111111111111)

        bins = {"no": 11}

        try:
            assert 0 == self.as_connection.put(key, bins)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == 'integer value for KEY exceeds sys.maxsize'
        except SystemError as exception:
            pass
