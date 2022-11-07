# -*- coding: utf-8 -*-

import pytest
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


class TestGetMany():

    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        self.keys = []
        for i in range(5):
            key = ('test', 'demo', i)
            rec = {'name': 'name%s' % (str(i)), 'age': i}
            as_connection.put(key, rec)
            self.keys.append(key)
        key = ('test', 'demo', 'float_value')
        as_connection.put(key, {"float_value": 4.3})
        self.keys.append(key)

        def teardown():
            """
            Teardown method.
            """
            for i in range(5):
                key = ('test', 'demo', i)
                as_connection.remove(key)
            key = ('test', 'demo', 'float_value')
            as_connection.remove(key)

        request.addfinalizer(teardown)

    def test_pos_get_many_without_policy(self):

        records = self.as_connection.get_many(self.keys)

        assert isinstance(records, list)
        assert len(records) == 6

    def test_pos_get_many_with_proper_parameters(self):
        '''
        Proper call to the method
        '''
        records = self.as_connection.get_many(self.keys, {'total_timeout': 30})

        assert isinstance(records, list)
        assert len(records) == 6
        assert Counter([x[0][2] for x in records]) == Counter([0, 1, 2, 3,
                                                               4, 'float_value'])
        assert records[5][2] == {'float_value': 4.3}

    def test_pos_get_many_with_none_policy(self):

        records = self.as_connection.get_many(self.keys, None)

        assert isinstance(records, list)
        assert len(records) == 6
        assert Counter([x[0][2] for x in records]) == Counter([0, 1, 2, 3,
                                                               4, 'float_value'])
        assert records[5][2] == {'float_value': 4.3}

    def test_pos_get_many_with_non_existent_keys(self):
        '''
        Verify that non existent keys show up in the result
        set with no associated records
        '''
        self.keys.append(('test', 'demo', 'non-existent'))

        records = self.as_connection.get_many(self.keys)

        assert isinstance(records, list)
        assert len(records) == 7
        assert Counter([x[0][2] for x in records]) == Counter([0, 1, 2, 3,
                                                               4, 'non-existent', 'float_value'])
        for x in records:
            if x[0][2] == 'non-existent':
                assert x[2] is None

    def test_pos_get_many_with_all_non_existent_keys(self):

        keys = [('test', 'demo', 'gm_non_existing_key')]

        records = self.as_connection.get_many(keys)

        assert len(records) == 1
        assert records == [(('test', 'demo', 'gm_non_existing_key',
                             bytearray(b"\x8da\xd1\x12\x1a\x8f\xa2\xfc*m\xbc\xc7}\xb0\xc8\x13\x80;\'\x07")), None, None)]

    def test_pos_get_many_with_initkey_as_digest(self):
        '''
        Verify that the method may be called with a key tuple
        with no primary key and only a digest
        '''
        keys = []
        key = (
            "test",
            "demo",
            None,
            bytearray(
                "asd;as[d'as;djk;uyfl",
                "utf-8"))
        rec = {'name': 'name1', 'age': 1}
        self.as_connection.put(key, rec)

        keys.append(key)

        key = (
            "test",
            "demo",
            None,
            bytearray(
                "ase;as[d'as;djk;uyfl",
                "utf-8"))
        rec = {'name': 'name2', 'age': 2}
        self.as_connection.put(key, rec)

        keys.append(key)

        records = self.as_connection.get_many(keys)

        for key in keys:
            self.as_connection.remove(key)

        assert isinstance(records, list)
        assert len(records) == 2
        i = 0
        for x in records:
            if i:
                assert x[0][3] == bytearray(b"ase;as[d'as;djk;uyfl")
            else:
                assert x[0][3] == bytearray(b"asd;as[d'as;djk;uyfl")
            i = i + 1

    def test_pos_get_many_with_non_existent_keys_in_middle(self):

        self.keys.append(('test', 'demo', 'some_key'))

        for i in range(15, 20):
            key = ('test', 'demo', i)
            rec = {'name': 'name%s' % (str(i)), 'age': i}
            self.as_connection.put(key, rec)
            self.keys.append(key)

        records = self.as_connection.get_many(self.keys)

        for i in range(15, 20):
            key = ('test', 'demo', i)
            self.as_connection.remove(key)

        assert isinstance(records, list)
        assert len(records) == 12
        assert Counter([x[0][2] for x in records]) == Counter([0, 1, 2, 3,
                                                               4, 'some_key', 15, 16, 17, 18, 19, 'float_value'])
        for x in records:
            if x[0][2] == 'some_key':
                assert x[2] is None

    def test_get_many_with_bytearray_key(self):
        '''
        Make sure that get many can handle a a key with a bytearray pk
        '''
        keys = [('test', 'demo', bytearray([1, 2, 3]))]
        for key in keys:
            self.as_connection.put(key, {'byte': 'array'})

        records = self.as_connection.get_many(keys)
        self.as_connection.remove(keys[0])

        bytearray_key = records[0][0]
        assert len(bytearray_key) == 4

        bytearray_pk = bytearray_key[2]
        assert bytearray_pk == bytearray([1, 2, 3])

    def test_pos_get_many_with_constructor_batch_direct_and_method_arg(self):
        '''
        This sets use batch_direct to true in the constructor
        and sets it to false in the policy argument to the function
        '''
        config = {'policies': {'use_batch_direct': True}}
        client_batch_direct = TestBaseClass.get_new_connection(add_config=config)

        policy = {'use_batch_direct': False}
        records = client_batch_direct.get_many(self.keys)

        assert isinstance(records, list)
        assert len(records) == 6

        client_batch_direct.close()

    def test_pos_get_many_with_use_batch_direct_as_method_arg(self):

        policies = {'use_batch_direct': True}

        records = self.as_connection.get_many(self.keys, policies)

        assert isinstance(records, list)
        assert len(records) == len(self.keys)

    # Negative Tests
    def test_neg_get_many_Invalid_Key_without_primary_key(self):
        """
        Invoke get_many() without primary key
        """
        key = ('test', 'set')
        with pytest.raises(e.ParamError):
            key, _, _ = self.as_connection.get(key)

    def test_neg_get_many_with_proper_parameters_without_connection(self):
        config = {'hosts': [('127.0.0.1', 3000)]}
        client1 = aerospike.client(config)
        with pytest.raises(e.ClusterError):
            client1.get_many(self.keys, {'total_timeout': 20})

    def test_neg_prepend_Invalid_Key_without_set_name(self):
        """
        Invoke prepend() without set name
        """
        key = ('test', 1)
        with pytest.raises(e.ParamError):
            key, _, _ = self.as_connection.get(key)

    def test_neg_get_many_with_invalid_key(self):

        with pytest.raises(e.ParamError):
            self.as_connection.get_many("key")

    def test_neg_get_many_with_invalid_timeout(self):

        policies = {'total_timeout': 0.2}
        with pytest.raises(e.ParamError):
            self.as_connection.get_many(self.keys, policies)

    def test_neg_get_many_without_any_parameter(self):

        with pytest.raises(TypeError) as typeError:
            self.as_connection.get_many()

        assert "argument 'keys' (pos 1)" in str(
            typeError.value)

    def test_neg_get_many_with_none_keys(self):

        with pytest.raises(e.ParamError):
            self.as_connection.get_many(None, {})

    def test_get_many_with_an_invalid_key_in_list_batch_direct(self):

        with pytest.raises(e.ParamError):
            self.as_connection.get_many([('test', 'demo', 1), ('test', 'demo', 2), None],
                                        {'use_batch_direct': True})

    def test_neg_prepend_Invalid_Key_Invalid_ns(self):
        """
        Invoke prepend() invalid namespace
        """
        key = ('test1', 'demo', 1)
        with pytest.raises(e.ClientError):
            key, _, _ = self.as_connection.get(key)
