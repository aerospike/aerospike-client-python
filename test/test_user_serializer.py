# -*- coding: utf-8 -*-

import pytest
import sys
import time
from test_base_class import TestBaseClass
import cPickle as pickle

aerospike = pytest.importorskip("aerospike")
try:
    from aerospike.exception import *
except:
    print "Please install aerospike python client."
    sys.exit(1)

class SomeClass(object):

    pass

test_list = []
def serialize_function(val):
    return pickle.dumps(val)

def client_serialize_function(val):
    test_list.append(val)
    return pickle.dumps(val)

def deserialize_function(val):
    return pickle.loads(val)

def client_deserialize_function(val):
    test_list.append(pickle.loads(val))
    return pickle.loads(val)


class TestUserSerializer(object):
    def setup_class(cls):
        """
            Setup class
        """
        hostlist, user, password = TestBaseClass.get_hosts()
        config = {'hosts': hostlist,
                'serialization': (client_serialize_function,
                    None)}
        if user == None and password == None:
            TestUserSerializer.client = aerospike.client(config).connect()
        else:
            TestUserSerializer.client = aerospike.client(config).connect(user, password)
        response = aerospike.set_serializer(serialize_function)
        response = aerospike.set_deserializer(deserialize_function)

    def teardown_class(cls):
        TestUserSerializer.client.close()

    def setup_method(self, method):
        """
            Setup method
        """

        self.delete_keys = []

    def teardown_method(self, method):
        """
            Teardown method
        """
        for key in self.delete_keys:
            TestUserSerializer.client.remove(key)

    def test_put_with_float_data_user_serializer(self):

        #    Invoke put() for float data record with user serializer.

        key = ('test', 'demo', 1)

        rec = {"pi": 3.14}

        res = TestUserSerializer.client.put(key, rec, {}, {},
                                        aerospike.SERIALIZER_USER)

        assert res == 0

        _, _, bins = TestUserSerializer.client.get(key)

        assert bins == {'pi': 3.14}

        self.delete_keys.append(key)



    def test_put_with_bool_data_user_serializer(self):
        """
            Invoke put() for bool data record with user serializer.
        """

        key = ('test', 'demo', 1)

        rec = {'status': True}

        res = TestUserSerializer.client.put(key, rec, {}, {},
                                        aerospike.SERIALIZER_USER)

        assert res == 0

        _, _, bins = TestUserSerializer.client.get(key)

        assert bins == {'status': True}

        self.delete_keys.append(key)


    """
    def test_put_with_object_data_user_serializer(self):

            #Invoke put() for object data record with user serializer.

        key = ( 'test', 'demo', 1 )

        obj1 = SomeClass()
        rec = {
            'object': obj1
        }

        res = TestUserSerializer.client.put( key, rec , {}, {}, aerospike.SERIALIZER_USER)

        assert res == 0

        _, _, bins = TestUserSerializer.client.get( key )

        #assert bins == { 'object': True }
        assert bins == { 'object': obj1 }

        self.delete_keys.append( key )

    def test_put_with_object_data_python_serializer(self):

            Invoke put() for object data record with python serializer.

        key = ( 'test', 'demo', 1 )

        obj1 = SomeClass()
        rec = {
                "object" : obj1
        }

        res = TestUserSerializer.client.put( key, rec , {}, {}, aerospike.SERIALIZER_PYTHON)

        assert res == 0
        _, _, bins = TestUserSerializer.client.get( key )

        assert bins == { 'object': True }

        self.delete_keys.append( key )
    """

    def test_put_with_float_data_user_serializer_none(self):
        """
            Invoke put() for float data record with user serializer.
        """

        try:
            response = aerospike.set_serializer(None)

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Parameter must be a callable"

    def test_put_with_float_data_user_deserializer_none(self):
        """
            Invoke put() for float data record with user deserializer None.
        """

        response = aerospike.set_serializer(serialize_function)

        key = ('test', 'demo', 1)

        rec = {"pi": 3.14}

        res = TestUserSerializer.client.put(key, rec, {}, {},
                                        aerospike.SERIALIZER_USER)

        assert res == 0

        self.delete_keys.append(key)

        try:
            response = aerospike.set_deserializer(None)

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Parameter must be a callable"

    def test_put_with_mixed_data_user_serializer(self):
        """
            Invoke put() for mixed data record with user serializer.
        """

        key = ('test', 'demo', 1)

        rec = {
            'map': {"key": "asd';q;'1';",
                    "pi": 3.14},
            'normal': 1234,
            'special': '!@#@#$QSDAsd;as',
            'list': ["nanslkdl", 1, bytearray("asd;as[d'as;d", "utf-8")],
            'bytes': bytearray("asd;as[d'as;d", "utf-8"),
            'nestedlist': ["nanslkdl", 1, bytearray("asd;as[d'as;d", "utf-8"),
                           [1, bytearray("asd;as[d'as;d", "utf-8")]],
            'nestedmap': {
                "key": "asd';q;'1';",
                "pi": 3.14,
                "nest": {"pi1": 3.12,
                         "t": 1}
            },
        }
        res = TestUserSerializer.client.put(key, rec, {}, {},
                                        aerospike.SERIALIZER_USER)

        assert res == 0

        _, _, bins = TestUserSerializer.client.get(key)

        assert bins == {
            'map': {"key": "asd';q;'1';",
                    "pi": 3.14},
            'normal': 1234,
            'special': '!@#@#$QSDAsd;as',
            'list': ["nanslkdl", 1, bytearray("asd;as[d'as;d", "utf-8")],
            'bytes': bytearray("asd;as[d'as;d", "utf-8"),
            'nestedlist': ["nanslkdl", 1, bytearray("asd;as[d'as;d", "utf-8"),
                           [1, bytearray("asd;as[d'as;d", "utf-8")]],
            'nestedmap':
            {"key": "asd';q;'1';",
             "pi": 3.14,
             "nest": {"pi1": 3.12,
                      "t": 1}},
        }

        self.delete_keys.append(key)

    def test_put_with_float_data_user_client_serializer_deserializer(self):

        #    Invoke put() for float data record with user client serializer.
        hostlist, user, password = TestBaseClass.get_hosts()
        method_config = {'hosts': hostlist,
                'serialization': (client_serialize_function,
                    client_deserialize_function)}
        if user == None and password == None:
            client = aerospike.client(method_config).connect()
        else:
            client = aerospike.client(method_config).connect(user, password)
        response = aerospike.set_serializer(serialize_function)
        response = aerospike.set_deserializer(deserialize_function)
        key = ('test', 'demo', 1)

        rec = {"pi": 3.14}

        res = client.put(key, rec, {}, {})

        assert res == 0

        assert test_list == [3.14]
        _, _, bins = client.get(key)

        assert bins == {'pi': 3.14}
        assert test_list == [3.14, 3.14]
        del test_list[:]
        self.delete_keys.append(key)

    def test_put_with_float_data_user_client_serializer_deserializer_with_spec_in_put(self):

        #    Invoke put() for float data record with user client serializer.
        hostlist, user, password = TestBaseClass.get_hosts()
        method_config = {'hosts': hostlist,
                'serialization': (client_serialize_function,
                    client_deserialize_function)}
        if user == None and password == None:
            client = aerospike.client(method_config).connect()
        else:
            client = aerospike.client(method_config).connect(user, password)
        response = aerospike.set_serializer(serialize_function)
        response = aerospike.set_deserializer(deserialize_function)
        key = ('test', 'demo', 1)

        rec = {"pi": 3.14}

        res = client.put(key, rec, {}, {}, aerospike.SERIALIZER_USER)

        assert res == 0

        assert test_list == []
        _, _, bins = client.get(key)

        assert bins == {'pi': 3.14}
        assert test_list == [3.14]
        del test_list[:]
        self.delete_keys.append(key)
