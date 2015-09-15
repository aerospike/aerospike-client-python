# -*- coding: utf-8 -*-

import pytest
import sys
import time
import marshal
import json
from test_base_class import TestBaseClass

aerospike = pytest.importorskip("aerospike")
try:
    from aerospike.exception import *
except:
    print "Please install aerospike python client."
    sys.exit(1)

def serialize_function(val):
    return json.dumps(marshal.dumps(val))

def client_serialize_function(val):
    return marshal.dumps(val)

def deserialize_function(val):
    return marshal.loads(json.loads(val))

def client_deserialize_function(val):
    return marshal.loads(val)

def client_deserialize_function_override(val):
    return json.loads(val)

class SomeClass(object):

    pass

class TestPythonSerializer(object):

    def setup_class(cls):
        """
            Setup class
        """
        hostlist, user, password = TestBaseClass.get_hosts()
        config = {'hosts': hostlist}
        if user == None and password == None:
            TestPythonSerializer.client = aerospike.client(config).connect()
        else:
            TestPythonSerializer.client = aerospike.client(config).connect(user, password)

    def teardown_class(cls):
        TestPythonSerializer.client.close()

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
            TestPythonSerializer.client.remove(key)


    def test_put_mixed_data_user_client_serializer_with_spec_in_put_no_class_serializer(self):

        #    Invoke put() for float data record with user client serializer.

        hostlist, user, password = TestBaseClass.get_hosts()
        method_config = {'hosts': hostlist,
                'serialization': (client_serialize_function,
                    client_deserialize_function)}
        if user == None and password == None:
            client = aerospike.client(method_config).connect()
        else:
            client = aerospike.client(method_config).connect(user, password)
        key = ('test', 'demo', 1)

        rec = {
            'map': {"key": "asd';q;'1';",
                    "pi": 3},
            'normal': 1234,
            'special': '!@#@#$QSDAsd;as',
            'list': ["nanslkdl", 1, bytearray("asd;as[d'as;d", "utf-8")],
            'bytes': bytearray("asd;as[d'as;d", "utf-8"),
            'nestedlist': ["nanslkdl", 1, bytearray("asd;as[d'as;d", "utf-8"),
                           [1, bytearray("asd;as[d'as;d", "utf-8")]],
            'nestedmap': {
                "key": "asd';q;'1';",
                "pi": 3.14,
                "nest": {"pi1": 312,
                         "t": 1}
            },
        }

        res = client.put(key, rec, {}, {}, aerospike.SERIALIZER_USER)

        assert res == 0

        _, _, bins = client.get(key)

        assert bins == {
            'map': {"key": "asd';q;'1';",
                    "pi": 3},
            'normal': 1234,
            'special': '!@#@#$QSDAsd;as',
            'list': ["nanslkdl", 1, "asd;as[d'as;d"],
            'bytes': "asd;as[d'as;d",
            'nestedlist': ["nanslkdl", 1, "asd;as[d'as;d",
                           [1, "asd;as[d'as;d"]],
            'nestedmap': {
                "key": "asd';q;'1';",
                "pi": 3.14,
                "nest": {"pi1": 312,
                         "t": 1}
            },
        }
        client.close()
        self.delete_keys.append(key)

    def test_put_with_float_data_python_serializer(self):

        #  Invoke put() for float data record with python serializer.
        key = ('test', 'demo', 1)

        rec = {"pi": 3.14}

        res = TestPythonSerializer.client.put(key, rec, {}, {},
                                        aerospike.SERIALIZER_PYTHON)

        assert res == 0

        _, _, bins = TestPythonSerializer.client.get(key)

        assert bins == {'pi': 3.14}

        self.delete_keys.append(key)

    def test_put_with_bool_data_python_serializer(self):
        """
            Invoke put() for bool data record with python serializer.
        """
        key = ('test', 'demo', 1)

        rec = {"status": True}

        res = TestPythonSerializer.client.put(key, rec, {}, {},
                                        aerospike.SERIALIZER_PYTHON)

        assert res == 0
        _, _, bins = TestPythonSerializer.client.get(key)

        assert bins == {'status': True}

        self.delete_keys.append(key)

    def test_put_with_mixed_data_python_serializer(self):
        """
            Invoke put() for mixed data record with python serializer.
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
                         "t": 1},
                "inlist": [1, 2]
            },
        }

        res = TestPythonSerializer.client.put(key, rec, {}, {},
                                        aerospike.SERIALIZER_PYTHON)

        assert res == 0

        _, _, bins = TestPythonSerializer.client.get(key)

        assert bins == {
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
                         "t": 1},
                "inlist": [1, 2]
            },
        }

        self.delete_keys.append(key)

    def test_put_with_mixed_data_python_serializer_with_client_serializer(self):

        #  Invoke put() for mixed data record with user client serializer.Should
        #  call python serializer
        hostlist, user, password = TestBaseClass.get_hosts()
        method_config = {'hosts': hostlist,
                  'serialization': (client_serialize_function,
                      client_deserialize_function)}
        if user == None and password == None:
            client = aerospike.client(method_config).connect()
        else:
            client = aerospike.client(method_config).connect(user, password)
        aerospike.set_serializer(serialize_function)
        aerospike.set_deserializer(deserialize_function)
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

        res = client.put(key, rec, {}, {}, aerospike.SERIALIZER_PYTHON)

        assert res == 0

        _, _, bins = client.get(key)

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
        client.close()
    """
    def test_put_with_float_data_user_client_serializer(self):

        #  Invoke put() for float data record with user client serializer.
        key = ('test', 'demo', 5)

        rec = {"bytes": bytearray("asd;as[d'as;d", "utf-8")}

        res = TestPythonSerializer.client.put(key, rec, {}, {})

        assert res == 0

        assert test_list == [bytearray("asd;as[d'as;d", "utf-8")]

        self.delete_keys.append(key)
        """
