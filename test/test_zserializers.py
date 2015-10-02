# -*- coding: utf-8 -*-

import pytest
import sys
import time
import marshal
import json
import pickle
from test_base_class import TestBaseClass

aerospike = pytest.importorskip("aerospike")
try:
    from aerospike.exception import *
except:
    print "Please install aerospike python client."
    sys.exit(1)


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

        # Unset previously set class serializers if set
        aerospike.unset_serializers()

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
            try:
                TestPythonSerializer.client.remove(key)
            except:
                pass


    def test_instance_serializer_and_no_class_serializer(self):

        #    Invoke put() for record with no class serializer. There is an
        #    instance serializer
        hostlist, user, password = TestBaseClass.get_hosts()
        method_config = {
            'hosts': hostlist,
            'serialization': ((lambda v: json.dumps(v)), (lambda v: json.loads(v)))}
        if user == None and password == None:
            client = aerospike.client(method_config).connect()
        else:
            client = aerospike.client(method_config).connect(user, password)
        key = ('test', 'demo', 11)
        try:
            client.remove(key)
        except:
            pass

        rec = {'normal': 1234, 'tuple': (1,2,3)}

        res = client.put(key, rec, serializer=aerospike.SERIALIZER_USER)

        assert res == 0

        _, _, bins = client.get(key)

        # tuples JSON-encode to a list, and we use this fact to check which
        # serializer ran:
        assert bins == {'normal': 1234, 'tuple': [1,2,3]}
        client.remove(key)
        client.close()


    def test_builtin_with_instance_serializer_and_no_class_serializer(self):

        #    Invoke put() for data record with builtin serializer and an
        #    instance serializer set
        hostlist, user, password = TestBaseClass.get_hosts()
        method_config = {
            'hosts': hostlist,
            'serialization': ((lambda v: json.dumps(v)), (lambda v: json.loads(v)))}
        if user == None and password == None:
            client = aerospike.client(method_config).connect()
        else:
            client = aerospike.client(method_config).connect(user, password)
        key = ('test', 'demo', 12)
        try:
            client.remove(key)
        except:
            pass

        rec = {'normal': 1234, 'tuple': (1,2,3)}

        res = client.put(key, rec, serializer=aerospike.SERIALIZER_PYTHON)

        assert res == 0

        _, _, bins = client.get(key)

        assert bins == {'normal': 1234, 'tuple': (1,2,3)}
        client.remove(key)
        client.close()


    def test_builtin_with_class_serializer(self):
        """
            Invoke put() for mixed data record with builtin serializer
        """
        aerospike.set_serializer((lambda v: json.dumps(v)))
        aerospike.set_deserializer((lambda v: json.loads(v)))
        key = ('test', 'demo', 13)
        try:
            TestPythonSerializer.client.remove(key)
        except:
            pass

        rec = {'normal': 1234, 'tuple': (1,2,3)}
        res = TestPythonSerializer.client.put(key, rec, serializer=aerospike.SERIALIZER_PYTHON)

        assert res == 0

        _, _, bins = TestPythonSerializer.client.get(key)

        # tuples JSON-encode to a list, and we use this fact to check which
        # serializer ran:
        assert bins == {'normal': 1234, 'tuple': (1,2,3)}
        self.delete_keys.append(key)


    def test_with_class_serializer(self):
        """
            Invoke put() for mixed data record with class serializer.
        """
        aerospike.set_serializer((lambda v: json.dumps(v)))
        aerospike.set_deserializer((lambda v: json.loads(v)))
        key = ('test', 'demo', 14)
        try:
            TestPythonSerializer.client.remove(key)
        except:
            pass

        rec = {'normal': 1234, 'tuple': (1,2,3)}
        res = TestPythonSerializer.client.put(key, rec, serializer=aerospike.SERIALIZER_USER)

        assert res == 0

        _, _, bins = TestPythonSerializer.client.get(key)

        # tuples JSON-encode to a list, and we use this fact to check which
        # serializer ran:
        assert bins == {'normal': 1234, 'tuple': [1,2,3]}
        self.delete_keys.append(key)

    def test_builtin_with_class_serializer_and_instance_serializer(self):
        """
            Invoke put() for mixed data record with python serializer.
        """
        aerospike.set_serializer((lambda v: json.dumps(v)))
        aerospike.set_deserializer((lambda v: json.loads(v)))
        hostlist, user, password = TestBaseClass.get_hosts()
        method_config = {
            'hosts': hostlist,
            'serialization': ((lambda v: marshal.dumps(v)), (lambda v: marshal.loads(v)))}
        if user == None and password == None:
            client = aerospike.client(method_config).connect()
        else:
            client = aerospike.client(method_config).connect(user, password)
        key = ('test', 'demo', 15)
        try:
            client.remove(key)
        except:
            pass

        rec = {'normal': 1234, 'tuple': (1,2,3)}
        res = client.put(key, rec, serializer=aerospike.SERIALIZER_PYTHON)

        assert res == 0

        _, _, bins = TestPythonSerializer.client.get(key)

        # tuples JSON-encode to a list, and we use this fact to check which
        # serializer ran:
        assert bins == {'normal': 1234, 'tuple': (1,2,3)}
        client.remove(key)


    def test_with_class_serializer_and_instance_serializer(self):
        """
            Invoke put() for mixed data record with class and instance serializer.
        """
        aerospike.set_serializer((lambda v: json.dumps(v)))
        aerospike.set_deserializer((lambda v: json.loads(v)))
        hostlist, user, password = TestBaseClass.get_hosts()
        method_config = {
            'hosts': hostlist,
            'serialization': ((lambda v: marshal.dumps(v)), (lambda v: marshal.loads(v)))}
        if user == None and password == None:
            client = aerospike.client(method_config).connect()
        else:
            client = aerospike.client(method_config).connect(user, password)
        key = ('test', 'demo', 16)
        try:
            TestPythonSerializer.client.remove(key)
        except:
            pass

        rec = {'normal': 1234, 'tuple': (1,2,3)}
        res = client.put(key, rec, serializer=aerospike.SERIALIZER_USER)

        assert res == 0

        _, _, bins = client.get(key)

        # tuples JSON-encode to a list, and we use this fact to check which
        # serializer ran:
        assert bins == {'normal': 1234, 'tuple': (1,2,3)}
        client.remove(key)

    def test_with_class_serializer_and_instance_serializer_with_unset_serializer(self):
        """
            Invoke put() for mixed data record with python serializer.
        """
        aerospike.set_serializer((lambda v: json.dumps(v)))
        aerospike.set_deserializer((lambda v: json.loads(v)))
        hostlist, user, password = TestBaseClass.get_hosts()
        method_config = {
            'hosts': hostlist
        }
        if user == None and password == None:
            client = aerospike.client(method_config).connect()
        else:
            client = aerospike.client(method_config).connect(user, password)
        key = ('test', 'demo', 16)
        try:
            TestPythonSerializer.client.remove(key)
        except:
            pass

        rec = {'normal': 1234, 'tuple': (1,2,3)}

        aerospike.unset_serializers()
        try:
            res = client.put(key, rec, serializer=aerospike.SERIALIZER_USER)
        except ClientError as exception:
            assert exception.code == -1
            assert exception.msg == "No serializer callback registered"

    def test_with_unset_serializer_python_serializer(self):
        """
            Invoke put() for mixed data record with python serializer and
            calling unset_serializers
        """
        aerospike.set_serializer((lambda v: json.dumps(v)))
        aerospike.set_deserializer((lambda v: json.loads(v)))
        hostlist, user, password = TestBaseClass.get_hosts()
        method_config = {
            'hosts': hostlist
        }
        if user == None and password == None:
            client = aerospike.client(method_config).connect()
        else:
            client = aerospike.client(method_config).connect(user, password)
        key = ('test', 'demo', 16)
        try:
            client.remove(key)
        except:
            pass

        rec = {'normal': 1234, 'tuple': (1,2,3)}

        aerospike.unset_serializers()
        res = client.put(key, rec, serializer=aerospike.SERIALIZER_PYTHON)
        
        assert res == 0

        _, _, bins = client.get(key)

        # tuples JSON-encode to a list, and we use this fact to check which
        # serializer ran:
        assert bins == {'normal': 1234, 'tuple': (1,2,3)}
        client.remove(key)
