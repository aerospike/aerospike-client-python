# -*- coding: utf-8 -*-

import pytest
import sys
import time
from test_base_class import TestBaseClass
import cPickle as pickle

try:
    import aerospike
except:
    print "Please install aerospike python client."
    sys.exit(1)
class SomeClass(object):

    pass

def serialize_function(val):
    return pickle.dumps(val)

def deserialize_function(val):
    return pickle.loads(val)

class TestSerializer(object):

    def setup_class(cls):
        """
            Setup class
        """
        hostlist, user, password = TestBaseClass.get_hosts()
        config = {
                'hosts': hostlist
                }
        if user == None and password == None:
            TestSerializer.client = aerospike.client(config).connect()
        else:
            TestSerializer.client = aerospike.client(config).connect(user, password)
        response = aerospike.set_serializer(serialize_function)
        response = aerospike.set_deserializer(deserialize_function)

    def teardown_class(cls):
        TestSerializer.client.close()

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
            TestSerializer.client.remove(key)
    
    def test_put_with_float_data_user_serializer(self):

        #    Invoke put() for float data record with user serializer.


        key = ( 'test', 'demo', 1 )

        rec = {
                "pi" : 3.14
                }

        res = TestSerializer.client.put( key, rec , {}, {}, aerospike.SERIALIZER_USER)

        assert res == 0
        

        _, _, bins = TestSerializer.client.get( key )

        assert bins == { 'pi': 3.14 }
        
        self.delete_keys.append( key )

    def test_put_with_float_data_python_serializer(self):

          #  Invoke put() for float data record with python serializer.
        key = ( 'test', 'demo', 1 )

        rec = {
                "pi" : 3.14
        }

        res = TestSerializer.client.put( key, rec , {}, {}, aerospike.SERIALIZER_PYTHON)

        assert res == 0
        
        _, _, bins = TestSerializer.client.get( key )

        assert bins == { 'pi': 3.14 }
    
        self.delete_keys.append( key )
    
    def test_put_with_bool_data_user_serializer(self):

        """
            Invoke put() for bool data record with user serializer.
        """

        key = ( 'test', 'demo', 1 )

        rec = {
                'status': True
                }

        res = TestSerializer.client.put( key, rec , {}, {}, aerospike.SERIALIZER_USER)

        assert res == 0

        _, _, bins = TestSerializer.client.get( key )

        assert bins == { 'status': True }

        self.delete_keys.append( key )

    def test_put_with_bool_data_python_serializer(self):

        """
            Invoke put() for bool data record with python serializer.
        """
        key = ( 'test', 'demo', 1 )

        rec = {
                "status" : True
        }

        res = TestSerializer.client.put( key, rec , {}, {}, aerospike.SERIALIZER_PYTHON)

        assert res == 0
        _, _, bins = TestSerializer.client.get( key )

        assert bins == { 'status': True }

        self.delete_keys.append( key )

    """
    def test_put_with_object_data_user_serializer(self):

            #Invoke put() for object data record with user serializer.

        key = ( 'test', 'demo', 1 )

        obj1 = SomeClass()
        rec = {
            'object': obj1
        }

        res = TestSerializer.client.put( key, rec , {}, {}, aerospike.SERIALIZER_USER)

        assert res == 0

        _, _, bins = TestSerializer.client.get( key )

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

        res = TestSerializer.client.put( key, rec , {}, {}, aerospike.SERIALIZER_PYTHON)

        assert res == 0
        _, _, bins = TestSerializer.client.get( key )

        assert bins == { 'object': True }

        self.delete_keys.append( key )
    """

    def test_put_with_float_data_user_serializer_none(self):

        """
            Invoke put() for float data record with user serializer.
        """

        with pytest.raises(Exception) as exception:
            response = aerospike.set_serializer(None)

        assert exception.value[0] == -2
        assert exception.value[1] == "Parameter must be a callable"

    def test_put_with_float_data_user_deserializer_none(self):

        """
            Invoke put() for float data record with user deserializer None.
        """

        response = aerospike.set_serializer(serialize_function)

        key = ( 'test', 'demo', 1 )

        rec = {
                "pi" : 3.14
                }

        res = TestSerializer.client.put( key, rec , {}, {}, aerospike.SERIALIZER_USER)

        assert res == 0

        self.delete_keys.append( key )

        with pytest.raises(Exception) as exception:
            response = aerospike.set_deserializer(None)

        assert exception.value[0] == -2
        assert exception.value[1] == "Parameter must be a callable"

    def test_put_with_mixed_data_user_serializer(self):

        """
            Invoke put() for mixed data record with user serializer.
        """

        key = ( 'test', 'demo', 1 )

        rec = {
            'map': { "key": "asd';q;'1';", "pi":3.14},
            'normal': 1234,
            'special': '!@#@#$QSDAsd;as',
            'list': [ "nanslkdl", 1, bytearray("asd;as[d'as;d", "utf-8") ],
            'bytes': bytearray("asd;as[d'as;d", "utf-8"),
            'nestedlist': [ "nanslkdl", 1, bytearray("asd;as[d'as;d", "utf-8"), [ 1, bytearray("asd;as[d'as;d", "utf-8")] ],
            'nestedmap': { "key": "asd';q;'1';", "pi":3.14, "nest" : {"pi1": 3.12, "t": 1}},
        }
        res = TestSerializer.client.put( key, rec , {}, {}, aerospike.SERIALIZER_USER)

        assert res == 0
        
        _, _, bins = TestSerializer.client.get( key )
        
        assert bins == {
            'map': { "key": "asd';q;'1';", "pi":3.14},
            'normal': 1234,
            'special': '!@#@#$QSDAsd;as',
            'list': [ "nanslkdl", 1, bytearray("asd;as[d'as;d", "utf-8") ],
            'bytes': bytearray("asd;as[d'as;d", "utf-8"),
            'nestedlist': [ "nanslkdl", 1, bytearray("asd;as[d'as;d", "utf-8"), [ 1, bytearray("asd;as[d'as;d", "utf-8")] ],
            'nestedmap': { "key": "asd';q;'1';", "pi":3.14, "nest" : {"pi1": 3.12, "t": 1}},
        }  
        
        self.delete_keys.append( key )

    def test_put_with_mixed_data_python_serializer(self):

        """
            Invoke put() for mixed data record with python serializer.
        """
        key = ( 'test', 'demo', 1 )

        rec = {
            'map': { "key": "asd';q;'1';", "pi":3.14},
            'normal': 1234,
            'special': '!@#@#$QSDAsd;as',
            'list': [ "nanslkdl", 1, bytearray("asd;as[d'as;d", "utf-8") ],
            'bytes': bytearray("asd;as[d'as;d", "utf-8"),
            'nestedlist': [ "nanslkdl", 1, bytearray("asd;as[d'as;d", "utf-8"), [ 1, bytearray("asd;as[d'as;d", "utf-8")] ],
            'nestedmap': { "key": "asd';q;'1';", "pi":3.14, "nest" : {"pi1":
                3.12, "t": 1}, "inlist": [1, 2]},
        }

        res = TestSerializer.client.put( key, rec , {}, {}, aerospike.SERIALIZER_PYTHON)

        assert res == 0
        
        _, _, bins = TestSerializer.client.get( key )

        assert bins == {
            'map': { "key": "asd';q;'1';", "pi":3.14},
            'normal': 1234,
            'special': '!@#@#$QSDAsd;as',
            'list': [ "nanslkdl", 1, bytearray("asd;as[d'as;d", "utf-8") ],
            'bytes': bytearray("asd;as[d'as;d", "utf-8"),
            'nestedlist': [ "nanslkdl", 1, bytearray("asd;as[d'as;d", "utf-8"), [ 1, bytearray("asd;as[d'as;d", "utf-8")] ],
            'nestedmap': { "key": "asd';q;'1';", "pi":3.14, "nest" : {"pi1":
                3.12, "t": 1}, "inlist": [1, 2]},
        }  
    
        self.delete_keys.append( key )

    def test_register_multiple_serializer(self):

        """
            Invoke put() for mixed data record with python serializer.
        """
        key = ( 'test', 'demo', 1 )

        rec = {
            'map': { "key": "asd';q;'1';", "pi":3.14},
            'normal': 1234,
            'special': '!@#@#$QSDAsd;as',
            'list': [ "nanslkdl", 1, bytearray("asd;as[d'as;d", "utf-8") ],
            'bytes': bytearray("asd;as[d'as;d", "utf-8"),
            'nestedlist': [ "nanslkdl", 1, bytearray("asd;as[d'as;d", "utf-8"), [ 1, bytearray("asd;as[d'as;d", "utf-8")] ],
            'nestedmap': { "key": "asd';q;'1';", "pi":3.14, "nest" : {"pi1":
                3.12, "t": 1}, "inlist": [1, 2]},
        }

        def serialize_function(val):
            return pickle.dumps(val)

        response = aerospike.set_serializer(serialize_function)

        res = TestSerializer.client.put( key, rec , {}, {}, aerospike.SERIALIZER_PYTHON)

        assert res == 0

        _, _, bins = TestSerializer.client.get( key )

        assert bins == {
            'map': { "key": "asd';q;'1';", "pi":3.14},
            'normal': 1234,
            'special': '!@#@#$QSDAsd;as',
            'list': [ "nanslkdl", 1, bytearray("asd;as[d'as;d", "utf-8") ],
            'bytes': bytearray("asd;as[d'as;d", "utf-8"),
            'nestedlist': [ "nanslkdl", 1, bytearray("asd;as[d'as;d", "utf-8"), [ 1, bytearray("asd;as[d'as;d", "utf-8")] ],
            'nestedmap': { "key": "asd';q;'1';", "pi":3.14, "nest" : {"pi1":
                3.12, "t": 1}, "inlist": [1, 2]},
        }

        self.delete_keys.append( key )
