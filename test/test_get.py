# -*- coding: utf-8 -*-

import pytest
import sys
import cPickle as pickle

try:
    import aerospike
except:
    print "Please install aerospike python client."
    sys.exit(1)

class SomeClass(object):

    pass


class TestGet(object):
    def setup_class(cls):
        """
        Setup method.
        """
        config = {
                'hosts': [('127.0.0.1', 3000)]
                }
        TestGet.client = aerospike.client(config).connect()

    def teardown_class(cls):
        TestGet.client.close()

    def setup_method(self, method):

        for i in xrange(5):
            key = ('test', 'demo', i)
            rec = {
                    'name' : 'name%s' % (str(i)),
                    'age'  : i
                    }
            TestGet.client.put(key, rec)

        key = ('test', 'demo', 'list_key')

        rec = {
                'names': ['John', 'Marlen', 'Steve']
            }

        TestGet.client.put(key, rec)

        key = ('test', 'demo', 'map_key')

        rec = {
                'names' :{
                    'name' : 'John',
                    'age'  : 24
                    }
                }

        TestGet.client.put(key, rec)

        key = ('test', 'demo', 'list_map_key')

        rec = {
                'names' : ['John', 'Marlen', 'Steve'],
                'names_and_age': [
                    {
                      'name': 'John',
                      'age': 24
                    },
                    {
                        'name': 'Marlen',
                        'age' : 25
                        }
                    ]
                }

        TestGet.client.put(key, rec)

        obj1, obj2 = SomeClass(), SomeClass()

        key = ('test', 'demo', 'objects')

        rec = {
                'objects' : [ pickle.dumps( obj1 ), pickle.dumps( obj2 ) ]
                }
        TestGet.client.put(key, rec)

        key = ('test', 'demo', 'bytes_key')

        rec = {
                'bytes': bytearray('John', 'utf-8')
                }
        TestGet.client.put(key, rec)

        key = ( 'test', 'demo', 'boolean_key' )

        rec = {
                'is_present': True
                }
        TestGet.client.put( key, rec )

    def teardown_method(self, method):

        """
        Teardoen method.
        """
        for i in xrange(5):
            key = ('test', 'demo', i)
            TestGet.client.remove(key)

        key = ('test', 'demo', 'list_key')
        TestGet.client.remove(key)

        key = ('test', 'demo', 'map_key')
        TestGet.client.remove(key)

        key = ('test', 'demo', 'objects')
        TestGet.client.remove(key)

    def test_get_with_no_parameter(self):

        """
            Invoke get() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            TestGet.client.get()

        assert "Required argument 'key' (pos 1) not found" in typeError.value

    def test_get_with_only_key(self):

        """
            Invoke get() with a key and not policy's dict.
        """
        key = ('test', 'demo', 1)

        key, meta, bins = TestGet.client.get( key )

        assert bins == { 'age': 1, 'name': 'name1'}

    def test_get_with_typed_key(self):

        """
            Invoke get() with a string key and not policy's dict.
        """
        key = ('test', 'demo', '1')

        key, meta, bins = TestGet.client.get( key )

        assert bins == None
        assert meta == None

    def test_get_with_none_set(self):

        """
            Invoke get() with None set in key tuple.
        """
        key = ('test', None, 2)

        key, meta, bins = TestGet.client.get( key )

        assert bins == None
        assert meta == None

    def test_get_with_none_namespace(self):

        """
            Invoke get() with None namespace in key tuple.
        """
        key = (None, 'demo', 2)

        with pytest.raises(Exception) as exception:
            key, meta, bins = TestGet.client.get( key )

        assert exception.value[0] == -2
        assert exception.value[1] == 'namespace must be a string'

    def test_get_with_none_pk(self):

        """
            Invoke get() with None as primary_key part of key tuple.
        """
        key = ('test', 'demo', None)

        with pytest.raises(Exception) as exception:
            key, meta, bins = TestGet.client.get( key )

        assert exception.value[0] == -2
        assert exception.value[1] == 'either key or digest is required'

    def test_get_with_none_key(self):

        """
            Invoke get() with None as a key.
        """
        with pytest.raises(Exception) as exception:
            key, meta, bins = TestGet.client.get(None)

        assert exception.value[0] == -2
        assert exception.value[1] == "key is invalid"

    def test_get_key_type_list(self):

        """
            Invoke get() with key specified as a list of ns, set and key/digest.
        """
        key = ['test', 'demo', '1']

        with pytest.raises(Exception) as exception:
            key, meta, bins = TestGet.client.get(key)

        assert exception.value[0] == -2
        assert exception.value[1] == "key is invalid"

    def test_get_with_non_existent_namespace(self):

        """
            Invoke get() for non-existent namespace.
        """
        key = ('namespace', 'demo', 1)

        with pytest.raises(Exception) as exception:
            key, meta, bins = TestGet.client.get(key)

        assert exception.value[0] == 20
        assert exception.value[1] == 'AEROSPIKE_ERR_NAMESPACE_NOT_FOUND'

    def test_get_with_non_existent_set(self):

        """
            Invoke get() for non-existent set.
        """
        key = ('test', 'some_random_set', 1)

        key, meta, bins = TestGet.client.get(key)

        assert meta == None
        assert bins == None

    def test_get_with_non_existent_key(self):

        """
            Invoke get() for non-existent key.
        """
        key = ('test', 'demo', 'non-existent')

        key, meta, bins = TestGet.client.get( key )

        assert bins == None
        assert meta == None

    def test_get_for_list_type_record(self):

        """
            Invoke get() for list typed record.
        """
        key = ('test', 'demo', 'list_key')

        key, meta, bins = TestGet.client.get(key)

        assert {'names': ['John', 'Marlen', 'Steve']} == bins

    def test_get_for_map_type_record(self):

        """
            Invoke get() for map type record.
        """
        key = ('test', 'demo', 'map_key')

        key, meta, bins = TestGet.client.get(key)

        assert bins == {'names' : {'name' : 'John', 'age' : 24}}

    def test_get_for_list_and_map_type_combined(self):

        """
            Invoke get() for list and map combined record.
        """
        key = ('test', 'demo', 'list_map_key')

        key, meta, bins = TestGet.client.get(key)

        assert bins == {'names': ['John', 'Marlen', 'Steve'], 'names_and_age': [{'age': 24, 'name': 'John'}, {'age': 25, 'name': 'Marlen'}]}

    def test_get_with_list_of_objects(self):

        """
            Invoke get() for list of objects.
        """
        key = ( 'test', 'demo', 'objects')

        key, meta, bins = TestGet.client.get(key)

        assert bins != None

    def test_get_with_bytearray(self):

        """
            Invoke get() for bytarray record.
        """
        key = ('test', 'demo', 'bytes_key')

        key, meta, bins = TestGet.client.get(key)

        assert bins == { "bytes" : bytearray('John', 'utf-8') }

    def test_get_with_key_digest(self):

        """
            Invoke get() with a key digest.
        """
        key = ('test', 'demo', 1)

        key, meta = TestGet.client.exists(key)

        key, meta, bins = TestGet.client.get( (key[0], key[1], None, key[3]) )

        assert bins == { "name" : "name1", "age" : 1 }

    def test_get_for_boolean_data(self):

        """
            Invoke get() for a record having boolean data.
        """
        key = ( 'test', 'demo', 'boolean_key' )

        _, _, bins = TestGet.client.get( key )

        assert bins == { 'is_present' : True }

    def test_get_initkey_with_digest(self):
    
        """
            Invoke get() for a record having string data.
        """
        key = ( 'test', 'demo', None, bytearray("asd;as[d'as;djk;uyfl",
               "utf-8"))
        rec = {
                'name' : 'john' 
                }

        policy = {
                'key' : aerospike.POLICY_KEY_DIGEST
                }

        status = TestGet.client.put(key, rec, policy)

        key, meta, bins = TestGet.client.get(key, policy)

        assert bins == { 'name' : 'john' }
        assert key == ('test', 'demo', None,
                bytearray(b"asd;as[d\'as;djk;uyfl"))


    def test_get_initkey_with_policy_send(self):
    
        """
            Invoke get() for a record having string data.
        """
        
        key = ( 'test', 'demo', 1)

        rec = {
                'name' : 'john',
                'age' : 1
                }

        policy = {
                'key' : aerospike.POLICY_KEY_SEND,
                'replica': aerospike.POLICY_REPLICA_ANY,
                'consistency': aerospike.POLICY_CONSISTENCY_ONE
                }

        status = TestGet.client.put(key, rec, policy)

        key, meta, bins = TestGet.client.get(key, policy)

        assert bins == { 'name' : 'john', 'age': 1 }
        assert key == ('test', 'demo', 1,
                bytearray(b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8'))
