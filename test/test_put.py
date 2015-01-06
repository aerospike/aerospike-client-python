# -*- coding: utf-8 -*-

import pytest
import sys
import time
import cPickle as pickle

try:
    import aerospike
except:
    print "Please install aerospike python client."
    sys.exit(1)

class TestPut(object):

    def setup_class(cls):
        """
            Setup class
        """
        config = {
                "hosts": [("127.0.0.1", 3000)]
                }
        TestPut.client = aerospike.client(config).connect()

    def teardown_class(cls):
        TestPut.client.close()

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
            TestPut.client.remove(key)

    def test_put_with_string_record(self):

        """
            Invoke put() for a record with string data.
        """
        key = ('test', 'demo', 1)

        rec = {
                "name" : "John"
                }

        assert 0 == TestPut.client.put( key, rec )

        (key , meta, bins) = TestPut.client.get(key)

        assert {"name": "John"} == bins
        self.delete_keys.append( key )

    def test_put_with_multiple_bins(self):

        """
            Invoke put() with multiple bins and multiple types of data.
            Covers list, map, bytearray, integer.
        """
        key = ('test', 'demo', 1)

        rec = {
                'i': [ "nanslkdl", 1, bytearray("asd;as[d'as;d", "utf-8") ],
                's': { "key": "asd';q;'1';" },
                'b': 1234,
                'l': '!@#@#$QSDAsd;as'
            }

        assert 0 == TestPut.client.put( key, rec)
        (key , meta, bins) = TestPut.client.get(key)

        assert {
                'i': [ "nanslkdl", 1, bytearray("asd;as[d'as;d", "utf-8") ],
                's': { "key": "asd';q;'1';" },
                'b': 1234,
                'l': '!@#@#$QSDAsd;as'
        } == bins
        self.delete_keys.append( key )

    def test_put_with_no_parameters(self):

        """
            Invoke put() without any parameters.
        """
        with pytest.raises(TypeError) as typeError:
            res = TestPut.client.put()

        assert "Required argument 'key' (pos 1) not found" in typeError.value

    def test_put_without_record(self):

        """
            Invoke put() without any record data.
        """
        key = ('test', 'demo', 1)

        with pytest.raises(TypeError) as typeError:
            res = TestPut.client.put( key )

        assert "Required argument 'record' (pos 2) not found" in typeError.value

    def test_put_with_none_key(self):

        """
            Invoke put() with None as key.
        """
        rec = {
                "name" : "John"
                }

        with pytest.raises(Exception) as exception:
            res = TestPut.client.put(None, rec)

        assert exception.value[0] == -2
        assert exception.value[1] == 'key is invalid'

    def test_put_with_none_namespace_in_key(self):

        """
            Invoke put() with None namespace in key.
        """
        key = (None, "demo", 1)

        rec = { "name" : "Steve" }

        with pytest.raises(Exception) as exception:
            TestPut.client.put(key, rec)

        assert exception.value[0] == -2
        assert exception.value[1] == "namespace must be a string"

    def test_put_and_get_with_none_set_in_key(self):

        """
            Invoke put() with None set in key.
        """
        key = ("test", None, 1)

        rec = { "name" : "John" }

        assert 0 == TestPut.client.put(key, rec)

        _, _, bins = TestPut.client.get(key)

        assert { "name" : "John" } == bins

        self.delete_keys.append(key)

    def test_put_with_none_primary_key_in_key(self):

        """
            Invoke put() with None primary key in key.
        """
        key = ("test", "demo", None)

        rec = { "name": "John" }

        with pytest.raises(Exception) as exception:
            TestPut.client.put(key, rec)

        assert exception.value[0] == -2
        assert exception.value[1] == "either key or digest is required"

    def test_put_with_string_type_record(self):

        """
            Invoke put() with string typed record.
        """
        key = ('test', 'demo', 15)

        rec = "Name : John"

        res = TestPut.client.put( key, rec )

        assert res == 0
        key, meta, bins = TestPut.client.get( key )

        assert bins == None

    def test_put_with_wrong_ns_and_set(self):

        """
            Invoke put() with wrong ns and set.
        """
        key = ('demo', 'test', 1)

        rec = {
                'a' : ['!@#!#$%#', bytearray('ASD@#$AR#$@#ERQ#', 'utf-8')]
                }

        with pytest.raises(Exception) as exception:
            res = TestPut.client.put( key, rec )

        assert exception.value[0] == 20
        assert exception.value[1] == 'AEROSPIKE_ERR_NAMESPACE_NOT_FOUND'

    def test_put_with_nonexistent_namespace(self):

        """
            Invoke put() with non-existent namespace.
        """
        key = ('test1', 'demo', 1)

        rec = {
                'i': 'asdadasd'
                }
        with pytest.raises(Exception) as exception:
            res = TestPut.client.put( key, rec )

        assert exception.value[0] == 20
        assert exception.value[1] == 'AEROSPIKE_ERR_NAMESPACE_NOT_FOUND'

    def test_put_with_nonexistent_set(self):

        """
            Invoke put() with non-existent set.
        """
        key = ('test', 'unknown_set', 1)

        rec = {
                'a': { 'k': [bytearray("askluy3oijs", "utf-8")] }
                }

        res = TestPut.client.put( key, rec )

        assert res == 0

        (key, meta, bins) = TestPut.client.get(key)

        assert bins == {'a': {'k': [bytearray(b'askluy3oijs')]}}

        self.delete_keys.append( key )

    def test_put_boolean_record(self):

        """
            Invoke put() for boolean data record.
        """
        key = ('test', 'demo', 1)

        rec = {
                "is_present": False
                }

        res = TestPut.client.put( key, rec )

        assert res == 0

        (key , meta, bins) = TestPut.client.get(key)

        assert bins == {"is_present": False}
        self.delete_keys.append( key )
    """ 
    def test_put_unicode_string(self):
            #Invoke put() for unicode record.
        key = ('test', 'demo', 1)

        rec = {
                "unicode_string": u"\ud83d\ude04"
        }

        res = TestPut.client.put( key, rec )

        assert res == 0

        (key , meta, bins) = TestPut.client.get(key)
        assert bins['unicode_string'] == u"\ud83d\ude04"
        self.delete_keys.append( key )
        #self.client.remove(key)
    
    def test_put_unicode_key(self):

        #    Invoke put() for unicode key.
        key = ('test', 'demo', u"\ud83d\ude04")
        rec = {
                "unicode_string": u"\ud83d\ude04"
        }

        res = TestPut.client.put( key, rec )

        assert res == 0

        (key , meta, bins) = TestPut.client.get(key)

        assert bins == rec

        key = ('test', 'demo', u"\ud83d\ude04")
        
        self.delete_keys.append( key )
    """

    def test_put_unicode_string_in_map(self):
            #Invoke put() for unicode record.
        key = ('test', 'demo', 1)

        rec = {'a': {u'aa': u'11'}, 'b': {u'bb': u'22'}}
        
        res = TestPut.client.put( key, rec )

        assert res == 0

        (key , meta, bins) = TestPut.client.get(key)
        assert bins == rec
        self.delete_keys.append( key )

    def test_put_unicode_string_in_list(self):
            #Invoke put() for unicode record.
        key = ('test', 'demo', 1)

        rec = {'a': [u'aa', u'bb', 1, u'bb', u'aa']}
        
        res = TestPut.client.put( key, rec )

        assert res == 0


        (key , meta, bins) = TestPut.client.get(key)

        assert bins == rec

        self.delete_keys.append( key )

    def test_put_unicode_string_in_key(self):
            #Invoke put() for unicode record.
        key = ('test', 'demo', u"bb")

        rec = { 'a': [u'aa', 2, u'aa', 4, u'cc', 3, 2, 1] } 
        res = TestPut.client.put( key, rec )

        assert res == 0


        (key , meta, bins) = TestPut.client.get(key)

        assert bins == rec

        self.delete_keys.append( key )

    """
    def test_put_with_float_data(self):

            #Invoke put() for float data record.
        key = ( 'test', 'demo', 1 )

        rec = {
                "pi" : 3.14
                }

        res = TestPut.client.put( key, rec )

        assert res == 0
        _, _, bins = TestPut.client.get( key )

        assert bins == None

        self.delete_keys.append( key )
        """

    def test_put_with_string_meta_and_string_policies(self):
        """
            Invoke put() for metadata and policies.
        """
        key = ('test', 'demo', 1)

        rec = {
                'i': 12
                }

        with pytest.raises(Exception) as exception:
            res = TestPut.client.put( key, rec, "meta", "policies")

        assert exception.value[0] == -2
        assert exception.value[1] == "policy must be a dict"

    def test_put_with_string_record_generation(self):

        """
            Invoke put() for a record with string data, metadata and ttl
        """
        key = ('test', 'demo', 1)

        rec = {
                "name" : "John"
                }
        meta = {
            'gen': 3,
            'ttl' :25000
        }
        policy = {
            'timeout': 1000
        }
        assert 0 == TestPut.client.put( key, rec, meta, policy )

        ( key, meta, bins) = TestPut.client.get(key)
        assert {"name": "John"} == bins
        assert meta['gen'] != None
        self.delete_keys.append( key )

    def test_put_with_generation_string(self):

        """
            Invoke put() for a record with generation as string
        """
        key = ('test', 'demo', 1)

        rec = {
                "name" : "John"
                }
        meta = {
            'gen': "wrong",
            'ttl' :25000
        }
        policy = {
            'timeout': 1000
        }

        with pytest.raises(Exception) as exception:
            TestPut.client.put( key, rec, meta, policy )

        assert exception.value[0] == -2
        assert exception.value[1] == "Generation should be an int or long"

        #self.delete_keys.append( key )

    def test_put_with_ttl_string(self):

        """
            Invoke put() for a record with ttl as string
        """
        key = ('test', 'demo', 1)

        rec = {
                "name" : "John"
                }
        meta = {
            'gen': 3,
            'ttl': "25000"
        }
        policy = {
            'timeout': 1000
        }
        with pytest.raises(Exception) as exception:
            TestPut.client.put( key, rec, meta, policy )

        assert exception.value[0] == -2
        assert exception.value[1] == "Ttl should be an int or long"

        #self.delete_keys.append( key )

    def test_put_with_generation_bool(self):

        """
            Invoke put() for a record with generation as boolean.
        """
        key = ('test', 'demo', 1)

        rec = {
                "name" : "John"
                }
        meta = {
            'gen': True,
            'ttl' :25000
        }
        policy = {
            'timeout': 1000
        }
        assert 0 == TestPut.client.put( key, rec, meta, policy )

        ( key, meta, bins) = TestPut.client.get(key)
        assert {"name": "John"} == bins
        assert meta['gen'] != None
        self.delete_keys.append( key )

    def test_put_with_ttl_boolean(self):

        """
            Invoke put() for a record with ttl as boolean.
        """
        key = ('test', 'demo', 1)

        rec = {
                "name" : "John"
                }
        meta = {
            'gen': 3,
            'ttl': True
        }
        policy = {
            'timeout': 1000
        }
        assert 0 == TestPut.client.put( key, rec, meta, policy )

        ( key, meta, bins) = TestPut.client.get(key)
        assert {"name": "John"} == bins
        assert meta['gen'] != None
        self.delete_keys.append( key )

    def test_put_with_policy_timeout_string(self):

        """
            Invoke put() for a record with policy timeout as string
        """
        key = ('test', 'demo', 1)

        rec = {
                "name" : "John"
                }
        meta = {
            'gen': 3,
            'ttl' :25000
        }
        policy = {
            'timeout': "1000"
        }
        with pytest.raises(Exception) as exception:
            TestPut.client.put( key, rec, meta, policy )

        assert exception.value[0] == -2
        assert exception.value[1] == 'timeout is invalid'

    def test_put_with_policy_gen_EQ_positive(self):

        """
            Invoke put() for a record with generation as EQ positive
        """
        key = ('test', 'demo', 1)

        rec = {
                "name" : "John"
                }
        meta = {
            'gen': 2,
            'ttl' :25000
        }
        policy = {
            'timeout': 1000
        }
        assert 0 == TestPut.client.put( key, rec, meta, policy )

        ( key, meta, bins) = TestPut.client.get(key)

        assert {"name": "John"} == bins
        gen =  meta['gen']
        rec = {
                "name" : "Smith"
        }
        policy = {
            'timeout': 1000,
            'gen': aerospike.POLICY_GEN_EQ,
            'commit_level': aerospike.POLICY_COMMIT_LEVEL_ALL
        }
        meta = {
            'gen': gen
        }

        assert 0 == TestPut.client.put( key, rec, meta, policy )

        ( key, meta, bins) = TestPut.client.get(key)
        assert {"name": "Smith"} == bins

        self.delete_keys.append( key )

    def test_put_with_policy_gen_EQ_less(self):

        """
            Invoke put() for a record with generation as EQ less
        """
        key = ('test', 'demo', 1)

        rec = {
                "name" : "John"
                }
        meta = {
            'gen': 2,
            'ttl' :25000
        }
        policy = {
            'timeout': 1000
        }
        assert 0 == TestPut.client.put( key, rec, meta, policy )

        ( key, meta, bins) = TestPut.client.get(key)

        assert {"name": "John"} == bins
        gen =  meta['gen']
        rec = {
                "name" : "Smith"
        }
        policy = {
            'timeout': 1000,
            'gen': aerospike.POLICY_GEN_EQ
        }
        meta = {
            'gen': 10
        }

        with pytest.raises(Exception) as exception:
            TestPut.client.put( key, rec, meta, policy )

        assert exception.value[0] == 3
        assert exception.value[1] == 'AEROSPIKE_ERR_RECORD_GENERATION'
        
        ( key, meta, bins) = TestPut.client.get(key)
        assert {"name": "John"} == bins

        self.delete_keys.append( key )

    def test_put_with_policy_exists_create_negative(self):

        """
            Invoke put() for a record with all policies.
        """
        key = ('test', 'demo', 1)

        rec = {
                "name" : "John"
                }
        meta = {
            'gen': 2,
            'ttl' :25000
        }
        policy = {
            'timeout': 1000,
            'gen': aerospike.POLICY_GEN_IGNORE,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND,
        }
        assert 0 == TestPut.client.put( key, rec, meta, policy )

        ( key, meta, bins) = TestPut.client.get(key)

        assert {"name": "John"} == bins

        rec = {
                "name" : "Smith"
        }
        policy = {
            'timeout': 1000,
            'exists': aerospike.POLICY_EXISTS_CREATE
        }
        meta = {
            'gen': 2
        }

        with pytest.raises(Exception) as exception:
            TestPut.client.put( key, rec, meta, policy )

        assert exception.value[0] == 5
        assert exception.value[1] == 'AEROSPIKE_ERR_RECORD_EXISTS'

        ( key, meta, bins) = TestPut.client.get(key)
        assert {"name": "John"} == bins

        self.delete_keys.append( key )

    def test_put_with_policy_exists_create_positive(self):

        """
            Invoke put() for a record with all policies.
        """
        key = ('test', 'demo', 1)

        rec = {
                "name" : "Smith"
                }
        meta = {
            'gen': 2,
            'ttl' :25000
        }
        policy = {
            'timeout': 1000,
            'exists': aerospike.POLICY_EXISTS_CREATE,
            'gen': aerospike.POLICY_GEN_IGNORE,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND,
            'commit_level': aerospike.POLICY_COMMIT_LEVEL_MASTER
        }
        assert 0 == TestPut.client.put( key, rec, meta, policy )
        
        ( key, meta, bins) = TestPut.client.get(key)
        assert {"name": "Smith"} == bins

        self.delete_keys.append( key )

    def test_put_with_policy_exists_replace_negative(self):

        """
            Invoke put() for a record with replace policy negative.
        """
        key = ('test', 'demo', 1)

        rec = {
                "name" : "John"
                }
        meta = {
            'gen': 2,
            'ttl' :25000
        }
        policy = {
            'timeout': 1000,
            'exists': aerospike.POLICY_EXISTS_REPLACE,
            'gen': aerospike.POLICY_GEN_IGNORE,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND
        }
        with pytest.raises(Exception) as exception:
            assert 0 == TestPut.client.put( key, rec, meta, policy )

        assert exception.value[0] == 2
        assert exception.value[1] == 'AEROSPIKE_ERR_RECORD_NOT_FOUND'

        #self.delete_keys.append( key )

    def test_put_with_policy_exists_create_or_replace_positive(self):

        """
            Invoke put() for a record with create or replace policy positive.
        """
        key = ('test', 'demo', 1)

        rec = {
                "name" : "Smith"
                }
        meta = {
            'gen': 2,
            'ttl' :25000
        }
        policy = {
            'timeout': 1000,
            'exists': aerospike.POLICY_EXISTS_CREATE_OR_REPLACE,
            'gen': aerospike.POLICY_GEN_IGNORE,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND
        }
        assert 0 == TestPut.client.put( key, rec, meta, policy )

        ( key, meta, bins) = TestPut.client.get(key)
        assert {"name": "Smith"} == bins

        self.delete_keys.append( key )

    def test_put_with_policy_exists_ignore(self):

        """
            Invoke put() for a record with ignore.
        """
        key = ('test', 'demo', 1)

        rec = {
                "name" : "Smith"
                }
        meta = {
            'gen': 2,
            'ttl' :25000
        }
        policy = {
            'timeout': 1000,
            'exists': aerospike.POLICY_EXISTS_IGNORE,
            'gen': aerospike.POLICY_GEN_IGNORE,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND
        }
        assert 0 == TestPut.client.put( key, rec, meta, policy )

        ( key, meta, bins) = TestPut.client.get(key)
        assert {"name": "Smith"} == bins

        self.delete_keys.append( key )

    def test_put_with_policy_replace_positive(self):

        """
            Invoke put() for a record with replace positive.
        """
        key = ('test', 'demo', 1)

        rec = {
                "name" : "John"
                }
        meta = {
            'gen': 2,
            'ttl' :25000
        }
        policy = {
            'timeout': 1000,
            'gen': aerospike.POLICY_GEN_IGNORE,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND,
        }
        assert 0 == TestPut.client.put( key, rec, meta, policy )

        ( key, meta, bins) = TestPut.client.get(key)

        assert {"name": "John"} == bins

        rec = {
                "name" : "Smith"
                }
        meta = {
            'gen': 2,
            'ttl' :25000
        }
        policy = {
            'timeout': 1000,
            'exists': aerospike.POLICY_EXISTS_REPLACE
        }
        assert 0 == TestPut.client.put( key, rec, meta, policy )

        ( key, meta, bins) = TestPut.client.get(key)

        assert {"name": "Smith"} == bins
        self.delete_keys.append( key )

    def test_put_with_policy_exists_update_positive(self):

        """
            Invoke put() for a record with all policies.
        """
        key = ('test', 'demo', 1)

        rec = {
                "name" : "John"
                }
        meta = {
            'gen': 2,
            'ttl' :25000
        }
        policy = {
            'timeout': 1000,
            'gen': aerospike.POLICY_GEN_IGNORE,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND,
        }
        assert 0 == TestPut.client.put( key, rec, meta, policy )

        ( key, meta, bins) = TestPut.client.get(key)

        assert {"name": "John"} == bins

        rec = {
                "name" : "Smith"
                }
        meta = {
            'gen': 2,
            'ttl' :25000
        }
        policy = {
            'timeout': 1000,
            'exists': aerospike.POLICY_EXISTS_UPDATE
        }
        assert 0 == TestPut.client.put( key, rec, meta, policy )

        ( key, meta, bins) = TestPut.client.get(key)

        assert {"name": "Smith"} == bins
        self.delete_keys.append( key )

    def test_put_with_policy_exists_update_negative(self):

        """
            Invoke put() for a record with update policy negative.
        """
        key = ('test', 'demo', 1)

        rec = {
                "name" : "John"
                }
        meta = {
            'gen': 2,
            'ttl' :25000
        }
        policy = {
            'timeout': 1000,
            'exists': aerospike.POLICY_EXISTS_UPDATE,
            'gen': aerospike.POLICY_GEN_IGNORE,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND
        }
        with pytest.raises(Exception) as exception:
            assert 0 == TestPut.client.put( key, rec, meta, policy )

        assert exception.value[0] == 2
        assert exception.value[1] == 'AEROSPIKE_ERR_RECORD_NOT_FOUND'

        #self.delete_keys.append( key )
    def test_put_with_policy_gen_GT_lesser(self):

        """
            Invoke put() for a record with generation as GT lesser
        """
        key = ('test', 'demo', 1)

        rec = {
                "name" : "John"
                }
        meta = {
            'gen': 2,
            'ttl' :25000
        }
        policy = {
            'timeout': 1000
        }
        assert 0 == TestPut.client.put( key, rec, meta, policy )

        ( key, meta, bins) = TestPut.client.get(key)

        assert {"name": "John"} == bins
        gen =  meta['gen']
        rec = {
                "name" : "Smith"
        }
        policy = {
            'timeout': 1000,
            'gen': aerospike.POLICY_GEN_GT
        }
        meta = {
            'gen': gen
        }

        with pytest.raises(Exception) as exception:
            TestPut.client.put( key, rec, meta, policy )

        assert exception.value[0] == 3
        assert exception.value[1] == 'AEROSPIKE_ERR_RECORD_GENERATION'

        ( key, meta, bins) = TestPut.client.get(key)
        assert {"name": "John"} == bins

        self.delete_keys.append( key )

    def test_put_with_policy_gen_GT_positive(self):

        """
            Invoke put() for a record with generation as GT positive
        """
        key = ('test', 'demo', 1)

        rec = {
                "name" : "John"
                }
        meta = {
            'gen': 2,
            'ttl' :25000
        }
        policy = {
            'timeout': 1000
        }
        assert 0 == TestPut.client.put( key, rec, meta, policy )

        ( key, meta, bins) = TestPut.client.get(key)

        assert {"name": "John"} == bins
        gen =  meta['gen']
        assert gen == 1
        rec = {
                "name" : "Smith"
        }
        policy = {
            'timeout': 1000,
            'gen': aerospike.POLICY_GEN_GT
        }
        meta = {
            'gen': gen + 5
        }

        TestPut.client.put( key, rec, meta, policy )

        ( key, meta, bins) = TestPut.client.get(key)
        assert {"name": "Smith"} == bins

        self.delete_keys.append( key )

    def test_put_with_policy_gen_ignore(self):

        """
            Invoke put() for a record with generation as gen_ignore
        """
        key = ('test', 'demo', 1)

        rec = {
                "name" : "John"
                }
        meta = {
            'gen': 2,
            'ttl' :25000
        }
        policy = {
            'timeout': 1000
        }
        assert 0 == TestPut.client.put( key, rec, meta, policy )

        ( key, meta, bins) = TestPut.client.get(key)

        assert {"name": "John"} == bins

        gen =  meta['gen']
        rec = {
                "name" : "Smith"
        }
        policy = {
            'timeout': 1000,
            'gen': aerospike.POLICY_GEN_IGNORE
        }
        meta = {
            'gen': gen
        }

        assert 0 == TestPut.client.put( key, rec, meta, policy )

        ( key, meta, bins) = TestPut.client.get(key)
        assert {"name": "Smith"} == bins

        self.delete_keys.append( key )
