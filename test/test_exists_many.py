# -*- coding: utf-8 -*-

import pytest
import sys

try:
    import aerospike
except:
    print "Please install aerospike python client."
    sys.exit(1)

class TestExistsMany(object):
    def setup_class(cls):
        """
        Setup method.
        """
        config = {
                'hosts': [('127.0.0.1', 3000)]
                }
        TestExistsMany.client = aerospike.client(config).connect()

    def teardwon_class(cls):
        TestExistsMany.client.close()

    def setup_method(self, method):

        self.keys = []

        for i in xrange(5):
            key = ('test', 'demo', i)
            rec = {
                    'name' : 'name%s' % (str(i)),
                    'age'  : i
                    }
            TestExistsMany.client.put(key, rec)
            self.keys.append(key)


    def teardown_method(self, method):

        """
        Teardown method.
        """
        for i in xrange(5):
            key = ('test', 'demo', i)
            TestExistsMany.client.remove(key)

    def test_exists_many_without_any_parameter(self):

        with pytest.raises(TypeError) as typeError:
            TestExistsMany.client.exists_many()

        assert "Required argument 'keys' (pos 1) not found" in typeError.value

    def test_exists_many_without_policy(self):

        records = TestExistsMany.client.exists_many( self.keys )

        assert type(records) == dict
        assert len(records.keys()) == 5

    def test_exists_many_with_proper_parameters(self):

        records = TestExistsMany.client.exists_many( self.keys, { 'timeout': 3 } )

        assert type(records) == dict
        assert len(records.keys()) == 5
        assert records.keys() == [0, 1, 2, 3, 4]

    def test_exists_many_with_none_policy(self):

        records = TestExistsMany.client.exists_many( self.keys, None )

        assert type(records) == dict
        assert len(records.keys()) == 5
        assert records.keys() == [0, 1, 2, 3, 4]

    def test_exists_many_with_none_keys(self):

        with pytest.raises(Exception) as exception:
            TestExistsMany.client.exists_many( None, {} )

        assert exception.value[0] == -1
        assert exception.value[1] == "Keys should be specified as a list or tuple."

    def test_exists_many_with_non_existent_keys(self):

        self.keys.append( ('test', 'demo', 10) )

        records = TestExistsMany.client.exists_many( self.keys )

        assert type(records) == dict
        assert len(records.keys()) == 5
        assert records.keys() == [0, 1, 2, 3, 4]

    def test_exists_many_with_all_non_existent_keys(self):

        keys = [( 'test', 'demo', 'key' )]

        records = TestExistsMany.client.exists_many( keys )

        assert len(records.keys()) == 0
        assert records == {}

    def test_exists_many_with_invalid_key(self):

        with pytest.raises(Exception) as exception:
            records = TestExistsMany.client.exists_many( "key" )

        assert exception.value[0] == -1
        assert exception.value[1] == "Keys should be specified as a list or tuple."

    def test_exists_many_with_invalid_timeout(self):

        policies = { 'timeout' : 0.2 }
        with pytest.raises(Exception) as exception:
            records = TestExistsMany.client.exists_many(self.keys, policies)

        assert exception.value[0] == -2
        assert exception.value[1] == "timeout is invalid"
    
    def test_exists_many_with_non_existent_keys_in_middle(self):

        self.keys.append( ('test', 'demo', 10) )

        for i in xrange(15,20):
            key = ('test', 'demo', i)
            rec = {
                    'name' : 'name%s' % (str(i)),
                    'age'  : i
                    }
            TestExistsMany.client.put(key, rec)
            self.keys.append(key)

        records = TestExistsMany.client.exists_many( self.keys )

        for i in xrange(15,20):
            key = ('test', 'demo', i)
            TestExistsMany.client.remove(key)

        assert type(records) == dict
        assert len(records.keys()) == 10
        assert records.keys() == [0, 1, 2, 3, 4, 15, 16, 17, 18, 19]
