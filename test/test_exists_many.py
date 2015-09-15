# -*- coding: utf-8 -*-

import pytest
import sys
from test_base_class import TestBaseClass
try:
    from collections import Counter
except ImportError:
    from counter26 import Counter

aerospike = pytest.importorskip("aerospike")
try:
    from aerospike.exception import *
except:
    print "Please install aerospike python client."
    sys.exit(1)

class TestExistsMany(TestBaseClass):
    def setup_class(cls):
        """
        Setup method.
        """
        hostlist, user, password = TestBaseClass.get_hosts()
        config = {'hosts': hostlist}
        if user == None and password == None:
            TestExistsMany.client = aerospike.client(config).connect()
        else:
            TestExistsMany.client = aerospike.client(config).connect(user,
                                                                     password)

    def teardown_class(cls):
        TestExistsMany.client.close()

    def setup_method(self, method):

        self.keys = []

        for i in xrange(5):
            key = ('test', 'demo', i)
            rec = {'name': 'name%s' % (str(i)), 'age': i}
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

        records = TestExistsMany.client.exists_many(self.keys)

        assert type(records) == list
        assert len(records) == 5

    def test_exists_many_with_proper_parameters(self):

        records = TestExistsMany.client.exists_many(self.keys, {'timeout': 1200})

        assert type(records) == list
        assert len(records) == 5
        assert Counter([x[0][2] for x in records]) == Counter([0, 1, 2, 3,
            4])

    def test_exists_many_with_none_policy(self):

        records = TestExistsMany.client.exists_many(self.keys, None)

        assert type(records) == list
        assert len(records) == 5
        assert Counter([x[0][2] for x in records]) == Counter([0, 1, 2, 3,
            4])

    def test_exists_many_with_none_keys(self):

        try:
            TestExistsMany.client.exists_many( None, {} )

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Keys should be specified as a list or tuple."

    def test_exists_many_with_non_existent_keys(self):

        self.keys.append(('test', 'demo', 'some_key'))

        records = TestExistsMany.client.exists_many(self.keys)

        assert type(records) == list
        assert len(records) == 6
        assert Counter([x[0][2] for x in records]) == Counter([0, 1, 2, 3,
            4, 'some_key'])
        for x in records:
            if x[0][2] == 'some_key':
                assert x[1] == None

    def test_exists_many_with_all_non_existent_keys(self):

        keys = [('test', 'demo', 'key')]

        records = TestExistsMany.client.exists_many(keys)

        assert len(records) == 1
        for x in records:
            if x[0][2] == 'key':
                assert x[1] == None

    def test_exists_many_with_invalid_key(self):

        try:
            records = TestExistsMany.client.exists_many( "key" )

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Keys should be specified as a list or tuple."

    def test_exists_many_with_invalid_timeout(self):

        policies = { 'timeout' : 0.2 }
        try:
            records = TestExistsMany.client.exists_many(self.keys, policies)

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "timeout is invalid"

    def test_exists_many_with_initkey_as_digest(self):

        keys = []
        key = ("test", "demo", None, bytearray("asd;as[d'as;djk;uyfl"))
        rec = {'name': 'name1', 'age': 1}
        TestExistsMany.client.put(key, rec)
        keys.append(key)

        key = ("test", "demo", None, bytearray("ase;as[d'as;djk;uyfl"))
        rec = {'name': 'name2', 'age': 2}
        TestExistsMany.client.put(key, rec)
        keys.append(key)

        records = TestExistsMany.client.exists_many(keys)

        for key in keys:
            TestExistsMany.client.remove(key)

        assert type(records) == list
        assert len(records) == 2
        i = 0
        for x in records:
            if i:
                assert x[0][3] == bytearray(b"ase;as[d'as;djk;uyfl")
            else:
                assert x[0][3] == bytearray(b"asd;as[d'as;djk;uyfl")
            i += 1

    def test_exists_many_with_non_existent_keys_in_middle(self):

        self.keys.append(('test', 'demo', 'some_key'))

        for i in xrange(15, 20):
            key = ('test', 'demo', i)
            rec = {'name': 'name%s' % (str(i)), 'age': i}
            TestExistsMany.client.put(key, rec)
            self.keys.append(key)

        records = TestExistsMany.client.exists_many(self.keys)

        for i in xrange(15, 20):
            key = ('test', 'demo', i)
            TestExistsMany.client.remove(key)

        assert type(records) == list
        assert len(records) == 11
        assert Counter([x[0][2] for x in records]) == Counter([0, 1, 2, 3,
            4, 'some_key', 15, 16, 17, 18, 19])
        for x in records:
            if x[0][2] == 'some_key':
                assert x[1] == None

    def test_exists_many_with_proper_parameters_without_connection(self):

        config = {'hosts': [('127.0.0.1', 3000)]}
        client1 = aerospike.client(config)

        try:
            records = client1.exists_many( self.keys, { 'timeout': 20 } )

        except ClusterError as exception:
            assert exception.code == 11L
            assert exception.msg == 'No connection to aerospike cluster'
