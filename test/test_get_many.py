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

class TestGetMany(TestBaseClass):
    def setup_class(cls):
        """
        Setup method.
        """
        hostlist, user, password = TestBaseClass.get_hosts()
        config = {'hosts': hostlist}
        if user == None and password == None:
            TestGetMany.client = aerospike.client(config).connect()
        else:
            TestGetMany.client = aerospike.client(config).connect(user,
                                                                  password)

    def teardown_class(cls):
        TestGetMany.client.close()

    def setup_method(self, method):
        self.keys = []

        for i in xrange(5):
            key = ('test', 'demo', i)
            rec = {'name': 'name%s' % (str(i)), 'age': i}
            TestGetMany.client.put(key, rec)
            self.keys.append(key)
        key = ('test', 'demo', 'float_value')
        TestGetMany.client.put(key, {"float_value": 4.3})
        self.keys.append(key)

    def teardown_method(self, method):
        """
        Teardown method.
        """
        for i in xrange(5):
            key = ('test', 'demo', i)
            TestGetMany.client.remove(key)
        key = ('test', 'demo', 'float_value')
        TestGetMany.client.remove(key)

    def test_get_many_without_any_parameter(self):

        with pytest.raises(TypeError) as typeError:
            TestGetMany.client.get_many()

        assert "Required argument 'keys' (pos 1) not found" in typeError.value

    def test_get_many_without_policy(self):

        records = TestGetMany.client.get_many(self.keys)

        assert type(records) == list
        assert len(records) == 6

    def test_get_many_with_proper_parameters(self):

        records = TestGetMany.client.get_many(self.keys, {'timeout': 30})

        assert type(records) == list
        assert len(records) == 6
        assert Counter([x[0][2] for x in records]) == Counter([0, 1, 2, 3,
            4, 'float_value'])
        assert records[5][2] == {'float_value': 4.3}

    def test_get_many_with_none_policy(self):

        records = TestGetMany.client.get_many(self.keys, None)

        assert type(records) == list
        assert len(records) == 6
        assert Counter([x[0][2] for x in records]) == Counter([0, 1, 2, 3,
            4, 'float_value'])
        assert records[5][2] == {'float_value': 4.3}

    def test_get_many_with_none_keys(self):

        try:
            TestGetMany.client.get_many( None, {} )

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Keys should be specified as a list or tuple."

    def test_get_many_with_non_existent_keys(self):

        self.keys.append(('test', 'demo', 'non-existent'))

        records = TestGetMany.client.get_many(self.keys)

        assert type(records) == list
        assert len(records) == 7
        assert Counter([x[0][2] for x in records]) == Counter([0, 1, 2, 3,
            4, 'non-existent', 'float_value'])
        for x in records:
            if x[0][2] == 'non-existent':
                assert x[2] == None

    def test_get_many_with_all_non_existent_keys(self):

        keys = [('test', 'demo', 'key')]

        records = TestGetMany.client.get_many(keys)

        assert len(records) == 1
        assert records == [(('test', 'demo', 'key',
            bytearray(b';\xd4u\xbd\x0cs\xf2\x10\xb6~\xa87\x930\x0e\xea\xe5v(]')), None, None)]

    def test_get_many_with_invalid_key(self):

        try:
            records = TestGetMany.client.get_many( "key" )

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Keys should be specified as a list or tuple."

    def test_get_many_with_invalid_timeout(self):

        policies = { 'timeout' : 0.2 }
        try:
            records = TestGetMany.client.get_many(self.keys, policies)

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "timeout is invalid"

    def test_get_many_with_initkey_as_digest(self):

        keys = []
        key = ("test", "demo", None, bytearray("asd;as[d'as;djk;uyfl"))
        rec = {'name': 'name1', 'age': 1}
        TestGetMany.client.put(key, rec)

        keys.append(key)

        key = ("test", "demo", None, bytearray("ase;as[d'as;djk;uyfl"))
        rec = {'name': 'name2', 'age': 2}
        TestGetMany.client.put(key, rec)

        keys.append(key)

        records = TestGetMany.client.get_many(keys)

        for key in keys:
            TestGetMany.client.remove(key)

        assert type(records) == list
        assert len(records) == 2
        i = 0
        for x in records:
            if i:
                assert x[0][3] == bytearray(b"ase;as[d'as;djk;uyfl")
            else:
                assert x[0][3] == bytearray(b"asd;as[d'as;djk;uyfl")
            i = i+1

    def test_get_many_with_non_existent_keys_in_middle(self):

        self.keys.append(('test', 'demo', 'some_key'))

        for i in xrange(15, 20):
            key = ('test', 'demo', i)
            rec = {'name': 'name%s' % (str(i)), 'age': i}
            TestGetMany.client.put(key, rec)
            self.keys.append(key)

        records = TestGetMany.client.get_many(self.keys)

        for i in xrange(15, 20):
            key = ('test', 'demo', i)
            TestGetMany.client.remove(key)

        assert type(records) == list
        assert len(records) == 12
        assert Counter([x[0][2] for x in records]) == Counter([0, 1, 2, 3,
            4, 'some_key', 15, 16, 17, 18, 19, 'float_value'])
        for x in records:
            if x[0][2] == 'some_key':
                assert x[2] == None

    def test_get_many_with_proper_parameters_without_connection(self):
        config = {'hosts': [('127.0.0.1', 3000)]}
        client1 = aerospike.client(config)
        try:
            records = client1.get_many( self.keys, { 'timeout': 20 } )

        except ClusterError as exception:
            assert exception.code == 11L
            assert exception.msg == 'No connection to aerospike cluster'
