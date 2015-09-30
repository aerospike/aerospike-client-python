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

class TestSelectMany(object):
    def setup_class(cls):
        """
        Setup method.
        """
        hostlist, user, password = TestBaseClass.get_hosts()
        config = {'hosts': hostlist}
        if user == None and password == None:
            TestSelectMany.client = aerospike.client(config).connect()
        else:
            TestSelectMany.client = aerospike.client(config).connect(user,
                                                                     password)

    def teardown_class(cls):
        TestSelectMany.client.close()

    def setup_method(self, method):
        self.keys = []

        for i in xrange(5):
            key = ('test', 'demo', i)
            rec = {
                'title': 'Mr.',
                'name': 'name%s' % (str(i)),
                'age': i,
                'addr': 'Minisota',
                'country': 'USA'
            }
            TestSelectMany.client.put(key, rec)
            self.keys.append(key)
        key = ('test', 'demo', 'float_value')
        TestSelectMany.client.put(key, {"float_value": 4.3})
        self.keys.append(key)

    def teardown_method(self, method):
        """
        Teardown method.
        """
        for i in xrange(5):
            key = ('test', 'demo', i)
            TestSelectMany.client.remove(key)
        key = ('test', 'demo', 'float_value')
        TestSelectMany.client.remove(key)

    def test_select_many_without_any_parameter(self):

        with pytest.raises(TypeError) as typeError:
            TestSelectMany.client.select_many()

        assert "Required argument 'keys' (pos 1) not found" in typeError.value

    def test_select_many_without_policy(self):

        filter_bins = ['title', 'name']
        records = TestSelectMany.client.select_many(self.keys, filter_bins)

        assert type(records) == list
        assert len(records) == 6
        for k in records:
            bins = k[2].keys()
            assert set(bins).intersection(set(filter_bins)) == set(bins)

    def test_select_many_with_proper_parameters(self):

        filter_bins = ['title', 'name', 'float_value']
        records = TestSelectMany.client.select_many(self.keys, filter_bins,
                                                    {'timeout': 50})

        assert type(records) == list
        assert len(records) == 6
        assert Counter([x[0][2] for x in records]) == Counter([0, 1, 2, 3,
            4, 'float_value'])
        for k in records:
            bins = k[2].keys()
            assert set(bins).intersection(set(filter_bins)) == set(bins)

    def test_select_many_with_none_policy(self):

        filter_bins = ['name']
        records = TestSelectMany.client.select_many(self.keys, filter_bins,
                                                    None)

        assert type(records) == list
        assert len(records) == 6
        assert Counter([x[0][2] for x in records]) == Counter([0, 1, 2, 3,
            4, 'float_value'])
        for k in records:
            bins = k[2].keys()
            assert set(bins).intersection(set(filter_bins)) == set(bins)

    def test_select_many_with_none_keys(self):

        try:
            TestSelectMany.client.select_many( None, [], {} )

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Keys should be specified as a list or tuple."
    def test_select_many_with_non_existent_keys(self):

        self.keys.append(('test', 'demo', 'non-existent'))

        filter_bins = ['title', 'name', 'addr']
        records = TestSelectMany.client.select_many(self.keys, filter_bins,
                                                    {'timeout': 1000})

        assert type(records) == list
        assert len(records) == 7
        assert Counter([x[0][2] for x in records]) == Counter([0, 1, 2, 3,
            4, 'non-existent', 'float_value'])
        for k in records:
            if k[0][2] == 'non-existent':
                assert k[2] == None
                continue
            bins = k[2].keys()
            assert set(bins).intersection(set(filter_bins)) == set(bins)
    def test_select_many_with_all_non_existent_keys(self):

        keys = [('test', 'demo', 'key')]

        filter_bins = ['title', 'name', 'country']
        records = TestSelectMany.client.select_many(keys, filter_bins)

        assert len(records) == 1
        assert records == [(('test', 'demo', 'key',
            bytearray(b';\xd4u\xbd\x0cs\xf2\x10\xb6~\xa87\x930\x0e\xea\xe5v(]')), None, None)]

    def test_select_many_with_invalid_key(self):

        try:
            records = TestSelectMany.client.select_many( "key", [] )

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Keys should be specified as a list or tuple."

    def test_select_many_with_invalid_timeout(self):

        policies = { 'timeout' : 0.2 }
        try:
            records = TestSelectMany.client.select_many(self.keys, [], policies)

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "timeout is invalid"

    def test_select_many_with_initkey_as_digest(self):

        keys = []
        key = ("test", "demo", None, bytearray("asd;as[d'as;djk;uyfl"))
        rec = {'name': 'name1', 'age': 1}
        TestSelectMany.client.put(key, rec)
        keys.append(key)

        key = ("test", "demo", None, bytearray("ase;as[d'as;djk;uyfl"))
        rec = {'name': 'name2', 'age': 2}
        TestSelectMany.client.put(key, rec)
        keys.append(key)

        records = TestSelectMany.client.select_many(keys, [u'name'])

        for key in keys:
            TestSelectMany.client.remove(key)

        assert type(records) == list
        assert len(records) == 2
        i = 0
        for x in records:
            if i:
                assert x[0][3] == bytearray(b"ase;as[d'as;djk;uyfl")
            else:
                assert x[0][3] == bytearray(b"asd;as[d'as;djk;uyfl")
            i += 1

    def test_select_many_with_non_existent_keys_in_middle(self):   
        self.keys.append(('test', 'demo', 'some_key'))

        for i in xrange(15, 20):
            key = ('test', 'demo', i)
            rec = {
                'name': 'name%s' % (str(i)),
                'age': i,
                'position': 'Sr. Engineer'
            }
            TestSelectMany.client.put(key, rec)
            self.keys.append(key)

        filter_bins = ['title', 'name', 'position']
        records = TestSelectMany.client.select_many(self.keys, filter_bins)

        for i in xrange(15, 20):
            key = ('test', 'demo', i)
            TestSelectMany.client.remove(key)

        assert type(records) == list
        assert len(records) == 12
        assert Counter([x[0][2] for x in records]) == Counter([0, 1, 2, 3, 4, 'some_key', 15, 16, 17, 18, 19,
                'float_value'])
        for k in records:
            if k[0][2] == 'some_key':
                assert k[2] == None
                continue
            bins = k[2].keys()
            assert set(bins).intersection(set(filter_bins)) == set(bins)

    def test_select_many_with_unicode_bins(self):

        filter_bins = [u'title', u'name', 'country', u'addr']
        records = TestSelectMany.client.select_many(self.keys, filter_bins)

        assert type(records) == list
        assert len(records) == 6
        for k in records:
            bins = k[2].keys()
            assert set(bins).intersection(set(filter_bins)) == set(bins)

    def test_select_many_with_empty_bins_list(self):

        records = TestSelectMany.client.select_many(self.keys, [])

        assert type(records) == list
        assert len(records) == 6

    def test_select_many_with_proper_parameters_without_connection(self):

        config = {'hosts': [('127.0.0.1', 3000)]}
        client1 = aerospike.client(config)

        filter_bins = ['title', 'name']

        try:
            records = client1.select_many( self.keys, filter_bins, { 'timeout':
                20} )

        except ClusterError as exception:
            assert exception.code == 11L
            assert exception.msg == 'No connection to aerospike cluster'
