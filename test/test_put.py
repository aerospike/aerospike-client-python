# -*- coding: utf-8 -*-

import pytest
import sys
import time
import marshal
from test_base_class import TestBaseClass

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
    from aerospike.exception import *
except:
    print "Please install aerospike python client."
    sys.exit(1)


class TestPut(TestBaseClass):
    def setup_class(cls):
        """
            Setup class
        """
        hostlist, user, password = TestBaseClass.get_hosts()
        config = {"hosts": hostlist}
        if user == None and password == None:
            TestPut.client = aerospike.client(config).connect()
        else:
            TestPut.client = aerospike.client(config).connect(user, password)
        TestPut.skip_old_server = True
        versioninfo = TestPut.client.info('version')
        for keys in versioninfo:
            for value in versioninfo[keys]:
                if value != None:
                    versionlist = value[value.find("build") + 6:value.find("\n")].split(".")
                    if int(versionlist[0]) >= 3 and int(versionlist[1]) >= 6:
                        TestPut.skip_old_server = False

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

        bins = {"name": "John"}

        assert 0 == TestPut.client.put(key, bins)

        (key, meta, bins) = TestPut.client.get(key)

        assert {"name": "John"} == bins
        self.delete_keys.append(key)

    def test_put_with_multiple_bins(self):
        """
            Invoke put() with multiple bins and multiple types of data.
            Covers list, map, bytearray, integer.
        """
        key = ('test', 'demo', 1)

        bins = {
            'i': ["nanslkdl", 1, bytearray("asd;as[d'as;d", "utf-8")],
            's': {"key": "asd';q;'1';"},
            'b': 1234,
            'l': '!@#@#$QSDAsd;as'
        }

        assert 0 == TestPut.client.put(key, bins)
        (key, meta, bins) = TestPut.client.get(key)

        assert {
            'i': ["nanslkdl", 1, bytearray("asd;as[d'as;d", "utf-8")],
            's': {"key": "asd';q;'1';"},
            'b': 1234,
            'l': '!@#@#$QSDAsd;as'
        } == bins
        self.delete_keys.append(key)

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
            res = TestPut.client.put(key)

        assert "Required argument 'bins' (pos 2) not found" in typeError.value

    def test_put_with_none_key(self):
        """
            Invoke put() with None as key.
        """
        bins = {"name": "John"}

        try:
            res = TestPut.client.put(None, bins)

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == 'key is invalid'

    def test_put_with_none_namespace_in_key(self):
        """
            Invoke put() with None namespace in key.
        """
        key = (None, "demo", 1)

        bins = {"name": "Steve"}

        try:
            TestPut.client.put(key, bins)

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "namespace must be a string"

    def test_put_and_get_with_none_set_in_key(self):
        """
            Invoke put() with None set in key.
        """
        key = ("test", None, 1)

        bins = {"name": "John"}

        assert 0 == TestPut.client.put(key, bins)

        _, _, bins = TestPut.client.get(key)

        assert {"name": "John"} == bins

        self.delete_keys.append(key)

    def test_put_with_none_primary_key_in_key(self):
        """
            Invoke put() with None primary key in key.
        """
        key = ("test", "demo", None)

        bins = {"name": "John"}

        try:
            TestPut.client.put(key, bins)

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "either key or digest is required"

    def test_put_with_bytearray_primary_key(self):
        """
            Invoke put() with bytearray primary key in key.
        """
        key = ("test", "demo", bytearray("asd;as[d'as;d", "utf-8"))

        bins = {"name": "John"}

        TestPut.client.put(key, bins)

        (key, meta, bins) = TestPut.client.get(key)

        assert bins == {"name": "John"}

        self.delete_keys.append(key)

    def test_put_with_string_type_record(self):
        """
            Invoke put() with string typed record.
        """
        key = ('test', 'demo', 15)

        kvs = "Name : John"

        try:
            res = TestPut.client.put( key, kvs )

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Record should be passed as bin-value pair"

    def test_put_with_wrong_ns_and_set(self):
        """
            Invoke put() with wrong ns and set.
        """
        key = ('demo', 'test', 1)

        bins = {'a': ['!@#!#$%#', bytearray('ASD@#$AR#$@#ERQ#', 'utf-8')]}

        try:
            res = TestPut.client.put( key, bins )

        except NamespaceNotFound as exception:
            assert exception.code == 20
            assert exception.msg == 'AEROSPIKE_ERR_NAMESPACE_NOT_FOUND'

    def test_put_with_nonexistent_namespace(self):
        """
            Invoke put() with non-existent namespace.
        """
        key = ('test1', 'demo', 1)

        bins = { 'i': 'asdadasd' }
        try:
            res = TestPut.client.put( key, bins )

        except NamespaceNotFound as exception:
            assert exception.code == 20
            assert exception.msg == 'AEROSPIKE_ERR_NAMESPACE_NOT_FOUND'

    def test_put_with_nonexistent_set(self):
        """
            Invoke put() with non-existent set.
        """
        key = ('test', 'unknown_set', 1)

        bins = {'a': {'k': [bytearray("askluy3oijs", "utf-8")]}}

        res = TestPut.client.put(key, bins)

        assert res == 0

        (key, meta, bins) = TestPut.client.get(key)

        assert bins == {'a': {'k': [bytearray(b'askluy3oijs')]}}

        self.delete_keys.append(key)

    def test_put_boolean_record(self):
        """
            Invoke put() for boolean data record.
        """
        key = ('test', 'demo', 1)

        bins = {"is_present": False}

        res = TestPut.client.put(key, bins)

        assert res == 0

        (key, meta, bins) = TestPut.client.get(key)

        assert bins == {"is_present": False}
        self.delete_keys.append(key)

    """ 
    def test_put_unicode_string(self):
            #Invoke put() for unicode record.
        key = ('test', 'demo', 1)

        bins = { "unicode_string": u"\ud83d\ude04" }

        res = TestPut.client.put( key, bins )

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

        res = TestPut.client.put(key, rec)

        assert res == 0

        (key, meta, bins) = TestPut.client.get(key)
        assert bins == rec
        self.delete_keys.append(key)

    def test_put_unicode_string_in_list(self):
        #Invoke put() for unicode record.
        key = ('test', 'demo', 1)

        rec = {'a': [u'aa', u'bb', 1, u'bb', u'aa']}

        res = TestPut.client.put(key, rec)

        assert res == 0

        (key, meta, bins) = TestPut.client.get(key)

        assert bins == rec

        self.delete_keys.append(key)

    def test_put_unicode_string_in_key(self):
        #Invoke put() for unicode record.
        key = ('test', 'demo', u"bb")

        rec = {'a': [u'aa', 2, u'aa', 4, u'cc', 3, 2, 1]}
        res = TestPut.client.put(key, rec)

        assert res == 0

        (key, meta, bins) = TestPut.client.get(key)

        assert bins == rec

        self.delete_keys.append(key)

    def test_put_with_float_data(self):

        #Invoke put() for float data record.
        key = ('test', 'demo', 1)

        rec = {"pi": 3.141}

        res = TestPut.client.put(key, rec)

        assert res == 0
        _, _, bins = TestPut.client.get(key)

        assert bins == {'pi': 3.141}

        self.delete_keys.append(key)

    def test_put_with_float_data_within_list(self):

        #Invoke put() for float data record within list.
        key = ('test', 'demo', 1)

        rec = {"double_list": [3.141, 4.123, 6.285]}

        res = TestPut.client.put(key, rec)

        assert res == 0
        _, _, bins = TestPut.client.get(key)

        assert bins == {'double_list': [3.141, 4.123, 6.285]}

        self.delete_keys.append(key)

    def test_put_with_float_data_within_map(self):

        #Invoke put() for float data record within map.
        key = ('test', 'demo', 1)

        rec = {"double_map": {"1": 3.141,"2": 4.123,"3": 6.285}}

        res = TestPut.client.put(key, rec)

        assert res == 0
        _, _, bins = TestPut.client.get(key)

        assert bins == {'double_map': {"1": 3.141, "2": 4.123, "3": 6.285}}

        self.delete_keys.append(key)

    def test_put_with_string_meta_and_string_policies(self):
        """
            Invoke put() for metadata and policies.
        """
        key = ('test', 'demo', 1)

        rec = {'i': 12}

        try:
            res = TestPut.client.put( key, rec, "meta", "policies")

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "policy must be a dict"

    def test_put_with_string_record_generation(self):
        """
            Invoke put() for a record with string data, metadata and ttl
        """
        key = ('test', 'demo', 1)

        rec = {"name": "John"}
        meta = {'gen': 3, 'ttl': 25000}
        policy = {'timeout': 1000}
        assert 0 == TestPut.client.put(key, rec, meta, policy)

        (key, meta, bins) = TestPut.client.get(key)
        assert {"name": "John"} == bins
        assert meta['gen'] != None
        self.delete_keys.append(key)

    def test_put_with_generation_string(self):
        """
            Invoke put() for a record with generation as string
        """
        key = ('test', 'demo', 1)

        rec = {"name": "John"}
        meta = {'gen': "wrong", 'ttl': 25000}
        policy = {'timeout': 1000}

        try:
            TestPut.client.put( key, rec, meta, policy )

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Generation should be an int or long"

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
        try:
            TestPut.client.put( key, rec, meta, policy )

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Ttl should be an int or long"

        #self.delete_keys.append( key )

    def test_put_with_generation_bool(self):
        """
            Invoke put() for a record with generation as boolean.
        """
        key = ('test', 'demo', 1)

        rec = {"name": "John"}
        meta = {'gen': True, 'ttl': 25000}
        policy = {'timeout': 1000}
        assert 0 == TestPut.client.put(key, rec, meta, policy)

        (key, meta, bins) = TestPut.client.get(key)
        assert {"name": "John"} == bins
        assert meta['gen'] != None
        self.delete_keys.append(key)

    def test_put_with_ttl_boolean(self):
        """
            Invoke put() for a record with ttl as boolean.
        """
        key = ('test', 'demo', 1)

        rec = {"name": "John"}
        meta = {'gen': 3, 'ttl': True}
        policy = {'timeout': 1000}
        assert 0 == TestPut.client.put(key, rec, meta, policy)

        (key, meta, bins) = TestPut.client.get(key)
        assert {"name": "John"} == bins
        assert meta['gen'] != None
        self.delete_keys.append(key)

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
        try:
            TestPut.client.put( key, rec, meta, policy )

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == 'timeout is invalid'

    def test_put_with_policy_gen_EQ_positive(self):
        """
            Invoke put() for a record with generation as EQ positive
        """
        key = ('test', 'demo', 1)

        rec = {"name": "John"}
        meta = {'gen': 2, 'ttl': 25000}
        policy = {'timeout': 1000}
        assert 0 == TestPut.client.put(key, rec, meta, policy)

        (key, meta, bins) = TestPut.client.get(key)

        assert {"name": "John"} == bins
        gen = meta['gen']
        rec = {"name": "Smith"}
        policy = {
            'timeout': 1000,
            'gen': aerospike.POLICY_GEN_EQ,
            'commit_level': aerospike.POLICY_COMMIT_LEVEL_ALL
        }
        meta = {'gen': gen}

        assert 0 == TestPut.client.put(key, rec, meta, policy)

        (key, meta, bins) = TestPut.client.get(key)
        assert {"name": "Smith"} == bins

        self.delete_keys.append(key)

    def test_put_with_policy_gen_EQ_less(self):
        """
            Invoke put() for a record with generation as EQ less
        """
        key = ('test', 'demo', 1)

        rec = {"name": "John"}
        meta = {'gen': 2, 'ttl': 25000}
        policy = {'timeout': 1000}
        assert 0 == TestPut.client.put(key, rec, meta, policy)

        (key, meta, bins) = TestPut.client.get(key)

        assert {"name": "John"} == bins
        gen = meta['gen']
        rec = {"name": "Smith"}
        policy = {'timeout': 1000, 'gen': aerospike.POLICY_GEN_EQ}
        meta = {'gen': 10}

        try:
            TestPut.client.put( key, rec, meta, policy )

        except RecordGenerationError as exception:
            assert exception.code == 3
            assert exception.msg == 'AEROSPIKE_ERR_RECORD_GENERATION'
        
        ( key, meta, bins) = TestPut.client.get(key)
        assert {"name": "John"} == bins

        self.delete_keys.append(key)

    def test_put_with_policy_exists_create_negative(self):
        """
            Invoke put() for a record with all policies.
        """
        key = ('test', 'demo', 1)

        rec = {"name": "John"}
        meta = {'gen': 2, 'ttl': 25000}
        policy = {
            'timeout': 1000,
            'gen': aerospike.POLICY_GEN_IGNORE,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND,
        }
        assert 0 == TestPut.client.put(key, rec, meta, policy)

        (key, meta, bins) = TestPut.client.get(key)

        assert {"name": "John"} == bins

        rec = {"name": "Smith"}
        policy = {'timeout': 1000, 'exists': aerospike.POLICY_EXISTS_CREATE}
        meta = {'gen': 2}

        try:
            TestPut.client.put( key, rec, meta, policy )

        except RecordExistsError as exception:
            assert exception.code == 5
            assert exception.msg == 'AEROSPIKE_ERR_RECORD_EXISTS'
            assert exception.bin == {'name': 'Smith'}

        (key, meta, bins) = TestPut.client.get(key)
        assert {"name": "John"} == bins

        self.delete_keys.append(key)

    def test_put_with_policy_exists_create_positive(self):
        """
            Invoke put() for a record with all policies.
        """
        key = ('test', 'demo', 1)

        rec = {"name": "Smith"}
        meta = {'gen': 2, 'ttl': 25000}
        policy = {
            'timeout': 1000,
            'exists': aerospike.POLICY_EXISTS_CREATE,
            'gen': aerospike.POLICY_GEN_IGNORE,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND,
            'commit_level': aerospike.POLICY_COMMIT_LEVEL_MASTER
        }
        assert 0 == TestPut.client.put(key, rec, meta, policy)

        (key, meta, bins) = TestPut.client.get(key)
        assert {"name": "Smith"} == bins

        self.delete_keys.append(key)

    def test_put_with_policy_exists_replace_negative(self):
        """
            Invoke put() for a record with replace policy negative.
        """
        key = ('test', 'demo', 1)

        rec = {"name": "John"}
        meta = {'gen': 2, 'ttl': 25000}
        policy = {
            'timeout': 1000,
            'exists': aerospike.POLICY_EXISTS_REPLACE,
            'gen': aerospike.POLICY_GEN_IGNORE,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND
        }
        try:
            assert 0 == TestPut.client.put( key, rec, meta, policy )

        except RecordNotFound as exception:
            assert exception.code == 2
            assert exception.msg == 'AEROSPIKE_ERR_RECORD_NOT_FOUND'

        #self.delete_keys.append( key )

    def test_put_with_policy_exists_create_or_replace_positive(self):
        """
            Invoke put() for a record with create or replace policy positive.
        """
        key = ('test', 'demo', 1)

        rec = {"name": "Smith"}
        meta = {'gen': 2, 'ttl': 25000}
        policy = {
            'timeout': 1000,
            'exists': aerospike.POLICY_EXISTS_CREATE_OR_REPLACE,
            'gen': aerospike.POLICY_GEN_IGNORE,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND
        }
        assert 0 == TestPut.client.put(key, rec, meta, policy)

        (key, meta, bins) = TestPut.client.get(key)
        assert {"name": "Smith"} == bins

        self.delete_keys.append(key)

    def test_put_with_policy_exists_ignore(self):
        """
            Invoke put() for a record with ignore.
        """
        key = ('test', 'demo', 1)

        rec = {"name": "Smith"}
        meta = {'gen': 2, 'ttl': 25000}
        policy = {
            'timeout': 1000,
            'exists': aerospike.POLICY_EXISTS_IGNORE,
            'gen': aerospike.POLICY_GEN_IGNORE,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND
        }
        assert 0 == TestPut.client.put(key, rec, meta, policy)

        (key, meta, bins) = TestPut.client.get(key)
        assert {"name": "Smith"} == bins

        self.delete_keys.append(key)

    def test_put_with_policy_replace_positive(self):
        """
            Invoke put() for a record with replace positive.
        """
        key = ('test', 'demo', 1)

        rec = {"name": "John"}
        meta = {'gen': 2, 'ttl': 25000}
        policy = {
            'timeout': 1000,
            'gen': aerospike.POLICY_GEN_IGNORE,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND,
        }
        assert 0 == TestPut.client.put(key, rec, meta, policy)

        (key, meta, bins) = TestPut.client.get(key)

        assert {"name": "John"} == bins

        rec = {"name": "Smith"}
        meta = {'gen': 2, 'ttl': 25000}
        policy = {'timeout': 1000, 'exists': aerospike.POLICY_EXISTS_REPLACE}
        assert 0 == TestPut.client.put(key, rec, meta, policy)

        (key, meta, bins) = TestPut.client.get(key)

        assert {"name": "Smith"} == bins
        self.delete_keys.append(key)

    def test_put_with_policy_exists_update_positive(self):
        """
            Invoke put() for a record with all policies.
        """
        key = ('test', 'demo', 1)

        rec = {"name": "John"}
        meta = {'gen': 2, 'ttl': 25000}
        policy = {
            'timeout': 1000,
            'gen': aerospike.POLICY_GEN_IGNORE,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND,
        }
        assert 0 == TestPut.client.put(key, rec, meta, policy)

        (key, meta, bins) = TestPut.client.get(key)

        assert {"name": "John"} == bins

        rec = {"name": "Smith"}
        meta = {'gen': 2, 'ttl': 25000}
        policy = {'timeout': 1000, 'exists': aerospike.POLICY_EXISTS_UPDATE}
        assert 0 == TestPut.client.put(key, rec, meta, policy)

        (key, meta, bins) = TestPut.client.get(key)

        assert {"name": "Smith"} == bins
        self.delete_keys.append(key)

    def test_put_with_policy_exists_update_negative(self):
        """
            Invoke put() for a record with update policy negative.
        """
        key = ('test', 'demo', 1)

        rec = {"name": "John"}
        meta = {'gen': 2, 'ttl': 25000}
        policy = {
            'timeout': 1000,
            'exists': aerospike.POLICY_EXISTS_UPDATE,
            'gen': aerospike.POLICY_GEN_IGNORE,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND
        }
        try:
            assert 0 == TestPut.client.put( key, rec, meta, policy )

        except RecordNotFound as exception:
            assert exception.code == 2
            assert exception.msg == 'AEROSPIKE_ERR_RECORD_NOT_FOUND'

        #self.delete_keys.append( key )
    def test_put_with_policy_gen_GT_lesser(self):
        """
            Invoke put() for a record with generation as GT lesser
        """
        key = ('test', 'demo', 1)

        rec = {"name": "John"}
        meta = {'gen': 2, 'ttl': 25000}
        policy = {'timeout': 1000}
        assert 0 == TestPut.client.put(key, rec, meta, policy)

        (key, meta, bins) = TestPut.client.get(key)

        assert {"name": "John"} == bins
        gen = meta['gen']
        rec = {"name": "Smith"}
        policy = {'timeout': 1000, 'gen': aerospike.POLICY_GEN_GT}
        meta = {'gen': gen}

        try:
            TestPut.client.put( key, rec, meta, policy )

        except RecordGenerationError as exception:
            assert exception.code == 3
            assert exception.msg == 'AEROSPIKE_ERR_RECORD_GENERATION'

        (key, meta, bins) = TestPut.client.get(key)
        assert {"name": "John"} == bins

        self.delete_keys.append(key)

    def test_put_with_policy_gen_GT_positive(self):
        """
            Invoke put() for a record with generation as GT positive
        """
        key = ('test', 'demo', 1)

        rec = {"name": "John"}
        meta = {'gen': 2, 'ttl': 25000}
        policy = {'timeout': 1000}
        assert 0 == TestPut.client.put(key, rec, meta, policy)

        (key, meta, bins) = TestPut.client.get(key)

        assert {"name": "John"} == bins
        gen = meta['gen']
        assert gen == 1
        rec = {"name": "Smith"}
        policy = {'timeout': 1000, 'gen': aerospike.POLICY_GEN_GT}
        meta = {'gen': gen + 5}

        TestPut.client.put(key, rec, meta, policy)

        (key, meta, bins) = TestPut.client.get(key)
        assert {"name": "Smith"} == bins

        self.delete_keys.append(key)

    def test_put_with_policy_gen_ignore(self):
        """
            Invoke put() for a record with generation as gen_ignore
        """
        key = ('test', 'demo', 1)

        rec = {"name": "John"}
        meta = {'gen': 2, 'ttl': 25000}
        policy = {'timeout': 1000}
        assert 0 == TestPut.client.put(key, rec, meta, policy)

        (key, meta, bins) = TestPut.client.get(key)

        assert {"name": "John"} == bins

        gen = meta['gen']
        rec = {"name": "Smith"}
        policy = {'timeout': 1000, 'gen': aerospike.POLICY_GEN_IGNORE}
        meta = {'gen': gen}

        assert 0 == TestPut.client.put(key, rec, meta, policy)

        (key, meta, bins) = TestPut.client.get(key)
        assert {"name": "Smith"} == bins

        self.delete_keys.append(key)

    def test_put_with_set_unicode_string(self):
        """
            Invoke put() with set is unicode string.
        """
        key = ('test', u'demo', 1)

        rec = {"name": "John"}

        assert 0 == TestPut.client.put(key, rec)

        (key, meta, bins) = TestPut.client.get(key)

        assert {"name": "John"} == bins
        self.delete_keys.append(key)

    def test_put_with_unicode_bin(self):
        """
            Invoke put() with unicode bin.
        """
        key = ('test', 'demo', 1)

        rec = {
            u'i': ["nanslkdl", 1, bytearray("asd;as[d'as;d", "utf-8")],
            's': {"key": "asd';q;'1';"},
            'b': 1234,
            'l': '!@#@#$QSDAsd;as'
        }

        assert 0 == TestPut.client.put(key, rec)
        (key, meta, bins) = TestPut.client.get(key)

        assert {
            'i': ["nanslkdl", 1, bytearray("asd;as[d'as;d", "utf-8")],
            's': {"key": "asd';q;'1';"},
            'b': 1234,
            'l': '!@#@#$QSDAsd;as'
        } == bins
        self.delete_keys.append(key)

    def test_put_set(self):
        """
            Invoke put() set.
        """
        key = ('test', 'demo', 1)

        rec = {"is_present": set([1, 2])}

        res = TestPut.client.put(key, rec)

        assert res == 0

        (key, meta, bins) = TestPut.client.get(key)

        assert bins == {"is_present": set([1, 2])}

        self.delete_keys.append(key)

    def test_put_frozenset(self):
        """
            Invoke put() frozenSet.
        """
        key = ('test', 'demo', 1)

        cities = frozenset(["Frankfurt", "Basel", "Freiburg"])

        rec = {'fSet': cities}

        res = TestPut.client.put(key, rec)

        assert res == 0

        (key, meta, bins) = TestPut.client.get(key)

        assert bins == {'fSet': frozenset(["Frankfurt", "Basel", "Freiburg"])}

        self.delete_keys.append(key)

    def test_put_tuple(self):
        """
            Invoke put() tuple.
        """
        key = ('test', 'demo', 1)

        rec = {'seq': tuple('abc')}

        res = TestPut.client.put(key, rec)

        assert res == 0

        (key, meta, bins) = TestPut.client.get(key)

        assert bins == {'seq': ('a', 'b', 'c')}

        self.delete_keys.append(key)

    def test_put_none_data(self):
        """
            Invoke put() None.
        """
        key = ('test', 'demo', 1)

        rec_none = {"is_present": None}

        res = TestPut.client.put(key, rec_none)

        assert res == 0

        (key, meta, bins) = TestPut.client.get(key)

        assert bins == {"is_present": None}
        self.delete_keys.append(key)

    def test_put_map_containing_tuple(self):
        """
            Invoke put() maap containing tuple.
        """
        key = ('test', 'demo', 1)

        rec = {'seq': {'bb': tuple('abc')}}

        res = TestPut.client.put(key, rec)

        assert res == 0

        (key, meta, bins) = TestPut.client.get(key)

        assert bins == {'seq': {u'bb': ('a', 'b', 'c')}}

        self.delete_keys.append(key)

    def test_put_serializer_default(self):
        """
            Invoke put() with mixed data record with no class or instance
            serializer or deserializer. Python option should get called by default
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

        res = TestPut.client.put(key, rec, {}, {})

        assert res == 0

        _, _, bins = TestPut.client.get(key)

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

    def test_put_user_serializer_no_deserializer(self):
        """
            Invoke put() for float data record with user serializer is
            registered, but deserializer is not registered.
        """

        key = ('test', 'demo', 1)

        rec = {"pi": 3.14}

        def serialize_function(val):
            return marshal.dumps(val)

        response = aerospike.set_serializer(serialize_function)

        res = TestPut.client.put(key, rec, {}, {}, aerospike.SERIALIZER_USER)

        assert res == 0

        _, _, bins = TestPut.client.get(key)

        if TestPut.skip_old_server == False:
            assert bins == {'pi': 3.14}
        else:
            assert bins == {'pi': bytearray(b'g\x1f\x85\xebQ\xb8\x1e\t@')}

        self.delete_keys.append(key)

    def test_put_record_with_bin_name_exceeding_max_limit(self):
        """
            Invoke put() with bin name exceeding the max limit of bin name.
        """
        key = ('test', 'demo', 'put_rec')
        put_record = {
            'containers_free': [],
            'containers_used': [
                {'cluster_id': 'bob',
                 'container_id': 1,
                 'port': 4000}
            ],
            'list_of_map': [{'test': 'bar'}],
            'map_of_list': {'fizz': ['b', 'u', 'z', 'z']},
            'ports_free': [],
            'ports_unused': [4100, 4200, 4300],
            'provider_id': u'i-f01fc206'
        }

        try:
            TestPut.client.put( key, put_record)

        except BinNameError as exception:
            assert exception.code == 21L
            assert exception.msg == "A bin name should not exceed 14 characters limit"

    def test_put_with_string_record_without_connection(self):
        """
            Invoke put() for a record with string data without connection
        """
        config = {"hosts": [("127.0.0.1", 3000)]}
        client1 = aerospike.client(config)

        key = ('test', 'demo', 1)

        bins = {"name": "John"}

        try:
            client1.put( key, bins )

        except ClusterError as exception:
            assert exception.code == 11L
            assert exception.msg == 'No connection to aerospike cluster'

    def test_put_with_integer_greater_than_maxisze(self):
        """
            Invoke put() for a record with integer greater than max size
        """
        key = ('test', 'demo', 1)

        bins = {"no": 111111111111111111111111111111111111111111111}

        try:
            assert 0 == TestPut.client.put(key, bins)

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == 'integer value exceeds sys.maxsize'

    def test_put_with_key_as_an_integer_greater_than_maxisze(self):
        """
            Invoke put() for a record with integer greater than max size
        """
        key = ('test', 'demo', 1111111111111111111111111111111111)

        bins = {"no": 11}

        try:
            assert 0 == TestPut.client.put(key, bins)

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == 'integer value for KEY exceeds sys.maxsize'

