# -*- coding: utf-8 -*-

import pytest
import sys
import cPickle as pickle
from test_base_class import TestBaseClass

aerospike = pytest.importorskip("aerospike")
try:
    from aerospike.exception import *
except:
    print "Please install aerospike python client."
    sys.exit(1)


class TestSelect(TestBaseClass):
    def setup_class(cls):
        """
        Setup class.
        """
        hostlist, user, password = TestBaseClass.get_hosts()
        config = {'hosts': hostlist}
        if user == None and password == None:
            TestSelect.client = aerospike.client(config).connect()
        else:
            TestSelect.client = aerospike.client(config).connect(user, password)

    def teardown_class(cls):
        TestSelect.client.close()

    def setup_method(self, method):
        """
        Setup method.
        """
        key = ('test', 'demo', 1)

        rec = {
            'a': ["nanslkdl", 1, bytearray("asd;as[d'as;d", "utf-8")],
            'b': {"key": "asd';q;'1';"},
            'c': 1234,
            'd': '!@#@#$QSDAsd;as',
            'n': None
        }

        TestSelect.client.put(key, rec)

    def teardown_method(self, method):
        """
        Teardoen method.
        """

        key = ("test", "demo", 1)

        TestSelect.client.remove(key)

    def test_select_with_key_and_empty_list_of_bins_to_select(self):

        key = ("test", "demo", 1)

        key, meta, bins = TestSelect.client.select(key, [])

        assert bins == {}

        assert meta != None

        assert key != None

    def test_select_with_key_and_bins(self):

        key = ("test", "demo", 1)

        bins_to_select = ['a']

        key, meta, bins = TestSelect.client.select(key, bins_to_select)

        assert bins == {
            'a': ["nanslkdl", 1, bytearray("asd;as[d'as;d", "utf-8")]
        }

        assert meta != None

        assert key != None

    def test_select_without_any_parameter(self):

        with pytest.raises(TypeError) as typeError:
            TestSelect.client.select()

        assert "Required argument 'key' (pos 1) not found" in typeError.value

    def test_select_with_none_key(self):

        bins_to_select = ['a']

        try:
            key, meta, bins = TestSelect.client.select(None, bins_to_select)

        except ParamError as exception:
            assert exception.code == -2

    def test_select_with_none_policy(self):

        key = ("test", "demo", 1)

        bins_to_select = ['b']

        key, meta, bins = TestSelect.client.select(key, bins_to_select, None)

        assert bins == {'b': {"key": "asd';q;'1';"}, }

        assert meta != None

        assert key != None

    def test_select_with_none_bins_to_select(self):

        key = ("test", "demo", 1)

        bins_to_select = None

        try:
            key, meta, bins = TestSelect.client.select( key, bins_to_select )

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == 'not a list or tuple'

    def test_select_with_non_existent_key(self):

        key = ("test", "demo", 'non-existent')

        bins_to_select = ['a', 'b']

        try:
            key, meta, bins = TestSelect.client.select( key, bins_to_select )

            """
            We are making the api backward compatible. In case of RecordNotFound an
            exception will not be raised. Instead Ok response is returned withe the
            meta as None. This might change with further releases.
            """
        except RecordNotFound as exception:
            assert True == False
            assert exception.code == 2
            assert exception.msg == 'AEROSPIKE_ERR_RECORD_NOT_FOUND'

        assert key != None
        assert meta == None
        assert bins == None

    def test_select_with_key_and_single_bin_to_select_not_a_list(self):

        key = ("test", "demo", 1)

        bin_to_select = 'a'  # Not a list

        try:
            key, meta, bins = TestSelect.client.select( key, bin_to_select )

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == 'not a list or tuple'

    def test_select_with_key_and_multiple_bins_to_select(self):

        key = ("test", "demo", 1)

        bins_to_select = ['c', 'd']

        key, meta, bins = TestSelect.client.select(key, bins_to_select)

        assert bins == {'c': 1234, 'd': '!@#@#$QSDAsd;as'}

        assert meta != None

    def test_select_with_key_and_multiple_bins_to_select_policy_key_send(self):

        key = ("test", "demo", 1)

        bins_to_select = ['c', 'd']
        policy = {'timeout': 1000, 'key': aerospike.POLICY_KEY_SEND}
        key, meta, bins = TestSelect.client.select(key, bins_to_select, policy)

        assert bins == {'c': 1234, 'd': '!@#@#$QSDAsd;as'}
        assert key == ('test', 'demo', 1, bytearray(
            b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8')
                      )
        assert meta != None

    def test_select_with_key_and_multiple_bins_to_select_policy_key_digest(self
                                                                          ):

        key = ('test', 'demo', None, bytearray("asd;as[d'as;djk;uyfl",
                                               "utf-8"))
        rec = {
            'a': ["nanslkdl", 1, bytearray("asd;as[d'as;d", "utf-8")],
            'b': {"key": "asd';q;'1';"},
            'c': 1234,
            'd': '!@#@#$QSDAsd;as'
        }

        TestSelect.client.put(key, rec)

        bins_to_select = ['c', 'd']
        policy = {'timeout': 1000, 'key': aerospike.POLICY_KEY_DIGEST}
        key, meta, bins = TestSelect.client.select(key, bins_to_select, policy)

        assert bins == {'c': 1234, 'd': '!@#@#$QSDAsd;as'}
        assert key == ('test', 'demo', None,
                       bytearray(b"asd;as[d\'as;djk;uyfl"))
        assert meta != None

        key = ('test', 'demo', None, bytearray("asd;as[d'as;djk;uyfl",
                                               "utf-8"))
        TestSelect.client.remove(key)

    def test_select_with_key_and_combination_of_existent_and_non_existent_bins_to_select(
        self
    ):

        key = ("test", "demo", 1)

        bins_to_select = ['c', 'd', 'n']

        key, meta, bins = TestSelect.client.select(key, bins_to_select)

        assert bins == {'c': 1234, 'd': '!@#@#$QSDAsd;as', 'n': None}

        assert meta != None

    def test_select_with_key_and_non_existent_bin_in_middle(self):

        key = ("test", "demo", 1)

        bins_to_select = ['c', 'e', 'd']

        key, meta, bins = TestSelect.client.select(key, bins_to_select)

        assert bins == {'c': 1234, 'd': '!@#@#$QSDAsd;as'}

        assert meta != None

    def test_select_with_key_and_non_existent_bins_to_select(self):

        key = ("test", "demo", 1)

        bins_to_select = ['e', 'f']

        key, meta, bins = TestSelect.client.select(key, bins_to_select)

        assert bins == {}

    def test_select_with_unicode_value(self):

        key = ('test', 'demo', 'aa')

        rec = {
            'a': ["nanslkdl", 1, bytearray("asd;as[d'as;d", "utf-8")],
            'b': {"key": "asd';q;'1';"},
            'c': 1234,
            'd': '!@#@#$QSDAsd;as'
        }

        assert 0 == TestSelect.client.put(key, rec)

        bins_to_select = ['a']

        key, meta, bins = TestSelect.client.select(key, bins_to_select)

        assert bins == {
            'a': ["nanslkdl", 1, bytearray("asd;as[d'as;d", "utf-8")]
        }

        assert meta != None

        assert key != None

        key = ('test', 'demo', 'aa')
        TestSelect.client.remove(key)

    def test_select_with_key_and_bins_without_connection(self):

        key = ("test", "demo", 1)

        bins_to_select = ['a']

        try:
            key, meta, bins = TestSelect.client.select( key, bins_to_select)

        except ClusterError as exception:
            assert exception.code == 11L
            assert exception.msg == 'No connection to aerospike cluster'
