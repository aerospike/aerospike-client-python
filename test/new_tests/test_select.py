# -*- coding: utf-8 -*-

import pytest
import sys
from aerospike import exception as e
from .as_status_codes import AerospikeStatus
import time

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)


@pytest.mark.usefixtures("connection_config")
class TestSelect(object):

    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        """
        Setup class.
        """
        self.test_key = ('test', 'demo', 1)

        rec = {
            'a': ["nanslkdl", 1, bytearray("asd;as[d'as;d", "utf-8")],
            'b': {"key": "asd';q;'1';"},
            'c': 1234,
            'd': '!@#@#$QSDAsd;as',
            'n': None,
            ('a' * 14): 'long_bin_14',
        }

        as_connection.put(self.test_key, rec)

        yield

        as_connection.remove(self.test_key)

    def test_select_with_key_and_bins(self):
        '''
        Test of selecting a single bin
        '''
        bins_to_select = ['a']

        key, meta, bins = self.as_connection.select(self.test_key,
                                                    bins_to_select)

        assert bins == {
            'a': ["nanslkdl", 1, bytearray("asd;as[d'as;d", "utf-8")]
        }

        assert meta is not None

        assert key is not None

    def test_select_with_key_and_duplicate_bins(self):
        '''
        Test that passing in duplicate bin names does not cause
        errors
        '''
        bins_to_select = ['a', 'a', 'a']

        key, meta, bins = self.as_connection.select(self.test_key,
                                                    bins_to_select)

        assert bins == {
            'a': ["nanslkdl", 1, bytearray("asd;as[d'as;d", "utf-8")]
        }

    def test_select_with_none_policy(self):

        bins_to_select = ['b']

        key, meta, bins = self.as_connection.select(
            self.test_key, bins_to_select, None)

        assert bins == {'b': {"key": "asd';q;'1';"}}
        assert meta is not None
        assert key is not None

    def test_select_with_non_existent_key(self):

        key = ("test", "demo", 'non-existent')

        bins_to_select = ['a', 'b']

        with pytest.raises(e.RecordNotFound):
            key, meta, bins = self.as_connection.select(key, bins_to_select)

    @pytest.mark.parametrize("bins",
                             ('a', {}, False, 1, None))
    def test_select_with_key_and_single_bin_to_select_not_a_list(self, bins):

        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.select(self.test_key, bins)

        assert err_info.value.code == AerospikeStatus.AEROSPIKE_ERR_PARAM

    def test_select_with_key_and_multiple_bins_to_select(self):

        bins_to_select = ['c', 'd']

        _, meta, bins = self.as_connection.select(
            self.test_key, bins_to_select)

        assert bins == {'c': 1234, 'd': '!@#@#$QSDAsd;as'}
        assert meta is not None

    def test_select_with_key_and_multiple_bins_to_select_policy_key_send(self):

        bins_to_select = ['c', 'd']
        policy = {'timeout': 1000, 'key': aerospike.POLICY_KEY_SEND}
        key, meta, bins = self.as_connection.select(
            self.test_key, bins_to_select, policy)

        # Calculate the key's digest
        key_digest = aerospike.calc_digest(*self.test_key)

        assert bins == {'c': 1234, 'd': '!@#@#$QSDAsd;as'}
        assert key == ('test', 'demo', 1, key_digest)
        assert meta is not None

    def test_select_with_key_and_multiple_bins_to_select_policy_key_digest(self
                                                                           ):

        # key with digest and no primary key
        key = ('test', 'demo', None, bytearray("asd;as[d'as;djk;uyfl",
                                               "utf-8"))
        rec = {
            'a': ["nanslkdl", 1, bytearray("asd;as[d'as;d", "utf-8")],
            'b': {"key": "asd';q;'1';"},
            'c': 1234,
            'd': '!@#@#$QSDAsd;as'
        }

        self.as_connection.put(key, rec)

        bins_to_select = ['c', 'd']
        policy = {'key': aerospike.POLICY_KEY_DIGEST}
        key, meta, bins = self.as_connection.select(
            key, bins_to_select, policy)

        assert bins == {'c': 1234, 'd': '!@#@#$QSDAsd;as'}
        assert key == ('test', 'demo', None,
                       bytearray(b"asd;as[d\'as;djk;uyfl"))
        assert meta is not None

        key = ('test', 'demo', None, bytearray("asd;as[d'as;djk;uyfl",
                                               "utf-8"))
        self.as_connection.remove(key)

    def test_select_with_key_with_existent_and_non_existent_bins_to_select(
        self
    ):

        bins_to_select = ['c', 'd', 'n', 'fake']

        _, meta, bins = self.as_connection.select(
            self.test_key, bins_to_select)

        assert bins == {'c': 1234, 'd': '!@#@#$QSDAsd;as', 'n': None}
        assert meta is not None

    def test_select_with_key_and_non_existent_bin_in_middle(self):

        bins_to_select = ['c', 'fake', 'd']

        key, meta, bins = self.as_connection.select(self.test_key,
                                                    bins_to_select)

        assert bins == {'c': 1234, 'd': '!@#@#$QSDAsd;as'}
        assert meta is not None

    def test_select_with_key_and_non_existent_bins_to_select(self):
        '''
        Test specifying only nonexistent bin names as the argument
        '''
        bins_to_select = ['fake1', 'fake2']

        _, _, bins = self.as_connection.select(self.test_key, bins_to_select)

        assert bins == {}

    def test_select_with_unicode_bin_value_tuple(self):
        '''
        Test for passing a unicode bin name in a tuple will work
        '''
        bins_to_select = (u'a',)

        _, _, bins = self.as_connection.select(self.test_key, bins_to_select)

        assert bins == {
            'a': ["nanslkdl", 1, bytearray("asd;as[d'as;d", "utf-8")]
        }

    # Tests for error causing conditions

    def test_select_without_any_parameter(self):

        with pytest.raises(TypeError) as typeError:
            self.as_connection.select()

        assert "argument 'key' (pos 1)" in str(
            typeError.value)

    def test_select_with_key_and_bins_without_connection(self):

        bins_to_select = ['a']
        config = self.connection_config
        disconnected_client = aerospike.client(config)

        with pytest.raises(e.ClusterError) as err_info:
            disconnected_client.select(self.test_key, bins_to_select)

        assert err_info.value.code == AerospikeStatus.AEROSPIKE_CLUSTER_ERROR

    def test_select_with_invalid_keys(self, invalid_key):
        '''
        Verify that different types of invalid keys will
        cause select to fail
        '''
        bins_to_select = ['a']

        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.select(invalid_key, bins_to_select)

        assert err_info.value.code == AerospikeStatus.AEROSPIKE_ERR_PARAM

    def test_select_with_key_and_empty_list_of_bins_to_select(self):
        '''
        Test that passing an empty list of bins to select will
        cause an error
        '''
        with pytest.raises(e.InvalidRequest) as err_info:
            key, meta, bins = self.as_connection.select(self.test_key, [])

        assert (err_info.value.code ==
                AerospikeStatus.AEROSPIKE_ERR_REQUEST_INVALID)

    def test_select_empty_bin_name_tuple_raises_error(self):

        bins_to_select = ()
        #  This test fails with pytest.raises, but does actually raise
        #  the error
        with pytest.raises(e.InvalidRequest) as err_info:
            self.as_connection.select(self.test_key, bins_to_select)

    def test_select_invalid_bin_name_tuple_raises_error(self):

        bins_to_select = (1, 2)

        with pytest.raises(e.ParamError):
            self.as_connection.select(self.test_key, bins_to_select)

    def test_select_with_invalid_bin_name_types_raises_error(self):
        bins_to_select = [1, 2]
        with pytest.raises(e.ParamError):
            self.as_connection.select(self.test_key, bins_to_select)

    def test_select_with_unicode_bin_value_list(self):

        bins_to_select = [u'a']

        _, _, bins = self.as_connection.select(self.test_key, bins_to_select)

        assert bins == {
            'a': ["nanslkdl", 1, bytearray("asd;as[d'as;d", "utf-8")]
        }

    @pytest.mark.skip(reason="Behavior is unexpected, but not wrong")
    def test_select_with_very_long_bin_name(self):
        bins_to_select = ['a' * 10000]  # max bin name size is 14
        # internally this only sends the first 14 characters to
        # the select function
        _, _, rec = self.as_connection.select(self.test_key, bins_to_select)
        assert rec['a' * 14] == 'long_bin_14'
