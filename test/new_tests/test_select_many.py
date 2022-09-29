# -*- coding: utf-8 -*-
import pytest
import sys
from aerospike import exception as e

from .as_status_codes import AerospikeStatus
try:
    from collections import Counter
except ImportError:
    from counter26 import Counter

from .test_base_class import TestBaseClass

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)

KEY_TYPE_ERROR_MSG = "Keys should be specified as a list or tuple."


def get_primary_key(record):
    '''
    Convenience function to extract a primary key from a key tuple
    '''
    return record[0][2]


class TestSelectMany(object):

    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        """
        Add test items before each method
        """
        self.keys = []

        for i in range(5):
            key = ('test', 'demo', i)
            rec = {
                'title': 'Mr.',
                'name': 'name%s' % (str(i)),
                'age': i,
                'addr': 'Minisota',
                'country': 'USA'
            }
            as_connection.put(key, rec)
            self.keys.append(key)
        key = ('test', 'demo', 'float_value')
        as_connection.put(key, {"float_value": 4.3})
        self.keys.append(key)

        def teardown():
            """
            Remove the items added in setup in order to preserve clean state
            """
            for key in self.keys:
                as_connection.remove(key)

        request.addfinalizer(teardown)

    def test_select_many_without_policy(self):
        """
        Verify that select_many without a policy argument will work
        """
        filter_bins = ['title', 'name']
        records = self.as_connection.select_many(self.keys, filter_bins)

        assert isinstance(records, list)
        assert len(records) == 6
        for k in records:
            bins = k[2].keys()
            #  Verify that only bins specified in filter bins are present
            assert set(bins) <= set(filter_bins)

    def test_select_many_with_unicode_name(self):
        """
        Verify that select_many without a policy argument will work
        """
        filter_bins = ['title', u'name']
        records = self.as_connection.select_many(self.keys, filter_bins)

        assert isinstance(records, list)
        assert len(records) == 6
        for k in records:
            bins = k[2].keys()
            #  Verify that only bins specified in filter bins are present
            assert set(bins) <= set(filter_bins)

    @pytest.mark.skip(reason="This is undocumented behavior")
    def test_select_many_without_policy_binname_tuple(self):
        """
        Verify that select_many without a policy argument will work
        """
        filter_bins = ('title', 'name')
        records = self.as_connection.select_many(self.keys, filter_bins)

        assert isinstance(records, list)
        assert len(records) == 6
        for k in records:
            bins = k[2].keys()
            #  Verify that only bins specified in filter bins are present
            assert set(bins) <= set(filter_bins)

    @pytest.mark.parametrize(
        "policy",
        [
            {'timeout': 50},
            None
        ],
        ids=["valid timeout", "None policy"]
    )
    def test_select_many_with_valid_policy_parameters(self, policy):

        filter_bins = ['title', 'name', 'float_value']
        records = self.as_connection.select_many(self.keys, filter_bins,
                                                 policy)

        assert isinstance(records, list)
        assert len(records) == len(self.keys)
        assert Counter(
            [get_primary_key(record) for record in records]) == Counter(
                [key[2] for key in self.keys])
        for record in records:
            bins = record[2].keys()
            assert set(bins) <= set(filter_bins)

    def test_select_many_with_non_existent_keys(self):
        '''
        Test that including an invalid key in the list
        of keys does not raise an error
        '''
        temp_keys = self.keys[:]
        temp_keys.append(('test', 'demo', 'non-existent'))

        filter_bins = ['title', 'name', 'addr']
        records = self.as_connection.select_many(
            temp_keys, filter_bins, {'timeout': 1000})

        record_counter = Counter(
            [get_primary_key(record) for record in records])
        assert isinstance(records, list)
        assert len(records) == len(temp_keys)

        assert record_counter == Counter(
            [0, 1, 2, 3, 4, 'non-existent', 'float_value'])
        for record in records:
            if get_primary_key(record) == 'non-existent':
                assert record[2] is None
                continue
            bins = record[2].keys()
            assert set(bins) <= set(filter_bins)

    def test_select_many_with_all_non_existent_key(self):
        """
        Test call to select_many with only non existent keys
        """
        keys = [('test', 'demo', 'key')]
        #  Precalculate the digest of this key
        desired_digest = aerospike.calc_digest(*keys[0])
        filter_bins = ['title', 'name', 'country']
        records = self.as_connection.select_many(keys, filter_bins)

        assert len(records) == 1
        assert records == [
            (
                ('test', 'demo', 'key', desired_digest),
                None,
                None
            )
        ]

    def test_get_many_with_bytearray_key(self):
        '''
        Make sure that get many can handle a a key with a bytearray pk
        '''
        keys = [('test', 'demo', bytearray([1, 2, 3]))]
        for key in keys:
            self.as_connection.put(key, {'byte': 'array'})

        records = self.as_connection.select_many(keys, [])
        self.as_connection.remove(keys[0])

        bytearray_key = records[0][0]
        assert len(bytearray_key) == 4

        bytearray_pk = bytearray_key[2]
        assert bytearray_pk == bytearray([1, 2, 3])

    def test_with_use_batch_direct_true_in_constructor_false_argument(self):

        config = {'policies': {'use_batch_direct': False}}
        client_batch_direct = TestBaseClass.get_new_connection(add_config=config)

        policies = {'use_batch_direct': False}
        records = client_batch_direct.select_many(self.keys, [], policies)
        assert isinstance(records, list)
        assert len(records) == len(self.keys)

        client_batch_direct.close()

    def test_with_use_batch_direct_true_in_constructor(self):

        config = {'policies': {'use_batch_direct': True}}
        client_batch_direct = TestBaseClass.get_new_connection(add_config=config)

        records = self.as_connection.select_many(self.keys, [])
        assert isinstance(records, list)
        assert len(records) == len(self.keys)

        client_batch_direct.close()

    @pytest.mark.skip(reason="This test checks for the same thing twice")
    def test_select_many_with_initkey_as_digest(self):

        keys = []
        key = ("test", "demo", None, bytearray(
            "asd;as[d'as;djk;uyfl", "utf-8"))
        rec = {'name': 'name1', 'age': 1}
        self.as_connection.put(key, rec)
        keys.append(key)

        key = ("test", "demo", None, bytearray(
            "ase;as[d'as;djk;uyfl", "utf-8"))
        rec = {'name': 'name2', 'age': 2}
        self.as_connection.put(key, rec)
        keys.append(key)

        records = self.as_connection.select_many(keys, [u'name'])

        for key in keys:
            self.as_connection.remove(key)

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

        temp_keys = self.keys[:]
        temp_keys.append(('test', 'demo', 'some_key'))
        default_primary_keys = [k[2] for k in self.keys]
        added_primary_keys = ['some_key']

        for i in range(15, 20):
            key = ('test', 'demo', i)
            rec = {
                'name': 'name%s' % (str(i)),
                'age': i,
                'position': 'Sr. Engineer'
            }
            self.as_connection.put(key, rec)
            temp_keys.append(key)
            added_primary_keys.append(key[2])

        filter_bins = ['title', 'name', 'position']
        records = self.as_connection.select_many(temp_keys, filter_bins)

        for i in range(15, 20):
            key = ('test', 'demo', i)
            self.as_connection.remove(key)

        assert isinstance(records, list)
        assert len(records) == (len(default_primary_keys) +
                                len(added_primary_keys))

        # check that the primary keys in the returned records match the
        # expected ones, and occur once each
        return_record_counter = Counter(
            [get_primary_key(record) for record in records])
        assert return_record_counter == Counter(
            default_primary_keys + added_primary_keys)
        for record in records:
            if get_primary_key(record) == 'some_key':
                assert record[2] is None
                continue
            bins = record[2].keys()
            # make sure that only filtered bins are in the returned bins
            # could probably do set(bins) <= set(filter_bins)
            assert set(bins).intersection(set(filter_bins)) == set(bins)

    def test_select_many_with_unicode_bins(self):

        filter_bins = [u'title', u'name', 'country', u'addr']
        records = self.as_connection.select_many(self.keys, filter_bins)

        assert isinstance(records, list)
        assert len(records) == len(self.keys)
        for record in records:
            bins = record[2].keys()
            assert set(bins) <= set(filter_bins)

    def test_select_many_with_empty_bins_list(self):

        records = self.as_connection.select_many(self.keys, [])

        assert isinstance(records, list)
        assert len(records) == len(self.keys)

    # Tests for invalid usage of the function

    @pytest.mark.parametrize(
        "bins",
        (
            ['a', []],
            [[], 'a'],
            [None, None],
            [True, 'a'],
        )
    )
    def test_select_many_with_invalid_bin_names_list(self, bins):
        with pytest.raises(e.ParamError):
            records = self.as_connection.select_many(self.keys, bins)

    def test_select_many_without_any_parameter(self):
        """
        Ensure that calling select_many without parameters
        raises an error
        """
        with pytest.raises(TypeError) as typeError:
            self.as_connection.select_many()

        assert "argument 'keys' (pos 1)" in str(
            typeError.value)

    def test_select_many_with_proper_parameters_without_connection(self):

        config = {'hosts': [('127.0.0.1', 3000)]}
        client1 = aerospike.client(config)

        filter_bins = ['title', 'name']

        with pytest.raises(e.ClusterError) as err_info:
            client1.select_many(self.keys, filter_bins, {'timeout':
                                                         20})

        assert err_info.value.code == AerospikeStatus.AEROSPIKE_CLUSTER_ERROR

    def test_select_many_with_invalid_keys(self, invalid_key):
        # invalid_key will be an invalid key_tuple, so we wrap
        # it with a valid key in a list
        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.select_many([self.keys[0], invalid_key],
                                           ['title'], {})

        assert err_info.value.code == AerospikeStatus.AEROSPIKE_ERR_PARAM

    def test_select_many_with_a_single_key(self):
        '''
        Verify that passing a single key instead of a list
        raises an error
        '''
        key = self.keys[0]
        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.select_many(key, ['name'], {})

    def test_select_many_with_invalid_timeout(self):

        policies = {'total_timeout': 0.2}
        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.select_many(self.keys, [], policies)

        assert err_info.value.code == AerospikeStatus.AEROSPIKE_ERR_PARAM

    def test_select_many_with_an_invalid_key_in_list_batch_direct(self):

        with pytest.raises(e.ParamError):
            self.as_connection.select_many([('test', 'demo', 1), ('test', 'demo', 2), None],
                                           ["title"],
                                           {'use_batch_direct': True})
    # Tests for invalid argument types

    @pytest.mark.parametrize('keys_arg', (None, {}, False, 'a', 1))
    def test_select_many_with_invalid_keys_type(self, keys_arg):
        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.select_many(keys_arg, ['name'], {})

    @pytest.mark.parametrize('bins', (None, {}, False, 'a', 1))
    def test_select_many_with_invalid_bins_arg_type(self, bins):
        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.select_many(self.keys, bins)

    @pytest.mark.parametrize('policy', ([], False, 1, 'policy'))
    def test_select_many_with_invalid_policy_arg_type(self, policy):
        bins = ['name']
        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.select_many(self.keys, bins, policy)
