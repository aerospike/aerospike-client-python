# -*- coding: utf-8 -*-

import pytest
import sys
from .test_base_class import TestBaseClass
from .as_status_codes import AerospikeStatus
from aerospike import exception as e
from .index_helpers import ensure_dropped_index

import pdb

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)


def add_map_keys(client):
    for i in range(5):
        key = ('test', u'demo', i)
        rec = {
            'name': 'name%s' % (str(i)),
            'addr': 'name%s' % (str(i)),
            'numeric_map': {1: 1,
                            2: 2,
                            3: 3},
            'string_map': {"sa": "a",
                           "sb": "b",
                           "sc": "c"},
            'age': i,
            'no': i
        }
        client.put(key, rec)


def remove_map_keys(client):
    for i in range(5):
        key = ('test', u'demo', i)
        client.remove(key)


@pytest.mark.usefixtures("connection_with_config_funcs")
class TestMapKeysIndex(object):

    def setup_class(cls):
        cls.connection_setup_functions = [add_map_keys]
        cls.connection_teardown_functions = [remove_map_keys]

    def test_mapkeysindex_with_no_paramters(self):
        """
            Invoke index_mapkeys_create() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            self.as_connection.index_map_keys_create()

        assert "argument 'ns' (pos 1)" in str(
            typeError.value)

    def test_mapkeysindex_with_extra_paramters(self):
        """
            Invoke index_mapkeys_create() with extra parameters.
        """
        policy = {}
        with pytest.raises(TypeError) as typeError:
            self.as_connection.index_map_keys_create(
                'test', 'demo', 'string_map', aerospike.INDEX_STRING,
                'test_string_map_index', policy, 1)

    def test_mapkeysindex_with_correct_parameters(self):
        """
            Invoke index_mapkeys_create() with correct arguments
        """
        policy = {}
        ensure_dropped_index(self.as_connection, 'test', 'test_string_map_index')
        response_code = self.as_connection.index_map_keys_create(
            'test', 'demo', 'string_map', aerospike.INDEX_STRING,
            'test_string_map_index', policy)

        assert response_code == AerospikeStatus.AEROSPIKE_OK
        self.as_connection.index_remove('test', 'test_string_map_index',
                                        policy)
        ensure_dropped_index(self.as_connection, 'test', 'test_string_map_index')

    def test_mapkeysindex_with_correct_parameters_numeric(self):
        """
            Invoke index_mapkeys_create() with correct arguments
        """
        policy = {}
        ensure_dropped_index(self.as_connection, 'test', 'test_numeric_map_index')
        response_code = self.as_connection.index_map_keys_create(
            'test', 'demo', 'numeric_map', aerospike.INDEX_NUMERIC,
            'test_numeric_map_index', policy)

        assert response_code == AerospikeStatus.AEROSPIKE_OK
        self.as_connection.index_remove('test', 'test_numeric_map_index',
                                        policy)
        ensure_dropped_index(self.as_connection, 'test', 'test_numeric_map_index')

    def test_mapkeysindex_with_correct_parameters_numeric_on_stringkeys(self):
        """
            Invoke index_mapkeys_create() with correct arguments
        """
        policy = {}
        ensure_dropped_index(self.as_connection, 'test', 'test_numeric_map_index')
        response_code = self.as_connection.index_map_keys_create(
            'test', 'demo', 'string_map', aerospike.INDEX_NUMERIC,
            'test_numeric_map_index', policy)

        assert response_code == AerospikeStatus.AEROSPIKE_OK
        self.as_connection.index_remove('test', 'test_numeric_map_index',
                                        policy)
        ensure_dropped_index(self.as_connection, 'test', 'test_numeric_map_index')

    def test_mapkeysindex_with_correct_parameters_string_on_numerickeys(self):
        """
            Invoke index_mapkeys_create() with correct arguments
        """
        policy = {}
        ensure_dropped_index(self.as_connection, 'test', 'test_numeric_map_index')
        response_code = self.as_connection.index_map_keys_create(
            'test', 'demo', 'numeric_map', aerospike.INDEX_STRING,
            'test_numeric_map_index', policy)

        assert response_code == AerospikeStatus.AEROSPIKE_OK
        self.as_connection.index_remove('test', 'test_numeric_map_index',
                                        policy)
        ensure_dropped_index(self.as_connection, 'test', 'test_numeric_map_index')

    @pytest.mark.parametrize(
        "ns, test_set, test_bin, index_name",
        (
            ('a' * 32, 'demo', 'string_map', 'test_string_map'),
            ('test', 'a' * 64, 'string_map', 'test_string_map'),
            ('test', 'demo', 'a' * 16, 'test_string_map'),
            ('test', 'demo', 'string_map', 'a' * 256),
        ),
        ids=(
            'ns too long',
            'set too long',
            'bin too long',
            'index name too long'
        )
    )
    def test_mapkeys_with_parameters_too_long(self, ns, test_set, test_bin,
                                              index_name):
            # Invoke index_map_keys_create() with correct arguments and set
            # length extra
        policy = {}

        with pytest.raises(e.InvalidRequest) as err_info:
            self.as_connection.index_map_keys_create(
                ns, test_set, test_bin, aerospike.INDEX_STRING,
                index_name, policy)

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_REQUEST_INVALID

    def test_mapkeysindex_with_incorrect_namespace(self):
        """
            Invoke createindex() with incorrect namespace
        """
        policy = {}
        with pytest.raises(e.InvalidRequest) as err_info:
            self.as_connection.index_map_keys_create(
                'test1', 'demo',
                'numeric_map', aerospike.INDEX_NUMERIC,
                'test_numeric_map_index', policy)

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_REQUEST_INVALID

    def test_mapkeysindex_with_nonexistent_set(self):
        """
            Invoke createindex() with incorrect set
        """
        policy = {}
        ensure_dropped_index(self.as_connection, 'test', 'test_numeric_map_index')
        response_code = self.as_connection.index_map_keys_create(
            'test', 'demo1', 'numeric_map', aerospike.INDEX_NUMERIC,
            'test_numeric_map_index', policy)

        assert response_code == AerospikeStatus.AEROSPIKE_OK
        self.as_connection.index_remove('test', 'test_numeric_map_index',
                                        policy)
        ensure_dropped_index(self.as_connection, 'test', 'test_numeric_map_index')

    def test_mapkeysindex_on_nonexistent_bin(self):
        """
            Invoke createindex() with nonexistent bin
        """
        policy = {}
        ensure_dropped_index(self.as_connection, 'test', 'test_string_map_index')
        response_code = self.as_connection.index_map_keys_create(
            'test', 'demo', 'string_map1', aerospike.INDEX_STRING,
            'test_string_map_index', policy)

        assert response_code == AerospikeStatus.AEROSPIKE_OK
        self.as_connection.index_remove('test', 'test_string_map_index',
                                        policy)
        ensure_dropped_index(self.as_connection, 'test', 'test_string_map_index')

    @pytest.mark.parametrize(
        "ns, test_set, test_bin, index_type, index_name, policy",
        (
            (None, 'demo', 'string_map', aerospike.INDEX_STRING,
                'test_string_map_index', {}),
            (1, 'demo', 'string_map', aerospike.INDEX_STRING,
                'test_string_map_index', {}),
            ('test', 1, 'string_map', aerospike.INDEX_STRING,
                'test_string_map_index', {}),
            ('test', 'demo', None, aerospike.INDEX_STRING,
                'test_string_map_index', {}),

            ('test', 'demo', 'string_map', None,
                'test_string_map_index', {}),

            ('test', 'demo', 'string_map', (),
                'test_string_map_index', {}),

            ('test', 'demo', 'string_map', 'numeric',
                'test_string_map_index', {}),

            ('test', 'demo', 'string_map', aerospike.INDEX_STRING, None, {}),
            ('test', 'demo', 'string_map', aerospike.INDEX_STRING, 15, {}),
            ('test', 'demo', 'string_map', aerospike.INDEX_STRING,
                'test_string_map_index', 1),
            ('test', 'demo', 'string_map', aerospike.INDEX_STRING,
                'test_string_map_index', {'timeout': 0.5})
        ),
        ids=(
            'Namespace is None',
            'Namespace is Int',
            'Set is int',
            'Bin is None',
            'Index type is None',
            'Index value is tuple',
            'Index type is string',
            'index name is None',
            'Index name is wrong type',
            'Policy is int',
            'Invalid timeout policy',
        )
    )
    def test_mapkeysindex_invalid_params(self, ns, test_set, test_bin,
                                         index_type, index_name, policy):
        '''
        Test with various conditions raising paramerrors
        '''

        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.index_map_keys_create(
                ns, test_set, test_bin, index_type, index_name, policy)
            self.as_connection.index_remove(ns, index_name)
            ensure_dropped_index(self.as_connection, ns, index_name)

    def test_mapkeysindex_with_set_is_none(self):
        """
            Invoke createindex() with set is None, should not raise an error
        """
        policy = {}
        ensure_dropped_index(self.as_connection, 'test', 'test_string_map_index')
        response_code = self.as_connection.index_map_keys_create(
            'test', None,
            'string_map', aerospike.INDEX_STRING,
            'test_string_map_index', policy)

        assert response_code == AerospikeStatus.AEROSPIKE_OK
        self.as_connection.index_remove("test", 'test_string_map_index')
        ensure_dropped_index(self.as_connection, 'test', 'test_string_map_index')

    def test_create_same_mapindex_multiple_times(self):
        """
            Invoke createindex() with same arguments
            multiple times on same bin
        """
        policy = {}
        ensure_dropped_index(self.as_connection, 'test', 'test_numeric_map_index')
        response_code = self.as_connection.index_map_keys_create(
            'test', 'demo', 'numeric_map', aerospike.INDEX_NUMERIC,
            'test_numeric_map_index', policy)
        assert response_code == AerospikeStatus.AEROSPIKE_OK

        with pytest.raises(e.IndexFoundError):
            response_code = self.as_connection.index_map_keys_create(
                'test', 'demo', 'numeric_map', aerospike.INDEX_NUMERIC,
                'test_numeric_map_index', policy)
            self.as_connection.index_remove('test', 'test_numeric_map_index',
                                            policy)
        self.as_connection.index_remove('test', 'test_numeric_map_index',
                                        policy)
        ensure_dropped_index(self.as_connection, 'test', 'test_numeric_map_index')

    def test_create_same_mapindex_multiple_times_different_bin(self):
        """
            Invoke createindex with the same name on multiple bins
        """
        policy = {}
        # Create the index the first time
        ensure_dropped_index(self.as_connection, 'test', 'test_string_map_index')
        response_code = self.as_connection.index_map_keys_create(
            'test', 'demo', 'string_map', aerospike.INDEX_STRING,
            'test_string_map_index', policy)
        assert response_code == AerospikeStatus.AEROSPIKE_OK

        # Create a different index with the same name
        with pytest.raises(e.IndexFoundError):
            response_code = self.as_connection.index_map_keys_create(
                'test', 'demo', 'numeric_map', aerospike.INDEX_NUMERIC,
                'test_string_map_index', policy)

        # Cleanup the index
        self.as_connection.index_remove(
            'test', 'test_string_map_index', policy)
        ensure_dropped_index(self.as_connection, 'test', 'test_string_map_index')

    def test_create_different_mapindex_multiple_times_same_bin(self):
        """
            Call createindex on the same bin with different names
        """
        policy = {}
        index_names = ('test_string_map_index', 'test_string_map_index1')

        # Make sure the indexes don't already exist
        for index_name in index_names:
            try:
                self.as_connection.index_remove('test', index_name, policy)
                ensure_dropped_index(self.as_connection, 'test', index_name)
            except:
                pass

        with pytest.raises(e.IndexFoundError):
            for index_name in index_names:
                response_code = self.as_connection.index_map_keys_create(
                    'test', 'demo', 'string_map', aerospike.INDEX_STRING,
                    index_name, policy)
                assert response_code == AerospikeStatus.AEROSPIKE_OK

        for index_name in index_names:
            try:
                self.as_connection.index_remove('test', index_name, policy)
                ensure_dropped_index(self.as_connection, 'test', index_name)
            except:
                pass

    def test_createmapindex_with_policy(self):
        """
            Invoke createindex() with policy
        """
        policy = {'timeout': 1000}
        ensure_dropped_index(self.as_connection, 'test', 'test_numeric_map_index')
        response_code = self.as_connection.index_map_keys_create(
            'test', 'demo', 'numeric_map', aerospike.INDEX_NUMERIC,
            'test_numeric_map_index', policy)

        assert response_code == AerospikeStatus.AEROSPIKE_OK
        self.as_connection.index_remove('test', 'test_numeric_map_index',
                                        policy)
        ensure_dropped_index(self.as_connection, 'test', 'test_numeric_map_index')

    def test_create_map_integer_index_unicode(self):
        """
            Invoke createindex() with correct arguments
        """
        policy = {}
        ensure_dropped_index(self.as_connection, 'test', u'uni_age_index')
        response_code = self.as_connection.index_map_keys_create(
            'test', u'demo', u'numeric_map', aerospike.INDEX_NUMERIC,
            u'uni_age_index', policy)

        assert response_code == AerospikeStatus.AEROSPIKE_OK
        self.as_connection.index_remove('test', u'uni_age_index', policy)
        ensure_dropped_index(self.as_connection, 'test', u'uni_age_index')

    def test_mapkeysindex_with_correct_parameters_no_connection(self):
        """
            Invoke index_map_keys_create() with correct arguments no connection
        """
        policy = {}
        config = {'hosts': [('127.0.0.1', 3000)]}
        client1 = aerospike.client(config)

        with pytest.raises(e.ClusterError) as err_info:
            client1.index_map_keys_create(
                'test', 'demo', 'string_map', aerospike.INDEX_STRING,
                'test_string_map_index', policy)

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_CLUSTER_ERROR
