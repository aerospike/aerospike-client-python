# -*- coding: utf-8 -*-

import pytest
import sys
from .as_status_codes import AerospikeStatus
from aerospike import exception as e
from .index_helpers import ensure_dropped_index

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)


def add_maps_to_client(client):
    """
    Setup method.
    """
    for i in range(5):
        key = ('test', u'demo', i)
        rec = {
            'name': 'name%s' % (str(i)),
            'addr': 'name%s' % (str(i)),
            'numeric_map': {"a": 1,
                            "b": 2,
                            "c": 3},
            'string_map': {"sa": "a",
                           "sb": "b",
                           "sc": "c"},
            'age': i,
            'no': i
        }
        client.put(key, rec)


def remove_maps_from_client(client):
    for i in range(5):
        key = ('test', u'demo', i)
        client.remove(key)


@pytest.mark.usefixtures("connection_with_config_funcs")
class TestMapValuesIndex(object):

    def setup_class(cls):
        """
        Setup method.
        """
        cls.connection_setup_functions = [add_maps_to_client]
        cls.connection_teardown_functions = [remove_maps_from_client]

    def test_mapvaluesindex_with_no_paramters(self):
        """
            Invoke index_mapkeys_create() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            self.as_connection.index_map_values_create()

        assert "argument 'ns' (pos 1)" in str(
            typeError.value)

    def test_mapvaluesindex_with_correct_parameters(self):
        """
            Invoke index_mapvalues_create() with correct arguments
            and a string_index
        """
        policy = {}
        retobj = self.as_connection.index_map_values_create(
            'test', 'demo', 'string_map', aerospike.INDEX_STRING,
            'test_string_map_index', policy)

        assert retobj == AerospikeStatus.AEROSPIKE_OK
        self.as_connection.index_remove('test', 'test_string_map_index',
                                        policy)
        ensure_dropped_index(self.as_connection, 'test', 'test_string_map_index')

    def test_mapvaluesindex_with_correct_parameters_no_policy(self):
        """
            Invoke index_mapvalues_create() with correct arguments
            and the policy argument not passed
        """
        retobj = self.as_connection.index_map_values_create(
            'test', 'demo', 'string_map', aerospike.INDEX_STRING,
            'test_string_map_index')

        assert retobj == AerospikeStatus.AEROSPIKE_OK
        self.as_connection.index_remove('test', 'test_string_map_index')
        ensure_dropped_index(self.as_connection, 'test', 'test_string_map_index')

    def test_mapvaluesindex_with_correct_parameters_numeric(self):
        """
            Invoke index_mapkeys_create() with correct arguments
            and a numeric index
        """
        policy = {}
        retobj = self.as_connection.index_map_values_create(
            'test', 'demo', 'numeric_map', aerospike.INDEX_NUMERIC,
            'test_numeric_map_index', policy)

        assert retobj == AerospikeStatus.AEROSPIKE_OK
        self.as_connection.index_remove(
            'test', 'test_numeric_map_index', policy)
        ensure_dropped_index(self.as_connection, 'test', 'test_numeric_map_index')

    def test_mapvalues_index_with_correct_parameters_set_length_extra(self):
            # Invoke index_map_values_create() with correct arguments and set
            # length extra
        set_name = 'a' * 256

        policy = {}
        with pytest.raises(e.InvalidRequest) as err_info:
            self.as_connection.index_map_values_create(
                'test', set_name,
                'string_map', aerospike.INDEX_STRING,
                "test_string_map_index", policy)

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_REQUEST_INVALID

    def test_mapvaluesindex_with_incorrect_namespace(self):
        """
            Invoke createindex() with incorrect namespace
        """
        policy = {}
        with pytest.raises(e.InvalidRequest) as err_info:
            self.as_connection.index_map_values_create(
                'test1', 'demo',
                'numeric_map', aerospike.INDEX_NUMERIC,
                'test_numeric_map_index', policy)

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_REQUEST_INVALID

    def test_mapvaluesindex_with_incorrect_set(self):
        """
            Invoke createindex() with incorrect set
        """
        policy = {}
        retobj = self.as_connection.index_map_values_create(
            'test', 'demo1', 'numeric_map', aerospike.INDEX_NUMERIC,
            'test_numeric_map_index', policy)

        assert retobj == AerospikeStatus.AEROSPIKE_OK
        self.as_connection.index_remove(
            'test', 'test_numeric_map_index', policy)
        ensure_dropped_index(self.as_connection, 'test', 'test_numeric_map_index')

    def test_mapvaluesindex_with_incorrect_bin(self):
        """
            Invoke createindex() with incorrect bin
        """
        policy = {}
        retobj = self.as_connection.index_map_values_create(
            'test', 'demo', 'string_map1', aerospike.INDEX_STRING,
            'test_string_map_index', policy)

        assert retobj == AerospikeStatus.AEROSPIKE_OK
        self.as_connection.index_remove('test', 'test_string_map_index',
                                        policy)
        ensure_dropped_index(self.as_connection, 'test', 'test_string_map_index')

    @pytest.mark.parametrize(
        "test_ns, test_set, test_bin, test_idx_name",
        (
            (None, 'demo', 'string_map', 'test_string_map_index'),
            (1, 'demo', 'string_map', 'test_string_map_index'),
            ('test', 1, 'string_map', 'test_string_map_index'),
            ('test', 'demo', None, 'test_string_map_index'),
            ('test', 'demo', 'string_map', None),
            ('test', 'demo', 'string_map', 1),
        ),
        ids=(
            'ns is None',
            'ns is int',
            'set is int',
            'bin is None',
            'index name is none',
            'index name is int'
        )
    )
    def test_mapvaluesindex_with_invalid_params(self, test_ns, test_set,
                                                test_bin, test_idx_name):

        policy = {}

        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.index_map_values_create(
                test_ns, test_set, test_bin, aerospike.INDEX_STRING,
                test_idx_name, policy)

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_PARAM

    @pytest.mark.parametrize("idx_val", (None, "a", ()))
    def test_mapvaluesindex_with_invalid_idx_values(self, idx_val):

        policy = {}

        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.index_map_values_create(
                'test', 'demo', 'string_map', idx_val,
                "test_string_map_index", policy)
            try:
                self.as_connection.index_remove(
                    'test', 'test_string_map_index')
                ensure_dropped_index(self.as_connection, 'test', 'test_string_map_index')
            except:
                pass

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_PARAM

    def test_create_same_mapvaluesindex_multiple_times(self):
        """
            Invoke createindex() with multiple times on same bin, with same
            name
        """
        policy = {}
        retobj = self.as_connection.index_map_values_create(
            'test', 'demo', 'numeric_map', aerospike.INDEX_NUMERIC,
            'test_numeric_map_index', policy)
        assert retobj == AerospikeStatus.AEROSPIKE_OK

        with pytest.raises(e.IndexFoundError):
            retobj = self.as_connection.index_map_values_create(
                'test', 'demo', 'numeric_map', aerospike.INDEX_NUMERIC,
                'test_numeric_map_index', policy)
            self.as_connection.index_remove(
                'test', 'test_numeric_map_index', policy)
            ensure_dropped_index(self.as_connection, 'test', 'test_numeric_map_index')

        self.as_connection.index_remove(
            'test', 'test_numeric_map_index', policy)
        ensure_dropped_index(self.as_connection, 'test', 'test_numeric_map_index')

    def test_create_same_mapvaluesindex_multiple_times_different_bin(self):
        """
            Invoke createindex() with multiple times on different bin,
            with the same name
        """
        policy = {}

        retobj = self.as_connection.index_map_values_create(
            'test', 'demo', 'string_map', aerospike.INDEX_STRING,
            'test_string_map_index', policy)
        assert retobj == AerospikeStatus.AEROSPIKE_OK

        with pytest.raises(e.IndexFoundError):
            retobj = self.as_connection.index_map_values_create(
                'test', 'demo', 'numeric_map', aerospike.INDEX_NUMERIC,
                'test_string_map_index', policy)
            self.as_connection.index_remove(
                'test', 'test_string_map_index', policy)
            ensure_dropped_index(self.as_connection, 'test', 'test_string_map_index')

        self.as_connection.index_remove(
            'test', 'test_string_map_index', policy)
        ensure_dropped_index(self.as_connection, 'test', 'test_string_map_index')

    def test_create_different_mapvaluesindex_multiple_times_same_bin(self):
        """
            Invoke createindex() with multiple times on same bin with different
            name
        """
        policy = {}
        retobj = self.as_connection.index_map_values_create(
            'test', 'demo', 'string_map', aerospike.INDEX_STRING,
            'test_string_map_index', policy)

        assert retobj == AerospikeStatus.AEROSPIKE_OK
        with pytest.raises(e.IndexFoundError):
            retobj = self.as_connection.index_map_values_create(
                'test', 'demo', 'string_map', aerospike.INDEX_STRING,
                'test_string_map_index1', policy)
            self.as_connection.index_remove(
                'test', 'test_string_map_index1', policy)
            ensure_dropped_index(self.as_connection, 'test', 'test_string_map_index1')
            self.as_connection.index_remove(
                'test', 'test_string_map_index', policy)
            ensure_dropped_index(self.as_connection, 'test', 'test_string_map_index')

        self.as_connection.index_remove(
            'test', 'test_string_map_index', policy)
        ensure_dropped_index(self.as_connection, 'test', 'test_string_map_index')

    def test_createmapvaluesindex_with_policy(self):
        """
            Invoke createindex() with policy
        """
        policy = {'timeout': 1000}
        retobj = self.as_connection.index_map_values_create(
            'test', 'demo', 'numeric_map', aerospike.INDEX_NUMERIC,
            'test_numeric_map_index', policy)

        assert retobj == AerospikeStatus.AEROSPIKE_OK
        self.as_connection.index_remove(
            'test', 'test_numeric_map_index', policy)
        ensure_dropped_index(self.as_connection, 'test', 'test_numeric_map_index')

    def test_createmapvaluesindex_with_policystring(self):
        """
            Invoke createindex() with policy
        """
        policy = {'timeout': 1000}
        retobj = self.as_connection.index_map_values_create(
            'test', 'demo', 'string_map', aerospike.INDEX_STRING,
            'test_string_map_index', policy)

        assert retobj == AerospikeStatus.AEROSPIKE_OK
        self.as_connection.index_remove('test', 'test_string_map_index',
                                        policy)
        ensure_dropped_index(self.as_connection, 'test', 'test_string_map_index')

    """
    This test case causes a db crash and hence has been commented. Work pending
on the C-client side
    def test_createindex_with_long_index_name(self):
            Invoke createindex() with long index name
        policy = {}
        retobj = self.as_connection.index_map_values_create(
            'test', 'demo',
'age',
'bin2_integer_indexsdadadfasdfasdfeartfqrgahfasdfheudsdfasdfawf312342q3453r\
f9qwfasdcfasdcalskdcbacfq34915rwcfasdcascnabscbaskjdbcalsjkbcdasc', policy)

        assert retobj == 0L
        self.as_connection.index_remove(policy, 'test',
'bin2_integer_indexsdadadfasdfasdfeartfqrgahfasdfheudsdfasdfawf312342q3453rf9\
qwfasdcfasdcalskdcbacfq34915rwcfasdcascnabscbaskjdbcalsjkbcdasc');
    """

    def test_create_mapvaluesindex_unicode_positive(self):
        """
            Invoke create string index() with correct arguments
        """
        policy = {}
        retobj = self.as_connection.index_map_values_create(
            'test', u'demo', u'string_map', aerospike.INDEX_STRING,
            u'uni_name_index', policy)

        assert retobj == AerospikeStatus.AEROSPIKE_OK
        self.as_connection.index_remove('test', u'uni_name_index',
                                        policy)
        ensure_dropped_index(self.as_connection, 'test', u'uni_name_index')

    def test_create_map_values_integer_index_unicode(self):
        """
            Invoke createindex() with correct arguments
        """
        policy = {}
        retobj = self.as_connection.index_map_values_create(
            'test', u'demo', u'numeric_map', aerospike.INDEX_NUMERIC,
            u'uni_age_index', policy)

        assert retobj == AerospikeStatus.AEROSPIKE_OK
        self.as_connection.index_remove(
            'test', u'uni_age_index', policy)
        ensure_dropped_index(self.as_connection, 'test', u'uni_age_index')

    def test_mapvaluesindex_with_correct_parameters_no_connection(self):
        """
            Invoke index_mapvalues_create() with correct arguments no
            connection
        """
        policy = {}
        config = {'hosts': [('127.0.0.1', 3000)]}
        client1 = aerospike.client(config)

        with pytest.raises(e.ClusterError) as err_info:
            client1.index_map_values_create(
                'test', 'demo', 'string_map', aerospike.INDEX_STRING,
                'test_string_map_index', policy)

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_CLUSTER_ERROR
