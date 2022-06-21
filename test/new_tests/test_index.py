# -*- coding: utf-8 -*-

import pytest
import sys
from .as_status_codes import AerospikeStatus
from .index_helpers import ensure_dropped_index
from aerospike import exception as e

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)

class TestIndex(object):

    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        for i in range(5):
            key = ('test', u'demo', i)
            rec = {
                'name': 'name%s' % (str(i)),
                'addr': 'name%s' % (str(i)),
                'age': i,
                'no': i
            }
            as_connection.put(key, rec)

        def teardown():
            ensure_dropped_index(self.as_connection, 'test', 'age_index')
            ensure_dropped_index(self.as_connection, 'test', 'name_index')
            for i in range(5):
                key = ('test', u'demo', i)
                rec = {
                    'name': 'name%s' % (str(i)),
                    'addr': 'name%s' % (str(i)),
                    'age': i,
                    'no': i
                }
                as_connection.remove(key)

        request.addfinalizer(teardown)

    def test_create_indexes_with_no_parameters(self):
        """
            Invoke indexc_string_reate() without any
            mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            self.as_connection.index_string_create()

        assert "argument 'ns' (pos 1)" in str(
            typeError.value)

        with pytest.raises(TypeError) as typeError:
            self.as_connection.index_integer_create()

        assert "argument 'ns' (pos 1)" in str(
            typeError.value)

    def test_create_integer_index_with_correct_parameters(self):
        """
            Invoke createindex() with correct arguments
        """
        policy = {}
        retobj = self.as_connection.index_integer_create('test', 'demo', 'age',
                                                         'age_index', policy)

        assert retobj == AerospikeStatus.AEROSPIKE_OK
        self.as_connection.index_remove('test', 'age_index', policy)
        ensure_dropped_index(self.as_connection, 'test', 'age_index')

    def test_create_integer_index_with_set_name_too_long(self):
            # Invoke createindex with a set name beyond the maximum
        set_name = 'a' * 128

        policy = {}
        with pytest.raises(e.InvalidRequest) as err_info:
            self.as_connection.index_integer_create(
                'test', set_name, 'age', 'age_index', policy)

        err_code = err_info.value.code
        assert err_code is AerospikeStatus.AEROSPIKE_ERR_REQUEST_INVALID

    def test_create_integer_index_with_incorrect_namespace(self):
        """
            Invoke createindex() with non existent namespace
        """
        policy = {}
        with pytest.raises(e.InvalidRequest) as err_info:
            self.as_connection.index_integer_create('fake_namespace', 'demo',
                                                    'age', 'age_index', policy)

        err_code = err_info.value.code
        assert err_code is AerospikeStatus.AEROSPIKE_ERR_REQUEST_INVALID

    def test_create_integer_index_with_incorrect_set(self):
        """
            Invoke createindex() with nonexistent set
            It should succeed
        """
        policy = {}
        retobj = self.as_connection.index_integer_create(
            'test', 'demo1', 'age', 'age_index', policy)

        assert retobj == AerospikeStatus.AEROSPIKE_OK
        self.as_connection.index_remove('test', 'age_index', policy)
        ensure_dropped_index(self.as_connection, 'test', 'age_index')

    def test_create_integer_index_with_incorrect_bin(self):
        """
            Invoke createindex() with a nonexistent bin
        """
        policy = {}
        retobj = self.as_connection.index_integer_create(
            'test', 'demo', 'fake_bin', 'age_index', policy)

        assert retobj == AerospikeStatus.AEROSPIKE_OK
        self.as_connection.index_remove('test', 'age_index', policy)
        ensure_dropped_index(self.as_connection, 'test', 'age_index')

    def test_create_integer_index_with_namespace_is_none(self):
        """
            Invoke createindex() with namespace is None
        """
        policy = {}
        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.index_integer_create(None, 'demo',
                                                    'age', 'age_index', policy)

        err_code = err_info.value.code
        assert err_code is AerospikeStatus.AEROSPIKE_ERR_PARAM

    def test_creat_integer_eindex_with_set_is_none(self):
            # Invoke createindex() with set is None
        policy = {}

        retobj = self.as_connection.index_integer_create(
            'test', None, 'age', 'age_index', policy)

        assert retobj == AerospikeStatus.AEROSPIKE_OK
        self.as_connection.index_remove('test', 'age_index', policy)
        ensure_dropped_index(self.as_connection, 'test', 'age_index')

    def test_create_integer_index_with_set_is_int(self):
            # Invoke createindex() with set is int
        policy = {}

        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.index_integer_create('test', 1, 'age',
                                                    'age_index', policy)
        err_code = err_info.value.code
        assert err_code is AerospikeStatus.AEROSPIKE_ERR_PARAM

    def test_create_integer_index_with_bin_is_none(self):
        """
            Invoke createindex() with bin is None
        """
        policy = {}
        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.index_integer_create('test', 'demo',
                                                    None, 'age_index', policy)

        err_code = err_info.value.code
        assert err_code is AerospikeStatus.AEROSPIKE_ERR_PARAM

    def test_create_integer_index_with_index_is_none(self):
        """
            Invoke createindex() with index_name is None
        """
        policy = {}
        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.index_integer_create('test', 'demo',
                                                    'age', None, policy)

        err_code = err_info.value.code
        assert err_code is AerospikeStatus.AEROSPIKE_ERR_PARAM

    def test_create_same_integer_index_multiple_times(self):
        """
            Invoke createindex() with the same arguments
            multiple times on the same bin

        """
        policy = {}
        retobj = self.as_connection.index_integer_create('test', 'demo', 'age',
                                                         'age_index', policy)
        assert retobj == AerospikeStatus.AEROSPIKE_OK
        with pytest.raises(e.IndexFoundError):
            retobj = self.as_connection.index_integer_create(
                'test', 'demo', 'age', 'age_index', policy)
            self.as_connection.index_remove('test', 'age_index', policy)

        ensure_dropped_index(self.as_connection, 'test', 'age_index')

    def test_create_same_integer_index_multiple_times_different_bin(self):
        """
            Invoke createindex() with the same index name,
            multiple times on different bin names
        """
        policy = {}
        retobj = self.as_connection.index_integer_create(
            'test', 'demo', 'age', 'age_index', policy)

        assert retobj == AerospikeStatus.AEROSPIKE_OK

        with pytest.raises(e.IndexFoundError):
            retobj = self.as_connection.index_integer_create(
                'test', 'demo', 'no', 'age_index', policy)
            self.as_connection.index_remove('test', 'age_index', policy)

        ensure_dropped_index(self.as_connection, 'test', 'age_index')

    def test_create_different_integer_index_multiple_times_same_bin(self):
        """
            Invoke createindex() with multiple times on same bin with different
name
        """
        policy = {}
        retobj = self.as_connection.index_integer_create(
            'test', 'demo', 'age', 'age_index', policy)
        assert retobj == AerospikeStatus.AEROSPIKE_OK

        with pytest.raises(e.IndexFoundError):
            retobj = self.as_connection.index_integer_create(
                'test', 'demo', 'age', 'age_index1', policy)
            self.as_connection.index_remove('test', 'age_index1', policy)

        ensure_dropped_index(self.as_connection, 'test', 'age_index')

    def test_create_integer_index_with_policy(self):
        """
            Invoke createindex() with policy
        """
        policy = {'timeout': 1000}
        retobj = self.as_connection.index_integer_create('test', 'demo', 'age',
                                                         'age_index', policy)

        ensure_dropped_index(self.as_connection, 'test', 'age_index')
        assert retobj == AerospikeStatus.AEROSPIKE_OK

    def test_create_string_index_positive(self):
        """
            Invoke create string index() with correct arguments
        """
        policy = {}
        retobj = self.as_connection.index_string_create('test', 'demo', 'name',
                                                        'name_index', policy)

        self.as_connection.index_remove('test', 'name_index', policy)
        ensure_dropped_index(self.as_connection, 'test', 'name_index')

        assert retobj == AerospikeStatus.AEROSPIKE_OK

    def test_create_string_index_with_set_length_too_long(self):
            # Invoke createindex() with correct arguments set length extra
        set_name = 'a' * 100
        policy = {}

        with pytest.raises(e.InvalidRequest) as err_info:
            self.as_connection.index_string_create(
                'test', set_name, 'name', 'name_index', policy)
        err_code = err_info.value.code
        assert err_code is AerospikeStatus.AEROSPIKE_ERR_REQUEST_INVALID

    def test_create_string_index_with_correct_parameters_ns_length_extra(self):
            # Invoke createindex() with correct arguments ns length extra
        ns_name = 'a' * 50
        policy = {}

        with pytest.raises(e.InvalidRequest) as err_info:
            self.as_connection.index_string_create(
                ns_name, 'demo', 'name', 'name_index', policy)

        err_code = err_info.value.code
        assert err_code is AerospikeStatus.AEROSPIKE_ERR_REQUEST_INVALID

    def test_create_string_index_with_incorrect_namespace(self):
        """
            Invoke create string index() with incorrect namespace
        """
        policy = {}

        with pytest.raises(e.InvalidRequest) as err_info:
            self.as_connection.index_string_create(
                'fake_namespace', 'demo', 'name', 'name_index', policy)

        err_code = err_info.value.code
        assert err_code is AerospikeStatus.AEROSPIKE_ERR_REQUEST_INVALID

    def test_create_string_index_with_incorrect_set(self):
        """
            Invoke create string index() with incorrect set
        """
        policy = {}
        retobj = self.as_connection.index_string_create(
            'test', 'demo1', 'name', 'name_index', policy)

        self.as_connection.index_remove('test', 'name_index', policy)
        ensure_dropped_index(self.as_connection, 'test', 'name_index')
        assert retobj == AerospikeStatus.AEROSPIKE_OK

    def test_create_string_index_with_incorrect_bin(self):
        """
            Invoke create string index() with incorrect bin
        """
        policy = {}
        retobj = self.as_connection.index_string_create(
            'test', 'demo', 'name1', 'name_index', policy)

        self.as_connection.index_remove('test', 'name_index', policy)
        ensure_dropped_index(self.as_connection, 'test', 'name_index')
        assert retobj == AerospikeStatus.AEROSPIKE_OK

    def test_create_string_index_with_namespace_is_none(self):
        """
            Invoke create string index() with namespace is None
        """
        policy = {}
        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.index_string_create(
                None, 'demo', 'name', 'name_index', policy)

        err_code = err_info.value.code
        assert err_code is AerospikeStatus.AEROSPIKE_ERR_PARAM

    def test_create_string_index_with_set_is_none(self):
            # Invoke create string index() with set is None
        policy = {}
        retobj = self.as_connection.index_string_create(
            'test', None, 'name', 'name_index', policy)

        self.as_connection.index_remove('test', 'name_index', policy)
        ensure_dropped_index(self.as_connection, 'test', 'name_index')
        assert retobj == AerospikeStatus.AEROSPIKE_OK

    def test_create_string_index_with_bin_is_none(self):
        """
            Invoke create string index() with bin is None
        """
        policy = {}
        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.index_string_create(
                'test', 'demo', None, 'name_index', policy)

        err_code = err_info.value.code
        assert err_code is AerospikeStatus.AEROSPIKE_ERR_PARAM

    def test_create_string_index_with_index_is_none(self):
        """
            Invoke create_string_index() with index name is None
        """
        policy = {}
        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.index_string_create(
                'test', 'demo', 'name', None, policy)

        err_code = err_info.value.code
        assert err_code is AerospikeStatus.AEROSPIKE_ERR_PARAM

    def test_create_same_string_index_multiple_times(self):
        """
            Invoke create string index() with multiple times on same bin
        """
        policy = {}
        retobj = self.as_connection.index_string_create(
            'test', 'demo', 'name', 'name_index', policy)
        assert retobj == AerospikeStatus.AEROSPIKE_OK

        with pytest.raises(e.IndexFoundError):
            retobj = self.as_connection.index_string_create(
                'test', 'demo', 'name', 'name_index', policy)
            self.as_connection.index_remove('test', 'name_index', policy)
            ensure_dropped_index(self.as_connection, 'test', 'name_index')

        self.as_connection.index_remove('test', 'name_index', policy)
        ensure_dropped_index(self.as_connection, 'test', 'name_index')

    def test_create_same_string_index_multiple_times_different_bin(self):
        """
            Invoke create string index() with multiple times on different bin
        """
        policy = {}
        retobj = self.as_connection.index_string_create('test', 'demo', 'name',
                                                        'name_index', policy)
        assert retobj == AerospikeStatus.AEROSPIKE_OK

        with pytest.raises(e.IndexFoundError):
            retobj = self.as_connection.index_string_create(
                'test', 'demo', 'addr', 'name_index', policy)
            self.as_connection.index_remove('test', 'name_index', policy)
            ensure_dropped_index(self.as_connection, 'test', 'name_index')

        self.as_connection.index_remove('test', 'name_index', policy)
        ensure_dropped_index(self.as_connection, 'test', 'name_index')

        assert retobj == AerospikeStatus.AEROSPIKE_OK

    def test_create_different_string_index_multiple_times_same_bin(self):
        """
            Invoke create string index() with multiple times on same
            bin with different name
        """
        policy = {}
        retobj = self.as_connection.index_string_create('test', 'demo', 'name',
                                                        'name_index', policy)
        assert retobj == AerospikeStatus.AEROSPIKE_OK
        with pytest.raises(e.IndexFoundError):
            retobj = self.as_connection.index_string_create(
                'test', 'demo', 'name', 'name_index1', policy)
            self.as_connection.index_remove('test', 'name_index1', policy)
            ensure_dropped_index(self.as_connection, 'test', 'name_index')

        self.as_connection.index_remove('test', 'name_index', policy)
        ensure_dropped_index(self.as_connection, 'test', 'name_index')

    def test_create_string_index_with_policy(self):
        """
            Invoke create string index() with policy
        """
        policy = {'timeout': 1000}
        retobj = self.as_connection.index_string_create('test', 'demo', 'name',
                                                        'name_index', policy)

        assert retobj == AerospikeStatus.AEROSPIKE_OK
        self.as_connection.index_remove('test', 'name_index', policy)
        ensure_dropped_index(self.as_connection, 'test', 'name_index')

    def test_drop_invalid_index(self):
        """
            Invoke drop invalid index()
        """
        policy = {}
        with pytest.raises(e.IndexNotFound):
            retobj = self.as_connection.index_remove('test', 'notarealindex',
                                                     policy)

    def test_drop_valid_index(self):
        """
            Invoke drop valid index()
        """
        policy = {}
        self.as_connection.index_integer_create('test', 'demo', 'age',
                                                'age_index', policy)
        retobj = self.as_connection.index_remove('test', 'age_index', policy)
        ensure_dropped_index(self.as_connection, 'test', 'age_index')
        assert retobj == AerospikeStatus.AEROSPIKE_OK

    def test_drop_valid_index_policy(self):
        """
            Invoke drop valid index() policy
        """
        policy = {'timeout': 1000}
        self.as_connection.index_integer_create('test', 'demo', 'age',
                                                'age_index', policy)
        retobj = self.as_connection.index_remove('test', 'age_index', policy)
        ensure_dropped_index(self.as_connection, 'test', 'age_index')
        assert retobj == AerospikeStatus.AEROSPIKE_OK

    def test_createindex_with_long_index_name(self):
        # Invoke createindex() with long index name
        policy = {}
        with pytest.raises(e.InvalidRequest):
            retobj = self.as_connection.index_integer_create(
                'test', 'demo', 'age', 'index' * 100, policy)

    def test_create_string_index_unicode_positive(self):
        """
            Invoke create string index() with correct arguments
        """
        policy = {}
        retobj = self.as_connection.index_string_create('test', u'demo',
                                                        u'name',
                                                        u'uni_name_index',
                                                        policy)

        self.as_connection.index_remove('test', u'uni_name_index', policy)
        ensure_dropped_index(self.as_connection, 'test', u'uni_name_index')
        assert retobj == AerospikeStatus.AEROSPIKE_OK

    def test_createindex_integer_unicode(self):
        """
            Invoke createindex() with correct arguments
        """
        policy = {}
        retobj = self.as_connection.index_integer_create('test', u'demo',
                                                         u'age',
                                                         u'uni_age_index',
                                                         policy)

        assert retobj == AerospikeStatus.AEROSPIKE_OK
        self.as_connection.index_remove('test', u'uni_age_index', policy)
        ensure_dropped_index(self.as_connection, 'test', u'uni_age_index')

    def test_createindex_with_correct_parameters_without_connection(self):
            # Invoke createindex() with correct arguments without connection
        policy = {}
        config = {'hosts': [('127.0.0.1', 3000)]}
        client1 = aerospike.client(config)

        with pytest.raises(e.ClusterError) as err_info:
            client1.index_integer_create(
                'test', 'demo', 'age', 'age_index', policy)

        err_code = err_info.value.code
        assert err_code is AerospikeStatus.AEROSPIKE_CLUSTER_ERROR

    def test_index_remove_no_args(self):

        with pytest.raises(TypeError):
            self.as_connection.index_remove()

    def test_index_remove_no_index(self):

        with pytest.raises(TypeError):
            self.as_connection.index_remove('test')

    def test_index_remove_extra_args(self):
        # pass 'ns', 'idx_name', 'policy', and an extra argument
        with pytest.raises(TypeError):
            self.as_connection.index_remove('test', 'demo', {}, 'index_name')

    @pytest.mark.parametrize(
        "ns, idx_name, policy",
        (
            ('test', 'idx', 'policy'),
            ('test', 5, {}),
            (5, 'idx', {}),
            ('test', None, {}),
            (None, 'idx', {})

        )
    )
    def test_index_remove_wrong_arg_types(self, ns, idx_name, policy):
        with pytest.raises(e.ParamError):
            self.as_connection.index_remove(ns, idx_name, policy)
