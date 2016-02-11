# -*- coding: utf-8 -*-

import pytest
import sys
from .test_base_class import TestBaseClass
from aerospike import exception as e

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)


class TestMapValuesIndex(object):

    def setup_class(cls):
        """
        Setup method.
        """
        hostlist, user, password = TestBaseClass.get_hosts()
        config = {'hosts': hostlist}
        if user is None and password is None:
            TestMapValuesIndex.client = aerospike.client(config).connect()
        else:
            TestMapValuesIndex.client = aerospike.client(config).connect(
                user, password)

    def teardown_class(cls):
        TestMapValuesIndex.client.close()

    def setup_method(self, method):
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
            TestMapValuesIndex.client.put(key, rec)

    def teardown_method(self, method):
        """
        Teardoen method.
        """
        for i in range(5):
            key = ('test', u'demo', i)
            TestMapValuesIndex.client.remove(key)

        # TestMapValuesIndex.client.close()

    def test_mapvaluesindex_with_no_paramters(self):
        """
            Invoke index_mapkeys_create() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            TestMapValuesIndex.client.index_map_values_create()

        assert "Required argument 'ns' (pos 1) not found" in str(
            typeError.value)

    def test_mapvaluesindex_with_correct_parameters(self):
        """
            Invoke index_mapvalues_create() with correct arguments
        """
        policy = {}
        retobj = TestMapValuesIndex.client.index_map_values_create(
            'test', 'demo', 'string_map', aerospike.INDEX_STRING,
            'test_string_map_index', policy)

        assert retobj == 0
        TestMapValuesIndex.client.index_remove('test', 'test_string_map_index',
                                               policy)

    def test_mapvaluesindex_with_correct_parameters_numeric(self):
        """
            Invoke index_mapkeys_create() with correct arguments
        """
        policy = {}
        retobj = TestMapValuesIndex.client.index_map_values_create(
            'test', 'demo', 'numeric_map', aerospike.INDEX_NUMERIC,
            'test_numeric_map_index', policy)

        assert retobj == 0
        TestMapValuesIndex.client.index_remove(
            'test', 'test_numeric_map_index', policy)

    def test_mapvalues_index_with_correct_parameters_set_length_extra(self):
            # Invoke index_map_values_create() with correct arguments and set
            # length extra
        set_name = 'a'
        for _ in range(100):
            set_name = set_name + 'a'
        policy = {}
        try:
            TestMapValuesIndex.client.index_map_values_create(
                'test', set_name,
                'string_map', aerospike.INDEX_STRING,
                "test_string_map_index", policy)
        except e.InvalidRequest as exception:
            assert exception.code == 4

    def test_mapvaluesindex_with_incorrect_namespace(self):
        """
            Invoke createindex() with incorrect namespace
        """
        policy = {}
        try:
            TestMapValuesIndex.client.index_map_values_create(
                'test1', 'demo',
                'numeric_map', aerospike.INDEX_NUMERIC,
                'test_numeric_map_index', policy)
        except e.InvalidRequest as exception:
            assert exception.code == 4

    def test_mapvaluesindex_with_incorrect_set(self):
        """
            Invoke createindex() with incorrect set
        """
        policy = {}
        retobj = TestMapValuesIndex.client.index_map_values_create(
            'test', 'demo1', 'numeric_map', aerospike.INDEX_NUMERIC,
            'test_numeric_map_index', policy)

        assert retobj == 0
        TestMapValuesIndex.client.index_remove(
            'test', 'test_numeric_map_index', policy)

    def test_mapvaluesindex_with_incorrect_bin(self):
        """
            Invoke createindex() with incorrect bin
        """
        policy = {}
        retobj = TestMapValuesIndex.client.index_map_values_create(
            'test', 'demo', 'string_map1', aerospike.INDEX_STRING,
            'test_string_map_index', policy)

        assert retobj == 0
        TestMapValuesIndex.client.index_remove('test', 'test_string_map_index',
                                               policy)

    def test_mapvaluesindex_with_namespace_is_none(self):
        """
            Invoke createindex() with namespace is None
        """
        policy = {}
        try:
            TestMapValuesIndex.client.index_map_values_create(
                None, 'demo',
                'string_map', aerospike.INDEX_STRING,
                'test_string_map_index', policy)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == 'Namespace should be a string'

    def test_mapvaluesindex_with_set_is_int(self):
        """
            Invoke createindex() with set is int
        """
        policy = {}
        try:
            TestMapValuesIndex.client.index_map_values_create(
                'test', 1, 'string_map', aerospike.INDEX_STRING,
                'test_string_map_index', policy)

            assert False
        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == 'Set should be string, unicode or None'
        except Exception as exception:
            assert type(exception) == e.ParamError

    def test_mapvaluesindex_with_set_is_none(self):
        """
            Invoke createindex() with set is None
        """
        policy = {}
        try:
            TestMapValuesIndex.client.index_map_values_create(
                'test', None,
                'string_map', aerospike.INDEX_STRING,
                'test_string_map_index', policy)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == 'Set should be a string'

    def test_mapvaluesindex_with_bin_is_none(self):
        """
            Invoke createindex() with bin is None
        """
        policy = {}
        try:
            TestMapValuesIndex.client.index_map_values_create(
                'test', 'demo',
                None, aerospike.INDEX_NUMERIC,
                'test_numeric_map_index', policy)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == 'Bin should be a string'

    def test_mapvaluesindex_with_index_is_none(self):
        """
            Invoke createindex() with index is None
        """
        policy = {}
        try:
            TestMapValuesIndex.client.index_map_values_create(
                'test', 'demo',
                'string_map', aerospike.INDEX_STRING, None, policy)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == 'Index name should be string or unicode'

    def test_create_same_mapvaluesindex_multiple_times(self):
        """
            Invoke createindex() with multiple times on same bin
        """
        policy = {}
        retobj = TestMapValuesIndex.client.index_map_values_create(
            'test', 'demo', 'numeric_map', aerospike.INDEX_NUMERIC,
            'test_numeric_map_index', policy)
        if retobj == 0:
            retobj = TestMapValuesIndex.client.index_map_values_create(
                'test', 'demo', 'numeric_map', aerospike.INDEX_NUMERIC,
                'test_numeric_map_index', policy)
            TestMapValuesIndex.client.index_remove(
                'test', 'test_numeric_map_index', policy)
            assert retobj == 0
        else:
            assert True is False

    def test_create_same_mapvaluesindex_multiple_times_different_bin(self):
        """
            Invoke createindex() with multiple times on different bin
        """
        policy = {}
        retobj = TestMapValuesIndex.client.index_map_values_create(
            'test', 'demo', 'string_map', aerospike.INDEX_STRING,
            'test_string_map_index', policy)
        if retobj == 0:
            retobj = TestMapValuesIndex.client.index_map_values_create(
                'test', 'demo', 'numeric_map', aerospike.INDEX_NUMERIC,
                'test_string_map_index', policy)
            assert retobj == 0
            TestMapValuesIndex.client.index_remove(
                'test', 'test_string_map_index', policy)
        else:
            assert True is False

    def test_create_different_mapvaluesindex_multiple_times_same_bin(self):
        """
            Invoke createindex() with multiple times on same bin with different
name
        """
        policy = {}
        retobj = TestMapValuesIndex.client.index_map_values_create(
            'test', 'demo', 'string_map', aerospike.INDEX_STRING,
            'test_string_map_index', policy)
        if retobj == 0:
            retobj = TestMapValuesIndex.client.index_map_values_create(
                'test', 'demo', 'string_map', aerospike.INDEX_STRING,
                'test_string_map_index1', policy)
            assert retobj == 0
            TestMapValuesIndex.client.index_remove(
                'test', 'test_string_map_index', policy)
            TestMapValuesIndex.client.index_remove(
                'test', 'test_string_map_index1', policy)
        else:
            assert True is False

    def test_createmapvaluesindex_with_policy(self):
        """
            Invoke createindex() with policy
        """
        policy = {'timeout': 1000}
        retobj = TestMapValuesIndex.client.index_map_values_create(
            'test', 'demo', 'numeric_map', aerospike.INDEX_NUMERIC,
            'test_numeric_map_index', policy)

        assert retobj == 0
        TestMapValuesIndex.client.index_remove(
            'test', 'test_numeric_map_index', policy)

    def test_createmapvaluesindex_with_policystring(self):
        """
            Invoke createindex() with policy
        """
        policy = {'timeout': 1000}
        retobj = TestMapValuesIndex.client.index_map_values_create(
            'test', 'demo', 'string_map', aerospike.INDEX_STRING,
            'test_string_map_index', policy)

        assert retobj == 0
        TestMapValuesIndex.client.index_remove('test', 'test_string_map_index',
                                               policy)

    """
    This test case causes a db crash and hence has been commented. Work pending
on the C-client side
    def test_createindex_with_long_index_name(self):
            Invoke createindex() with long index name
        policy = {}
        retobj = TestMapValuesIndex.client.index_map_values_create(
            'test', 'demo',
'age',
'bin2_integer_indexsdadadfasdfasdfeartfqrgahfasdfheudsdfasdfawf312342q3453r\
f9qwfasdcfasdcalskdcbacfq34915rwcfasdcascnabscbaskjdbcalsjkbcdasc', policy)

        assert retobj == 0L
        TestMapValuesIndex.client.index_remove(policy, 'test',
'bin2_integer_indexsdadadfasdfasdfeartfqrgahfasdfheudsdfasdfawf312342q3453rf9\
qwfasdcfasdcalskdcbacfq34915rwcfasdcascnabscbaskjdbcalsjkbcdasc');
    """

    def test_create_mapvaluesindex_unicode_positive(self):
        """
            Invoke create string index() with correct arguments
        """
        policy = {}
        retobj = TestMapValuesIndex.client.index_map_values_create(
            'test', u'demo', u'string_map', aerospike.INDEX_STRING,
            u'uni_name_index', policy)

        assert retobj == 0
        TestMapValuesIndex.client.index_remove('test', u'uni_name_index',
                                               policy)

    def test_create_map_values_integer_index_unicode(self):
        """
            Invoke createindex() with correct arguments
        """
        policy = {}
        retobj = TestMapValuesIndex.client.index_map_values_create(
            'test', u'demo', u'numeric_map', aerospike.INDEX_NUMERIC,
            u'uni_age_index', policy)

        assert retobj == 0
        TestMapValuesIndex.client.index_remove(
            'test', u'uni_age_index', policy)

    def test_mapvaluesindex_with_correct_parameters_no_connection(self):
        """
            Invoke index_mapvalues_create() with correct arguments no
            connection
        """
        policy = {}
        config = {'hosts': [('127.0.0.1', 3000)]}
        client1 = aerospike.client(config)

        try:
            client1.index_map_values_create(
                'test', 'demo', 'string_map', aerospike.INDEX_STRING,
                'test_string_map_index', policy)

        except e.ClusterError as exception:
            assert exception.code == 11
            assert exception.msg == 'No connection to aerospike cluster'
