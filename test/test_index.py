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

class TestIndex(TestBaseClass):
    def setup_class(cls):
        """
        Setup method.
        """
        hostlist, user, password = TestBaseClass.get_hosts()
        config = {'hosts': hostlist}
        if user == None and password == None:
            TestIndex.client = aerospike.client(config).connect()
        else:
            TestIndex.client = aerospike.client(config).connect(user, password)

    def teardown_class(cls):
        TestIndex.client.close()

    def setup_method(self, method):
        """
        Setup method. 
        """

        for i in xrange(5):
            key = ('test', u'demo', i)
            rec = {
                'name': 'name%s' % (str(i)),
                'addr': 'name%s' % (str(i)),
                'age': i,
                'no': i
            }
            TestIndex.client.put(key, rec)

    def teardown_method(self, method):
        """
        Teardoen method.
        """
        for i in xrange(5):
            key = ('test', u'demo', i)
            TestIndex.client.remove(key)

    def test_creatindex_with_no_paramters(self):
        """
            Invoke indexcreate() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            TestIndex.client.index_string_create()

        assert "Required argument 'ns' (pos 1) not found" in typeError.value

    def test_createindex_with_correct_parameters(self):
        """
            Invoke createindex() with correct arguments
        """
        policy = {}
        retobj = TestIndex.client.index_integer_create('test', 'demo', 'age',
                                                       'age_index', policy)

        assert retobj == 0L
        TestIndex.client.index_remove('test', 'age_index', policy)

    def test_createindex_with_correct_parameters_set_length_extra(self):
            #Invoke createindex() with correct arguments
        set_name = 'a'
        for i in xrange(100):
            set_name = set_name + 'a'
        policy = {}
        with pytest.raises(Exception) as exception:
            retobj = TestIndex.client.index_integer_create('test', set_name,
'age', 'age_index', policy)

        assert exception.value[0] == 4
        assert exception.value[1] == 'Invalid Set Name'

    def test_createindex_with_incorrect_namespace(self):
        """
            Invoke createindex() with incorrect namespace
        """
        policy = {}
        try:
            retobj = TestIndex.client.index_integer_create( 'test1', 'demo',
'age', 'age_index', policy )

        except InvalidRequest as exception:
            assert exception.code == 4
            assert exception.msg == 'Namespace Not Found'

    def test_createindex_with_incorrect_set(self):
        """
            Invoke createindex() with incorrect set
        """
        policy = {}
        retobj = TestIndex.client.index_integer_create('test', 'demo1', 'age',
                                                       'age_index', policy)

        assert retobj == 0L
        TestIndex.client.index_remove('test', 'age_index', policy)

    def test_createindex_with_incorrect_bin(self):
        """
            Invoke createindex() with incorrect bin
        """
        policy = {}
        retobj = TestIndex.client.index_integer_create('test', 'demo', 'age1',
                                                       'age_index', policy)

        assert retobj == 0L
        TestIndex.client.index_remove('test', 'age_index', policy)

    def test_createindex_with_namespace_is_none(self):
        """
            Invoke createindex() with namespace is None
        """
        policy = {}
        try:
            retobj = TestIndex.client.index_integer_create( None, 'demo',
'age', 'age_index', policy )

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == 'Namespace should be a string'

    def test_createindex_with_set_is_none(self):
            #Invoke createindex() with set is None
        policy = {}

        retobj = TestIndex.client.index_integer_create( 'test', None,
'age', 'age_index' , policy)

        assert retobj == 0L
        TestIndex.client.index_remove('test', 'age_index', policy);

    def test_createindex_with_set_is_int(self):
            #Invoke createindex() with set is int
        policy = {}
        with pytest.raises(Exception) as exception:
            retobj = TestIndex.client.index_integer_create('test', 1, 'age',
                                                           'age_index', policy)
        assert exception.value[0] == -2
        assert exception.value[1] == 'Set should be string, unicode or None'

    def test_createindex_with_bin_is_none(self):
        """
            Invoke createindex() with bin is None
        """
        policy = {}
        try:
            retobj = TestIndex.client.index_integer_create( 'test', 'demo',
None, 'age_index' , policy)

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == 'Bin should be a string'

    def test_createindex_with_index_is_none(self):
        """
            Invoke createindex() with index is None
        """
        policy = {}
        try:
            retobj = TestIndex.client.index_integer_create( 'test', 'demo',
'age', None, policy )

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == 'Index name should be string or unicode'

    def test_create_same_index_multiple_times(self):
        """
            Invoke createindex() with multiple times on same bin
        """
        policy = {}
        retobj = TestIndex.client.index_integer_create('test', 'demo', 'age',
                                                       'age_index', policy)
        if retobj == 0L:
            retobj = TestIndex.client.index_integer_create('test', 'demo',
                                                           'age', 'age_index',
                                                           policy)
            TestIndex.client.index_remove('test', 'age_index', policy)
            assert retobj == 0L
        else:
            assert True == False

    def test_create_same_index_multiple_times_different_bin(self):
        """
            Invoke createindex() with multiple times on different bin
        """
        policy = {}
        retobj = TestIndex.client.index_integer_create('test', 'demo', 'age',
                                                       'age_index', policy)
        if retobj == 0L:
            retobj = TestIndex.client.index_integer_create('test', 'demo',
                                                           'no', 'age_index',
                                                           policy)
            assert retobj == 0L
            TestIndex.client.index_remove('test', 'age_index', policy)
        else:
            assert True == False

    def test_create_different_index_multiple_times_same_bin(self):
        """
            Invoke createindex() with multiple times on same bin with different
name
        """
        policy = {}
        retobj = TestIndex.client.index_integer_create('test', 'demo', 'age',
                                                       'age_index', policy)
        if retobj == 0L:
            retobj = TestIndex.client.index_integer_create('test', 'demo',
                                                           'age', 'age_index1',
                                                           policy)
            assert retobj == 0L
            TestIndex.client.index_remove('test', 'age_index', policy)
            TestIndex.client.index_remove('test', 'age_index1', policy)
        else:
            assert True == False

    def test_createindex_with_policy(self):
        """
            Invoke createindex() with policy
        """
        policy = {'timeout': 1000}
        retobj = TestIndex.client.index_integer_create('test', 'demo', 'age',
                                                       'age_index', policy)

        assert retobj == 0L
        TestIndex.client.index_remove('test', 'age_index', policy)

    def test_create_string_index_positive(self):
        """
            Invoke create string index() with correct arguments
        """
        policy = {}
        retobj = TestIndex.client.index_string_create('test', 'demo', 'name',
                                                      'name_index', policy)

        assert retobj == 0L
        TestIndex.client.index_remove('test', 'name_index', policy)

    def test_create_string_index_with_correct_parameters_set_length_extra(self):
            #Invoke createindex() with correct arguments set length extra
        set_name = 'a'
        for i in xrange(100):
            set_name = set_name + 'a'
        policy = {}
        with pytest.raises(Exception) as exception:
            retobj = TestIndex.client.index_string_create('test', set_name,
'name', 'name_index', policy)

        assert exception.value[0] == 4
        assert exception.value[1] == 'Invalid Set Name'

    def test_create_string_index_with_correct_parameters_ns_length_extra(self):
            #Invoke createindex() with correct arguments ns length extra
        ns_name = 'a'
        for i in xrange(50):
            ns_name = ns_name + 'a'
        policy = {}
        with pytest.raises(Exception) as exception:
            retobj = TestIndex.client.index_string_create(ns_name, 'demo',
'name', 'name_index', policy)

        assert exception.value[0] == 4
        assert exception.value[1] == 'Namespace Not Found'

    def test_create_string_index_with_incorrect_namespace(self):
        """
            Invoke create string index() with incorrect namespace
        """
        policy = {}
        try:
            retobj = TestIndex.client.index_string_create('test1', 'demo',
'name', 'name_index', policy)

        except InvalidRequest as exception:
            assert exception.code == 4
            assert exception.msg == 'Namespace Not Found'

    def test_create_string_index_with_incorrect_set(self):
        """
            Invoke create string index() with incorrect set
        """
        policy = {}
        retobj = TestIndex.client.index_string_create('test', 'demo1', 'name',
                                                      'name_index', policy)

        assert retobj == 0L
        TestIndex.client.index_remove('test', 'name_index', policy)

    def test_create_string_index_with_incorrect_bin(self):
        """
            Invoke create string index() with incorrect bin
        """
        policy = {}
        retobj = TestIndex.client.index_string_create('test', 'demo', 'name1',
                                                      'name_index', policy)

        assert retobj == 0L
        TestIndex.client.index_remove('test', 'name_index', policy)

    def test_create_string_index_with_namespace_is_none(self):
        """
            Invoke create string index() with namespace is None
        """
        policy = {}
        try:
            retobj = TestIndex.client.index_string_create( None, 'demo', 'name', 'name_index', policy )

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == 'Namespace should be a string'

    def test_create_string_index_with_set_is_none(self):
            #Invoke create string index() with set is None
        policy = {}
        retobj = TestIndex.client.index_string_create('test', None, 'name',
                                                          'name_index', policy)

        assert retobj == 0L
        TestIndex.client.index_remove('test', 'name_index', policy);

    def test_create_string_index_with_bin_is_none(self):
        """
            Invoke create string index() with bin is None
        """
        policy = {}
        try:
            retobj = TestIndex.client.index_string_create('test', 'demo',
None, 'name_index', policy)

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == 'Bin should be a string'

    def test_create_string_index_with_index_is_none(self):
        """
            Invoke create_string_index() with index is None
        """
        policy = {}
        try:
            retobj = TestIndex.client.index_string_create('test', 'demo',
'name', None, policy)

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == 'Index name should be string or unicode'

    def test_create_same_string_index_multiple_times(self):
        """
            Invoke create string index() with multiple times on same bin
        """
        policy = {}
        retobj = TestIndex.client.index_string_create('test', 'demo', 'name',
                                                      'name_index', policy)
        if retobj == 0L:
            retobj = TestIndex.client.index_string_create('test', 'demo',
                                                          'name', 'name_index',
                                                          policy)
            assert retobj == 0L
            TestIndex.client.index_remove('test', 'name_index', policy)
        else:
            assert True == False

    def test_create_same_string__index_multiple_times_different_bin(self):
        """
            Invoke create string index() with multiple times on different bin
        """
        policy = {}
        retobj = TestIndex.client.index_string_create('test', 'demo', 'name',
                                                      'name_index', policy)
        if retobj == 0L:
            retobj = TestIndex.client.index_string_create('test', 'demo',
                                                          'addr', 'name_index',
                                                          policy)
            assert retobj == 0L
            TestIndex.client.index_remove('test', 'name_index', policy)
        else:
            assert True == False

    def test_create_different_string_index_multiple_times_same_bin(self):
        """
            Invoke create string index() with multiple times on same bin with different
name
        """
        policy = {}
        retobj = TestIndex.client.index_string_create('test', 'demo', 'name',
                                                      'name_index', policy)
        if retobj == 0L:
            retobj = TestIndex.client.index_string_create(
                'test', 'demo', 'name', 'name_index1', policy)
            assert retobj == 0L
            TestIndex.client.index_remove('test', 'name_index', policy)
            TestIndex.client.index_remove('test', 'name_index1', policy)
        else:
            assert True == False

    def test_create_string_index_with_policy(self):
        """
            Invoke create string index() with policy
        """
        policy = {'timeout': 1000}
        retobj = TestIndex.client.index_string_create('test', 'demo', 'name',
                                                      'name_index', policy)

        assert retobj == 0L
        TestIndex.client.index_remove('test', 'name_index', policy)

    def test_drop_invalid_index(self):
        """
            Invoke drop invalid index() 
        """
        policy = {}
        retobj = TestIndex.client.index_remove('test', 'age_index_new', policy)
        assert retobj == 0L

    def test_drop_valid_index(self):
        """
            Invoke drop valid index()
        """
        policy = {}
        retobj = TestIndex.client.index_integer_create('test', 'demo', 'age',
                                                       'age_index', policy)
        retobj = TestIndex.client.index_remove('test', 'age_index', policy)
        assert retobj == 0L

    def test_drop_valid_index_policy(self):
        """
            Invoke drop valid index() policy
        """
        policy = {'timeout': 1000}
        retobj = TestIndex.client.index_integer_create('test', 'demo', 'age',
                                                       'age_index', policy)
        retobj = TestIndex.client.index_remove('test', 'age_index', policy)
        assert retobj == 0L

    """
    This test case causes a db crash and hence has been commented. Work pending
on the C-client side
    def test_createindex_with_long_index_name(self):
            Invoke createindex() with long index name
        policy = {}
        retobj = TestIndex.client.index_integer_create( 'test', 'demo',
'age',
'bin2_integer_indexsdadadfasdfasdfeartfqrgahfasdfheudsdfasdfawf312342q3453rf9qwfasdcfasdcalskdcbacfq34915rwcfasdcascnabscbaskjdbcalsjkbcdasc', policy)

        assert retobj == 0L
        TestIndex.client.index_remove(policy, 'test',
'bin2_integer_indexsdadadfasdfasdfeartfqrgahfasdfheudsdfasdfawf312342q3453rf9qwfasdcfasdcalskdcbacfq34915rwcfasdcascnabscbaskjdbcalsjkbcdasc');
    """

    def test_create_string_index_unicode_positive(self):
        """
            Invoke create string index() with correct arguments
        """
        policy = {}
        retobj = TestIndex.client.index_string_create('test', u'demo', u'name',
                                                      u'uni_name_index',
                                                      policy)

        assert retobj == 0L
        TestIndex.client.index_remove('test', u'uni_name_index', policy)

    def test_createindex_integer_unicode(self):
        """
            Invoke createindex() with correct arguments
        """
        policy = {}
        retobj = TestIndex.client.index_integer_create('test', u'demo', u'age',
                                                       u'uni_age_index',
                                                       policy)

        assert retobj == 0L
        TestIndex.client.index_remove('test', u'age_index', policy)

    def test_createindex_with_correct_parameters_without_connection(self):
            #Invoke createindex() with correct arguments without connection
        policy = {}
        config = {'hosts': [('127.0.0.1', 3000)]}
        client1 = aerospike.client(config)

        try:
            etobj = client1.index_integer_create('test', 'demo', 'age', 'age_index', policy)

        except ClusterError as exception:
            assert exception.code == 11L
            assert exception.msg == 'No connection to aerospike cluster'
