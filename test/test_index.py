
# -*- coding: utf-8 -*-

import pytest
import sys
import cPickle as pickle
try:
    import aerospike
except:
    print "Please install aerospike python client."
    sys.exit(1)

class TestIndex(object):
    def setup_class(cls):
        """
        Setup method.
        """
        config = {
                'hosts': [('127.0.0.1', 3000)]
                }
        TestIndex.client = aerospike.client(config).connect()
    
    def teardown_class(cls):
        TestIndex.client.close()

    def setup_method(self, method):

        """
        Setup method.
        
        config = {
                'hosts': [('127.0.0.1', 3000)]
                }
        TestIndex.client = aerospike.client(config).connect()
        """
        for i in xrange(5):
            key = ('test', 'demo', i)
            rec = {
                    'name' : 'name%s' % (str(i)),
                    'addr' : 'name%s' % (str(i)),
                    'age'  : i,
                    'no'   : i
                    }
            TestIndex.client.put(key, rec)

    def teardown_method(self, method):
        """
        Teardoen method.
        """
        for i in xrange(5):
            key = ('test', 'demo', i)
            TestIndex.client.remove(key)

        #TestIndex.client.close()

    def test_creatindex_with_no_paramters(self):
        """
            Invoke indexcreate() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            TestIndex.client.index_string_create()

        assert "Required argument 'policy' (pos 1) not found" in typeError.value

    def test_createindex_with_correct_parameters(self):
        """
            Invoke createindex() with correct arguments
        """
        policy = {}
        retobj = TestIndex.client.index_integer_create( policy, 'test', 'demo',
'age', 'age_index'  )

        assert retobj == 0L
        TestIndex.client.index_remove(policy, 'test', 'age_index');

    def test_createindex_with_incorrect_namespace(self):
        """
            Invoke createindex() with incorrect namespace
        """
        policy = {}
        with pytest.raises(Exception) as exception:
            retobj = TestIndex.client.index_integer_create( policy, 'test1', 'demo',
'age', 'age_index'  )
        assert exception.value[0] == 4
        assert exception.value[1] == 'AEROSPIKE_ERR_REQUEST_INVALID'

    def test_createindex_with_incorrect_set(self):
        """
            Invoke createindex() with incorrect set
        """
        policy = {}
        retobj = TestIndex.client.index_integer_create( policy, 'test', 'demo1',
'age', 'age_index'  )

        assert retobj == 0L
        TestIndex.client.index_remove(policy, 'test', 'age_index');

    def test_createindex_with_incorrect_bin(self):
        """
            Invoke createindex() with incorrect bin
        """
        policy = {}
        retobj = TestIndex.client.index_integer_create( policy, 'test', 'demo',
'age1', 'age_index'  )

        assert retobj == 0L
        TestIndex.client.index_remove(policy, 'test', 'age_index');

    def test_createindex_with_namespace_is_none(self):
        """
            Invoke createindex() with namespace is None
        """
        policy = {}
        with pytest.raises(Exception) as exception:
            retobj = TestIndex.client.index_integer_create( policy, None, 'demo',
'age', 'age_index'  )
        assert exception.value[0] == -2
        assert exception.value[1] == 'Namespace should be a string'

    def test_createindex_with_set_is_none(self):
        """
            Invoke createindex() with set is None
        """
        policy = {}
        with pytest.raises(Exception) as exception:
            retobj = TestIndex.client.index_integer_create( policy, 'test', None,
'age', 'age_index'  )
        assert exception.value[0] == -2
        assert exception.value[1] == 'Set should be a string'

    def test_createindex_with_bin_is_none(self):
        """
            Invoke createindex() with bin is None
        """
        policy = {}
        with pytest.raises(Exception) as exception:
            retobj = TestIndex.client.index_integer_create( policy, 'test', 'demo',
None, 'age_index'  )
        assert exception.value[0] == -2
        assert exception.value[1] == 'Bin should be a string'

    def test_createindex_with_index_is_none(self):
        """
            Invoke createindex() with index is None
        """
        policy = {}
        with pytest.raises(Exception) as exception:
            retobj = TestIndex.client.index_integer_create( policy, 'test', 'demo',
'age', None )
        assert exception.value[0] == -2
        assert exception.value[1] == 'Index name should be a string'

    def test_create_same_index_multiple_times(self):
        """
            Invoke createindex() with multiple times on same bin
        """
        policy = {}
        retobj = TestIndex.client.index_integer_create( policy, 'test', 'demo',
'age', 'age_index' )
        if retobj == 0L:
            retobj = TestIndex.client.index_integer_create( policy, 'test', 'demo',
'age', 'age_index' )
            TestIndex.client.index_remove(policy, 'test', 'age_index');
            assert retobj == 0L
        else:
            assert True == False

    def test_create_same_index_multiple_times_different_bin(self):
        """
            Invoke createindex() with multiple times on different bin
        """
        policy = {}
        retobj = TestIndex.client.index_integer_create( policy, 'test', 'demo',
'age', 'age_index' )
        if retobj == 0L:
            retobj = TestIndex.client.index_integer_create( policy, 'test', 'demo',
'no', 'age_index' )
            assert retobj == 0L
            TestIndex.client.index_remove(policy, 'test', 'age_index');
        else:
            assert True == False

    def test_create_different_index_multiple_times_same_bin(self):
        """
            Invoke createindex() with multiple times on same bin with different
name
        """
        policy = {}
        retobj = TestIndex.client.index_integer_create( policy, 'test', 'demo',
'age', 'age_index' )
        if retobj == 0L:
            retobj = TestIndex.client.index_integer_create( policy, 'test', 'demo',
'age', 'age_index1' )
            assert retobj == 0L
            TestIndex.client.index_remove(policy, 'test', 'age_index');
            TestIndex.client.index_remove(policy, 'test', 'age_index1');
        else:
            assert True == False

    def test_createindex_with_policy(self):
        """
            Invoke createindex() with policy
        """
        policy = {
            'timeout': 1000
            }
        retobj = TestIndex.client.index_integer_create( policy, 'test', 'demo',
'age', 'age_index'  )

        assert retobj == 0L
        TestIndex.client.index_remove(policy, 'test', 'age_index');

    def test_create_string_index_positive(self):
        """
            Invoke create string index() with correct arguments
        """
        policy = {}
        retobj = TestIndex.client.index_string_create( policy, 'test', 'demo',
'name', 'name_index'  )

        assert retobj == 0L
        TestIndex.client.index_remove(policy, 'test', 'name_index');

    def test_create_string_index_with_incorrect_namespace(self):
        """
            Invoke create string index() with incorrect namespace
        """
        policy = {}
        with pytest.raises(Exception) as exception:
            retobj = TestIndex.client.index_string_create( policy, 'test1', 'demo',
'name', 'name_index'  )
        assert exception.value[0] == 4
        assert exception.value[1] == 'AEROSPIKE_ERR_REQUEST_INVALID'

    def test_create_string_index_with_incorrect_set(self):
        """
            Invoke create string index() with incorrect set
        """
        policy = {}
        retobj = TestIndex.client.index_string_create( policy, 'test', 'demo1',
'name', 'name_index'  )

        assert retobj == 0L
        TestIndex.client.index_remove(policy, 'test', 'name_index');

    def test_create_string_index_with_incorrect_bin(self):
        """
            Invoke create string index() with incorrect bin
        """
        policy = {}
        retobj = TestIndex.client.index_string_create( policy, 'test', 'demo',
'name1', 'name_index'  )

        assert retobj == 0L
        TestIndex.client.index_remove(policy, 'test', 'name_index');

    def test_create_string_index_with_namespace_is_none(self):
        """
            Invoke create string index() with namespace is None
        """
        policy = {}
        with pytest.raises(Exception) as exception:
            retobj = TestIndex.client.index_string_create( policy, None, 'demo',
'name', 'name_index' )
        assert exception.value[0] == -2
        assert exception.value[1] == 'Namespace should be a string'

    def test_create_string_index_with_set_is_none(self):
        """
            Invoke create string index() with set is None
        """
        policy = {}
        with pytest.raises(Exception) as exception:
            retobj = TestIndex.client.index_string_create( policy, 'test', None,
'name', 'name_index' )
        assert exception.value[0] == -2
        assert exception.value[1] == 'Set should be a string'

    def test_create_string_index_with_bin_is_none(self):
        """
            Invoke create string index() with bin is None
        """
        policy = {}
        with pytest.raises(Exception) as exception:
            retobj = TestIndex.client.index_string_create( policy, 'test', 'demo',
None, 'name_index'  )
        assert exception.value[0] == -2
        assert exception.value[1] == 'Bin should be a string'

    def test_create_string_index_with_index_is_none(self):
        """
            Invoke create_string_index() with index is None
        """
        policy = {}
        with pytest.raises(Exception) as exception:
            retobj = TestIndex.client.index_string_create( policy, 'test', 'demo',
'name', None )
        assert exception.value[0] == -2
        assert exception.value[1] == 'Index name should be a string'

    def test_create_same_string_index_multiple_times(self):
        """
            Invoke create string index() with multiple times on same bin
        """
        policy = {}
        retobj = TestIndex.client.index_string_create( policy, 'test', 'demo',
'name', 'name_index' )
        if retobj == 0L:
            retobj = TestIndex.client.index_string_create( policy, 'test', 'demo',
'name', 'name_index' )
            assert retobj == 0L
            TestIndex.client.index_remove(policy, 'test', 'name_index');
        else:
            assert True == False

    def test_create_same_string__index_multiple_times_different_bin(self):
        """
            Invoke create string index() with multiple times on different bin
        """
        policy = {}
        retobj = TestIndex.client.index_string_create( policy, 'test', 'demo',
'name', 'name_index' )
        if retobj == 0L:
            retobj = TestIndex.client.index_string_create( policy, 'test', 'demo',
'addr', 'name_index' )
            assert retobj == 0L
            TestIndex.client.index_remove(policy, 'test', 'name_index');
        else:
            assert True == False

    def test_create_different_string_index_multiple_times_same_bin(self):
        """
            Invoke create string index() with multiple times on same bin with different
name
        """
        policy = {}
        retobj = TestIndex.client.index_string_create( policy, 'test', 'demo',
'name', 'name_index' )
        if retobj == 0L:
            retobj = TestIndex.client.index_string_create( policy, 'test', 'demo',
'name', 'name_index1' )
            assert retobj == 0L
            TestIndex.client.index_remove(policy, 'test', 'name_index');
            TestIndex.client.index_remove(policy, 'test', 'name_index1');
        else:
            assert True == False

    def test_create_string_index_with_policy(self):
        """
            Invoke create string index() with policy
        """
        policy = {
            'timeout': 1000
            }
        retobj = TestIndex.client.index_string_create( policy, 'test', 'demo',
'name', 'name_index'  )

        assert retobj == 0L
        TestIndex.client.index_remove(policy, 'test', 'name_index');

    def test_drop_invalid_index(self):
        """
            Invoke drop invalid index() 
        """
        policy = {}
        retobj = TestIndex.client.index_remove(policy, 'test', 'age_index_new')
        assert retobj == 0L

    def test_drop_valid_index(self):
        """
            Invoke drop valid index()
        """
        policy = {}
        retobj = TestIndex.client.index_integer_create(policy, 'test', 'demo',
'age', 'age_index')
        retobj = TestIndex.client.index_remove(policy, 'test', 'age_index')
        assert retobj == 0L

    def test_drop_valid_index_policy(self):
        """
            Invoke drop valid index() policy
        """
        policy = {
            'timeout': 1000
        }
        retobj = TestIndex.client.index_integer_create(policy, 'test', 'demo',
'age', 'age_index')
        retobj = TestIndex.client.index_remove(policy, 'test', 'age_index')
        assert retobj == 0L
    """
    This test case causes a db crash and hence has been commented. Work pending
on the C-client side
    def test_createindex_with_long_index_name(self):
            Invoke createindex() with long index name
        policy = {}
        retobj = TestIndex.client.index_integer_create( policy, 'test', 'demo',
'age',
'bin2_integer_indexsdadadfasdfasdfeartfqrgahfasdfheudsdfasdfawf312342q3453rf9qwfasdcfasdcalskdcbacfq34915rwcfasdcascnabscbaskjdbcalsjkbcdasc')

        assert retobj == 0L
        TestIndex.client.index_remove(policy, 'test',
'bin2_integer_indexsdadadfasdfasdfeartfqrgahfasdfheudsdfasdfawf312342q3453rf9qwfasdcfasdcalskdcbacfq34915rwcfasdcascnabscbaskjdbcalsjkbcdasc');
    """
