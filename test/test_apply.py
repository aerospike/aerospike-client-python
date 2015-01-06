
# -*- coding: utf-8 -*-

import pytest
import sys
import cPickle as pickle
try:
    import aerospike
except:
    print "Please install aerospike python client."
    sys.exit(1)

from aerospike import predicates as p
class TestApply(object):

    def setup_class(cls):
        config = {
                'hosts': [('127.0.0.1', 3000)]
                }
        TestApply.client = aerospike.client(config).connect()
        policy = {}
        TestApply.client.index_integer_create(policy, 'test', 'demo',
'age', 'age_index')
        policy = {}
        TestApply.client.index_integer_create(policy, 'test', 'demo',
'age1', 'age_index1')

        policy = {}
        filename = "sample.lua"
        udf_type = 0

        status = TestApply.client.udf_put( policy, filename, udf_type )
        filename = "test_record_udf.lua"
        status = TestApply.client.udf_put( policy, filename, udf_type )

    def teardown_class(cls):
        policy = {}
        TestApply.client.index_remove(policy, 'test', 'age_index');
        TestApply.client.index_remove(policy, 'test', 'age_index1');
        policy = { 'timeout' : 0 }
        module = "sample.lua"

        status = TestApply.client.udf_remove( policy, module )
        TestApply.client.close()

    def setup_method(self, method):

        """
        Setup method.
        """

        for i in xrange(5):
            key = ('test', 'demo', i)
            rec = {
                    'name' : ['name%s' % (str(i))],
                    'addr' : 'name%s' % (str(i)),
                    'age'  : i,
                    'no'   : i,
                    'basic_map': {"k30": 6, "k20": 5, "k10": 1}
            }
            TestApply.client.put(key, rec)

    def teardown_method(self, method):
        """
        Teardown method.
        """
        for i in xrange(5):
            key = ('test', 'demo', i)
            TestApply.client.remove(key)

    def test_apply_with_no_paramters(self):
        """
            Invoke apply() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            TestApply.client.apply()

        assert "Required argument 'key' (pos 1) not found" in typeError.value

    def test_apply_with_correct_parameters(self):
        """
            Invoke apply() with correct arguments
        """
        key = ('test', 'demo', 1)
        retval = TestApply.client.apply(key, 'sample', 'list_append', ['name', 'car'])
        (key, meta, bins) = TestApply.client.get(key)

        assert bins['name'] == ['name1', 'car']
        assert retval == 0

    def test_apply_with_policy(self):
        """
            Invoke apply() with policy
        """
        policy = {
            'timeout': 1000
        }
        key = ('test', 'demo', 1)
        retval = TestApply.client.apply(key, 'sample', 'list_append', ['name',
'car'], policy)
        (key, meta, bins) = TestApply.client.get(key)
        assert retval == 0
        assert bins['name'] == ['name1', 'car']

    def test_apply_with_incorrect_policy(self):
        """
            Invoke apply() with incorrect policy
        """
        policy = {
            'timeout': 0.1
        }
        key = ('test', 'demo', 1)
        with pytest.raises(Exception) as exception:
            retval = TestApply.client.apply(key, 'sample', 'list_append', ['name',
'car'], policy)

        assert exception.value[0] == -2L
        assert exception.value[1] == 'timeout is invalid'

    def test_apply_with_extra_argument(self):
        """
            Invoke apply() with extra argument
        """
        policy = {
            'timeout': 1000
        }
        key = ('test', 'demo', 1)
        with pytest.raises(TypeError) as typeError:
            TestApply.client.apply(key, 'sample', 'list_append', ['name', 'car'], policy, "")

        assert "apply() takes at most 5 arguments (6 given)" in typeError.value

    def test_apply_with_incorrect_bin(self):
        """
            Invoke apply() with incorrect bin
        """
        policy = {
            'timeout': 1000
        }
        key = ('test', 'demo', 1)
        with pytest.raises(Exception) as exception:
            retval = TestApply.client.apply(key, 'sample', 'list_append', ['addr',
'car'], policy)

        assert exception.value[0] == 100L

    def test_apply_with_empty_module_function(self):
        """
            Invoke apply() with empty module and function
        """
        with pytest.raises(Exception) as exception:
            key = ('test', 'demo', 1)
            retval = TestApply.client.apply(key, '', '', ['name' , 'car'])

        assert exception.value[0] == 100L
        assert exception.value[1] == 'UDF: Execution Error 1'

    def test_apply_with_incorrect_module(self):
        """
            Invoke apply() with incorrect module
        """
        with pytest.raises(Exception) as exception:
            key = ('test', 'demo', 1)
            TestApply.client.apply(key, 'samplewrong', 'list_append', ['name',
'car'])

        assert exception.value[0] == 100L
        assert exception.value[1] == 'UDF: Execution Error 1'

    def test_apply_with_incorrect_function(self):
        """
            Invoke apply() with incorrect function
        """
        with pytest.raises(Exception) as exception:
            key = ('test', 'demo', 1)
            TestApply.client.apply(key, 'sample', 'list_prepend', ['name', 'car'])

        assert exception.value[0] == 100L
        assert exception.value[1] == 'function not found'

    def test_apply_with_key_as_string(self):
        """
            Invoke apply() with key as string
        """
        with pytest.raises(Exception) as exception:
            TestApply.client.apply("", 'sample', 'list_append', ['name', 'car'])

        assert exception.value[0] == -2L
        assert exception.value[1] == 'key is invalid'

    def test_apply_with_incorrect_ns_set(self):
        """
            Invoke apply() with incorrect ns and set
        """
        with pytest.raises(Exception) as exception:
            key = ('test1', 'demo1', 1)
            TestApply.client.apply(key, 'sample', 'list_prepend', ['name', 'car'])

        assert exception.value[0] == 20L
        assert exception.value[1] == 'AEROSPIKE_ERR_NAMESPACE_NOT_FOUND'

    def test_apply_with_key_as_none(self):
        """
            Invoke apply() with key as none
        """
        with pytest.raises(Exception) as exception:
            TestApply.client.apply(None, 'sample', 'list_append', ['name', 'car'])

        assert exception.value[0] == -2L
        assert exception.value[1] == 'key is invalid'

    def test_apply_with_append_integer(self):
        """
            Invoke apply() with append an integer
        """
        key = ('test', 'demo', 1)
        retval = TestApply.client.apply(key, 'sample', 'list_append', ['name', 1])
        assert retval == 0
        (key, meta, bins) = TestApply.client.get(key)
        assert bins['name'] == ['name1', 1]

    def test_apply_with_append_list(self):
        """
            Invoke apply() with append an list
        """
        key = ('test', 'demo', 1)
        retval = TestApply.client.apply(key, 'sample', 'list_append', ['name', [1, 2]])
        assert retval == 0
        (key, meta, bins) = TestApply.client.get(key)
        assert bins['name'] == ['name1', [1, 2]]

    def test_apply_with_integer(self):
        """
            Invoke apply() with integer
        """
        key = ('test', 'demo', 1)
        retval = TestApply.client.apply(key, 'test_record_udf', 'bin_udf_operation_integer',
                ['age', 2, 20])
        assert retval == 23
        (key, meta, bins) = TestApply.client.get(key)
        assert bins['age'] == 23

    def test_apply_with_string(self):
        """
            Invoke apply() with string
        """
        key = ('test', 'demo', 1)
        retval = TestApply.client.apply(key, 'test_record_udf',
                'bin_udf_operation_string',
                ['addr', " world"])
        assert retval == "name1 world"
        (key, meta, bins) = TestApply.client.get(key)
        assert bins['addr'] == "name1 world"

    def test_apply_with_map(self):
        """
            Invoke apply() with map
        """
        key = ('test', 'demo', 1)
        retval = TestApply.client.apply(key, 'test_record_udf',
                'map_iterate',
                ['basic_map', 555])
        assert retval == None
        (key, meta, bins) = TestApply.client.get(key)
        assert bins['basic_map'] == {"k30": 555,
                        "k20": 555,
                        "k10": 555
                    }

    def test_apply_with_record(self):
        """
            Invoke apply() with record
        """
        key = ('test', 'demo', 1)
        retval = TestApply.client.apply(key, 'test_record_udf',
                'udf_returns_record',
                None)
        assert retval != None

    def test_apply_with_extra_argument_to_lua(self):
        """
            Invoke apply() with extra argument to lua
        """
        key = ('test', 'demo', 1)
        retval = TestApply.client.apply(key, 'sample', 'list_append', ['name',
'car', 1])
        assert retval == 0
        (key, meta, bins) = TestApply.client.get(key)
        assert bins['name'] == ['name1', 'car']

    def test_apply_with_extra_argument_in_lua(self):
        """
            Invoke apply() with extra argument in lua
        """
        key = ('test', 'demo', 1)
        retval = TestApply.client.apply(key, 'sample', 'list_append_extra', ['name',
'car'])
        assert retval == 0
        (key, meta, bins) = TestApply.client.get(key)
        assert bins['name'] == ['name1', 'car']

    def test_apply_with_no_argument_in_lua(self):
        """
            Invoke apply() with no argument in lua
        """
        key = ('test', 'demo', 1)
        with pytest.raises(TypeError) as typeError:
            retval = TestApply.client.apply(key, 'sample', 'list_append_extra')
        assert "Required argument 'args' (pos 4) not found" in typeError.value

    def test_apply_with_append_list(self):
        """
            Invoke apply() with append an list
        """
        key = ('test', 'demo', 1)
        retval = TestApply.client.apply(key, 'sample', 'list_append', ['name', [1, 2]])
        assert retval == 0
        (key, meta, bins) = TestApply.client.get(key)
        assert bins['name'] == ['name1', [1, 2]]
