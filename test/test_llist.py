# -*- coding: utf-8 -*-

import pytest
import sys
import time
from test_base_class import TestBaseClass

aerospike = pytest.importorskip("aerospike")
try:
    from aerospike.exception import *
except:
    print "Please install aerospike python client."
    sys.exit(1)


class TestLList(TestBaseClass):

    pytestmark = pytest.mark.skipif(
        TestBaseClass.has_ldt_support() == False,
        reason="LDTs are not enabled for namespace 'test'")

    llist_integer = None
    llist_string = None
    client = None
    key1 = None
    key2 = None

    def setup_class(self):

        print "setup class invoked..."
        hostlist, user, password = TestBaseClass.get_hosts()
        config = {'hosts': hostlist}
        if user == None and password == None:
            self.client = aerospike.client(config).connect()
        else:
            self.client = aerospike.client(config).connect(user, password)

        TestLList.key1 = ('test', 'demo', 'integer_llist_ky')
        TestLList.llist_integer = TestLList.client.llist(TestLList.key1,
                                                         'integer_bin')
        TestLList.key2 = ('test', 'demo', 'string_llist_ky')
        TestLList.llist_string = TestLList.client.llist(TestLList.key2,
                                                        'string_bin')
        TestLList.key3 = ('test', 'demo', 'float_llist_ky')
        TestLList.llist_float = TestLList.client.llist(TestLList.key3,
                                                       'float_bin')


    def teardown_class(self):
        print "teardown class invoked..."
        try:
            TestLList.llist_integer.destroy()
            TestLList.llist_string.destroy()
            TestLList.list_float.destroy()
        except:
            pass
        self.client.close()

    #Add() - Add an object to the llist.
    #Get() - Get an object from the llist.
    #Size() - Get the current item count of the llist.
    def test_llist_add_get_size_positive(self):
        """
            Invoke add() an object to LList.
        """

        assert 0 == TestLList.llist_integer.add(11)
        assert [11] == TestLList.llist_integer.get(11)

        assert 0 == TestLList.llist_string.add("abc")
        assert ['abc'] == TestLList.llist_string.get('abc')

        assert 1 == TestLList.llist_integer.size()

    #Add() - Add() unsupported type data to llist.
    def test_llist_add_float_positive(self):
        """
            Invoke add() float type data.
        """
        rec = {"pi": 3.14}

        try:
            TestLList.llist_float.add(rec)

        except LDTKeyFunctionNotFound as exception:
            assert exception.code == 1433
            assert exception.msg == "LDT-Key Field Not Found"

    #Add() - Add() without any mandatory parameters.
    def test_llist_no_parameter_negative(self):
        """
            Invoke add() without any mandatory parameters.
        """

        with pytest.raises(TypeError) as typeError:
            TestLList.llist_integer.add()

    #Add_many() - Add a list of objects to the set.
    def test_llist_add_many_positive(self):
        """
            Invoke add_many() to add a list of objects to the set.
        """

        policy = {'timeout': 7000}
        assert 0 == TestLList.llist_integer.add_many([122, 56, 871], policy)

        assert [122] == TestLList.llist_integer.get(122)
        assert [56] == TestLList.llist_integer.get(56)
        assert [871] == TestLList.llist_integer.get(871)

    #Get() - Get without any mandatory parameters.
    def test_llist_get_element_negative(self):
        """
            Invoke get() without any mandatory parameters.
        """

        with pytest.raises(TypeError) as typeError:
            TestLList.llist_integer.get()

    #Remove() and Get()- Remove an object from the set and get non-existent element.
    def test_llist_remove_positive(self):
        """
            Invoke remove() to remove element.
        """

        assert 0 == TestLList.llist_string.add('remove')
        assert 0 == TestLList.llist_string.remove('remove')

        try:
            TestLList.llist_string.get('remove')

        except UDFError as exception:
            assert exception.code == 100L
        except LargeItemNotFound as exception:
            assert exception.code == 125L

    #Remove() - Remove non-existent object from the llist.
    def test_llist_remove_element_negative(self):
        """
            Invoke remove() to remove non-existent element.
        """

        try:
            TestLList.llist_string.remove('kk')

        except UDFError as exception:
            assert exception.code == 100L
        except LargeItemNotFound as exception:
            assert exception.code == 125L

    #Destroy() - Delete the entire LList(LDT Remove).
    def test_llist_destroy_positive(self):
        """
            Invoke destroy() to delete entire LDT.
        """
        key = ('test', 'demo', 'remove')
        llist = self.client.llist(key, 'llist_add')
        try:
            llist.add(876)
        except:
            pass
        assert 0 == llist.destroy()

    def test_llist_ldt_initialize_negative(self):
        """
            Initialize ldt with wrong key.
        """
        key = ('test', 'demo', 12.3)
        try:
            llist = self.client.llist(key, 'ldt_stk')
        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Parameters are incorrect"

    def test_llist_find_first_positive_without_policy(self):
        """
            Invoke find_first() to access elements
        """
        elements_list = TestLList.llist_integer.find_first(2)

        assert elements_list == [11, 56]

    def test_llist_find_first_positive(self):
        """
            Invoke find_first() to access elements
        """
        elements_list = TestLList.llist_integer.find_first(2, {'timeout': 1000})
        assert elements_list == [11, 56]

    def test_llist_find_first_count_large_positive(self):
        """
            Invoke find_first() to access elements with a larger count
        """
        elements_list = TestLList.llist_integer.find_first(10, {'timeout': 1000})
        assert elements_list == [11, 56, 122, 871]

    def test_llist_find_first_count_negative(self):
        """
            Invoke find_first() to access elements with a negative count
        """
        elements_list = TestLList.llist_integer.find_first(-8, {'timeout': 1000})
        assert elements_list == [11, 56, 122, 871]

    def test_llist_find_last_positive_without_policy(self):
        """
            Invoke find_last() to access elements
        """
        elements_list = TestLList.llist_integer.find_last(2)
        assert elements_list == [871, 122]

    def test_llist_find_last_positive(self):
        """
            Invoke find_last() to access elements
        """
        elements_list = TestLList.llist_integer.find_last(2, {'timeout': 1000})
        assert elements_list == [871, 122]

    def test_llist_find_last_count_large(self):
        """
            Invoke find_last() to access elements
        """
        elements_list = TestLList.llist_integer.find_last(15, {'timeout': 1000})
        assert elements_list == [871, 122, 56, 11]

    def test_llist_find_last_count_negative(self):
        """
            Invoke find_last() to access elements
        """
        elements_list = TestLList.llist_integer.find_last(-2, {'timeout': 1000})
        assert elements_list == [871, 122, 56, 11]

    def test_llist_find_last_no_params(self):
        """
            Invoke find_last() to access elements
        """
        with pytest.raises(TypeError) as typeError:
            TestLList.llist_integer.find_last()
        assert "Required argument 'count' (pos 1) not found" in typeError.value

    def test_llist_find_last_no_parameters_negative(self):
        """
            Invoke find_last() to access elements
        """
        with pytest.raises(TypeError) as typeError:
            TestLList.llist_integer.find_last()
        assert "Required argument 'count' (pos 1) not found" in typeError.value

    def test_llist_find_from_positive_without_policy(self):
        """
            Invoke find_from() to access elements from a given key
        """
        elements_list = TestLList.llist_integer.find_from(56, 2)
        assert elements_list == [56, 122]

    def test_llist_find_from_positive(self):
        """
            Invoke find_from() to access elements from a given key
        """
        elements_list = TestLList.llist_integer.find_from(56, 2, {'timeout': 1000})
        assert elements_list == [56, 122]

    def test_llist_find_from_positive_non_existent_key(self):
        """
            Invoke find_from() to access elements from a non-existent key
        """
        elements_list = TestLList.llist_integer.find_from(21, 2, {'timeout': 1000})
        assert elements_list == [56, 122]

    def test_llist_range_limit_positive_without_policy(self):
        """
            Invoke range_limit() to access elements
        """
        elements_list = TestLList.llist_integer.range_limit(56, 871, 2, None, None)
        assert elements_list == [56, 122, 871]

    def test_llist_range_limit_positive(self):
        """
            Invoke range_limit() to access elements
        """
        elements_list = TestLList.llist_integer.range_limit(56, 871, 2, None, None, {'timeout': 1000})
        assert elements_list == [56, 122, 871]

    def test_llist_range_limit_negative_keys(self):
        """
            Invoke range_limit() to access elements with negative keys
        """
        elements_list = TestLList.llist_integer.range_limit(-56, -871, 2, None, None, {'timeout': 1000})
        assert elements_list == []

    def test_llist_range_limit_larger_count_positive(self):
        """
            Invoke range_limit() to access elements with larger count than list
            size
        """
        elements_list = TestLList.llist_integer.range_limit(56, 871, 8, None, None, {'timeout': 1000})
        assert elements_list == [56, 122, 871]

    def test_llist_range_limit_count_negative(self):
        """
            Invoke range_limit() to access elements
        """
        elements_list = TestLList.llist_integer.range_limit(56, 871, -2, None, None, {'timeout': 1000})
        assert elements_list == [56, 122, 871]

    def test_llist_set_page_size_without_policy(self):
        #Invoke set_page_size() to set page size of ldt bin.
        assert 0 == TestLList.llist_integer.set_page_size(8192)

    def test_llist_set_page_size(self):
        #Invoke set_page_size() to set page size of ldt bin.
        assert 0 == TestLList.llist_integer.set_page_size(8192, {'timeout': 0})

    def test_llist_set_page_size_string_negative(self):
        with pytest.raises(TypeError) as typeError:
            TestLList.llist_integer.set_page_size("8192", {'timeout': 0})
        assert "an integer is required" in typeError.value

    """ Causes db to shutdown
    def test_llist_find_from_positive_negative_count(self):
        #Invoke find_from() to access elements with a negative count
        elements_list = TestLList.llist_integer.find_from(56, -2, {'timeout': 1000})

        assert elements_list == [56, 122]
        """
