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

class TestLList(object):

    llist_integer = None
    llist_string = None
    client = None
    key1 = None
    key2 = None

    def setup_class(cls):

        print "setup class invoked..."
        hostlist, user, password = TestBaseClass.get_hosts()
        config = {'hosts': hostlist}
        if user == None and password == None:
            TestLList.client = aerospike.client(config).connect()
        else:
            TestLList.client = aerospike.client(config).connect(user, password)

        TestLList.key1 = ('test', 'demo', 'integer_llist_ky')

        TestLList.llist_integer = TestLList.client.llist(TestLList.key1,
                                                         'integer_bin')

        TestLList.key2 = ('test', 'demo', 'string_llist_ky')

        TestLList.llist_string = TestLList.client.llist(TestLList.key2,
                                                        'string_bin')

        TestLList.key3 = ('test', 'demo', 'float_llist_ky')

        TestLList.llist_float = TestLList.client.llist(TestLList.key3,
                                                       'float_bin')

    def teardown_class(cls):
        print "teardown class invoked..."

        TestLList.llist_integer.destroy()

        TestLList.llist_string.destroy()

        cls.client.close()

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
    @pytest.mark.xfail
    def test_llist_add_float_positive(self):
        """
            Invoke add() float type data.
        """
        rec = {"pi": 3.14}

        try:
            TestLList.llist_float.add(rec)

        except LDTKeyFunctionNotFound as exception:
            assert exception.code == 1433
            assert exception.msg == "LDT-Key (Unique) Function Not Found"

    #Add() - Add() without any mandatory parameters.
    def test_llist_no_parameter_negative(self):
        """
            Invoke add() without any mandatory parameters.
        """

        with pytest.raises(TypeError) as typeError:
            TestLList.llist_integer.add()

        assert "Required argument 'value' (pos 1) not found" in typeError.value

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
    @pytest.mark.xfail
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

        llist.add(876)

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
