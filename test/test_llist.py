# -*- coding: utf-8 -*-

import pytest
import sys
import time
try:
    import aerospike
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
        config = {
                "hosts": [("172.20.25.24", 3000)]
                }

        TestLList.client = aerospike.client(config).connect()

        TestLList.key1 = ('test', 'demo', 'integer_llist_key1')

        TestLList.llist_integer = TestLList.client.llist(TestLList.key1,
                'integer_bin')

        TestLList.key2 = ('test', 'demo', 'string_llist_key1')

        TestLList.llist_string = TestLList.client.llist(TestLList.key2,
                'string_bin')


    def teardown_class(cls):
        print "teardown class invoked..."

        TestLList.llist_integer.destroy()

        TestLList.llist_string.destroy()

        cls.client.close()

    #add() - Add an object to the set.
    def test_llist_add_integer_positive(self):

        """
            Invoke add() integer type data.
        """

        assert 0 == TestLList.llist_integer.add(11)

    def test_llist_add_string_positive(self):

        """
            Invoke add() string type data.
        """

        assert 0 == TestLList.llist_string.add("abc")

    def test_llist_add_char_positive(self):

        """
            Invoke add() char type data.
        """
        assert 0 == TestLList.llist_string.add('k')

    def test_llist_add_float_positive(self):

        """
            Invoke add() float type data.
        """
        rec = {
                "pi" : 3.14
                }

        with pytest.raises(Exception) as exception: 
            TestLList.llist_integer.add(rec)

        assert exception.value[0] == -1
        assert exception.value[1] == "value is not a supported type."

    def test_llist_no_parameter_negative(self):

        """
            Invoke add() without any mandatory parameters.
        """

        with pytest.raises(TypeError) as typeError:
            TestLList.llist_integer.add()

        assert "Required argument 'value' (pos 1) not found" in typeError.value

    def test_llist_empty_string_positive(self):

        """
            Invoke add() integer type data.
        """
        assert 0 == TestLList.llist_string.add('')

    #Add_all() - Add a list of objects to the set.
    def test_llist_add_all_positive(self):

        """
            Invoke add_all() to add a list of objects to the set.
        """

        assert 0 == TestLList.llist_integer.add_all([122, 56, 871])

    #Get() - Get an object from the llist.
    def test_llist_get_element_positive(self):

        """
            Invoke get() to get list from set.
        """

        assert ['abc'] == TestLList.llist_string.get('abc')

    def test_llist_get_element_negative(self):

        """
            Invoke get() without any mandatory parameters.
        """

        with pytest.raises(TypeError) as typeError: 
            TestLList.llist_integer.get()

    def test_llist_get_non_existent_element_positive(self):

        """
            Invoke get() non-existent element from set.
        """

        with pytest.raises(Exception) as exception: 
            TestLList.llist_integer.get(1000)

        assert exception.value[0] == 100
        assert exception.value[1] == "/opt/aerospike/sys/udf/lua/ldt/lib_llist.lua:5085: 1401:LDT-Item Not Found"

    #Size() - Get the current item count of the set.
    def test_llist_size_positive(self):

        """
            Invoke size() on llist.
        """
        assert 4 == TestLList.llist_integer.size()

    #Remove() - Remove an object from the set.
    def test_llist_remove_element_positive(self):

        """
            Invoke remove() to remove element.
        """
        assert 0 == TestLList.llist_string.remove('k')

    #Remove() - Remove non-existent object from the set.
    def test_llist_remove_element_negative(self):

        """
            Invoke remove() to remove non-existent element.
        """

        with pytest.raises(Exception) as exception:
            TestLList.llist_string.remove('kk')

        assert exception.value[0] == 100
        assert exception.value[1] == '/opt/aerospike/sys/udf/lua/ldt/lib_llist.lua:5454: 1401:LDT-Item Not Found'

    #Destroy() - Delete the entire set(LDT Remove).
    def test_llist_destroy_positive(self):

        """
            Invoke destroy() to delete entire LDT.
        """
        key = ('test', 'demo', 'remove')

        llist = self.client.llist(key, 'llist_add')

        llist.add(876)

        assert 0 == llist.destroy()
        
