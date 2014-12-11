# -*- coding: utf-8 -*-

import pytest
import sys
import time
try:
    import aerospike
except:
    print "Please install aerospike python client."
    sys.exit(1)

class TestLSet(object):

    lset = None
    client = None
    key = None

    def setup_class(cls):

        print "setup class invoked..."
        config = {
                "hosts": [("172.20.25.24", 3000)]
                }

        TestLSet.client = aerospike.client(config).connect()

        TestLSet.key = ('test', 'demo', 'lset_add_key')

        TestLSet.lset = TestLSet.client.lset(TestLSet.key, 'lset_add_bin')


    def teardown_class(cls):
        print "teardown class invoked..."

        TestLSet.lset.destroy()

        cls.client.close()

    #Add() and Get() - Add and Get an object from the set.
    def test_lset_add_get_positive(self):

        """
            Invoke add() to add object to the set.
        """
        
        assert 0 == TestLSet.lset.add(566)
        assert 566 == TestLSet.lset.get(566)

        assert 0 == TestLSet.lset.add("abc")
        assert "abc" == TestLSet.lset.get("abc")

        assert 0 == TestLSet.lset.add('k')
        assert 'k' == TestLSet.lset.get('k')

    #Add() - Add float type data to the set.
    def test_lset_add_float_positive(self):

        """
            Invoke add() float type data.
        """
        rec = {
                "pi" : 3.14
                }

        with pytest.raises(Exception) as exception: 
            TestLSet.lset.add(rec)

        assert exception.value[0] == -1
        assert exception.value[1] == "value is not a supported type."

    #Add and Get list from lset.
    def test_lset_add_list_positive(self):

        """
            Invoke add() method for list.
        """
        list = [12, 'a', bytearray("asd;as[d'as;d", "utf-8")]

        assert 0 == TestLSet.lset.add(list)
        assert [12, u'a', bytearray("asd;as[d'as;d", "utf-8")] == TestLSet.lset.get(list)

    #Add and Get map from lset.
    def test_lset_add_map_positive(self):

        """
            Invoke add() method for map.
        """
        map = {
                'a' : 12,
                '!@#@#$QSDAsd;as' : bytearray("asd;as[d'as;d", "utf-8")
                }

        assert 0 == TestLSet.lset.add(map)
        assert {u'a' : 12, u'!@#@#$QSDAsd;as' : bytearray("asd;as[d'as;d",
            "utf-8")} == TestLSet.lset.get(map)

    #Add() - add without any mandatory parameters.
    def test_lset_no_parameter_negative(self):

        """
            Invoke add() without any mandatory parameters.
        """

        with pytest.raises(TypeError) as typeError:
            TestLSet.lset.add()

        assert "Required argument 'value' (pos 1) not found" in typeError.value

    #Add - Add an object to the lset which is already present.
    def test_lset_add_duplicate_element_negative(self):

        """
            Invoke add() duplicate element into the set.
        """

        print  TestLSet.key

        with pytest.raises(Exception) as exception: 
            TestLSet.lset.add(566)

        assert exception.value[0] == 100
        assert exception.value[1] == "/opt/aerospike/sys/udf/lua/ldt/lib_lset.lua:2459: 1402:LDT-Unique Key or Value Violation"

    #Add_all() - Add a list of objects to the set.
    def test_lset_add_all_positive(self):

        """
            Invoke add_all() to add a list of objects to the set.
        """

        list = [100, 200, 'z']
        map = {
                'k78' : 66,
                'pqr' : 202
                }
        add_all = ['', 12, 56, 'as', bytearray("asd;as[d'as;d", "utf-8"), list, map]
        assert 0 == TestLSet.lset.add_all(add_all)
        assert u'as' == TestLSet.lset.get('as')

    #Get() - Get an object from the lset without any mandatory parameters.
    def test_lset_get_element_negative(self):

        """
            Invoke get() without any mandatory parameters.
        """

        with pytest.raises(TypeError) as typeError: 
            TestLSet.lset.get()

    #Get() - Get non-existent element from lset.
    def test_lset_get_non_existent_element_positive(self):

        """
            Invoke get() non-existent element from set.
        """

        with pytest.raises(Exception) as exception: 
            TestLSet.lset.get(1000)

        assert exception.value[0] == 100
        assert exception.value[1] == "/opt/aerospike/sys/udf/lua/ldt/lib_lset.lua:3931: 1401:LDT-Item Not Found"

    #Exists() and Remove() - Test existence of an object and remove from the set.
    def test_lset_exists_element_positive(self):

        """
            Invoke exists() on lset.
        """

        assert 0 == TestLSet.lset.add(999)
        assert True == TestLSet.lset.exists(999)
        assert 0 == TestLSet.lset.remove(999)

    def test_lset_exists_random_element_positive(self):

        """
            Invoke exists() on lset where non-existent element is passed.
        """

        assert False == TestLSet.lset.exists(444)

    #Size() - Get the current item count of the set.
    def test_lset_size_positive(self):

        """
            Invoke size() on lset.
        """
        assert 12 == TestLSet.lset.size()

    #Destroy() - Delete the entire set(LDT Remove).
    def test_lset_destroy_positive(self):

        """
            Invoke destroy() to delete entire LDT.
        """
        key = ('test', 'demo', 'remove')

        lset = self.client.lset(key, 'lset_ad')

        lset.add(876)

        assert 0 == lset.destroy()
