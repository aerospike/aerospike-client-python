# -*- coding: utf-8 -*-

import pytest
import sys
import time
try:
    import aerospike
except:
    print "Please install aerospike python client."
    sys.exit(1)

class TestLMap(object):

    lmap = None
    client = None
    key = None

    def setup_class(cls):

        print "setup class invoked..."
        config = {
                "hosts": [("172.20.25.24", 3000)]
                }

        TestLMap.client = aerospike.client(config).connect()

        TestLMap.key = ('test', 'demo', 'lmap_add_key')

        TestLMap.lmap = TestLMap.client.lmap(TestLMap.key, 'lmap_add_bin')


    def teardown_class(cls):
        print "teardown class invoked..."

        TestLMap.lmap.destroy()

        cls.client.close()

    #add() - Add an object to the map.
    def test_lmap_add_integer_positive(self):

        """
            Invoke add() integer type data.
        """

        assert 0 == TestLMap.lmap.add('k1', 8) 
        assert {u'k1' : 8} == TestLMap.lmap.get('k1')

        assert 0 == TestLMap.lmap.add('k2', 86)
        assert {u'k2' : 86} == TestLMap.lmap.get('k2')

        assert 0 == TestLMap.lmap.add(12, 'a')
        assert {12 : u'a'} == TestLMap.lmap.get(12)

        assert 0 == TestLMap.lmap.add('k', 'a')
        assert {u'k' : u'a'} == TestLMap.lmap.get('k')

    #Add() - Add unsupported datatype to lmap.
    def test_lmap_add_float_positive(self):

        """
            Invoke add() float type data.
        """
        rec = {
                "pi" : 3.14
                }

        with pytest.raises(Exception) as exception: 
            TestLMap.lmap.add('k11', rec)

        assert exception.value[0] == -1
        assert exception.value[1] == "value is not a supported type."

    #Add() and Get() - Add list to lmap.  
    def test_lmap_add_get_list_positive(self):

        """
            Invoke add() method for list.
        """
        list = [12, 'a', bytearray("asd;as[d'as;d", "utf-8")]

        assert 0 == TestLMap.lmap.add('list', list)
        assert {u'list' : [12, 'a', bytearray("asd;as[d'as;d", "utf-8")]} == TestLMap.lmap.get('list')

    #Add() and Get() - Add map to lmap.
    def test_lmap_add_map_positive(self):

        """
            Invoke add() method for map.
        """
        map = {
                'a' : 12,
                '!@#@#$QSDAsd;as' : bytearray("asd;as[d'as;d", "utf-8"),
                }

        assert 0 == TestLMap.lmap.add('', map)
        assert {u'' : {'a' : 12, '!@#@#$QSDAsd;as' : bytearray("asd;as[d'as;d",
            "utf-8")}} == TestLMap.lmap.get('')

    #Add() - LMap add duplicate key parameter.
    def test_lmap_add_duplicate_key(self):

        """
            Invoke add() method with duplicate key.
        """
        assert 0 == TestLMap.lmap.add('name', 'aa')
        assert 0 == TestLMap.lmap.add('name', 'bb')
        assert {u'name' : u'aa'} == TestLMap.lmap.get('name')

    #Add() - Add() without any mandatory parameters.
    def test_lmap_no_parameter_negative(self):

        """
            Invoke add() without any mandatory parameters.
        """

        with pytest.raises(TypeError) as typeError:
            TestLMap.lmap.add()

        assert "Required argument 'key' (pos 1) not found" in typeError.value
    
    #Add_all() - Add a list of objects to the set.
    def test_lmap_put_all_positive(self):

        """
            Invoke put_all() to add a map of objects to the set.
        """

        map1 = {
                'k76' : 662,
                'pq' : 2022
                }

        map2 = {
                53 : bytearray("aassd;as[d'as;d", "utf-8"),
                'k98' : map1
                }
        assert 0 == TestLMap.lmap.add_all(map2)
   
    #Get() - Get() without any mandatory parameters.
    def test_lmap_get_element_negative(self):

        """
            Invoke get() without any mandatory parameters.
        """

        with pytest.raises(TypeError) as typeError: 
            TestLMap.lmap.get()

    #Get() - Get() on non-existent key.
    def test_lmap_get_non_existent_element_positive(self):

        """
            Invoke get() non-existent element from set.
        """

        with pytest.raises(Exception) as exception: 
            TestLMap.lmap.get(1000)

        assert exception.value[0] == 100
        assert exception.value[1] == "/opt/aerospike/sys/udf/lua/ldt/lib_lmap.lua:2999: 1401:LDT-Item Not Found"

    #Size() - Get the current item count of the set.
    def test_lmap_size_positive(self):

        """
            Invoke size() on lmap.
        """
        assert 9 == TestLMap.lmap.size()

    #Remove() - Remove an object from the set.
    def test_lmap_remove_element_positive(self):

        """
            Invoke remove() to remove element.
        """
        
        assert 0 == TestLMap.lmap.add('k47', 'aa')
        assert 0 == TestLMap.lmap.remove('k47')

    #Destroy() - Delete the entire lmap(LDT Remove).
    def test_lmap_destroy_positive(self):

        """
            Invoke destroy() to delete entire LDT.
        """
        key = ('test', 'demo', 'remove')

        lmap = self.client.lmap(key, 'lmap_add')

        lmap.add('k67', 876)

        assert 0 == lmap.destroy()
