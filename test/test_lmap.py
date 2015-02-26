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
                "hosts": [("127.0.0.1", 3000)]
                }

        TestLMap.client = aerospike.client(config).connect()

        TestLMap.key = ('test', 'demo', 'lmap_add_key')

        TestLMap.lmap = TestLMap.client.lmap(TestLMap.key, 'lmap_add_bin')


    def teardown_class(cls):
        print "teardown class invoked..."

        TestLMap.lmap.destroy()

        cls.client.close()

    #Put() - put an object to the map.
    #Get() - Get an object to the map.
    #Size() - Get the current item count of the set.
    def test_lmap_put_get_size_positive(self):

        """
            Invoke put() integer type data.
        """

        assert 0 == TestLMap.lmap.put('k1', 8) 
        assert {u'k1' : 8} == TestLMap.lmap.get('k1')

        assert 0 == TestLMap.lmap.put('k2', 86)
        assert {u'k2' : 86} == TestLMap.lmap.get('k2')

        assert 0 == TestLMap.lmap.put(12, 'a')
        assert {12 : u'a'} == TestLMap.lmap.get(12)

        assert 0 == TestLMap.lmap.put('k', 'a')
        assert {u'k' : u'a'} == TestLMap.lmap.get('k')
        
        policy = {
                'key' : aerospike.POLICY_KEY_SEND 
                }

        assert 4 == TestLMap.lmap.size(policy)

    #put() - put unsupported datatype to lmap.
    def test_lmap_put_float_positive(self):

        """
            Invoke put() float type data.
        """
        rec = {
                "pi" : 3.14
                }

        assert 0 == TestLMap.lmap.put('k11', rec)
        assert {u'k11': {u'pi' : 3.14}} == TestLMap.lmap.get('k11')

    #put() and Get() - put list to lmap.  
    def test_lmap_put_get_list_positive(self):

        """
            Invoke put() method for list.
        """
        list = [12, 'a', bytearray("asd;as[d'as;d", "utf-8")]

        assert 0 == TestLMap.lmap.put('list', list)
        assert {u'list' : [12, 'a', bytearray("asd;as[d'as;d", "utf-8")]} == TestLMap.lmap.get('list')

    #put() and Get() - put map to lmap.
    def test_lmap_put_map_positive(self):

        """
            Invoke put() method for map.
        """
        map = {
                'a' : 12,
                '!@#@#$QSDAsd;as' : bytearray("asd;as[d'as;d", "utf-8"),
                }

        assert 0 == TestLMap.lmap.put('', map)
        assert {u'' : {'a' : 12, '!@#@#$QSDAsd;as' : bytearray("asd;as[d'as;d",
            "utf-8")}} == TestLMap.lmap.get('')

    #put() - LMap put duplicate key parameter.
    def test_lmap_put_duplicate_key(self):

        """
            Invoke put() method with duplicate key.
        """
        assert 0 == TestLMap.lmap.put('name', 'aa')
        assert 0 == TestLMap.lmap.put('name', 'bb')
        assert {u'name' : u'bb'} == TestLMap.lmap.get('name')

    #put() - put() without any mandatory parameters.
    def test_lmap_no_parameter_negative(self):

        """
            Invoke put() without any mandatory parameters.
        """

        with pytest.raises(TypeError) as typeError:
            TestLMap.lmap.put()

        assert "Required argument 'key' (pos 1) not found" in typeError.value
    
    #Put_many() - put a map containing the entries to put to the lmap.
    def test_lmap_put_many_positive(self):

        """
            Invoke put_many() to put a map of objects to the set.
        """

        map1 = {
                'k76' : 662,
                'pq' : 2022
                }

        map2 = {
                53 : bytearray("aassd;as[d'as;d", "utf-8"),
                'k98' : map1
                }
        assert 0 == TestLMap.lmap.put_many(map2)
   
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

        assert {} == TestLMap.lmap.get(1000)

    #Remove() - Remove an object from the set.
    def test_lmap_remove_element_positive(self):

        """
            Invoke remove() to remove element.
        """
        
        assert 0 == TestLMap.lmap.put('k47', 'aa')
        assert 0 == TestLMap.lmap.remove('k47')

    #Destroy() - Delete the entire lmap(LDT Remove).
    def test_lmap_destroy_positive(self):

        """
            Invoke destroy() to delete entire LDT.
        """
        key = ('test', 'demo', 'remove')

        lmap = self.client.lmap(key, 'lmap_put')

        lmap.put('k67', 876)

        assert 0 == lmap.destroy()

    def test_lmap_ldt_initialize_negative(self):

        """
            Initialize ldt with wrong key.
        """
        key = ('test', 'demo', 12.3)

        with pytest.raises(Exception) as exception: 
            lmap = self.client.lmap(key, 'ldt_stk')

        assert exception.value[0] == -1
        assert exception.value[1] == "Parameters are incorrect"
