# -*- coding: utf-8 -*-

import pytest
import sys
import time
try:
    import aerospike
except:
    print "Please install aerospike python client."
    sys.exit(1)

class TestLStack(object):

    lstack = None
    client = None
    key = None

    def setup_class(cls):

        print "setup class invoked..."
        config = {
                "hosts": [("172.20.25.24", 3000)]
                }

        TestLStack.client = aerospike.client(config).connect()

        TestLStack.key = ('test', 'demo', 'lstack_push_key')

        TestLStack.lstack = TestLStack.client.lstack(TestLStack.key, 'lstack_pu')


    def teardown_class(cls):
        print "teardown class invoked..."

        TestLStack.lstack.destroy()

        cls.client.close()

    def test_lstack_peek_from_empty_stack_positive(self):

        """
            Invoke peek() from empty stack.
        """

        with pytest.raises(Exception) as exception: 
            TestLStack.lstack.peek(10)

        assert exception.value[0] == 100
        assert exception.value[1] == "/opt/aerospike/sys/udf/lua/ldt/lib_lstack.lua:3039: 1415:LDT-Top Record Not Found"

    #push() - push an object to the set.
    def test_lstack_push_integer_positive(self):

        """
            Invoke push() integer type data.
        """

        print  TestLStack.key

        assert 0 == TestLStack.lstack.push(566)

    def test_lstack_push_string_positive(self):

        """
            Invoke push() for string type data.
        """

        assert 0 == TestLStack.lstack.push("lsrack_string")

    def test_lstack_push_char_positive(self):

        """
            Invoke push() char type data.
        """
        assert 0 == TestLStack.lstack.push('k')

    def test_lstack_push_float_positive(self):

        """
            Invoke push() float type data.
        """
        rec = {
                "pi" : 3.14
                }

        with pytest.raises(Exception) as exception: 
            TestLStack.lstack.push(rec)

        assert exception.value[0] == -1
        assert exception.value[1] == "value is not a supported type."

    #Set_capacity() - Set the capacity of the stack.
    def test_lstack_set_capacity_positive(self):

        """
            Invoke set_capacity() of lstack.
        """
        assert 0 == TestLStack.lstack.set_capacity(25)

    def test_lstack_push_list_positive(self):

        """
            Invoke push() method for list.
        """
        list = [12, 'a', bytearray("asd;as[d'as;d", "utf-8")]

        assert 0 == TestLStack.lstack.push(list)

    def test_lstack_push_map_positive(self):

        """
            Invoke push() method for map.
        """
        map = {
                'a' : 12,
                '!@#@#$QSDAsd;as' : bytearray("asd;as[d'as;d", "utf-8")
                }

        assert 0 == TestLStack.lstack.push(map)

    def test_lstack_push_empty_string_positive(self):

        """
            Invoke push() empty string.
        """
        assert 0 == TestLStack.lstack.push('')
    
    def test_lstack_push_no_parameter_negative(self):

        """
            Invoke push() without any mandatory parameters.
        """

        with pytest.raises(TypeError) as typeError:
            TestLStack.lstack.push()

        assert "Required argument 'value' (pos 1) not found" in typeError.value


    #Push_many() - list of objects to the set.
    def test_lstack_push_many_positive(self):

        """
            Invoke push_many() to add a list of objects to the stack.
        """

        list = [100, 200, 'z']
        map = {
                'k78' : 66,
                'pqr' : 202
                }
        assert 0 == TestLStack.lstack.push_many([12, 56, 'as',
            bytearray("asd;as[d'as;d", "utf-8"), list, map])

    #Peek() - Peek an object from the lstack.
    def test_lstack_get_element_positive(self):

        """
            Invoke peek() to get elements from the stack.
        """

        list = [100, 200, 'z']
        map = {
                'k78' : 66,
                'pqr' : 202
                }
        assert 0 == TestLStack.lstack.push_many([12, 56, 'as',
            bytearray("asd;as[d'as;d", "utf-8"), list, map])
        assert [{'k78' : 66, 'pqr' : 202}, [100, 200, 'z']] == TestLStack.lstack.peek(2)

    def test_lstack_get_element_negative(self):

        """
            Invoke peek() without any mandatory parameters.
        """

        with pytest.raises(TypeError) as typeError: 
            TestLStack.lstack.peek()

    #Get_capacity() - Get the capacity of the stack.
    def test_lstack_get_capacity_positive(self):

        """
            Invoke get_capacity() of lstack.
        """
        assert 25 == TestLStack.lstack.get_capacity()

    #Size() - Get the current item count of the stack.
    def test_lstack_size_positive(self):

        """
            Invoke size() on lstack.
        """
        assert 18 == TestLStack.lstack.size()

    #Destroy() - Delete the entire set(LDT Remove).
    def test_lstack_destroy_positive(self):

        """
            Invoke destroy() to delete entire LDT.
        """
        key = ('test', 'demo', 'remove')

        lstack = self.client.lstack(key, 'lstack_ad')

        lstack.push(876)

        assert 0 == lstack.destroy()
