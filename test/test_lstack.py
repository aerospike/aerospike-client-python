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

        TestLStack.lstack = TestLStack.client.lstack(TestLStack.key, 'lstack_push')


    def teardown_class(cls):
        print "teardown class invoked..."

        TestLStack.lstack.destroy()

        cls.client.close()

    #Peek - Peek element from empty stack.
    def test_lstack_peek_from_empty_stack_positive(self):

        """
            Invoke peek() from empty stack.
        """

        with pytest.raises(Exception) as exception: 
            TestLStack.lstack.peek(10)

        assert exception.value[0] == 100
        assert exception.value[1] == "/opt/aerospike/sys/udf/lua/ldt/lib_lstack.lua:3039: 1415:LDT-Top Record Not Found"

    #Push() - push an object(integer, string, byte, map) onto the stack.
    def test_lstack_push_peek_positive(self):

        """
            Invoke push() data onto the stack.
        """

        assert 0 == TestLStack.lstack.push('')
        assert 0 == TestLStack.lstack.push(566)
        map = {
                'a' : 12,
                '!@#@#$QSDAsd;as' : bytearray("asd;as[d'as;d", "utf-8")
                }
        assert 0 == TestLStack.lstack.push(map)
        
        assert [{'a' : 12, '!@#@#$QSDAsd;as' : bytearray("asd;as[d'as;d",
            "utf-8")}, 566, u''] == TestLStack.lstack.peek(3)

    #Push() - Push an object(float) onto the stack.
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

    #Push - Push without any mandatory parameters.
    def test_lstack_push_no_parameter_negative(self):

        """
            Invoke push() without any mandatory parameters.
        """

        with pytest.raises(TypeError) as typeError:
            TestLStack.lstack.push()

        assert "Required argument 'value' (pos 1) not found" in typeError.value

    #Push_many() - List of objects to the stack.
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
    
        assert [{u'k78': 66, u'pqr': 202}, [100, 200, u'z'],
                bytearray(b"asd;as[d\'as;d"), u'as', 56, 12] == TestLStack.lstack.peek(6)
    
    #Size() - Get the current item count of the stack.
    def test_lstack_size_positive(self):

        """
            Invoke size() on lstack.
        """
        assert 9 == TestLStack.lstack.size()

    #Set_capacity() - Set the capacity of the stack.
    def test_lstack_set_and_get_capacity_positive(self):

        """
            Invoke set_capacity() of lstack.
        """
        assert 0 == TestLStack.lstack.set_capacity(10)
        assert 10 == TestLStack.lstack.get_capacity()

    #Destroy() - Delete the entire set(LDT Remove).
    def test_lstack_destroy_positive(self):

        """
            Invoke destroy() to delete entire LDT.
        """
        key = ('test', 'demo', 'remove')

        lstack = self.client.lstack(key, 'lstack_rem')

        lstack.push(876)

        assert 0 == lstack.destroy()
