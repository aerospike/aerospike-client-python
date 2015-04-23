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

class TestLStack(object):

    lstack = None
    client = None
    key = None

    def setup_class(cls):

        print "setup class invoked..."
        hostlist, user, password = TestBaseClass.get_hosts()
        config = {'hosts': hostlist}
        if user == None and password == None:
            TestLStack.client = aerospike.client(config).connect()
        else:
            TestLStack.client = aerospike.client(config).connect(user, password)

        TestLStack.key = ('test', 'demo', 'lstack_push_key')

        TestLStack.lstack = TestLStack.client.lstack(TestLStack.key,
                                                     'lstack_push')

    def teardown_class(cls):
        print "teardown class invoked..."

        TestLStack.lstack.destroy()

        cls.client.close()

    #Peek - Peek element from empty stack.
    def test_lstack_peek_from_empty_stack_positive(self):
        """
            Invoke peek() from empty stack.
        """

        try:
            TestLStack.lstack.peek(10)

        except RecordNotFound as exception:
            assert exception.code == 2L
            assert exception.msg == "AEROSPIKE_ERR_RECORD_NOT_FOUND"

    #Push() - push an object(integer, string, byte, map) onto the stack.
    #Size() - Get the current item count of the stack.
    #Peek - Peek element from empty stack.
    def test_lstack_push_peek_size_positive(self):
        """
            Invoke push() data onto the stack.
        """

        #assert 0 == TestLStack.lstack.push('')
        assert 0 == TestLStack.lstack.push(566)
        map = {'a': 12, '!@#@#$QSDAsd;as': bytearray("asd;as[d'as;d", "utf-8")}
        assert 0 == TestLStack.lstack.push(map)

        policy = {'timeout': 9000, 'key': aerospike.POLICY_KEY_SEND}
        assert 2 == TestLStack.lstack.size(policy)

        assert [
            {'a': 12,
             '!@#@#$QSDAsd;as': bytearray("asd;as[d'as;d", "utf-8")}, 566
        ] == TestLStack.lstack.peek(3)

    #Push() - Push an object(float) onto the stack.
    def test_lstack_push_float_positive(self):
        """
            Invoke push() float type data.
        """
        rec = {"pi": 3.14}

        assert 0 == TestLStack.lstack.push(rec)
        assert [{"pi": 3.14}] == TestLStack.lstack.peek(1)

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
        map = {'k78': 66, 'pqr': 202}
        policy = {'timeout': 8000}
        assert 0 == TestLStack.lstack.push_many([12, 56, 'as', bytearray(
            "asd;as[d'as;d", "utf-8"), list, map], policy)

        assert [{u'k78': 66,
                 u'pqr': 202}, [100, 200, u'z'], bytearray(b"asd;as[d\'as;d"),
                u'as', 56, 12] == TestLStack.lstack.peek(6)

    #Set_capacity() - Set the capacity of the stack.
    def test_lstack_set_and_get_capacity_positive(self):
        """
            Invoke set_capacity() of lstack.
        """
        assert 0 == TestLStack.lstack.set_capacity(10)
        assert 10 == TestLStack.lstack.get_capacity()

    #Set_capacity() - Set the capacity of the stack to 0.
    def test_lstack_set_capacity_with_zero_positive(self):
        """
            Invoke set_capacity() of lstack with zero value.
        """
        try:
            TestLStack.lstack.set_capacity(0)

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "invalid parameter. as/key/ldt/capacity cannot be null"
    
    #Destroy() - Delete the entire set(LDT Remove).
    def test_lstack_destroy_positive(self):
        """
            Invoke destroy() to delete entire LDT.
        """
        key = ('test', 'demo', 'remove')

        lstack = self.client.lstack(key, 'lstack_rem')

        lstack.push(876)

        assert 0 == lstack.destroy()

    def test_lstack_ldt_initialize_negative(self):
        """
            Initialize ldt with wrong key.
        """
        key = ('test', 'demo', 12.3)

        try:
            lstack = self.client.lstack(key, 'ldt_stk')

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Parameters are incorrect"
