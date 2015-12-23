# -*- coding: utf-8 -*-
import pytest
import time
import sys
import random
import cPickle as pickle
from test_base_class import TestBaseClass

aerospike = pytest.importorskip("aerospike")
try:
    from aerospike.exception import *
except:
    print "Please install aerospike python client."
    sys.exit(1)

class TestListSet(object):
    def setup_class(cls):
        """
        Setup method.
        """
        hostlist, user, password = TestBaseClass.get_hosts()
        config = {'hosts': hostlist}
        if user == None and password == None:
            TestListSet.client = aerospike.client(config).connect()
        else:
            TestListSet.client = aerospike.client(config).connect(user, password)

    def teardown_class(cls):
        TestListSet.client.close()

    def setup_method(self, method):
        for i in xrange(5):
            key = ('test', 'demo', i)
            rec = {'name': 'name%s' % (str(i)), 'contact_no': [i, i+1], 'city' : ['Pune', 'Dehli']}
            TestListSet.client.put(key, rec)
        key = ('test', 'demo', 2)
        TestListSet.client.list_append(key, "contact_no", [45, 50, 80])

    def teardown_method(self, method):
        """
        Teardown method.
        """
        #time.sleep(1)
        for i in xrange(5):
            key = ('test', 'demo', i)
            TestListSet.client.remove(key)

    def test_list_set_with_correct_paramters(self):
        """
        Invoke list_set() sets list element with correct parameters
        """
        key = ('test', 'demo', 1)
        
        status = TestListSet.client.list_set(key, "contact_no", 5, [500, 1000])
        assert status == 0L

        key, meta, bins = TestListSet.client.get(key)
        assert bins == {'city': ['Pune', 'Dehli'], 'contact_no': [1, 2, None, None, None, [500, 1000]], 'name': 'name1'}

    def test_list_set_with_correct_policy(self):
        """
        Invoke list_append() append list with correct policy
        """
        key = ('test', 'demo', 2)
        policy = {
            'timeout': 1000,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'commit_level': aerospike.POLICY_COMMIT_LEVEL_MASTER
        }

        status = TestListSet.client.list_set(key, 'city', 7, 'Mumbai', {}, policy)
        assert status == 0L

        key, meta, bins = TestListSet.client.get(key)
        assert bins == {'city': ['Pune', 'Dehli', None, None, None, None, None, 'Mumbai'],
            'contact_no': [2, 3, [45, 50, 80]], 'name': 'name2'}

    def test_list_set_with_no_parameters(self):
        """
        Invoke list_set() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            TestListSet.client.list_set()
        assert "Required argument 'key' (pos 1) not found" in typeError.value

    def test_list_set_with_incorrect_policy(self):
        """
        Invoke list_set() with incorrect policy
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 0.5
        }
        try:
            TestListSet.client.list_set(key, "contact_no", 0, 850, {}, policy)

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "timeout is invalid"

    def test_list_set_with_nonexistent_key(self):
        """
        Invoke list_set() with non-existent key
        """
        charSet = 'abcdefghijklmnopqrstuvwxyz1234567890'
        minLength = 5
        maxLength = 30
        length = random.randint(minLength, maxLength)
        key = ('test', 'demo', ''.join(map(lambda unused :
            random.choice(charSet), range(length)))+".com")
        try:
            TestListSet.client.list_set(key, "contact_no", 0, 100)

        except BinIncompatibleType as exception:
            assert exception.code == 12L

    def test_list_set_with_extra_parameter(self):
        """
        Invoke list_set() with extra parameter.
        """
        key = ('test', 'demo', 1)
        policy = {'timeout': 1000}
        with pytest.raises(TypeError) as typeError:
            TestListSet.client.list_set(key, "contact_no", 1, 999, {}, policy, "")

        assert "list_set() takes at most 6 arguments (7 given)" in typeError.value

    def test_list_set_policy_is_string(self):
        """
        Invoke list_set() with policy is string
        """
        key = ('test', 'demo', 1)
        try:
            TestListSet.client.list_set(key, "contact_no", 1, 30, {}, "")

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "policy must be a dict"

    def test_list_set_key_is_none(self):
        """
        Invoke list_set() with key is none
        """
        try:
            TestListSet.client.list_set(None, "contact_no", 0, 89)

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "key is invalid"

    def test_list_set_bin_is_none(self):
        """
        Invoke list_set() with bin is none
        """
        key = ('test', 'demo', 1)
        try:
            TestListSet.client.list_set(key, None, 1, 555)

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Bin name should be of type string"
    
    def test_list_set_with_negative_index(self):
        """
        Invoke list_set() with negative index
        """
        key = ('test', 'demo', 1)
        try:
            bins = TestListSet.client.list_set(key, "contact_no", -56, 12)
        except InvalidRequest as exception:
            assert exception.code == 4
