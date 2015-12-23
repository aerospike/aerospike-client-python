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

class TestListCount(object):
    def setup_class(cls):
        """
        Setup method.
        """
        hostlist, user, password = TestBaseClass.get_hosts()
        config = {'hosts': hostlist}
        if user == None and password == None:
            TestListCount.client = aerospike.client(config).connect()
        else:
            TestListCount.client = aerospike.client(config).connect(user, password)

    def teardown_class(cls):
        TestListCount.client.close()

    def setup_method(self, method):
        for i in xrange(5):
            key = ('test', 'demo', i)
            rec = {'name': 'name%s' % (str(i)), 'contact_no': [i, i+1], 'city' : ['Pune', 'Dehli']}
            TestListCount.client.put(key, rec)

    def teardown_method(self, method):
        """
        Teardoen method.
        """
        #time.sleep(1)
        for i in xrange(5):
            key = ('test', 'demo', i)
            TestListCount.client.remove(key)

    def test_list_count_with_correct_paramters(self):
        """
        Invoke list_count() with correct parameters
        """
        key = ('test', 'demo', 1)

        count = TestListCount.client.list_count(key, 'contact_no')

        assert 2 == count

    def test_list_count_list_with_correct_policy(self):
        """
        Invoke list_count() count list elements with correct policy
        """
        key = ('test', 'demo', 2)
        policy = {
            'timeout': 1000,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'commit_level': aerospike.POLICY_COMMIT_LEVEL_MASTER
        }
        count = TestListCount.client.list_count(key, "contact_no", {}, policy)

        assert 2 == count

    def test_list_count_with_no_parameters(self):
        """
        Invoke list_count() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            TestListCount.client.list_count()
        assert "Required argument 'key' (pos 1) not found" in typeError.value

    def test_list_count_with_incorrect_policy(self):
        """
        Invoke list_count() with incorrect policy
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 0.5
        }
        try:
            TestListCount.client.list_count(key, "contact_no", {}, policy)

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "timeout is invalid"

    def test_list_count_with_nonexistent_key(self):
        """
        Invoke list_count() with non-existent key
        """
        charSet = 'abcdefghijklmnopqrstuvwxyz1234567890'
        minLength = 5
        maxLength = 30
        length = random.randint(minLength, maxLength)
        key = ('test', 'demo', ''.join(map(lambda unused :
            random.choice(charSet), range(length)))+".com")
        try:
            TestListCount.client.list_count(key, "contact_no")
        except RecordNotFound as exception:
            assert exception.code == 2

    def test_list_count_with_extra_parameter(self):
        """
        Invoke list_count() with extra parameter.
        """
        key = ('test', 'demo', 1)
        policy = {'timeout': 1000}
        with pytest.raises(TypeError) as typeError:
            TestListCount.client.list_count(key, "contact_no", {}, policy, "")

        assert "list_count() takes at most 4 arguments (5 given)" in typeError.value

    def test_list_count_policy_is_string(self):
        """
        Invoke list_count() with policy is string
        """
        key = ('test', 'demo', 1)
        try:
            TestListCount.client.list_count(key, "contact_no", {}, "")

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "policy must be a dict"

    def test_list_count_key_is_none(self):
        """
        Invoke list_count() with key is none
        """
        try:
            TestListCount.client.list_count(None, "contact_no")

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "key is invalid"

    def test_list_count_bin_is_none(self):
        """
        Invoke list_count() with bin is none
        """
        key = ('test', 'demo', 1)
        try:
            TestListCount.client.list_count(key, None)

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Bin name should be of type string"
