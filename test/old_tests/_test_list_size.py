# -*- coding: utf-8 -*-
import pytest
import sys
import random
from .test_base_class import TestBaseClass
from aerospike import exception as e

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)


class TestListSize(object):

    def setup_class(cls):
        """
        Setup method.
        """
        hostlist, user, password = TestBaseClass.get_hosts()
        config = {'hosts': hostlist}
        if user is None and password is None:
            TestListSize.client = aerospike.client(config).connect()
        else:
            TestListSize.client = aerospike.client(
                config).connect(user, password)

    def teardown_class(cls):
        TestListSize.client.close()

    def setup_method(self, method):
        for i in range(5):
            key = ('test', 'demo', i)
            rec = {'name': 'name%s' %
                   (str(i)), 'contact_no': [i, i + 1],
                   'city': ['Pune', 'Dehli']}
            TestListSize.client.put(key, rec)

    def teardown_method(self, method):
        """
        Teardoen method.
        """
        # time.sleep(1)
        for i in range(5):
            key = ('test', 'demo', i)
            TestListSize.client.remove(key)

    def test_list_size_with_correct_paramters(self):
        """
        Invoke list_size() with correct parameters
        """
        key = ('test', 'demo', 1)

        count = TestListSize.client.list_size(key, 'contact_no')

        assert 2 == count

    def test_list_size_list_with_correct_policy(self):
        """
        Invoke list_size() count list elements with correct policy
        """
        key = ('test', 'demo', 2)
        policy = {
            'timeout': 1000,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'commit_level': aerospike.POLICY_COMMIT_LEVEL_MASTER
        }
        count = TestListSize.client.list_size(key, "contact_no", {}, policy)

        assert 2 == count

    def test_list_size_with_no_parameters(self):
        """
        Invoke list_size() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            TestListSize.client.list_size()
        assert "argument 'key' (pos 1)" in str(
            typeError.value)

    def test_list_size_with_incorrect_policy(self):
        """
        Invoke list_size() with incorrect policy
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 0.5
        }
        try:
            TestListSize.client.list_size(key, "contact_no", {}, policy)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "timeout is invalid"

    def test_list_size_with_nonexistent_key(self):
        """
        Invoke list_size() with non-existent key
        """
        charSet = 'abcdefghijklmnopqrstuvwxyz1234567890'
        minLength = 5
        maxLength = 30
        length = random.randint(minLength, maxLength)
        key = ('test', 'demo', ''.join(map(lambda unused:
                                           random.choice(charSet),
                                           range(length))) + ".com")
        try:
            TestListSize.client.list_size(key, "contact_no")
        except e.RecordNotFound as exception:
            assert exception.code == 2

    def test_list_size_with_nonexistent_bin(self):
        """
        Invoke list_size() with non-existent bin
        """
        key = ('test', 'demo', 1)
        charSet = 'abcdefghijklmnopqrstuvwxyz1234567890'
        minLength = 5
        maxLength = 10
        length = random.randint(minLength, maxLength)
        bin = ''.join(map(lambda unused:
                          random.choice(charSet), range(length))) + ".com"
        try:
            TestListSize.client.list_size(key, bin)
        except e.RecordNotFound as exception:
            assert exception.code == 2

    def test_list_size_with_extra_parameter(self):
        """
        Invoke list_size() with extra parameter.
        """
        key = ('test', 'demo', 1)
        policy = {'timeout': 1000}
        with pytest.raises(TypeError) as typeError:
            TestListSize.client.list_size(key, "contact_no", {}, policy, "")

        assert "list_size() takes at most 4 arguments (5 given)" in str(
            typeError.value)

    def test_list_size_policy_is_string(self):
        """
        Invoke list_size() with policy is string
        """
        key = ('test', 'demo', 1)
        try:
            TestListSize.client.list_size(key, "contact_no", {}, "")

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "policy must be a dict"

    def test_list_size_key_is_none(self):
        """
        Invoke list_size() with key is none
        """
        try:
            TestListSize.client.list_size(None, "contact_no")

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "key is invalid"

    def test_list_size_bin_is_none(self):
        """
        Invoke list_size() with bin is none
        """
        key = ('test', 'demo', 1)
        try:
            TestListSize.client.list_size(key, None)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Bin name should be of type string"

    def test_list_size_meta_type_integer(self):
        """
        Invoke list_size() with metadata input is of type integer
        """
        key = ('test', 'demo', 1)
        try:
            TestListSize.client.list_size(key, "contact_no", 888)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Metadata should be of type dictionary"
