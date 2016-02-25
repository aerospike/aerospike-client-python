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


class TestListClear(object):

    def setup_class(cls):
        """
        Setup method.
        """
        hostlist, user, password = TestBaseClass.get_hosts()
        config = {'hosts': hostlist}
        if user is None and password is None:
            TestListClear.client = aerospike.client(config).connect()
        else:
            TestListClear.client = aerospike.client(
                config).connect(user, password)

    def teardown_class(cls):
        TestListClear.client.close()

    def setup_method(self, method):
        for i in range(5):
            key = ('test', 'demo', i)
            rec = {'name': 'name%s' %
                   (str(i)), 'contact_no': [i, i + 1],
                   'city': ['Pune', 'Dehli']}
            TestListClear.client.put(key, rec)

    def teardown_method(self, method):
        """
        Teardoen method.
        """
        # time.sleep(1)
        for i in range(5):
            key = ('test', 'demo', i)
            TestListClear.client.remove(key)

    def test_list_clear_with_correct_paramters(self):
        """
        Invoke list_clear() with correct parameters
        """
        key = ('test', 'demo', 1)

        status = TestListClear.client.list_clear(key, 'contact_no')

        assert status == 0

        (key, _, bins) = TestListClear.client.get(key)
        assert bins == {
            'city': ['Pune', 'Dehli'], 'contact_no': [], 'name': 'name1'}

    def test_list_clear_list_with_correct_policy(self):
        """
        Invoke list_clear() removes all list elements with correct policy
        """

        key = ('test', 'demo', 2)

        status = TestListClear.client.list_clear(key, 'city')
        assert status == 0

        (key, _, bins) = TestListClear.client.get(key)
        assert bins == {'city': [], 'contact_no': [2, 3], 'name': 'name2'}

    def test_list_clear_with_no_parameters(self):
        """
        Invoke list_clear() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            TestListClear.client.list_clear()
        assert "Required argument 'key' (pos 1) not found" in str(
            typeError.value)

    def test_list_clear_with_incorrect_policy(self):
        """
        Invoke list_clear() with incorrect policy
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 0.5
        }
        try:
            TestListClear.client.list_clear(key, "contact_no", {}, policy)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "timeout is invalid"

    def test_list_clear_with_nonexistent_key(self):
        """
        Invoke list_clear() with non-existent key
        """
        charSet = 'abcdefghijklmnopqrstuvwxyz1234567890'
        minLength = 5
        maxLength = 30
        length = random.randint(minLength, maxLength)
        key = ('test', 'demo', ''.join(map(lambda unused:
                                           random.choice(charSet),
                                           range(length))) + ".com")
        try:
            TestListClear.client.list_clear(key, "contact_no")
        except e.BinIncompatibleType as exception:
            assert exception.code == 12

    def test_list_clear_with_nonexistent_bin(self):
        """
        Invoke list_clear() with non-existent bin
        """
        key = ('test', 'demo', 1)
        charSet = 'abcdefghijklmnopqrstuvwxyz1234567890'
        minLength = 5
        maxLength = 10
        length = random.randint(minLength, maxLength)
        bin = ''.join(map(lambda unused:
                          random.choice(charSet), range(length))) + ".com"
        try:
            TestListClear.client.list_clear(key, bin)
        except e.BinIncompatibleType as exception:
            assert exception.code == 12

    def test_list_clear_with_extra_parameter(self):
        """
        Invoke list_clear() with extra parameter.
        """
        key = ('test', 'demo', 1)
        policy = {'timeout': 1000}
        with pytest.raises(TypeError) as typeError:
            TestListClear.client.list_clear(key, "contact_no", {}, policy, "")

        assert "list_clear() takes at most 4 arguments (5 given)" in str(
            typeError.value)

    def test_list_clear_policy_is_string(self):
        """
        Invoke list_clear() with policy is string
        """
        key = ('test', 'demo', 1)
        try:
            TestListClear.client.list_clear(key, "contact_no", {}, "")

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "policy must be a dict"

    def test_list_clear_key_is_none(self):
        """
        Invoke list_clear() with key is none
        """
        try:
            TestListClear.client.list_clear(None, "contact_no")

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "key is invalid"

    def test_list_clear_bin_is_none(self):
        """
        Invoke list_clear() with bin is none
        """
        key = ('test', 'demo', 1)
        try:
            TestListClear.client.list_clear(key, None)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Bin name should be of type string"

    def test_list_clear_meta_type_integer(self):
        """
        Invoke list_clear() with metadata input is of type integer
        """
        key = ('test', 'demo', 1)
        try:
            TestListClear.client.list_clear(key, "contact_no", 888)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Metadata should be of type dictionary"
