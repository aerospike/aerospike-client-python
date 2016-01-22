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


class TestListTrim(object):

    def setup_class(cls):
        """
        Setup method.
        """
        hostlist, user, password = TestBaseClass.get_hosts()
        config = {'hosts': hostlist}
        if user is None and password is None:
            TestListTrim.client = aerospike.client(config).connect()
        else:
            TestListTrim.client = aerospike.client(
                config).connect(user, password)

    def teardown_class(cls):
        TestListTrim.client.close()

    def setup_method(self, method):
        for i in range(5):
            key = ('test', 'demo', i)
            rec = {'name': 'name%s' % (str(i)),
                   'contact_no': [i, i + 1, i + 2, i + 3,
                                  i + 4, i + 5],
                   'city': ['Pune', 'Dehli', 'Mumbai']}
            TestListTrim.client.put(key, rec)
        key = ('test', 'demo', 1)
        TestListTrim.client.list_append(key, "contact_no", [45, 50, 80])

    def teardown_method(self, method):
        """
        Teardown method.
        """
        for i in range(5):
            key = ('test', 'demo', i)
            TestListTrim.client.remove(key)

    def test_list_trim_with_correct_paramters(self):
        """
        Invoke list_trim() removes elements from list with correct parameters
        """
        key = ('test', 'demo', 1)

        status = TestListTrim.client.list_trim(key, "contact_no", 3, 4)
        assert status == 0
        # assert list == [4, 5, 6, [45, 50, 80]]

        (key, _, bins) = TestListTrim.client.get(key)
        assert bins == {'city': ['Pune', 'Dehli', 'Mumbai'], 'contact_no': [
            4, 5, 6, [45, 50, 80]], 'name': 'name1'}

    def test_list_trim_with_correct_policy(self):
        """
        Invoke list_trim() removes elements from list with correct policy
        """
        key = ('test', 'demo', 2)
        policy = {
            'timeout': 1000,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'commit_level': aerospike.POLICY_COMMIT_LEVEL_MASTER
        }

        status = TestListTrim.client.list_trim(
            key, 'contact_no', 0, 3, {}, policy)
        assert status == 0
        # assert list == [2, 3, 4]

        (key, _, bins) = TestListTrim.client.get(key)
        assert bins == {
            'city': ['Pune', 'Dehli', 'Mumbai'], 'contact_no': [2, 3, 4],
            'name': 'name2'}

    def test_list_trim_with_no_parameters(self):
        """
        Invoke list_trim() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            TestListTrim.client.list_trim()
        assert "Required argument 'key' (pos 1) not found" in str(
            typeError.value)

    def test_list_trim_with_incorrect_policy(self):
        """
        Invoke list_trim() with incorrect policy
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 0.5
        }
        try:
            TestListTrim.client.list_trim(key, "contact_no", 0, 2, {}, policy)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "timeout is invalid"

    def test_list_trim_with_nonexistent_key(self):
        """
        Invoke list_trim() with non-existent key
        """
        charSet = 'abcdefghijklmnopqrstuvwxyz1234567890'
        minLength = 5
        maxLength = 30
        length = random.randint(minLength, maxLength)
        key = ('test', 'demo', ''.join(map(lambda unused:
                                           random.choice(charSet),
                                           range(length))) + ".com")
        try:
            TestListTrim.client.list_trim(key, "abc", 0, 1)

        except e.BinIncompatibleType as exception:
            assert exception.code == 12

    def test_list_trim_with_nonexistent_bin(self):
        """
        Invoke list_trim() with non-existent bin
        """
        key = ('test', 'demo', 1)
        charSet = 'abcdefghijklmnopqrstuvwxyz1234567890'
        minLength = 5
        maxLength = 10
        length = random.randint(minLength, maxLength)
        bin = ''.join(map(lambda unused:
                          random.choice(charSet), range(length))) + ".com"
        try:
            TestListTrim.client.list_trim(key, bin, 0, 1)

        except e.BinIncompatibleType as exception:
            assert exception.code == 12

    def test_list_trim_with_extra_parameter(self):
        """
        Invoke list_trim() with extra parameter.
        """
        key = ('test', 'demo', 1)
        policy = {'timeout': 1000}
        with pytest.raises(TypeError) as typeError:
            TestListTrim.client.list_trim(
                key, "contact_no", 1, 1, {}, policy, "")

        assert "list_trim() takes at most 6 arguments (7 given)" in str(
            typeError.value)

    def test_list_trim_policy_is_string(self):
        """
        Invoke list_trim() with policy is string
        """
        key = ('test', 'demo', 1)
        try:
            TestListTrim.client.list_trim(key, "contact_no", 0, 1, {}, "")

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "policy must be a dict"

    def test_list_trim_key_is_none(self):
        """
        Invoke list_trim() with key is none
        """
        try:
            TestListTrim.client.list_trim(None, "contact_no", 0, 2)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "key is invalid"

    def test_list_trim_bin_is_none(self):
        """
        Invoke list_trim() with bin is none
        """
        key = ('test', 'demo', 1)
        try:
            TestListTrim.client.list_trim(key, None, 1, 3)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Bin name should be of type string"

    def test_list_trim_with_negative_index(self):
        """
        Invoke list_trim() with negative index
        """
        key = ('test', 'demo', 1)
        try:
            TestListTrim.client.list_trim(key, "contact_no", -56, 5)
        except e.InvalidRequest as exception:
            assert exception.code == 4

    def test_list_trim_meta_type_integer(self):
        """
        Invoke list_trim() with metadata input is of type integer
        """
        key = ('test', 'demo', 1)
        try:
            TestListTrim.client.list_trim(key, "contact_no", 0, 2, 888)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Metadata should be of type dictionary"

    def test_list_trim_index_type_string(self):
        """
        Invoke list_trim() with index is of type string
        """
        key = ('test', 'demo', 1)

        with pytest.raises(TypeError) as typeError:
            TestListTrim.client.list_trim(key, "contact_no", "Fifth", 2)
        assert "an integer is required" in str(typeError.value)
