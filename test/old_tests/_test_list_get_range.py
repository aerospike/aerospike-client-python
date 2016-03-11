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


class TestListGetRange(object):

    def setup_class(cls):
        """
        Setup method.
        """
        hostlist, user, password = TestBaseClass.get_hosts()
        config = {'hosts': hostlist}
        if user is None and password is None:
            TestListGetRange.client = aerospike.client(config).connect()
        else:
            TestListGetRange.client = aerospike.client(
                config).connect(user, password)

    def teardown_class(cls):
        TestListGetRange.client.close()

    def setup_method(self, method):
        for i in range(5):
            key = ('test', 'demo', i)
            rec = {'name': 'name%s' % (str(i)), 'contact_no': [
                i, i + 1, i + 2, i + 3, i + 4, i + 5],
                'city': ['Pune', 'Dehli']}
            TestListGetRange.client.put(key, rec)
        key = ('test', 'demo', 1)
        TestListGetRange.client.list_append(key, "contact_no", [45, 50, 80])

    def teardown_method(self, method):
        """
        Teardown method.
        """
        # time.sleep(1)
        for i in range(5):
            key = ('test', 'demo', i)
            TestListGetRange.client.remove(key)

    def test_list_get_range_with_correct_paramters(self):
        """
        Invoke list_get_range() get back elements from the list with
        correct parameters
        """
        key = ('test', 'demo', 1)

        bins = TestListGetRange.client.list_get_range(key, "contact_no", 4, 3)

        assert bins == [5, 6, [45, 50, 80]]

    def test_list_get_range_with_correct_policy(self):
        """
        Invoke list_get_range() get back elements from the list with
        correct policy
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'commit_level': aerospike.POLICY_COMMIT_LEVEL_MASTER
        }

        bins = TestListGetRange.client.list_get_range(
            key, 'city', 0, 1, {}, policy)

        assert bins == ['Pune']

    def test_list_get_range_with_no_parameters(self):
        """
        Invoke list_get_range() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            TestListGetRange.client.list_get_range()
        assert "Required argument 'key' (pos 1) not found" in str(
            typeError.value)

    def test_list_get_range_with_incorrect_policy(self):
        """
        Invoke list_get_range() with incorrect policy
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 0.5
        }
        try:
            TestListGetRange.client.list_get_range(
                key, "contact_no", 0, 2, {}, policy)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "timeout is invalid"

    def test_list_get_range_with_nonexistent_key(self):
        """
        Invoke list_get_range() with non-existent key
        """
        charSet = 'abcdefghijklmnopqrstuvwxyz1234567890'
        minLength = 5
        maxLength = 30
        length = random.randint(minLength, maxLength)
        key = ('test', 'demo', ''.join(map(lambda unused:
                                           random.choice(charSet),
                                           range(length))) + ".com")
        try:
            TestListGetRange.client.list_get_range(key, "abc", 0, 1)

        except e.RecordNotFound as exception:
            assert exception.code == 2

    def test_list_get_range_with_nonexistent_bin(self):
        """
        Invoke list_get_range() with non-existent bin
        """
        key = ('test', 'demo', 1)
        charSet = 'abcdefghijklmnopqrstuvwxyz1234567890'
        minLength = 5
        maxLength = 10
        length = random.randint(minLength, maxLength)
        bin = ''.join(map(lambda unused:
                          random.choice(charSet), range(length))) + ".com"
        bins = TestListGetRange.client.list_get_range(key, bin, 0, 1)
        assert [] == bins

    def test_list_get_range_with_extra_parameter(self):
        """
        Invoke list_get_range() with extra parameter.
        """
        key = ('test', 'demo', 1)
        policy = {'timeout': 1000}
        with pytest.raises(TypeError) as typeError:
            TestListGetRange.client.list_get_range(
                key, "contact_no", 1, 1, {}, policy, "")

        assert "list_get_range() takes at most 6 arguments (7 given)" in str(
            typeError.value)

    def test_list_get_range_policy_is_string(self):
        """
        Invoke list_get_range() with policy is string
        """
        key = ('test', 'demo', 1)
        try:
            TestListGetRange.client.list_get_range(
                key, "contact_no", 0, 1, {}, "")

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "policy must be a dict"

    def test_list_get_range_key_is_none(self):
        """
        Invoke list_get_range() with key is none
        """
        try:
            TestListGetRange.client.list_get_range(None, "contact_no", 0, 2)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "key is invalid"

    def test_list_get_range_bin_is_none(self):
        """
        Invoke list_get_range() with bin is none
        """
        key = ('test', 'demo', 1)
        try:
            TestListGetRange.client.list_get_range(key, None, 1, 3)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Bin name should be of type string"

    def test_list_get_range_with_negative_index(self):
        """
        Invoke list_get_range() with negative index
        """
        key = ('test', 'demo', 1)
        try:
            TestListGetRange.client.list_get_range(key, "contact_no", -56, 5)
        except e.InvalidRequest as exception:
            assert exception.code == 4

    def test_list_get_range_with_negative_length(self):
        """
        Invoke list_get_range() with negative count
        """
        key = ('test', 'demo', 1)
        try:
            TestListGetRange.client.list_get_range(key, "contact_no", 0, -59)
        except e.InvalidRequest as exception:
            assert exception.code == 4

    def test_list_get_range_meta_type_integer(self):
        """
        Invoke list_get_range() with metadata input is of type integer
        """
        key = ('test', 'demo', 1)
        try:
            TestListGetRange.client.list_get_range(
                key, "contact_no", 0, 2, 888)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Metadata should be of type dictionary"

    def test_list_get_range_index_type_string(self):
        """
        Invoke list_get_range() with index is of type string
        """
        key = ('test', 'demo', 1)

        with pytest.raises(TypeError) as typeError:
            TestListGetRange.client.list_get_range(
                key, "contact_no", "Fifth", 2)
        assert "an integer is required" in str(typeError.value)
