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


class TestListExtend(object):

    def setup_class(cls):
        """
        Setup method.
        """
        hostlist, user, password = TestBaseClass.get_hosts()
        config = {'hosts': hostlist}
        if user is None and password is None:
            TestListExtend.client = aerospike.client(config).connect()
        else:
            TestListExtend.client = aerospike.client(
                config).connect(user, password)

    def teardown_class(cls):
        TestListExtend.client.close()

    def setup_method(self, method):
        for i in range(5):
            key = ('test', 'demo', i)
            rec = {'name': 'name%s' %
                   (str(i)), 'contact_no': [i, i + 1],
                   'city': ['Pune', 'Dehli']}
            TestListExtend.client.put(key, rec)

    def teardown_method(self, method):
        """
        Teardoen method.
        """
        # time.sleep(1)
        for i in range(5):
            key = ('test', 'demo', i)
            TestListExtend.client.remove(key)

    def test_list_extend_with_list_of_integers(self):
        """
        Invoke list_extend() extend the list with integer values
        """
        key = ('test', 'demo', 1)
        TestListExtend.client.list_extend(key, "contact_no", [12, 56, 89])

        (key, _, bins) = TestListExtend.client.get(key)

        assert bins == {
            'contact_no': [1, 2, 12, 56, 89], 'name': 'name1',
            'city': ['Pune', 'Dehli']}

    def test_list_extend_with_policy(self):
        """
        Invoke list_extend() extend the list with integer values and
        policy is passed
        """
        key = ('test', 'demo', 1)

        policy = {
            'timeout': 1000,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'commit_level': aerospike.POLICY_COMMIT_LEVEL_MASTER
        }

        TestListExtend.client.list_extend(
            key, "contact_no", [12, 56, 89], {}, policy)

        (key, _, bins) = TestListExtend.client.get(key)

        assert bins == {
            'contact_no': [1, 2, 12, 56, 89], 'name': 'name1',
            'city': ['Pune', 'Dehli']}

    def test_list_extend_with_floats(self):
        """
        Invoke list_extend() extend the list with float values
        """
        key = ('test', 'demo', 2)

        TestListExtend.client.list_extend(key, "contact_no", [85.12, 85.46])

        (key, _, bins) = TestListExtend.client.get(key)

        assert bins == {
            'contact_no': [2, 3, 85.12, 85.46], 'city': ['Pune', 'Dehli'],
            'name': 'name2'}

    def test_list_extend_with_all_entries(self):
        """
        Invoke list_extend() extend with all type entries
        """
        key = ('test', 'demo', 1)

        TestListExtend.client.list_extend(
            key, "contact_no", [False, [789, 45], 88, 15.2, 'aa'])

        (key, _, bins) = TestListExtend.client.get(key)

        assert bins == {'contact_no': [1, 2, 0, [789, 45], 88, 15.2, 'aa'],
                        'city': ['Pune', 'Dehli'], 'name': 'name1'}

    def test_list_extend_with_nonexistent_key(self):
        """
        Invoke list_extend() with non-existent key
        """
        charSet = 'abcdefghijklmnopqrstuvwxyz1234567890'
        minLength = 5
        maxLength = 30
        length = random.randint(minLength, maxLength)
        key = ('test', 'demo', ''.join(map(lambda unused:
                                           random.choice(charSet),
                                           range(length))) + ".com")
        status = TestListExtend.client.list_extend(key, "abc", [122, 789])
        assert status == 0

        (key, _, bins) = TestListExtend.client.get(key)

        assert status == 0
        assert bins == {'abc': [122, 789]}

        TestListExtend.client.remove(key)

    def test_list_extend_with_nonexistent_bin(self):
        """
        Invoke list_extend() with non-existent bin
        """
        key = ('test', 'demo', 1)
        charSet = 'abcdefghijklmnopqrstuvwxyz1234567890'
        minLength = 5
        maxLength = 10
        length = random.randint(minLength, maxLength)
        bin = ''.join(map(lambda unused:
                          random.choice(charSet), range(length))) + ".com"
        status = TestListExtend.client.list_extend(key, bin, [585, 789, 45])
        assert status == 0

        (key, _, bins) = TestListExtend.client.get(key)

        assert status == 0
        assert bins == {'contact_no': [1, 2], 'name': 'name1',
                        'city': ['Pune', 'Dehli'], bin: [585, 789, 45]}

    def test_list_extend_with_no_parameters(self):
        """
        Invoke list_extend() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            TestListExtend.client.list_extend()
        assert "Required argument 'key' (pos 1) not found" in str(
            typeError.value)

    def test_list_extend_with_incorrect_policy(self):
        """
        Invoke list_extend() with incorrect policy
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 0.5
        }
        try:
            TestListExtend.client.list_extend(
                key, "contact_no", ["str"], {}, policy)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "timeout is invalid"

    def test_list_extend_with_extra_parameter(self):
        """
        Invoke list_extend() with extra parameter.
        """
        key = ('test', 'demo', 1)
        policy = {'timeout': 1000}
        with pytest.raises(TypeError) as typeError:
            TestListExtend.client.list_extend(
                key, "contact_no", [999], {}, policy, "")

        assert "list_extend() takes at most 5 arguments (6 given)" in str(
            typeError.value)

    def test_list_extend_policy_is_string(self):
        """
        Invoke list_extend() with policy is string
        """
        key = ('test', 'demo', 1)
        try:
            TestListExtend.client.list_extend(key, "contact_no", [85], {}, "")

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "policy must be a dict"

    def test_list_extend_key_is_none(self):
        """
        Invoke list_extend() with key is none
        """
        try:
            TestListExtend.client.list_extend(None, "contact_no", [45])

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "key is invalid"

    def test_list_extend_bin_is_none(self):
        """
        Invoke list_extend() with bin is none
        """
        key = ('test', 'demo', 1)
        try:
            TestListExtend.client.list_extend(key, None, ["str"])

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Bin name should be of type string"

    def test_list_extend_with_string_instead_of_list(self):
        """
        Invoke list_extend() with string is passed in place of list
        """
        key = ('test', 'demo', 1)
        try:
            TestListExtend.client.list_extend(key, "contact_no", "str")

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Items should be of type list"

    def test_list_extend_meta_type_integer(self):
        """
        Invoke list_extend() with metadata input is of type integer
        """
        key = ('test', 'demo', 1)
        try:
            TestListExtend.client.list_extend(key, "contact_no", [85], 888)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Metadata should be of type dictionary"
