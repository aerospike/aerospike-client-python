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


class TestListAppend(object):

    def setup_class(cls):
        """
        Setup method.
        """
        hostlist, user, password = TestBaseClass.get_hosts()
        config = {'hosts': hostlist}
        if user is None and password is None:
            TestListAppend.client = aerospike.client(config).connect()
        else:
            TestListAppend.client = aerospike.client(
                config).connect(user, password)

    def teardown_class(cls):
        TestListAppend.client.close()

    def setup_method(self, method):
        for i in range(5):
            key = ('test', 'demo', i)
            rec = {'name': 'name%s' %
                   (str(i)),
                   'contact_no': [i, i + 1], 'city': ['Pune', 'Dehli']}
            TestListAppend.client.put(key, rec)

    def teardown_method(self, method):
        """
        Teardoen method.
        """
        # time.sleep(1)
        for i in range(5):
            key = ('test', 'demo', i)
            TestListAppend.client.remove(key)

    def test_list_append_integer(self):
        """
        Invoke list_append() append integer value to a list
        """
        key = ('test', 'demo', 1)
        TestListAppend.client.list_append(key, "contact_no", 50000)

        (key, _, bins) = TestListAppend.client.get(key)

        assert bins == {
            'contact_no': [1, 2, 50000],
            'name': 'name1', 'city': ['Pune', 'Dehli']}

    def test_list_append_string(self):
        """
        Invoke list_append() append value to a list
        """
        key = ('test', 'demo', 1)
        TestListAppend.client.list_append(key, "city", "Chennai")

        (key, _, bins) = TestListAppend.client.get(key)

        assert bins == {
            'contact_no': [1, 2], 'name': 'name1',
            'city': ['Pune', 'Dehli', 'Chennai']}

    def test_list_append_unicode_string(self):
        """
        Invoke list_append() append unicode string
        """
        key = ('test', 'demo', 1)
        TestListAppend.client.list_append(key, "city", u"Mumbai")

        key, _, bins = TestListAppend.client.get(key)
        assert bins == {'contact_no': [1, 2], 'city': [
            'Pune', 'Dehli', u'Mumbai'], 'name': 'name1'}

    def test_list_append_list_with_correct_policy(self):
        """
        Invoke list_append() append list with correct policy options
        """
        key = ('test', 'demo', 2)
        policy = {
            'timeout': 1000,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'commit_level': aerospike.POLICY_COMMIT_LEVEL_MASTER
        }
        TestListAppend.client.list_append(
            key, "contact_no", [45, 50, 80], {}, policy)

        (key, _, bins) = TestListAppend.client.get(key)

        assert bins == {
            'contact_no': [2, 3, [45, 50, 80]],
            'city': ['Pune', 'Dehli'], 'name': 'name2'}

    def test_list_append_float(self):
        """
        Invoke list_append() append float into the list
        """
        key = ('test', 'demo', 2)
        TestListAppend.client.list_append(key, "contact_no", 85.12)

        (key, _, bins) = TestListAppend.client.get(key)

        assert bins == {
            'contact_no': [2, 3, 85.12],
            'city': ['Pune', 'Dehli'], 'name': 'name2'}

    def test_list_append_map(self):
        """
        Invoke list_append() append map into the list
        """
        key = ('test', 'demo', 3)

        TestListAppend.client.list_append(key, "contact_no", {'k1': 29})

        (key, _, bins) = TestListAppend.client.get(key)

        assert bins == {
            'contact_no': [3, 4, {'k1': 29}],
            'city': ['Pune', 'Dehli'], 'name': 'name3'}

    def test_list_append_bytearray(self):
        """
        Invoke list_append() append bytearray into the list
        """
        key = ('test', 'demo', 1)

        TestListAppend.client.list_append(
            key, "contact_no", bytearray("asd;as[d'as;d", "utf-8"))

        (key, _, bins) = TestListAppend.client.get(key)

        assert bins == {'contact_no': [
            1, 2, bytearray(b"asd;as[d\'as;d")],
            'city': ['Pune', 'Dehli'], 'name': 'name1'}

    def test_list_append_boolean(self):
        """
        Invoke list_append() append boolean into the list
        """
        key = ('test', 'demo', 1)

        TestListAppend.client.list_append(key, "contact_no", False)

        (key, _, bins) = TestListAppend.client.get(key)

        assert bins == {
            'contact_no': [1, 2, 0],
            'city': ['Pune', 'Dehli'], 'name': 'name1'}

    def test_list_append_with_nonexistent_key(self):
        """
        Invoke list_append() with non-existent key
        """
        charSet = 'abcdefghijklmnopqrstuvwxyz1234567890'
        minLength = 5
        maxLength = 30
        length = random.randint(minLength, maxLength)
        key = ('test', 'demo', ''.join(map(lambda unused:
                                           random.choice(charSet),
                                           range(length))) + ".com")
        status = TestListAppend.client.list_append(key, "abc", 122)
        assert status == 0

        (key, _, bins) = TestListAppend.client.get(key)
        TestListAppend.client.remove(key)

        assert status == 0
        assert bins == {'abc': [122]}

    def test_list_append_with_nonexistent_bin(self):
        """
        Invoke list_append() with non-existent bin
        """
        key = ('test', 'demo', 1)
        charSet = 'abcdefghijklmnopqrstuvwxyz1234567890'
        minLength = 5
        maxLength = 10
        length = random.randint(minLength, maxLength)
        bin = ''.join(map(lambda unused:
                          random.choice(charSet), range(length))) + ".com"
        status = TestListAppend.client.list_append(key, bin, 585)
        assert status == 0

        (key, _, bins) = TestListAppend.client.get(key)

        assert status == 0
        assert bins == {'contact_no': [1, 2], 'name': 'name1',
                        'city': ['Pune', 'Dehli'], bin: [585]}

    def test_list_append_with_no_parameters(self):
        """
        Invoke list_append() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            TestListAppend.client.list_append()
        assert "Required argument 'key' (pos 1) not found" in str(
            typeError.value)

    def test_list_append_with_incorrect_policy(self):
        """
        Invoke list_append() with incorrect policy
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 0.5
        }
        try:
            TestListAppend.client.list_append(
                key, "contact_no", "str", {}, policy)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "timeout is invalid"

    def test_list_append_with_extra_parameter(self):
        """
        Invoke list_append() with extra parameter.
        """
        key = ('test', 'demo', 1)
        policy = {'timeout': 1000}
        with pytest.raises(TypeError) as typeError:
            TestListAppend.client.list_append(
                key, "contact_no", 999, {}, policy, "")

        assert "list_append() takes at most 5 arguments (6 given)" in str(
            typeError.value)

    def test_list_append_policy_is_string(self):
        """
        Invoke list_append() with policy is string
        """
        key = ('test', 'demo', 1)
        try:
            TestListAppend.client.list_append(key, "contact_no", 85, {}, "")

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "policy must be a dict"

    def test_list_append_key_is_none(self):
        """
        Invoke list_append() with key is none
        """
        try:
            TestListAppend.client.list_append(None, "contact_no", 45)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "key is invalid"

    def test_list_append_bin_is_none(self):
        """
        Invoke list_append() with bin is none
        """
        key = ('test', 'demo', 1)
        try:
            TestListAppend.client.list_append(key, None, "str")

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Bin name should be of type string"

    def test_list_append_meta_type_integer(self):
        """
        Invoke list_append() with metadata input is of type integer
        """
        key = ('test', 'demo', 1)
        try:
            TestListAppend.client.list_append(key, "contact_no", 85, 888)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Metadata should be of type dictionary"
