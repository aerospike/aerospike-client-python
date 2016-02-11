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


class TestListInsert(object):

    def setup_class(cls):
        """
        Setup method.
        """
        hostlist, user, password = TestBaseClass.get_hosts()
        config = {'hosts': hostlist}
        if user is None and password is None:
            TestListInsert.client = aerospike.client(config).connect()
        else:
            TestListInsert.client = aerospike.client(
                config).connect(user, password)

    def teardown_class(cls):
        TestListInsert.client.close()

    def setup_method(self, method):
        for i in range(5):
            key = ('test', 'demo', i)
            rec = {'name': 'name%s' %
                   (str(i)), 'age': [i, i + 1], 'city': ['Pune', 'Dehli']}
            TestListInsert.client.put(key, rec)
        key = ('test', 'demo', 'bytearray_key')
        TestListInsert.client.put(
            key, {"bytearray_bin": bytearray("asd;as[d'as;d", "utf-8")})

    def teardown_method(self, method):
        """
        Teardoen method.
        """
        # time.sleep(1)
        for i in range(5):
            key = ('test', 'demo', i)
            TestListInsert.client.remove(key)
        key = ('test', 'demo', 'bytearray_key')
        TestListInsert.client.remove(key)

    def test_list_insert_integer(self):
        """
        Invoke list_insert() insert integer value with correct parameters
        """
        key = ('test', 'demo', 1)
        TestListInsert.client.list_insert(key, "age", 0, 999)

        (key, _, bins) = TestListInsert.client.get(key)

        assert bins == {
            'age': [999, 1, 2], 'name': 'name1', 'city': ['Pune', 'Dehli']}

    def test_list_insert_string(self):
        """
        Invoke list_insert() inserts string with correct parameters
        """
        key = ('test', 'demo', 1)
        TestListInsert.client.list_insert(key, "city", 0, "Chennai")

        (key, _, bins) = TestListInsert.client.get(key)

        assert bins == {
            'age': [1, 2], 'name': 'name1',
            'city': ['Chennai', 'Pune', 'Dehli']}

    def test_list_insert_unicode_string(self):
        """
        Invoke list_insert() inserts unicode string
        """
        key = ('test', 'demo', 1)
        TestListInsert.client.list_insert(key, "city", 3, u"Mumbai")

        key, _, bins = TestListInsert.client.get(key)
        assert bins == {
            'age': [1, 2], 'city': ['Pune', 'Dehli', None, u'Mumbai'],
            'name': 'name1'}

    def test_list_insert_list_with_correct_policy(self):
        """
        Invoke list_insert() inserts list with correct policy
        """
        key = ('test', 'demo', 2)
        policy = {
            'timeout': 1000,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'commit_level': aerospike.POLICY_COMMIT_LEVEL_MASTER
        }
        TestListInsert.client.list_insert(
            key, "age", 5, [45, 50, 80], {}, policy)

        (key, _, bins) = TestListInsert.client.get(key)

        assert bins == {'age': [2, 3, None, None, None, [45, 50, 80]],
                        'city': ['Pune', 'Dehli'], 'name': 'name2'}

    def test_list_insert_float(self):
        """
        Invoke list_insert() insert float into the list
        """
        key = ('test', 'demo', 2)
        TestListInsert.client.list_insert(key, "age", 7, 85.12)

        (key, _, bins) = TestListInsert.client.get(key)

        assert bins == {'age': [2, 3, None, None, None, None, None, 85.12],
                        'city': ['Pune', 'Dehli'], 'name': 'name2'}

    def test_list_insert_map(self):
        """
        Invoke list_insert() insert map into the list
        """
        key = ('test', 'demo', 3)

        TestListInsert.client.list_insert(key, "age", 1, {'k1': 29})

        (key, _, bins) = TestListInsert.client.get(key)

        assert bins == {
            'age': [3, {'k1': 29}, 4], 'city': ['Pune', 'Dehli'],
            'name': 'name3'}

    def test_list_insert_bytearray(self):
        """
        Invoke list_insert() insert bytearray into the list
        """
        key = ('test', 'demo', 1)

        TestListInsert.client.list_insert(
            key, "age", 2, bytearray("asd;as[d'as;d", "utf-8"))

        (key, _, bins) = TestListInsert.client.get(key)

        assert bins == {'age': [
            1, 2, bytearray(b"asd;as[d\'as;d")], 'city': ['Pune', 'Dehli'],
            'name': 'name1'}

    def test_list_insert_boolean(self):
        """
        Invoke list_insert() insert boolean into the list
        """
        key = ('test', 'demo', 1)

        TestListInsert.client.list_insert(key, "age", 6, False)

        (key, _, bins) = TestListInsert.client.get(key)

        assert bins == {'age': [1, 2, None, None, None, None, 0], 'city': [
            'Pune', 'Dehli'], 'name': 'name1'}

    def test_list_insert_with_nonexistent_key(self):
        """
        Invoke list_insert() with non-existent key
        """
        charSet = 'abcdefghijklmnopqrstuvwxyz1234567890'
        minLength = 5
        maxLength = 30
        length = random.randint(minLength, maxLength)
        key = ('test', 'demo', ''.join(map(lambda unused:
                                           random.choice(charSet),
                                           range(length))) + ".com")
        status = TestListInsert.client.list_insert(key, "abc", 2, 122)
        assert status == 0

        (key, _, bins) = TestListInsert.client.get(key)

        assert status == 0
        assert bins == {'abc': [None, None, 122]}

        TestListInsert.client.remove(key)

    def test_list_insert_with_nonexistent_bin(self):
        """
        Invoke list_insert() with non-existent bin
        """
        key = ('test', 'demo', 1)
        charSet = 'abcdefghijklmnopqrstuvwxyz1234567890'
        minLength = 5
        maxLength = 10
        length = random.randint(minLength, maxLength)
        bin = ''.join(map(lambda unused:
                          random.choice(charSet), range(length))) + ".com"
        status = TestListInsert.client.list_insert(key, bin, 3, 585)
        assert status == 0

        (key, _, bins) = TestListInsert.client.get(key)
        assert status == 0

        assert bins == {'age': [1, 2], 'name': 'name1',
                        'city': ['Pune', 'Dehli'],
                        bin: [None, None, None, 585]}

    def test_list_insert_with_no_parameters(self):
        """
        Invoke list_insert() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            TestListInsert.client.list_insert()
        assert "Required argument 'key' (pos 1) not found" in str(
            typeError.value)

    def test_list_insert_with_incorrect_policy(self):
        """
        Invoke list_insert() with incorrect policy
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 0.5
        }
        try:
            TestListInsert.client.list_insert(key, "age", 6, "str", {}, policy)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "timeout is invalid"

    def test_list_insert_with_extra_parameter(self):
        """
        Invoke list_insert() with extra parameter.
        """
        key = ('test', 'demo', 1)
        policy = {'timeout': 1000}
        with pytest.raises(TypeError) as typeError:
            TestListInsert.client.list_insert(
                key, "age", 3, 999, {}, policy, "")

        assert "list_insert() takes at most 6 arguments (7 given)" in str(
            typeError.value)

    def test_list_insert_policy_is_string(self):
        """
        Invoke list_insert() with policy is string
        """
        key = ('test', 'demo', 1)
        try:
            TestListInsert.client.list_insert(key, "age", 1, 85, {}, "")

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "policy must be a dict"

    def test_list_insert_key_is_none(self):
        """
        Invoke list_insert() with key is none
        """
        try:
            TestListInsert.client.list_insert(None, "age", 1, 45)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "key is invalid"

    def test_list_insert_bin_is_none(self):
        """
        Invoke list_insert() with bin is none
        """
        key = ('test', 'demo', 1)
        try:
            TestListInsert.client.list_insert(key, None, 2, "str")

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Bin name should be of type string"

    def test_list_insert_meta_type_integer(self):
        """
        Invoke list_insert() with metadata input is of type integer
        """
        key = ('test', 'demo', 1)
        try:
            TestListInsert.client.list_insert(key, "contact_no", 1, 85, 888)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Metadata should be of type dictionary"

    def test_list_insert_index_negative(self):
        """
        Invoke list_insert() insert with index is negative integer
        """
        key = ('test', 'demo', 1)

        try:
            TestListInsert.client.list_insert(key, "age", -6, False)
        except e.InvalidRequest as exception:
            assert exception.code == 4
            assert exception.msg == 'AEROSPIKE_ERR_REQUEST_INVALID'

    def test_list_insert_index_type_string(self):
        """
        Invoke list_insert() insert with index is of type string
        """
        key = ('test', 'demo', 1)

        with pytest.raises(TypeError) as typeError:
            TestListInsert.client.list_insert(key, "age", "Fifth", False)
        assert "an integer is required" in str(typeError.value)
