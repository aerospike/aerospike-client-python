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


class TestListSet(object):

    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        keys = []
        for i in range(5):
            key = ('test', 'demo', i)
            rec = {'name': 'name%s' %
                   (str(i)),
                   'contact_no': [i, i + 1],
                   'city': ['Pune', 'Dehli']}
            self.as_connection.put(key, rec)
            keys.append(key)
        key = ('test', 'demo', 2)
        self.as_connection.list_append(key, "contact_no", [45, 50, 80])
        keys.append(key)

        def teardown():
            """
            Teardown method.
            """
            for key in keys:
                try:
                    as_connection.remove(key)
                except e.RecordNotFound:
                    pass

        request.addfinalizer(teardown)

    @pytest.mark.parametrize("key, field, index, value, expected", [
        (('test', 'demo', 1),      # list element with integer
            "contact_no",
            5,
            1000,
            {'city': ['Pune', 'Dehli'],
             'contact_no': [1, 2, None,
                            None, None, 1000], 'name': 'name1'}),
        (('test', 'demo', 1),      # list element with list
            "contact_no",
            5,
            [500, 1000],
            {'city': ['Pune', 'Dehli'], 'contact_no': [
                1, 2, None, None, None, [500, 1000]], 'name': 'name1'}),
        (('test', 'demo', 1),      # list element with string
            "contact_no",
            5,
            'string',
            {'city': ['Pune', 'Dehli'], 'contact_no': [
                1, 2, None, None, None, 'string'], 'name': 'name1'}),
        (('test', 'demo', 1),      # float
            "contact_no",
            5,
            45.896,
            {'city': ['Pune', 'Dehli'],
             'contact_no': [1, 2, None,
                            None, None, 45.896], 'name': 'name1'}),
        (('test', 'demo', 1),      # Boolean
            "contact_no",
            5,
            False,
            {'city': ['Pune', 'Dehli'],
             'contact_no': [1, 2, None,
                            None, None, 0], 'name': 'name1'}),
        (('test', 'demo', 1),      # Bytearray
            "contact_no",
            0,
            bytearray("asd;as[d'as;d", "utf-8"),
            {'contact_no': [
                bytearray(b"asd;as[d\'as;d"), 2], 'city': ['Pune', 'Dehli'],
             'name': 'name1'}),
    ])
    def test_pos_list_set_with_elements(
            self, key, field, index, value, expected):
        """
        Invoke list_set() sets
        """
        status = self.as_connection.list_set(key, field, index, value)
        assert status == 0

        key, _, bins = self.as_connection.get(key)
        assert bins == expected

    def test_pos_list_set_with_element_map(self):
        """
        Invoke list_set() sets list element with map
        """
        key = ('test', 'demo', 1)

        status = self.as_connection.list_set(key, "contact_no", 5, {'k1': 56})
        assert status == 0

        key, _, bins = self.as_connection.get(key)
        assert bins == {'city': ['Pune', 'Dehli'],
                        'contact_no': [1, 2, None,
                                       None, None,
                                       {'k1': 56}], 'name': 'name1'}

    def test_pos_list_set_with_correct_policy(self):
        """
        Invoke list_append() append list with correct policy
        """
        key = ('test', 'demo', 2)
        policy = {
            'timeout': 1000,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'commit_level': aerospike.POLICY_COMMIT_LEVEL_MASTER
        }

        status = self.as_connection.list_set(
            key, 'city', 7, 'Mumbai', {}, policy)
        assert status == 0

        key, _, bins = self.as_connection.get(key)
        assert bins == {
            'city': ['Pune', 'Dehli', None, None, None, None, None, 'Mumbai'],
            'contact_no': [2, 3, [45, 50, 80]], 'name': 'name2'}

    def test_pos_list_set_with_nonexistent_key(self):
        """
        Invoke list_set() with non-existent key
        """
        charSet = 'abcdefghijklmnopqrstuvwxyz1234567890'
        minLength = 5
        maxLength = 30
        length = random.randint(minLength, maxLength)
        key = ('test', 'demo', ''.join(map(lambda unused:
                                           random.choice(charSet),
                                           range(length))) + ".com")
        status = self.as_connection.list_set(key, "contact_no", 0, 100)
        assert status == 0
        key, _, bins = self.as_connection.get(key)
        assert bins == {'contact_no': [100]}
        self.as_connection.remove(key)

    # Negative Tests
    def test_neg_list_set_with_no_parameters(self):
        """
        Invoke list_set() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            self.as_connection.list_set()
        assert "argument 'key' (pos 1)" in str(
            typeError.value)

    def test_neg_list_set_with_incorrect_policy(self):
        """
        Invoke list_set() with incorrect policy
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 0.5
        }
        try:
            self.as_connection.list_set(key, "contact_no", 0, 850, {}, policy)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "timeout is invalid"

    def test_neg_list_set_with_nonexistent_bin(self):
        """
        Invoke list_set() with non-existent bin
        """
        key = ('test', 'demo', 1)
        charSet = 'abcdefghijklmnopqrstuvwxyz1234567890'
        minLength = 5
        maxLength = 10
        length = random.randint(minLength, maxLength)
        bin = ''.join(map(lambda unused:
                          random.choice(charSet), range(length))) + ".com"
        try:
            self.as_connection.list_set(key, bin, 0, 75)

        except e.BinIncompatibleType as exception:
            assert exception.code == 12

    def test_neg_list_set_with_extra_parameter(self):
        """
        Invoke list_set() with extra parameter.
        """
        key = ('test', 'demo', 1)
        policy = {'timeout': 1000}
        with pytest.raises(TypeError) as typeError:
            self.as_connection.list_set(
                key, "contact_no", 1, 999, {}, policy, "")

        assert "list_set() takes at most 6 arguments (7 given)" in str(
            typeError.value)

    def test_neg_list_set_policy_is_string(self):
        """
        Invoke list_set() with policy is string
        """
        key = ('test', 'demo', 1)
        try:
            self.as_connection.list_set(key, "contact_no", 1, 30, {}, "")

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "policy must be a dict"

    def test_neg_list_set_key_is_none(self):
        """
        Invoke list_set() with key is none
        """
        try:
            self.as_connection.list_set(None, "contact_no", 0, 89)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "key is invalid"

    def test_neg_list_set_bin_is_none(self):
        """
        Invoke list_set() with bin is none
        """
        key = ('test', 'demo', 1)
        try:
            self.as_connection.list_set(key, None, 1, 555)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Bin name should be of type string"

    def test_neg_list_set_with_negative_index(self):
        """
        Invoke list_set() with negative index
        """
        key = ('test', 'demo', 1)
        try:
            self.as_connection.list_set(key, "contact_no", -56, 12)
        except e.OpNotApplicable as exception:
            assert exception.code == 26

    def test_neg_list_set_meta_type_integer(self):
        """
        Invoke list_set() with metadata input is of type integer
        """
        key = ('test', 'demo', 1)
        try:
            self.as_connection.list_set(key, "contact_no", 0, 679, 888)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Metadata should be of type dictionary"

    def test_neg_list_set_index_type_string(self):
        """
        Invoke list_set() with index is of type string
        """
        key = ('test', 'demo', 1)

        with pytest.raises(TypeError) as typeError:
            self.as_connection.list_set(key, "contact_no", "Fifth", 448)
        assert "an integer is required" in str(typeError.value)
