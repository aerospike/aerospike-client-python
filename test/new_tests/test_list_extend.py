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

    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        keys = []
        for i in range(5):
            key = ('test', 'demo', i)
            rec = {'name': 'name%s' %
                   (str(i)), 'contact_no': [i, i + 1],
                   'city': ['Pune', 'Dehli']}
            as_connection.put(key, rec)
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

    @pytest.mark.parametrize("key, field, extend_value, expected", [
        (('test', 'demo', 1),       # extend the list with integer values
            "contact_no",
            [12, 56, 89],
            {'contact_no': [1, 2, 12, 56, 89], 'name': 'name1',
             'city': ['Pune', 'Dehli']}),
        (('test', 'demo', 2),                      # with float values
            "contact_no",
            [85.12, 85.46],
            {'contact_no': [2, 3, 85.12, 85.46], 'city': ['Pune', 'Dehli'],
             'name': 'name2'}),
        (('test', 'demo', 1),                       # all values
            "contact_no",
            [False, [789, 45], 88, 15.2, 'aa'],
            {'contact_no': [1, 2, 0, [789, 45], 88, 15.2, 'aa'],
             'city': ['Pune', 'Dehli'], 'name': 'name1'}),

    ])
    def test_pos_list_extend_with_list_of_values(self,
                                                 key,
                                                 field,
                                                 extend_value,
                                                 expected):
        """
        Invoke list_extend() extend the list with values
        """
        self.as_connection.list_extend(key, field, extend_value)
        (key, _, bins) = self.as_connection.get(key)
        assert bins == expected

    def test_pos_list_extend_with_policy(self):
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

        self.as_connection.list_extend(
            key, "contact_no", [12, 56, 89], {}, policy)

        (key, _, bins) = self.as_connection.get(key)

        assert bins == {
            'contact_no': [1, 2, 12, 56, 89], 'name': 'name1',
            'city': ['Pune', 'Dehli']}

    def test_pos_list_extend_with_nonexistent_key(self):
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
        status = self.as_connection.list_extend(key, "abc", [122, 789])
        assert status == 0

        (key, _, bins) = self.as_connection.get(key)

        assert status == 0
        assert bins == {'abc': [122, 789]}

        self.as_connection.remove(key)

    def test_pos_list_extend_with_nonexistent_bin(self):
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
        status = self.as_connection.list_extend(key, bin, [585, 789, 45])
        assert status == 0

        (key, _, bins) = self.as_connection.get(key)

        assert status == 0
        assert bins == {'contact_no': [1, 2], 'name': 'name1',
                        'city': ['Pune', 'Dehli'], bin: [585, 789, 45]}

    # Negative Tests
    def test_neg_list_extend_with_no_parameters(self):
        """
        Invoke list_extend() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            self.as_connection.list_extend()
        assert "argument 'key' (pos 1)" in str(
            typeError.value)

    def test_neg_list_extend_with_incorrect_policy(self):
        """
        Invoke list_extend() with incorrect policy
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 0.5
        }
        try:
            self.as_connection.list_extend(
                key, "contact_no", ["str"], {}, policy)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "timeout is invalid"

    def test_neg_list_extend_with_extra_parameter(self):
        """
        Invoke list_extend() with extra parameter.
        """
        key = ('test', 'demo', 1)
        policy = {'timeout': 1000}
        with pytest.raises(TypeError) as typeError:
            self.as_connection.list_extend(
                key, "contact_no", [999], {}, policy, "")

        assert "list_extend() takes at most 5 arguments (6 given)" in str(
            typeError.value)

    def test_neg_list_extend_policy_is_string(self):
        """
        Invoke list_extend() with policy is string
        """
        key = ('test', 'demo', 1)
        try:
            self.as_connection.list_extend(key, "contact_no", [85], {}, "")

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "policy must be a dict"

    def test_neg_list_extend_key_is_none(self):
        """
        Invoke list_extend() with key is none
        """
        try:
            self.as_connection.list_extend(None, "contact_no", [45])

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "key is invalid"

    def test_neg_list_extend_bin_is_none(self):
        """
        Invoke list_extend() with bin is none
        """
        key = ('test', 'demo', 1)
        try:
            self.as_connection.list_extend(key, None, ["str"])

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Bin name should be of type string"

    def test_neg_list_extend_with_string_instead_of_list(self):
        """
        Invoke list_extend() with string is passed in place of list
        """
        key = ('test', 'demo', 1)
        try:
            self.as_connection.list_extend(key, "contact_no", "str")

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Items should be of type list"

    def test_neg_list_extend_meta_type_integer(self):
        """
        Invoke list_extend() with metadata input is of type integer
        """
        key = ('test', 'demo', 1)
        try:
            self.as_connection.list_extend(key, "contact_no", [85], 888)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Metadata should be of type dictionary"
