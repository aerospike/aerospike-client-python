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


class TestListInsertItems(object):

    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        keys = []
        for i in range(5):
            key = ('test', 'demo', i)
            rec = {'name': 'name%s' %
                   (str(i)), 'age': [i, i + 1], 'city': ['Pune', 'Dehli']}
            self.as_connection.put(key, rec)
            keys.append(key)
        key = ('test', 'demo', 'bytearray_key')
        self.as_connection.put(
            key, {"bytearray_bin": bytearray("asd;as[d'as;d", "utf-8")})
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
        (('test', 'demo', 1),   # list of integers
            "age",
            0,
            [500, 1500, 3000],
            {'age': [500, 1500, 3000, 1, 2],
             'name': 'name1', 'city': ['Pune', 'Dehli']}),
        (('test', 'demo', 1),   # list of string
            "city",
            0,
            ["Chennai"],
            {'age': [1, 2], 'name': 'name1',
             'city': ['Chennai', 'Pune', 'Dehli']}),
        (('test', 'demo', 1),   # list of unicode string
            "city",
            3,
            [u"Mumbai"],
            {'age': [1, 2], 'city': ['Pune', 'Dehli', None,
                                     u'Mumbai'], 'name': 'name1'}),
        (('test', 'demo', 2),   # list of float
            "age",
            7,
            [85.12],
            {'age': [2, 3, None, None, None, None, None, 85.12],
             'city': ['Pune', 'Dehli'], 'name': 'name2'}),
        (('test', 'demo', 3),   # list of map
            "age",
            1,
            [{'k1': 29}],
            {'age': [3, {'k1': 29}, 4], 'city': ['Pune', 'Dehli'],
             'name': 'name3'}),
        (('test', 'demo', 1),   # list of bytearray
            "age",
            2,
            [555, bytearray("asd;as[d'as;d", "utf-8")],
            {'age': [1, 2, 555, bytearray(b"asd;as[d\'as;d")],
             'city': ['Pune', 'Dehli'], 'name': 'name1'}),
    ])
    def test_pos_list_insert_items(self, key, field, index, value, expected):
        """
        Invoke list_insert_items() inserts items
        """
        self.as_connection.list_insert_items(
            key, field, index, value)

        (key, _, bins) = self.as_connection.get(key)

        assert bins == expected

    def test_pos_list_insert_items_list_with_correct_policy(self):
        """
        Invoke list_insert_items() inserts list with correct policy
        """
        key = ('test', 'demo', 2)
        policy = {
            'timeout': 1000,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'commit_level': aerospike.POLICY_COMMIT_LEVEL_MASTER
        }
        self.as_connection.list_insert_items(
            key, "age", 5, [45, 50, 80], {}, policy)

        (key, _, bins) = self.as_connection.get(key)

        assert bins == {'age': [2, 3, None, None, None, 45, 50, 80], 'city': [
            'Pune', 'Dehli'], 'name': 'name2'}

    def test_pos_list_insert_items_boolean(self):
        """
        Invoke list_insert_items() insert boolean into the list
        """

        key1 = ('test', 'demo', 1)
        self.as_connection.list_insert_items(key1, "age", 1, [False])
        (_, _, b1) = self.as_connection.get(key1)
        key2 = ('test', 'demo', 2)
        self.as_connection.list_insert_items(
            key2, "age", 1, [False, True])
        (_, _, b2) = self.as_connection.get(key2)
        assert b1['age'] == [1, False, 2]
        assert b2['age'] == [2, False, True, 3]

    def test_pos_list_insert_items_with_nonexistent_key(self):
        """
        Invoke list_insert_items() with non-existent key
        """
        charSet = 'abcdefghijklmnopqrstuvwxyz1234567890'
        minLength = 5
        maxLength = 30
        length = random.randint(minLength, maxLength)
        key = ('test', 'demo', ''.join(map(lambda unused:
                                           random.choice(charSet),
                                           range(length))) + ".com")
        status = self.as_connection.list_insert_items(key, "abc", 2,
                                                      [122, 878])
        assert status == 0

        (key, _, bins) = self.as_connection.get(key)

        assert status == 0
        assert bins == {'abc': [None, None, 122, 878]}

        self.as_connection.remove(key)

    def test_pos_list_insert_items_with_nonexistent_bin(self):
        """
        Invoke list_insert_items() with non-existent bin
        """
        key = ('test', 'demo', 1)
        charSet = 'abcdefghijklmnopqrstuvwxyz1234567890'
        minLength = 5
        maxLength = 10
        length = random.randint(minLength, maxLength)
        bin = ''.join(map(lambda unused:
                          random.choice(charSet), range(length))) + ".com"

        status = self.as_connection.list_insert_items(
            key, bin, 1, [585, 789])
        assert status == 0

        (key, _, bins) = self.as_connection.get(key)

        assert status == 0
        assert bins == {'age': [1, 2], 'name': 'name1',
                        'city': ['Pune', 'Dehli'], bin: [None, 585, 789]}

    # Negative Tests
    def test_neg_list_insert_items_with_no_parameters(self):
        """
        Invoke list_insert_items() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            self.as_connection.list_insert_items()
        assert "argument 'key' (pos 1)" in str(
            typeError.value)

    def test_neg_list_insert_items_with_incorrect_policy(self):
        """
        Invoke list_insert_items() with incorrect policy
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 0.5
        }
        try:
            self.as_connection.list_insert_items(
                key, "age", 6, ["str"], {}, policy)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "timeout is invalid"

    def test_neg_list_insert_items_with_extra_parameter(self):
        """
        Invoke list_insert_items() with extra parameter.
        """
        key = ('test', 'demo', 1)
        policy = {'timeout': 1000}
        with pytest.raises(TypeError) as typeError:
            self.as_connection.list_insert_items(
                key, "age", 3, [999], {}, policy, "")

        assert "list_insert_items() takes at most 6 arguments (7 given)" \
               in str(typeError.value)

    def test_neg_ist_insert_items_policy_is_string(self):
        """
        Invoke list_insert_items() with policy is string
        """
        key = ('test', 'demo', 1)
        try:
            self.as_connection.list_insert_items(
                key, "age", 1, [85], {}, "")

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "policy must be a dict"

    def test_neg_list_insert_items_key_is_none(self):
        """
        Invoke list_insert_items() with key is none
        """
        try:
            self.as_connection.list_insert_items(None, "age", 1, [45])

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "key is invalid"

    def test_neg_list_insert_items_bin_is_none(self):
        """
        Invoke list_insert_items() with bin is none
        """
        key = ('test', 'demo', 1)
        try:
            self.as_connection.list_insert_items(key, None, 2, ["str"])

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Bin name should be of type string"

    def test_neg_list_insert_items_with_items_type_string(self):
        """
        Invoke list_insert_items() insert items is of type string
        """
        key = ('test', 'demo', 1)

        try:
            self.as_connection.list_insert_items(key, "age", 6, "abc")

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Items should be of type list"

    def test_neg_list_insert_items_meta_type_integer(self):
        """
        Invoke list_insert_items() with metadata input is of type integer
        """
        key = ('test', 'demo', 1)
        try:
            self.as_connection.list_insert_items(key, "contact_no", 0,
                                                 [85], 888)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Metadata should be of type dictionary"

    def test_neg_list_insert_items_index_type_string(self):
        """
        Invoke list_insert_items() insert with index is of type string
        """
        key = ('test', 'demo', 1)

        with pytest.raises(TypeError) as typeError:
            self.as_connection.list_insert_items(
                key, "age", "Fifth", [False])
        assert "an integer is required" in str(typeError.value)
