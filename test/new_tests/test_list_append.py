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

    @pytest.mark.parametrize("key, field, append_value, expected", [
        (('test', 'demo', 1),    # append integer value to a list
            "contact_no",
            50000,
            {'contact_no': [1, 2, 50000],
             'name': 'name1', 'city': ['Pune', 'Dehli']}),
        (('test', 'demo', 1),                       # string
            "city",
            "Chennai",
            {'contact_no': [1, 2], 'name': 'name1',
             'city': ['Pune', 'Dehli', 'Chennai']}),
        (('test', 'demo', 1),                       # Unicode string
            "city",
            u"Mumbai",
            {'contact_no': [1, 2], 'city': [
             'Pune', 'Dehli', u'Mumbai'], 'name': 'name1'}),
        (('test', 'demo', 2),                      # float
            "contact_no",
            85.12,
            {'contact_no': [2, 3, 85.12],
             'city': ['Pune', 'Dehli'], 'name': 'name2'}),
        (('test', 'demo', 3),                       # map
            "contact_no",
            {'k1': 29},
            {'contact_no': [3, 4, {'k1': 29}],
             'city': ['Pune', 'Dehli'], 'name': 'name3'}),
        (('test', 'demo', 1),                       # bytearray
            "contact_no",
            bytearray("asd;as[d'as;d", "utf-8"),
            {'contact_no': [
             1, 2, bytearray(b"asd;as[d\'as;d")],
             'city': ['Pune', 'Dehli'], 'name': 'name1'}),
        (('test', 'demo', 1),                       # boolean
            "contact_no",
            False,
            {'contact_no': [1, 2, 0],
             'city': ['Pune', 'Dehli'], 'name': 'name1'})
    ])
    def test_pos_list_append(self, key, field, append_value, expected):
        """
        Invoke list_append() append value to a list
        """
        self.as_connection.list_append(key, field, append_value)

        (key, _, bins) = self.as_connection.get(key)

        assert bins == expected

    def test_pos_list_append_list_with_correct_policy(self):
        """
        Invoke list_append() append list with correct policy options
        """
        key = ('test', 'demo', 2)
        policy = {
            'timeout': 1000,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'commit_level': aerospike.POLICY_COMMIT_LEVEL_MASTER
        }
        self.as_connection.list_append(
            key, "contact_no", [45, 50, 80], {}, policy)

        (key, _, bins) = self.as_connection.get(key)

        assert bins == {
            'contact_no': [2, 3, [45, 50, 80]],
            'city': ['Pune', 'Dehli'], 'name': 'name2'}

    def test_pos_list_append_with_nonexistent_key(self):
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
        status = self.as_connection.list_append(key, "abc", 122)
        assert status == 0

        (key, _, bins) = self.as_connection.get(key)
        self.as_connection.remove(key)

        assert status == 0
        assert bins == {'abc': [122]}

    def test_pos_list_append_with_nonexistent_bin(self):
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
        status = self.as_connection.list_append(key, bin, 585)
        assert status == 0

        (key, _, bins) = self.as_connection.get(key)

        assert status == 0
        assert bins == {'contact_no': [1, 2], 'name': 'name1',
                        'city': ['Pune', 'Dehli'], bin: [585]}

    # Negative Tests
    def test_neg_list_append_with_no_parameters(self):
        """
        Invoke list_append() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            self.as_connection.list_append()
        assert "argument 'key' (pos 1)" in str(
            typeError.value)

    def test_neg_list_append_with_incorrect_policy(self):
        """
        Invoke list_append() with incorrect policy
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 0.5
        }
        try:
            self.as_connection.list_append(
                key, "contact_no", "str", {}, policy)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "timeout is invalid"

    def test_neg_list_append_with_extra_parameter(self):
        """
        Invoke list_append() with extra parameter.
        """
        key = ('test', 'demo', 1)
        policy = {'timeout': 1000}
        with pytest.raises(TypeError) as typeError:
            self.as_connection.list_append(
                key, "contact_no", 999, {}, policy, "")

        assert "list_append() takes at most 5 arguments (6 given)" in str(
            typeError.value)

    def test_neg_list_append_policy_is_string(self):
        """
        Invoke list_append() with policy is string
        """
        key = ('test', 'demo', 1)
        try:
            self.as_connection.list_append(key, "contact_no", 85, {}, "")

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "policy must be a dict"

    def test_neg_list_append_key_is_none(self):
        """
        Invoke list_append() with key is none
        """
        try:
            self.as_connection.list_append(None, "contact_no", 45)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "key is invalid"

    def test_neg_list_append_bin_is_none(self):
        """
        Invoke list_append() with bin is none
        """
        key = ('test', 'demo', 1)
        try:
            self.as_connection.list_append(key, None, "str")

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Bin name should be of type string"

    def test_neg_list_append_meta_type_integer(self):
        """
        Invoke list_append() with metadata input is of type integer
        """
        key = ('test', 'demo', 1)
        try:
            self.as_connection.list_append(key, "contact_no", 85, 888)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Metadata should be of type dictionary"

    def test_list_append_with_no_connection(self):
        client = aerospike.client({'hosts': [('localhost', 3000)]})
        k = ('test', 'demo', 'no_con')
        with pytest.raises(e.ClusterError):
            client.list_append(k, 'bob', 'item')
