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

    def test_pos_list_clear_with_correct_paramters(self):
        """
        Invoke list_clear() with correct parameters
        """
        key = ('test', 'demo', 1)

        status = self.as_connection.list_clear(key, 'contact_no')

        assert status == 0

        (key, _, bins) = self.as_connection.get(key)
        assert bins == {
            'city': ['Pune', 'Dehli'], 'contact_no': [], 'name': 'name1'}

    def test_pos_list_clear_list_with_correct_policy(self):
        """
        Invoke list_clear() removes all list elements with correct policy
        """

        key = ('test', 'demo', 2)

        status = self.as_connection.list_clear(key, 'city')
        assert status == 0

        (key, _, bins) = self.as_connection.get(key)
        assert bins == {'city': [], 'contact_no': [2, 3], 'name': 'name2'}

    # Negative Tests
    def test_neg_list_clear_with_incorrect_policy(self):
        """
        Invoke list_clear() with incorrect policy
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 0.5
        }
        try:
            self.as_connection.list_clear(key, "contact_no", {}, policy)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "timeout is invalid"

    def test_neg_list_clear_with_nonexistent_key(self):
        """
        Invoke list_clear() with non-existent key
        """
        if self.server_version < [3, 15, 2]:
            pytest.skip("Change of error beginning in 3.15")
        key = ('test', 'demo', 'test_neg_list_clear_with_nonexistent_key')
        with pytest.raises(e.RecordNotFound):
            self.as_connection.list_clear(key, "contact_no")

    def test_neg_list_clear_with_nonexistent_bin(self):
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
            self.as_connection.list_clear(key, bin)
        except e.BinIncompatibleType as exception:
            assert exception.code == 12

    def test_neg_list_clear_with_extra_parameter(self):
        """
        Invoke list_clear() with extra parameter.
        """
        key = ('test', 'demo', 1)
        policy = {'timeout': 1000}
        with pytest.raises(TypeError) as typeError:
            self.as_connection.list_clear(key, "contact_no", {}, policy, "")

        assert "list_clear() takes at most 4 arguments (5 given)" in str(
            typeError.value)

    def test_neg_list_clear_policy_is_string(self):
        """
        Invoke list_clear() with policy is string
        """
        key = ('test', 'demo', 1)
        try:
            self.as_connection.list_clear(key, "contact_no", {}, "")

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "policy must be a dict"

    def test_neg_list_clear_key_is_none(self):
        """
        Invoke list_clear() with key is none
        """
        try:
            self.as_connection.list_clear(None, "contact_no")

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "key is invalid"

    def test_neg_list_clear_bin_is_none(self):
        """
        Invoke list_clear() with bin is none
        """
        key = ('test', 'demo', 1)
        try:
            self.as_connection.list_clear(key, None)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Bin name should be of type string"

    def test_neg_list_clear_meta_type_integer(self):
        """
        Invoke list_clear() with metadata input is of type integer
        """
        key = ('test', 'demo', 1)
        try:
            self.as_connection.list_clear(key, "contact_no", 888)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Metadata should be of type dictionary"

    def test_neg_list_clear_with_no_parameters(self):
        """
        Invoke list_clear() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            self.as_connection.list_clear()
        assert "argument 'key' (pos 1)" in str(
            typeError.value)
