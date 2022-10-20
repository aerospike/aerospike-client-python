# -*- coding: utf-8 -*-
import pytest
import sys
import random
from aerospike import exception as e

import aerospike


class TestListPopRange(object):
    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        keys = []
        for i in range(5):
            key = ("test", "demo", i)
            rec = {
                "name": "name%s" % (str(i)),
                "contact_no": [i, i + 1, i + 2, i + 3, i + 4, i + 5],
                "city": ["Pune", "Dehli"],
            }
            self.as_connection.put(key, rec)
            keys.append(key)
        key = ("test", "demo", 1)
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

    def test_pos_list_pop_range_with_correct_paramters(self):
        """
        Invoke list_pop_range() get back elements from the list with
        correct parameters
        """
        key = ("test", "demo", 1)

        bins = self.as_connection.list_pop_range(key, "contact_no", 4, 3)

        assert bins == [5, 6, [45, 50, 80]]

    def test_pos_list_pop_range_with_correct_policy(self):
        """
        Invoke list_pop_range() get back elements from the list with
        correct policy
        """
        key = ("test", "demo", 1)
        policy = {
            "timeout": 1000,
            "retry": aerospike.POLICY_RETRY_ONCE,
            "commit_level": aerospike.POLICY_COMMIT_LEVEL_MASTER,
        }

        bins = self.as_connection.list_pop_range(key, "city", 0, 2, {}, policy)

        assert bins == ["Pune", "Dehli"]

    # Negative Tests
    def test_neg_list_pop_range_with_no_parameters(self):
        """
        Invoke list_pop_range() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            self.as_connection.list_pop_range()
        assert "argument 'key' (pos 1)" in str(typeError.value)

    def test_neg_list_pop_range_with_incorrect_policy(self):
        """
        Invoke list_pop_range() with incorrect policy
        """
        key = ("test", "demo", 1)
        policy = {"timeout": 0.5}
        try:
            self.as_connection.list_pop_range(key, "contact_no", 0, 2, {}, policy)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "timeout is invalid"

    def test_neg_list_pop_range_with_nonexistent_key(self):
        """
        Invoke list_pop_range() with non-existent key
        """
        if self.server_version < [3, 15, 2]:
            pytest.skip("Change of error beginning in 3.15")
        charSet = "abcdefghijklmnopqrstuvwxyz1234567890"
        minLength = 5
        maxLength = 30
        length = random.randint(minLength, maxLength)
        key = ("test", "demo", "".join(map(lambda unused: random.choice(charSet), range(length))) + ".com")

        with pytest.raises(e.RecordNotFound):
            self.as_connection.list_pop_range(key, "abc", 0, 1)

    def test_neg_list_pop_range_with_nonexistent_bin(self):
        """
        Invoke list_pop_range() with non-existent bin
        """
        key = ("test", "demo", 1)
        charSet = "abcdefghijklmnopqrstuvwxyz1234567890"
        minLength = 5
        maxLength = 10
        length = random.randint(minLength, maxLength)
        bin = "".join(map(lambda unused: random.choice(charSet), range(length))) + ".com"
        try:
            self.as_connection.list_pop_range(key, bin, 0, 1)

        except e.BinIncompatibleType as exception:
            assert exception.code == 12

    def test_neg_list_pop_range_with_extra_parameter(self):
        """
        Invoke list_pop_range() with extra parameter.
        """
        key = ("test", "demo", 1)
        policy = {"timeout": 1000}
        with pytest.raises(TypeError) as typeError:
            self.as_connection.list_pop_range(key, "contact_no", 1, 1, {}, policy, "")

        assert "list_pop_range() takes at most 6 arguments (7 given)" in str(typeError.value)

    def test_neg_list_pop_range_policy_is_string(self):
        """
        Invoke list_pop_range() with policy is string
        """
        key = ("test", "demo", 1)
        try:
            self.as_connection.list_pop_range(key, "contact_no", 0, 1, {}, "")

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "policy must be a dict"

    def test_neg_list_pop_range_key_is_none(self):
        """
        Invoke list_pop_range() with key is none
        """
        try:
            self.as_connection.list_pop_range(None, "contact_no", 0, 2)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "key is invalid"

    def test_neg_list_pop_range_bin_is_none(self):
        """
        Invoke list_pop_range() with bin is none
        """
        key = ("test", "demo", 1)
        try:
            self.as_connection.list_pop_range(key, None, 1, 3)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Bin name should be of type string"

    def test_neg_list_pop_range_with_negative_index(self):
        """
        Invoke list_pop_range() with negative index
        """
        key = ("test", "demo", 1)
        try:
            self.as_connection.list_pop_range(key, "contact_no", -56, 5)
        except e.InvalidRequest as exception:
            assert exception.code == 4

    def test_neg_list_pop_range_with_negative_length(self):
        """
        Invoke list_pop_range() with negative count
        """
        key = ("test", "demo", 1)
        try:
            self.as_connection.list_pop_range(key, "contact_no", 0, -59)
        except e.InvalidRequest as exception:
            assert exception.code == 4

    def test_neg_list_pop_range_meta_type_integer(self):
        """
        Invoke list_pop_range() with metadata input is of type integer
        """
        key = ("test", "demo", 1)
        try:
            self.as_connection.list_pop_range(key, "contact_no", 0, 2, 888)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Metadata should be of type dictionary"

    def test_neg_list_pop_range_index_type_string(self):
        """
        Invoke list_pop_range() with index is of type string
        """
        key = ("test", "demo", 1)

        with pytest.raises(TypeError) as typeError:
            self.as_connection.list_pop_range(key, "contact_no", "Fifth", 2)
        assert "an integer is required" or "cannot be interpreted as an integer" in str(typeError.value)
