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


class TestListGet(object):

    def setup_class(cls):
        """
        Setup method.
        """
        hostlist, user, password = TestBaseClass.get_hosts()
        config = {'hosts': hostlist}
        if user is None and password is None:
            TestListGet.client = aerospike.client(config).connect()
        else:
            TestListGet.client = aerospike.client(
                config).connect(user, password)

    def teardown_class(cls):
        TestListGet.client.close()

    def setup_method(self, method):
        for i in range(5):
            key = ('test', 'demo', i)
            rec = {'name': 'name%s' %
                   (str(i)), 'contact_no': [i, i + 1],
                   'city': ['Pune', 'Dehli']}
            TestListGet.client.put(key, rec)
        key = ('test', 'demo', 2)
        TestListGet.client.list_append(key, "contact_no", [45, 50, 80])

    def teardown_method(self, method):
        """
        Teardown method.
        """
        # time.sleep(1)
        for i in range(5):
            key = ('test', 'demo', i)
            TestListGet.client.remove(key)

    def test_list_get_with_correct_paramters(self):
        """
        Invoke list_get() get string with correct parameters
        """
        key = ('test', 'demo', 1)

        val = TestListGet.client.list_get(key, "city", 0)

        assert val == 'Pune'

    def test_list_get_with_correct_policy(self):
        """
        Invoke list_get() get with correct policy
        """
        key = ('test', 'demo', 2)
        policy = {
            'timeout': 1000,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'commit_level': aerospike.POLICY_COMMIT_LEVEL_MASTER
        }

        val = TestListGet.client.list_get(key, 'contact_no', 2, {}, policy)

        assert val == [45, 50, 80]

    def test_list_get_with_no_parameters(self):
        """
        Invoke list_get() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            TestListGet.client.list_get()
        assert "Required argument 'key' (pos 1) not found" in str(
            typeError.value)

    def test_list_get_with_incorrect_policy(self):
        """
        Invoke list_get() with incorrect policy
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 0.5
        }
        try:
            TestListGet.client.list_get(key, "contact_no", 0, {}, policy)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "timeout is invalid"

    def test_list_get_with_nonexistent_key(self):
        """
        Invoke list_get() with non-existent key
        """
        charSet = 'abcdefghijklmnopqrstuvwxyz1234567890'
        minLength = 5
        maxLength = 30
        length = random.randint(minLength, maxLength)
        key = ('test', 'demo', ''.join(map(lambda unused:
                                           random.choice(charSet),
                                           range(length))) + ".com")
        try:
            TestListGet.client.list_get(key, "contact_no", 0)

        except e.RecordNotFound as exception:
            assert exception.code == 2

    def test_list_get_with_extra_parameter(self):
        """
        Invoke list_get() with extra parameter.
        """
        key = ('test', 'demo', 1)
        policy = {'timeout': 1000}
        with pytest.raises(TypeError) as typeError:
            TestListGet.client.list_get(key, "contact_no", 1, {}, policy, "")

        assert "list_get() takes at most 5 arguments (6 given)" in str(
            typeError.value)

    def test_list_get_policy_is_string(self):
        """
        Invoke list_get() with policy is string
        """
        key = ('test', 'demo', 1)
        try:
            TestListGet.client.list_get(key, "contact_no", 1, {}, "")

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "policy must be a dict"

    def test_list_get_key_is_none(self):
        """
        Invoke list_get() with key is none
        """
        try:
            TestListGet.client.list_get(None, "contact_no", 0)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "key is invalid"

    def test_list_get_bin_is_none(self):
        """
        Invoke list_get() with bin is none
        """
        key = ('test', 'demo', 1)
        try:
            TestListGet.client.list_get(key, None, 1)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Bin name should be of type string"

    def test_list_get_with_negative_index(self):
        """
        Invoke list_get() with negative index
        """
        key = ('test', 'demo', 1)
        try:
            TestListGet.client.list_get(key, "contact_no", -56)
        except e.InvalidRequest as exception:
            assert exception.code == 4

    def test_list_get_meta_type_integer(self):
        """
        Invoke list_get() with metadata input is of type integer
        """
        key = ('test', 'demo', 1)
        try:
            TestListGet.client.list_get(key, "contact_no", 0, 888)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Metadata should be of type dictionary"

    def test_list_get_index_type_string(self):
        """
        Invoke list_get() with index is of type string
        """
        key = ('test', 'demo', 1)

        with pytest.raises(TypeError) as typeError:
            TestListGet.client.list_get(key, "contact_no", "Fifth")
        assert "an integer is required" in str(typeError.value)
