# -*- coding: utf-8 -*-

import pytest
import sys
import cPickle as pickle

try:
    import aerospike
except:
    print "Please install aerospike python client."
    sys.exit(1)

class SomeClass(object):

    pass


class TestGetKeyDigest(object):
    def setup_class(cls):
        """
        Setup method.
        """
        config = {
                'hosts': [('127.0.0.1', 3000)]
                }
        TestGetKeyDigest.client = aerospike.client(config).connect()

    def teardown_class(cls):
        TestGetKeyDigest.client.close()

    def setup_method(self, method):

        key = ('test', 'demo', "get_digest_key")
        rec = {
            'name' : 'name1',
            'age' : 1
        }
        TestGetKeyDigest.client.put(key, rec)

    def teardown_method(self, method):

        """
        Teardoen method.
        """
        key = ('test', 'demo', "get_digest_key")
        TestGetKeyDigest.client.remove(key)

    def test_get_key_digest_with_no_parameter(self):

        """
            Invoke get_key_digest() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            TestGetKeyDigest.client.get_key_digest()

        assert "Required argument 'ns' (pos 1) not found" in typeError.value

    def test_get_key_digest_with_only_ns(self):

        """
            Invoke get_key_digest() with a ns and no other parameters
        """
        with pytest.raises(TypeError) as typeError:
            TestGetKeyDigest.client.get_key_digest("test")

        assert "Required argument 'set' (pos 2) not found" in typeError.value

    def test_get_key_digest_with_integer_key(self):

        """
            Invoke get_key_digest() with integer key
        """

        digest = TestGetKeyDigest.client.get_key_digest("test", "demo", 1)

        assert type(digest) == bytearray

    def test_get_key_digest_with_string_key(self):

        """
            Invoke get_key_digest() with string key
        """

        digest = TestGetKeyDigest.client.get_key_digest("test", "demo", "get_digest_key")

        assert type(digest) == bytearray

    def test_get_key_digest_with_integer_ns(self):

        """
            Invoke get_key_digest() with integer ns
        """

        with pytest.raises(Exception) as exception:
            digest = TestGetKeyDigest.client.get_key_digest(1, "demo", "get_digest_key")

        assert exception.value[0] == -2
        assert exception.value[1] == 'namespace must be a string'

    def test_get_key_digest_with_integer_set(self):

        """
            Invoke get_key_digest() with integer set
        """

        with pytest.raises(Exception) as exception:
            digest = TestGetKeyDigest.client.get_key_digest("test", 1, "get_digest_key")

        assert exception.value[0] == -2
        assert exception.value[1] == 'set must be a string'

    def test_get_key_digest_with_list_key(self):

        #Invoke get_key_digest() with key as a list
        mylist = [1, 2, 3]

        with pytest.raises(Exception) as exception:
            TestGetKeyDigest.client.get_key_digest("test", "demo", mylist)

        assert exception.value[0] == -2
        assert exception.value[1] == 'key is invalid'


    def test_get_key_digest_with_map_key(self):
	"""
            Invoke get_key_digest() with key as a map
	"""
        with pytest.raises(Exception) as exception:
            digest = TestGetKeyDigest.client.get_key_digest("test", "set", {"a":
                1, "b": 2})

        assert exception.value[0] == -2
        assert exception.value[1] == 'key is invalid'

    def test_get_key_digest_with_none_key(self):

        """
            Invoke get_key_digest() with key as a None
        """

        with pytest.raises(Exception) as exception:
            digest = TestGetKeyDigest.client.get_key_digest("test", "demo", None)

        assert exception.value[0] == -2
        assert exception.value[1] == 'either key or digest is required'
