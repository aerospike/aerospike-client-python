# -*- coding: utf-8 -*-

import pytest
import sys

from .test_base_class import TestBaseClass

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)


@pytest.mark.usefixtures("as_connection")
class TestGetKeyDigest(object):

    def test_get_key_digest_with_integer_key(self):
        """
            Invoke get_key_digest() with integer key
        """

        digest = self.as_connection.get_key_digest("test", "demo", 1)

        assert isinstance(digest, bytearray)

    def test_get_key_digest_with_string_key(self):
        """
            Invoke get_key_digest() with string key
        """

        digest = self.as_connection.get_key_digest("test", "demo",
                                                   "get_digest_key")

        assert isinstance(digest, bytearray)

    def test_get_key_digest_with_byte_key(self):
        '''
        Test calling the method with a bytearray primary key
        '''
        key = bytearray('a' * 5, 'utf-8')
        digest = self.as_connection.get_key_digest("test", "demo",
                                                   key)

        assert isinstance(digest, bytearray)

    @pytest.mark.xfail()
    def test_get_key_digest_with_none_set(self):
        '''
        Test the value of calling calc digest with None for a set
        '''
        digest = self.as_connection.get_key_digest('test', None, 1)

        assert isinstance(digest, bytearray)

    # **Beginning of tests for invalid usage**

    def test_get_key_digest_with_no_parameter(self):
        """
            Invoke get_key_digest() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            self.as_connection.get_key_digest()

        assert "argument 'ns' (pos 1)" in str(
            typeError.value)

    def test_get_key_digest_with_only_ns(self):
        """
            Invoke get_key_digest() with a ns and no other parameters
        """
        with pytest.raises(TypeError) as typeError:
            self.as_connection.get_key_digest("test")

    def test_get_key_digest_with_only_ns_and_set(self):
        """
            Invoke get_key_digest() with a ns and no other parameters
        """
        with pytest.raises(TypeError) as typeError:
            self.as_connection.get_key_digest("test", "set")

    @pytest.mark.parametrize("ns", [1, None, False, {}, [], ()])
    def test_get_key_digest_with_invalid_ns_type(self, ns):
        """
            Invoke get_key_digest() with integer ns
        """

        with pytest.raises(TypeError):
            self.as_connection.get_key_digest(ns, "demo",
                                              "get_digest_key")

    @pytest.mark.parametrize('set_val', [1, None, False, {}, [], ()])
    def test_get_key_digest_with_integer_set(self, set_val):
        """
            Invoke get_key_digest() with integer set
        """

        with pytest.raises(TypeError):
            self.as_connection.get_key_digest("test", set_val,
                                              "get_digest_key")

    @pytest.mark.parametrize(
        "namespace, set, primary_key",
        [
            ('test', 'demo', [1, 2, 3]),
            ('test', 'demo', {'a': 1, 'b': 2}),
            ('test', 'demo', None)
        ],
        ids=("list key", "map key", "None key")
    )
    def test_get_key_digest_with_invalid_key_type(self,
                                                  namespace, set, primary_key):
        with pytest.raises(TypeError):
            self.as_connection.get_key_digest(namespace, set, primary_key)
