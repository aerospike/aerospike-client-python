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


class TestCalcDigest(object):

    @pytest.mark.parametrize("ns, set, key", [
        ("test", "demo", 1),  # integer key
        ("test", "demo", "get_key_digest"),  # string key
        ("test", "demo", u"get_key_digest"),  # unicode key
        ("test", u"set", "get_key_digest"),  # unicode set
        ("test", "demo", bytearray("askluy3oijs", "utf-8"))  # bytearray key
    ])
    def test_pos_calc_digest_with_key(self, ns, set, key):
        """
            Invoke calc_digest() with key
        """
        digest = aerospike.calc_digest(ns, set, key)

        assert isinstance(digest, bytearray)

    def test_pos_calc_digest_on_inserted_key(self):
        """
            Invoke calc_digest() on inserted key
        """
        client = TestBaseClass.get_new_connection()

        key = ('test', 'demo', 'get_digest_key')
        client.put(key, {"bin1": 1})
        key, meta, bins = client.get(key)

        assert key[3] == aerospike.calc_digest(
            "test", "demo", "get_digest_key")

        client.remove(key)
        client.close()

    # Negative Tests
    def test_neg_calc_digest_with_no_parameter(self):
        """
            Invoke calc_digest() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            aerospike.calc_digest()

        assert "argument 'ns' (pos 1)" in str(
            typeError.value)

    def test_neg_calc_digest_with_only_ns(self):
        """
            Invoke calc_digest() with a ns and no other parameters
        """
        with pytest.raises(TypeError) as typeError:
            aerospike.calc_digest("test")

        assert "argument 'set' (pos 2)" in str(
            typeError.value)

    @pytest.mark.parametrize("ns, set, key, err_msg", [
        (1,
         "demo",
         "get_digest_key",
         "Namespace should be a string"),
        # Namespace is a int
        ("test",
         1,
         "get_digest_key",
         "Set should be a string or unicode"),
        # Namespace is a int
        ("test", "demo", [1, 2, 3], "Key is invalid"),  # Key is a list
        ("test", "set", {"a": 1, "b": 2}, "Key is invalid"),  # Key is a map
        ("test", "demo", None, "Key is invalid"),  # Key is None
    ])
    def test_neg_calc_digest_with_invalid_parameters(
            self, ns, set, key, err_msg):
        """
            Invoke calc_digest() with invalid parameters
        """
        with pytest.raises(TypeError) as typeError:
            aerospike.calc_digest(ns, set, key)

        assert err_msg in str(typeError.value)
