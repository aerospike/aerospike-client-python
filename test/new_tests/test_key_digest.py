'''
Tests to demonstrate correct calculations of key digests
'''
import pytest
import aerospike
import sys
from binascii import b2a_hex


NAMESPACE = 'namespace'
SET = 'set'

# Tests for integer keys


def convert_bytestr_to_str(byte_str):
    converted = byte_str
    # This is true in Python 3
    if bytes is not str:
        converted = str(converted, 'utf-8')
    return converted


@pytest.mark.parametrize(
    "pk, expected",
    (

        (-1, '22116d253745e29fc63fdf760b6e26f7e197e01d'),
        (0, '93d943aae37b017ad7e011b0c1d2e2143c2fb37d'),
        (1, '82d7213b469812947c109a6d341e3b5b1dedec1f'),
        # 8it boundaries
        (-128, '93191e549f8f3548d7e2cfc958ddc8c65bcbe4c6'),
        (127, 'a58f7d98bf60e10fe369c82030b1c9dee053def9'),
        (255, '5a7dd3ea237c30c8735b051524e66fd401a10f6a'),
        # 16 bit boundaries
        (-2 ** 15, '7f41e9dd1f3fe3694be0430e04c8bfc7d51ec2af'),
        (2 ** 15 - 1, '309fc9c2619c4f65ff7f4cd82085c3ee7a31fc7c'),
        (2 ** 16 - 1, '3f0dd44352749a9fd5b7ec44213441ef54c46d57'),
        # 32 bit boundaries
        (-2 ** 31, 'd635a867b755f8f54cdc6275e6fb437df82a728c'),
        (2 ** 31 - 1, 'fa8c47b8b898af1bbcb20af0d729ca68359a2645'),
        (2 ** 32 - 1, '2cdf52bf5641027042b9cf9a499e509a58b330e2'),
        # 64 bit boundaries
        (-2 ** 63, '7185c2a47fb02c996daed26b4e01b83240aee9d4'),
        (2 ** 63 - 1, '1698328974afa62c8e069860c1516f780d63dbb8'),
    )
)
def test_validate_digest_ints(pk, expected):
    digest = aerospike.calc_digest(NAMESPACE, SET, pk)
    hex_digest = b2a_hex(digest)
    assert convert_bytestr_to_str(hex_digest) == expected


'''
These strings are tested in the other clients' digest tests,
but the max binname allowed is 14 characters (15 bytes):
https://github.com/aerospike/aerospike-server/blob/master/as/include/base/datamodel.h#L130
'''


@pytest.mark.parametrize(
    "pk, expected",
    (
        ('', '2819b1ff6e346a43b4f5f6b77a88bc3eaac22a83'),
        ('s', '607cddba7cd111745ef0a3d783d57f0e83c8f311'),
        ('a' * 10, '5979fb32a80da070ff356f7695455592272e36c2'),
        ('m' * 100, 'f00ad7dbcb4bd8122d9681bca49b8c2ffd4beeed'),
        ('t' * 1000, '07ac412d4c33b8628ab147b8db244ce44ae527f8'),
        ('-' * 10000, 'b42e64afbfccb05912a609179228d9249ea1c1a0'),
        ('+' * 100000, '0a3e888c20bb8958537ddd4ba835e4070bd51740')
    )
)
def test_validate_digest_str(pk, expected):
    digest = aerospike.calc_digest(NAMESPACE, SET, pk)
    hex_digest = b2a_hex(digest)
    assert convert_bytestr_to_str(hex_digest) == expected


'''
Some clients seem to allow computation with a 0 length bytearray,
Python does not, so we omit it
'''


@pytest.mark.parametrize(
    "pk, expected",
    (
        ('s', 'ca2d96dc9a184d15a7fa2927565e844e9254e001'),
        ('a' * 10, 'd10982327b2b04c7360579f252e164a75f83cd99'),
        ('m' * 100, '475786aa4ee664532a7d1ea69cb02e4695fcdeed'),
        ('t' * 1000, '5a32b507518a49bf47fdaa3deca53803f5b2e8c3'),
        ('-' * 10000, 'ed65c63f7a1f8c6697eb3894b6409a95461fd982'),
        ('+' * 100000, 'fe19770c371774ba1a1532438d4851b8a773a9e6')
    )
)
def test_validate_digest_bytes(pk, expected):
    # encode the primary key as bytes
    byte_key = bytearray(pk, 'utf-8')
    digest = aerospike.calc_digest(NAMESPACE, SET, byte_key)
    hex_digest = b2a_hex(digest)
    assert convert_bytestr_to_str(hex_digest) == expected


def test_calculating_a_digest_with_no_set():
    expected = 'a443f05d05d962202b59abb402afae1737dbf66a'
    digest = aerospike.calc_digest(NAMESPACE, '', 1)
    hex_digest = b2a_hex(digest)
    assert convert_bytestr_to_str(hex_digest) == expected
