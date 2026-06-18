##########################################################################
# Copyright 2026 Aerospike, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
##########################################################################

"""
Helper functions to create string operation dictionary arguments.

Index orientation is left-to-right with Unicode codepoint addressing.
Negative indexes count from the end of the string (-1 is the last
codepoint). Out-of-bounds indexes are clamped by the server.

String operations require server version 8.1.3 or later. When ctx is not
:py:obj:`None` and not empty, the operation targets a string nested inside a list or
map. The ctx-navigated leaf must already be an Aerospike string; operations
on non-string leaves return :exc:`~aerospike.exception.BinIncompatibleType`.

:py:meth:`to_string` is a top-level conversion operation and does not
accept ``ctx`` because it is sent as its own wire operation instead of a string
sub-operation with a msgpack payload.

All string arguments (needle, value, separator, pattern, etc.) are passed as
Python strings, but they cannot embedded NULL bytes.
"""

import aerospike
from ..string_helpers import NumericType, RegexFlags, StringPolicy, __generate_docstrings_for_all_func_members
import sys

TypeCTX = list | None


def strlen(bin_name: str, ctx: TypeCTX = None):
    """
    Create string ``strlen`` operation. The server returns the number of Unicode
    codepoints in the string bin as an int64. This is not the number of UTF-8
    bytes and it is not a grapheme cluster count.

    Examples: precomposed "e with acute" counts as 1 codepoint, while "e" plus
    a combining acute accent counts as 2 codepoints. Use
    :py:meth:`byte_length` for UTF-8 byte length.

    Args:

        {bin_name}
        {ctx}
    """
    op = aerospike._OP_STRING_STRLEN
    return locals()


def substr(bin_name: str, start: int, end: int | None = None, ctx: TypeCTX = None):
    """
    Create string ``substr`` operation that returns the half-open codepoint range ``[start, end)``.
        Negative indexes count from the end of the string.
        If end is :py:obj:`None`, the operation will continue to the end of the string.

    Args:

        {bin_name}
        start (int): Starting codepoint index, inclusive.
        end (int | None): Ending codepoint index, exclusive.
        {ctx}
    """
    op = aerospike._OP_STRING_SUBSTR
    return locals()


def char_at(bin_name: str, index: int, ctx: TypeCTX = None):
    """
    Create string ``char_at`` operation that returns the codepoint at index as a
    one-codepoint string. Negative indexes count from the end.

    Args:

        {bin_name}
        index (int): Index of the codepoint to return.
        {ctx}
    """
    op = aerospike._OP_STRING_CHAR_AT
    return locals()


def find(bin_name: str, needle: str, occurrence: int = 1, ctx: TypeCTX = None):
    """
    Create string ``find`` operation that returns the codepoint index of the first
    occurrence of needle, or ``-1`` if not found.

    Args:

        {bin_name}
        {needle_get}
        occurrence (int): The occurrence of the string to search for.
        index (int): Index of the codepoint to return.
        {ctx}
    """
    op = aerospike._OP_STRING_FIND
    return locals()


def contains(bin_name: str, needle: int, ctx: TypeCTX = None):
    """
    Create string ``contains`` operation that returns true if the bin contains needle.

    Args:

        {bin_name}
        {needle_get}
        {ctx}
    """
    op = aerospike._OP_STRING_CONTAINS
    return locals()


def starts_with(bin_name: str, prefix: str, ctx: TypeCTX = None):
    """
    Create string ``starts_with`` operation that returns true if the bin begins with
    prefix.

    Args:

        {bin_name}
        prefix (str): The string to search for.
        {ctx}
    """
    op = aerospike._OP_STRING_STARTS_WITH
    return locals()


def ends_with(bin_name: str, suffix: str, ctx: TypeCTX = None):
    """
    Create string ``ends_with`` operation that returns true if the bin ends with
    suffix.

    Args:

        {bin_name}
        suffix (str): The string to search for.
        {ctx}
    """
    op = aerospike._OP_STRING_ENDS_WITH
    return locals()


def to_integer(bin_name: str, ctx: TypeCTX = None):
    """
    Create string ``to_integer`` operation that parses the string as an unsigned 64-bit integer.
    Raises :exc:`~aerospike.exception.ParamError` if the bin cannot be parsed as an integer.

    Args:

        {bin_name}
        {ctx}
    """
    op = aerospike._OP_STRING_TO_INTEGER
    return locals()


def to_double(bin_name: str, ctx: TypeCTX = None):
    """
    Create string ``to_double`` operation that parses the string as a 64-bit float.
    Returns :exc:`~aerospike.exception.ParamError` if the bin cannot be parsed as a double.

    Args:

        {bin_name}
        {ctx}
    """
    op = aerospike._OP_STRING_TO_DOUBLE
    return locals()


def byte_length(bin_name: str, ctx: TypeCTX = None):
    """
    Create string ``byte_length`` operation that returns the number of UTF-8 bytes in
    the string as an unsigned 64-bit integer. This differs from ``strlen`` for non-ASCII text.

    Args:

        {bin_name}
        {ctx}
    """
    op = aerospike._OP_STRING_BYTE_LENGTH
    return locals()


def is_numeric(bin_name: str, numeric_type: NumericType = NumericType.ANY, ctx: TypeCTX = None):
    """
    Create string ``is_numeric`` operation that returns true if the bin contains a
    valid integer or floating-point number.

    Args:

        {bin_name}
        numeric_type (:py:class:`~aerospike_helpers.string_helpers.NumericType`): The numeric type to filter for.
        {ctx}
    """
    op = aerospike._OP_STRING_IS_NUMERIC
    return locals()


def is_upper(bin_name: str, ctx: TypeCTX = None):
    """
    Create string ``is_upper`` operation that returns true if every cased codepoint in
    the bin is uppercase.

    Args:

        {bin_name}
        {ctx}
    """
    op = aerospike._OP_STRING_IS_UPPER
    return locals()


def is_lower(bin_name: str, ctx: TypeCTX = None):
    """
    Create string ``is_lower`` operation that returns true if every cased codepoint in
    the bin is lowercase.

    Args:

        {bin_name}
        {ctx}
    """
    op = aerospike._OP_STRING_IS_LOWER
    return locals()


def to_blob(bin_name: str, ctx: TypeCTX = None):
    """
    Create string ``to_blob`` operation that returns the UTF-8 bytes of the string as
    a blob.

    Args:

        {bin_name}
        {ctx}
    """
    op = aerospike._OP_STRING_TO_BLOB
    return locals()


# TODO: all values with default of None need to be marked with type optional
def split(bin_name: str, separator: str | None = None, ctx: TypeCTX = None):
    """
    Create string ``split`` operation that splits by Unicode codepoint.

    Args:

        {bin_name}
        separator (str): The separator to split by. If this is :py:obj:`None`, Each codepoint
            becomes one string element in the returned list. If the separator is not found,
            the server returns a singleton list containing the whole string.
        {ctx}
    """
    op = aerospike._OP_STRING_SPLIT
    return locals()


def base64_decode(bin_name: str, ctx: TypeCTX = None):
    """
    Create string ``b64_decode`` operation that treats the bin as base64 text and
    returns the decoded bytes as a blob.

    Args:

        {bin_name}
        {ctx}
    """
    op = aerospike._OP_STRING_B64_DECODE
    return locals()


def regex_compare(bin_name: str, pattern: str, regex_flags: RegexFlags = RegexFlags.DEFAULT, ctx: TypeCTX = None):
    """
    Create string ``regex_compare`` operation that matches an ICU regex pattern
    against the bin and returns true on match.

    Args:

        {bin_name}
        {pattern}
        {regex_flags}
        {ctx}
    """
    op = aerospike._OP_STRING_REGEX_COMPARE
    return locals()


def insert(bin_name: str, index: int, value: str, policy: StringPolicy | None = None, ctx: TypeCTX = None):
    """
    Create string ``insert`` operation that splices value into the bin at codepoint
    index. Negative indexes count from the end of the string.

    Args:

        {bin_name}
        index (int): Index of the codepoint to insert at.
        value (str): The value to insert.
        {str_policy}
        {ctx}
    """
    op = aerospike._OP_STRING_INSERT
    return locals()


def overwrite(bin_name: str, index: int, value: str, policy: StringPolicy | None = None, ctx: TypeCTX = None):
    """
    Create string ``overwrite`` operation that overwrites codepoints starting at index
    with value. The result may grow beyond the original length when value extends
    past the end.

    Args:

        {bin_name}
        index (int): Index of the codepoint to overwrite at.
        value (str): The value to overwrite.
        {str_policy}
        {ctx}
    """
    op = aerospike._OP_STRING_OVERWRITE
    return locals()


def append(bin_name: str, value: str, policy: StringPolicy | None = None, ctx: TypeCTX = None):
    """
    Create string ``append`` operation that appends value to the end of the bin.
    Unlike :func:`~aerospike_helpers.operations.operations.append`, this string-package operation
    uses Unicode codepoint semantics and supports string policy and ctx.

    Args:

        {bin_name}
        value (str): The value to append.
        {str_policy}
        {ctx}
    """
    op = aerospike._OP_STRING_APPEND
    return locals()


def prepend(bin_name: str, value: str, policy: StringPolicy | None = None, ctx: TypeCTX = None):
    """
    Create string ``prepend`` operation that prepend value to the start of the bin.
    Unlike :func:`~aerospike_helpers.operations.operations.prepend`, this string-package operation
    uses Unicode codepoint semantics and supports string policy and ctx.

    Args:

        {bin_name}
        value (str): The value to prepend.
        {str_policy}
        {ctx}
    """
    op = aerospike._OP_STRING_PREPEND
    return locals()


def concat(bin_name: str, value_list: list[str], policy: StringPolicy | None = None, ctx: TypeCTX = None):
    """
    Create string ``concat`` operation that appends each string element in values to
    the bin in order.

    Args:

        {bin_name}
        value_list (str): The list of values to append.
        {str_policy}
        {ctx}
    """
    op = aerospike._OP_STRING_CONCAT
    return locals()


def snip(bin_name: str, start: int, end: int, policy: StringPolicy | None = None, ctx: TypeCTX = None):
    """
    Create string ``snip`` operation that removes codepoints from start to end.

    Args:

        {bin_name}
        start (int): First codepoint to remove, inclusive.
        end (int): One past the last codepoint to remove, exclusive.
        {str_policy}
        {ctx}
    """
    op = aerospike._OP_STRING_SNIP
    return locals()


def replace(bin_name: str, needle: str, replacement: str, policy: StringPolicy | None = None, ctx: TypeCTX = None):
    """
    Create string ``replace`` operation that replaces the first occurrence of needle
    with replacement.

    Args:

        {bin_name}
        {needle_to_replace}
        {replacement}
        {str_policy}
        {ctx}
    """
    op = aerospike._OP_STRING_REPLACE
    return locals()


def replace_all(bin_name: str, needle: str, replacement: str, policy: StringPolicy | None = None, ctx: TypeCTX = None):
    """
    Create string ``replace_all`` operation that replaces every occurrence of needle
    with replacement.

    Args:

        {bin_name}
        {needle_to_replace}
        {replacement}
        {str_policy}
        {ctx}
    """
    op = aerospike._OP_STRING_REPLACE_ALL
    return locals()


def upper(bin_name: str, policy: StringPolicy | None = None, ctx: TypeCTX = None):
    """
    Create string ``upper`` operation that uppercases the bin in place.

    Args:

        {bin_name}
        {str_policy}
        {ctx}
    """
    op = aerospike._OP_STRING_UPPER
    return locals()


def lower(bin_name: str, policy: StringPolicy | None = None, ctx: TypeCTX = None):
    """
    Create string ``lower`` operation that lowercases the bin in place.

    Args:

        {bin_name}
        {str_policy}
        {ctx}
    """
    op = aerospike._OP_STRING_LOWER
    return locals()


# TODO: read up how this works
def casefold(bin_name: str, policy: StringPolicy | None = None, ctx: TypeCTX = None):
    """
    Create string ``case_fold`` operation that applies locale-independent case folding
    (lowercase) to the bin. This is useful for normalized comparison keys.

    Args:

        {bin_name}
        {str_policy}
        {ctx}
    """
    op = aerospike._OP_STRING_CASE_FOLD
    return locals()


# TODO: read up how this works
def normalize_nfc(bin_name: str, policy: StringPolicy | None = None, ctx: TypeCTX = None):
    """
    Create string ``normalize_nfc`` operation that normalizes the bin to Unicode NFC.
    Already-normalized strings are unchanged.

    Args:

        {bin_name}
        {str_policy}
        {ctx}
    """
    op = aerospike._OP_STRING_NORMALIZE_NFC
    return locals()


def trim_start(bin_name: str, policy: StringPolicy | None = None, ctx: TypeCTX = None):
    """
    Create string ``trim_start`` operation that removes whitespace from the start of
    the bin.

    Args:

        {bin_name}
        {str_policy}
        {ctx}
    """
    op = aerospike._OP_STRING_TRIM_START
    return locals()


def trim_end(bin_name: str, policy: StringPolicy | None = None, ctx: TypeCTX = None):
    """
    Create string ``trim_end`` operation that removes whitespace from the end of the
    bin.

    Args:

        {bin_name}
        {str_policy}
        {ctx}
    """
    op = aerospike._OP_STRING_TRIM_END
    return locals()


def trim(bin_name: str, policy: StringPolicy | None = None, ctx: TypeCTX = None):
    """
    Create string ``trim`` operation that removes whitespace from both ends of the bin.

    Args:

        {bin_name}
        {str_policy}
        {ctx}
    """
    op = aerospike._OP_STRING_TRIM
    return locals()


def pad_start(
    bin_name: str,
    target_length: int,
    pad_string: str,
    policy: StringPolicy | None = None,
    ctx: TypeCTX = None
):
    """
    Create string ``pad_start`` operation that prepends ``pad_string`` repeatedly until
    the bin reaches ``target_length`` codepoints. No-op when the bin is already at or
    above the target length.

    Args:

        {bin_name}
        {target_length}
        {pad_string}
        {str_policy}
        {ctx}
    """
    op = aerospike._OP_STRING_PAD_START
    return locals()


def pad_end(
    bin_name: str,
    target_length: int,
    pad_string: str,
    policy: StringPolicy | None = None,
    ctx: TypeCTX = None
):
    """
    Create string ``pad_end`` operation that appends ``pad_string`` repeatedly until the
    bin reaches ``target_length`` codepoints. No-op when the bin is already at or
    above the target length.

    Args:

        {bin_name}
        {target_length}
        {pad_string}
        {str_policy}
        {ctx}
    """
    op = aerospike._OP_STRING_PAD_END
    return locals()


def repeat(bin_name: str, count: int, policy: StringPolicy | None = None, ctx: TypeCTX = None):
    """
    Create string ``repeat`` operation that repeats the bin contents count times.

    Args:

        {bin_name}
        count (int): The number of times to repeat the string. Must be non-negative.
        {str_policy}
        {ctx}
    """
    op = aerospike._OP_STRING_REPEAT
    return locals()


def regex_replace(
    bin_name: str,
    pattern: str,
    replacement: str,
    regex_flags: RegexFlags = RegexFlags.DEFAULT,
    policy: StringPolicy | None = None,
    ctx: TypeCTX = None
):
    """
    Create string ``regex_replace`` operation that replaces the first match of pattern
    with replacement. Pass :py:attr:`~aerospike_helpers.string_helpers.RegexFlags.GLOBAL` to replace every match.
    This server operation accepts regex flags but not string policy flags.

    Args:

        {bin_name}
        {pattern}
        {replacement}
        {regex_flags}
        {str_policy}
        {ctx}
    """
    op = aerospike._OP_STRING_REGEX_REPLACE
    return locals()


def to_string(bin_name: str):
    """
    Create ``to_string`` operation that converts an integer, double, string, or blob
    bin to its string representation. Raises :exc:`~aerospike.exception.BinIncompatibleType` for
    any other bin type. This top-level operation does not accept ctx and does not
    send a msgpack payload.

    Args:

        {bin_name}
    """
    op = aerospike._OP_STRING_TO_STRING
    return locals()


# These descriptions are shared across all the string operations


kwargs = {
    "bin_name": "bin_name (str): name of string bin.",
    "ctx": "ctx (list | None): Optional path into a string nested inside a list or map."
}
__generate_docstrings_for_all_func_members(sys.modules[__name__], kwargs)
