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
from ..string_helpers import NumericType, RegexFlags, StringPolicy

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

        bin_name: name of string bin.
        ctx: Optional path into a string nested inside a list or map.
    """
    return {
        "op": aerospike._OP_STRING_STRLEN,
        "bin": bin_name,
        "ctx": ctx
    }


def substr(bin_name: str, start: int, ctx: TypeCTX = None):
    """
    Create string ``substr`` operation from start to the end of the string.
        Negative indexes count from the end of the string.

    Args:

        bin_name: name of string bin.
        start: Starting codepoint index.
        ctx: Optional path into a string nested inside a list or map.
    """
    return {
        "op": aerospike._OP_STRING_SUBSTR,
        "bin": bin_name,
        "start": start,
        "ctx": ctx
    }


def substr_range(bin_name: str, start: int, end: int, ctx: TypeCTX = None):
    """
    Create string ``substr`` operation that returns the half-open codepoint range ``[start, end)``.
        Negative indexes count from the end of the string.

    Args:

        bin_name: name of string bin.
        start: Starting codepoint index, inclusive.
        end: Ending codepoint index, exclusive.
        ctx: Optional path into a string nested inside a list or map.
    """
    return {
        "op": aerospike._OP_STRING_SUBSTR_RANGE,
        "bin": bin_name,
        "start": start,
        "end": end,
        "ctx": ctx
    }


def char_at(bin_name: str, index: int, ctx: TypeCTX = None):
    """
    Create string ``char_at`` operation that returns the codepoint at index as a
    one-codepoint string. Negative indexes count from the end.

    Args:

        bin_name: name of string bin.
        index: Index of the codepoint to return.
        ctx: Optional path into a string nested inside a list or map.
    """
    return {
        "op": aerospike._OP_STRING_CHAR_AT,
        "bin": bin_name,
        "index": index,
        "ctx": ctx
    }


def find(bin_name: str, needle: str, occurrence: int = 1, ctx: TypeCTX = None):
    """
    Create string ``find`` operation that returns the codepoint index of the first
    occurrence of needle, or ``-1`` if not found.

    Args:

        bin_name: name of string bin.
        needle: the string to search for.
        occurrence: The occurrence of the string to search for.
        index: Index of the codepoint to return.
        ctx: Optional path into a string nested inside a list or map.
    """
    return {
        "op": aerospike._OP_STRING_FIND,
        "bin": bin_name,
        "needle": needle,
        "occurrence": occurrence,
        "ctx": ctx
    }


def contains(bin_name: str, needle: int, ctx: TypeCTX = None):
    """
    Create string ``contains`` operation that returns true if the bin contains needle.

    Args:

        bin_name: name of string bin.
        needle: the string to search for.
        ctx: Optional path into a string nested inside a list or map.
    """
    return {
        "op": aerospike._OP_STRING_CONTAINS,
        "bin": bin_name,
        "needle": needle,
        "ctx": ctx
    }


def starts_with(bin_name: str, prefix: str, ctx: TypeCTX = None):
    """
    Create string ``starts_with`` operation that returns true if the bin begins with
    prefix.

    Args:

        bin_name: name of string bin.
        prefix: The string to search for.
        ctx: Optional path into a string nested inside a list or map.
    """
    return {
        "op": aerospike._OP_STRING_STARTS_WITH,
        "bin": bin_name,
        "prefix": prefix,
        "ctx": ctx
    }


def ends_with(bin_name: str, suffix: str, ctx: TypeCTX = None):
    """
    Create string ``ends_with`` operation that returns true if the bin ends with
    suffix.

    Args:

        bin_name: name of string bin.
        suffix: The string to search for.
        ctx: Optional path into a string nested inside a list or map.
    """
    return {
        "op": aerospike._OP_STRING_ENDS_WITH,
        "bin": bin_name,
        "suffix": suffix,
        "ctx": ctx
    }


def to_integer(bin_name: str, ctx: TypeCTX = None):
    """
    Create string ``to_integer`` operation that parses the string as an unsigned 64-bit integer.
    Raises :exc:`~aerospike.exception.ParamError` if the bin cannot be parsed as an integer.

    Args:

        bin_name: name of string bin.
        ctx: Optional path into a string nested inside a list or map.
    """
    return {
        "op": aerospike._OP_STRING_TO_INTEGER,
        "bin": bin_name,
        "ctx": ctx
    }


def to_double(bin_name: str, ctx: TypeCTX = None):
    """
    Create string ``to_double`` operation that parses the string as a 64-bit float.
    Returns :exc:`~aerospike.exception.ParamError` if the bin cannot be parsed as a double.

    Args:

        bin_name: name of string bin.
        ctx: Optional path into a string nested inside a list or map.
    """
    return {
        "op": aerospike._OP_STRING_TO_DOUBLE,
        "bin": bin_name,
        "ctx": ctx
    }


def byte_length(bin_name: str, ctx: TypeCTX = None):
    """
    Create string ``byte_length`` operation that returns the number of UTF-8 bytes in
    the string as an unsigned 64-bit integer. This differs from ``strlen`` for non-ASCII text.

    Args:

        bin_name: name of string bin.
        ctx: Optional path into a string nested inside a list or map.
    """
    return {
        "op": aerospike._OP_STRING_BYTE_LENGTH,
        "bin": bin_name,
        "ctx": ctx
    }


def is_numeric(bin_name: str, numeric_type: NumericType = NumericType.ANY, ctx: TypeCTX = None):
    """
    Create string ``is_numeric`` operation that returns true if the bin contains a
    valid integer or floating-point number.

    Args:

        bin_name: name of string bin.
        numeric_type: The numeric type to filter for.
        ctx: Optional path into a string nested inside a list or map.
    """
    return {
        "op": aerospike._OP_STRING_IS_NUMERIC,
        "numeric_type": numeric_type,
        "bin": bin_name,
        "ctx": ctx
    }


def is_upper(bin_name: str, ctx: TypeCTX = None):
    """
    Create string ``is_upper`` operation that returns true if every cased codepoint in
    the bin is uppercase.

    Args:

        bin_name: name of string bin.
        ctx: Optional path into a string nested inside a list or map.
    """
    return {
        "op": aerospike._OP_STRING_IS_UPPER,
        "bin": bin_name,
        "ctx": ctx
    }


def is_lower(bin_name: str, ctx: TypeCTX = None):
    """
    Create string ``is_lower`` operation that returns true if every cased codepoint in
    the bin is lowercase.

    Args:

        bin_name: name of string bin.
        ctx: Optional path into a string nested inside a list or map.
    """
    return {
        "op": aerospike._OP_STRING_IS_LOWER,
        "bin": bin_name,
        "ctx": ctx
    }


def to_blob(bin_name: str, ctx: TypeCTX = None):
    """
    Create string ``to_blob`` operation that returns the UTF-8 bytes of the string as
    a blob.

    Args:

        bin_name: name of string bin.
        ctx: Optional path into a string nested inside a list or map.
    """
    return {
        "op": aerospike._OP_STRING_TO_BLOB,
        "bin": bin_name,
        "ctx": ctx
    }


def split(bin_name: str, ctx: TypeCTX = None):
    """
    Create string ``split`` operation that splits by Unicode codepoint. Each codepoint
    becomes one string element in the returned list.

    Args:

        bin_name: name of string bin.
        ctx: Optional path into a string nested inside a list or map.
    """
    return {
        "op": aerospike._OP_STRING_SPLIT,
        "bin": bin_name,
        "ctx": ctx
    }


# We define a separate function for this op instead of overloading the above one
# since the expressions as_exp_string_split and as_exp_string_split_separator take up different
# sizes. We create a one to one mapping of those expressions to operations in here.
def split_separator(bin_name: str, separator: str, ctx: TypeCTX = None):
    """
    Create string split operation that splits by separator. If separator is not
    found, the server returns a singleton list containing the whole string.

    Args:

        bin_name: name of string bin.
        separator: The separator to split by.
        ctx: Optional path into a string nested inside a list or map.
    """
    return {
        "op": aerospike._OP_STRING_SPLIT_SEPARATOR,
        "bin": bin_name,
        "separator": separator,
        "ctx": ctx
    }


def base64_decode(bin_name: str, ctx: TypeCTX = None):
    """
    Create string ``b64_decode`` operation that treats the bin as base64 text and
    returns the decoded bytes as a blob.

    Args:

        bin_name: name of string bin.
        ctx: Optional path into a string nested inside a list or map.
    """
    return {
        "op": aerospike._OP_STRING_B64_DECODE,
        "bin": bin_name,
        "ctx": ctx
    }


def regex_compare(bin_name: str, pattern: str, regex_flags: RegexFlags = RegexFlags.DEFAULT, ctx: TypeCTX = None):
    """
    Create string ``regex_compare`` operation that matches an ICU regex pattern
    against the bin and returns true on match.

    Args:

        bin_name: name of string bin.
        pattern: the regex pattern to match against.
        regex_flags: The regex flags to use.
        ctx: Optional path into a string nested inside a list or map.
    """
    return {
        "op": aerospike._OP_STRING_REGEX_COMPARE,
        "bin": bin_name,
        "pattern": pattern,
        "regex_flags": regex_flags,
        "ctx": ctx
    }


def insert(bin_name: str, index: int, value: str, policy: StringPolicy | None = None, ctx: TypeCTX = None):
    """
    Create string ``insert`` operation that splices value into the bin at codepoint
    index.

    Negative indexes count from the end of the string.
    If the bin doesn't exist, this operation will create a new bin.

    Args:

        bin_name: name of string bin.
        index: Index of the codepoint to insert at.
        value: The value to insert.
        policy: String policy.
        ctx: Optional path into a string nested inside a list or map.
    """
    return {
        "op": aerospike._OP_STRING_INSERT,
        "bin": bin_name,
        "index": index,
        "value": value,
        "policy": policy,
        "ctx": ctx
    }


def overwrite(bin_name: str, index: int, value: str, policy: StringPolicy | None = None, ctx: TypeCTX = None):
    """
    Create string ``overwrite`` operation that overwrites codepoints starting at index
    with value.

    The result may grow beyond the original length when value extends past the end.
    If the bin doesn't exist, this operation will create a new bin.

    Args:

        bin_name: name of string bin.
        index: Index of the codepoint to overwrite at.
        value: The value to overwrite.
        policy: String policy.
        ctx: Optional path into a string nested inside a list or map.
    """
    return {
        "op": aerospike._OP_STRING_OVERWRITE,
        "bin": bin_name,
        "index": index,
        "value": value,
        "policy": policy,
        "ctx": ctx
    }


def append(bin_name: str, value: str, policy: StringPolicy | None = None, ctx: TypeCTX = None):
    """
    Create string ``append`` operation that appends value to the end of the bin.

    Unlike :func:`~aerospike_helpers.operations.operations.append`, this string-package operation
    uses Unicode codepoint semantics and supports string policy and ctx.
    If the bin doesn't exist, this operation will create a new bin.

    Args:

        bin_name: name of string bin.
        value: The value to append.
        policy: String policy.
        ctx: Optional path into a string nested inside a list or map.
    """
    return {
        "op": aerospike._OP_STRING_APPEND,
        "bin": bin_name,
        "value": value,
        "policy": policy,
        "ctx": ctx
    }


def prepend(bin_name: str, value: str, policy: StringPolicy | None = None, ctx: TypeCTX = None):
    """
    Create string ``prepend`` operation that prepend value to the start of the bin.

    Unlike :func:`~aerospike_helpers.operations.operations.prepend`, this string-package operation
    uses Unicode codepoint semantics and supports string policy and ctx.
    If the bin doesn't exist, this operation will create a new bin.

    Args:

        bin_name: name of string bin.
        value: The value to prepend.
        policy: String policy.
        ctx: Optional path into a string nested inside a list or map.
    """
    return {
        "op": aerospike._OP_STRING_PREPEND,
        "bin": bin_name,
        "value": value,
        "policy": policy,
        "ctx": ctx
    }


def concat(bin_name: str, value_list: list[str], policy: StringPolicy | None = None, ctx: TypeCTX = None):
    """
    Create string ``concat`` operation that appends each string element in values to
    the bin in order.

    If the bin doesn't exist, this operation will create a new bin.

    Args:

        bin_name: name of string bin.
        value_list: The list of values to append.
        policy: String policy.
        ctx: Optional path into a string nested inside a list or map.
    """
    return {
        "op": aerospike._OP_STRING_CONCAT,
        "bin": bin_name,
        "value_list": value_list,
        "policy": policy,
        "ctx": ctx
    }


def snip(bin_name: str, start: int, end: int, policy: StringPolicy | None = None, ctx: TypeCTX = None):
    """
    Create string ``snip`` operation that removes codepoints from start to end.

    If the bin doesn't exist, this operation will be a no-op.

    Args:

        bin_name: name of string bin.
        start: First codepoint to remove, inclusive.
        end: One past the last codepoint to remove, exclusive.
        policy: String policy.
        ctx: Optional path into a string nested inside a list or map.
    """
    return {
        "op": aerospike._OP_STRING_SNIP,
        "bin": bin_name,
        "start": start,
        "end": end,
        "policy": policy,
        "ctx": ctx
    }


def replace(bin_name: str, needle: str, replacement: str, policy: StringPolicy | None = None, ctx: TypeCTX = None):
    """
    Create string ``replace`` operation that replaces the first occurrence of needle
    with replacement.

    If the bin doesn't exist, this operation will be a no-op.

    Args:

        bin_name: name of string bin.
        needle: the string to replace.
        replacement: the string to replace with.
        policy: String policy.
        ctx: Optional path into a string nested inside a list or map.
    """
    return {
        "op": aerospike._OP_STRING_REPLACE,
        "bin": bin_name,
        "needle": needle,
        "replacement": replacement,
        "policy": policy,
        "ctx": ctx
    }


def replace_all(bin_name: str, needle: str, replacement: str, policy: StringPolicy | None = None, ctx: TypeCTX = None):
    """
    Create string ``replace_all`` operation that replaces every occurrence of needle
    with replacement.

    If the bin doesn't exist, this operation will be a no-op.

    Args:

        bin_name: name of string bin.
        needle: the string to replace.
        replacement: the string to replace with.
        policy: String policy.
        ctx: Optional path into a string nested inside a list or map.
    """
    return {
        "op": aerospike._OP_STRING_REPLACE_ALL,
        "bin": bin_name,
        "needle": needle,
        "replacement": replacement,
        "policy": policy,
        "ctx": ctx
    }


def upper(bin_name: str, policy: StringPolicy | None = None, ctx: TypeCTX = None):
    """
    Create string ``upper`` operation that uppercases the bin in place.

    If the bin doesn't exist, this operation will be a no-op.

    Args:

        bin_name: name of string bin.
        policy: String policy.
        ctx: Optional path into a string nested inside a list or map.
    """
    return {
        "op": aerospike._OP_STRING_UPPER,
        "bin": bin_name,
        "policy": policy,
        "ctx": ctx
    }


def lower(bin_name: str, policy: StringPolicy | None = None, ctx: TypeCTX = None):
    """
    Create string ``lower`` operation that lowercases the bin in place.

    If the bin doesn't exist, this operation will be a no-op.

    Args:

        bin_name: name of string bin.
        policy: String policy.
        ctx: Optional path into a string nested inside a list or map.
    """
    return {
        "op": aerospike._OP_STRING_LOWER,
        "bin": bin_name,
        "policy": policy,
        "ctx": ctx
    }


def casefold(bin_name: str, policy: StringPolicy | None = None, ctx: TypeCTX = None):
    """
    Create string ``case_fold`` operation that applies locale-independent case folding
    (lowercase) to the bin.

    This is useful for normalized comparison keys.
    If the bin doesn't exist, this operation will be a no-op.

    Args:

        bin_name: name of string bin.
        policy: String policy.
        ctx: Optional path into a string nested inside a list or map.
    """
    return {
        "op": aerospike._OP_STRING_CASE_FOLD,
        "bin": bin_name,
        "policy": policy,
        "ctx": ctx
    }


def normalize_nfc(bin_name: str, policy: StringPolicy | None = None, ctx: TypeCTX = None):
    """
    Create string ``normalize_nfc`` operation that normalizes the bin to Unicode NFC.

    Already-normalized strings are unchanged.
    If the bin doesn't exist, this operation will be a no-op.

    Args:

        bin_name: name of string bin.
        policy: String policy.
        ctx: Optional path into a string nested inside a list or map.
    """
    return {
        "op": aerospike._OP_STRING_NORMALIZE_NFC,
        "bin": bin_name,
        "policy": policy,
        "ctx": ctx
    }


def trim_start(bin_name: str, policy: StringPolicy | None = None, ctx: TypeCTX = None):
    """
    Create string ``trim_start`` operation that removes whitespace from the start of
    the bin.

    If the bin doesn't exist, this operation will be a no-op.

    Args:

        bin_name: name of string bin.
        policy: String policy.
        ctx: Optional path into a string nested inside a list or map.
    """
    return {
        "op": aerospike._OP_STRING_TRIM_START,
        "bin": bin_name,
        "policy": policy,
        "ctx": ctx
    }


def trim_end(bin_name: str, policy: StringPolicy | None = None, ctx: TypeCTX = None):
    """
    Create string ``trim_end`` operation that removes whitespace from the end of the
    bin.

    If the bin doesn't exist, this operation will be a no-op.

    Args:

        bin_name: name of string bin.
        policy: String policy.
        ctx: Optional path into a string nested inside a list or map.
    """
    return {
        "op": aerospike._OP_STRING_TRIM_END,
        "bin": bin_name,
        "policy": policy,
        "ctx": ctx
    }


def trim(bin_name: str, policy: StringPolicy | None = None, ctx: TypeCTX = None):
    """
    Create string ``trim`` operation that removes whitespace from both ends of the bin.

    If the bin doesn't exist, this operation will be a no-op.

    Args:

        bin_name: name of string bin.
        policy: String policy.
        ctx: Optional path into a string nested inside a list or map.
    """
    return {
        "op": aerospike._OP_STRING_TRIM,
        "bin": bin_name,
        "policy": policy,
        "ctx": ctx
    }


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

    If the bin doesn't exist, this operation will create a new bin.

    Args:

        bin_name: name of string bin.
        target_length: the target length of the string.
        pad_string: the string to pad with.
        policy: String policy.
        ctx: Optional path into a string nested inside a list or map.
    """
    return {
        "op": aerospike._OP_STRING_PAD_START,
        "target_length": target_length,
        "pad_string": pad_string,
        "bin": bin_name,
        "policy": policy,
        "ctx": ctx
    }


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

    If the bin doesn't exist, this operation will create a new bin.

    Args:

        bin_name: name of string bin.
        target_length: the target length of the string.
        pad_string: the string to pad with.
        policy: String policy.
        ctx: Optional path into a string nested inside a list or map.
    """
    return {
        "op": aerospike._OP_STRING_PAD_END,
        "target_length": target_length,
        "pad_string": pad_string,
        "bin": bin_name,
        "policy": policy,
        "ctx": ctx
    }


def repeat(bin_name: str, count: int, policy: StringPolicy | None = None, ctx: TypeCTX = None):
    """
    Create string ``repeat`` operation that repeats the bin contents count times.

    If the bin doesn't exist, this operation will create a new bin.

    Args:

        bin_name: name of string bin.
        count: The number of times to repeat the string. Must be non-negative.
        policy: String policy.
        ctx: Optional path into a string nested inside a list or map.
    """
    return {
        "op": aerospike._OP_STRING_REPEAT,
        "bin": bin_name,
        "count": count,
        "policy": policy,
        "ctx": ctx
    }


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

    If the bin doesn't exist, this operation will be a no-op.

    Args:

        bin_name: name of string bin.
        pattern: the regex pattern to match against.
        replacement: the string to replace with.
        regex_flags: The regex flags to use.
        policy: No-op.
        ctx: Optional path into a string nested inside a list or map.
    """
    return {
        "op": aerospike._OP_STRING_REGEX_REPLACE,
        "bin": bin_name,
        "pattern": pattern,
        "replacement": replacement,
        "regex_flags": regex_flags,
        "policy": policy,
        "ctx": ctx
    }


def to_string(bin_name: str):
    """
    Create ``to_string`` operation that converts an integer, double, string, or blob
    bin to its string representation. Raises :exc:`~aerospike.exception.BinIncompatibleType` for
    any other bin type. This top-level operation does not accept ctx and does not
    send a msgpack payload.

    Args:

        bin_name: name of string bin.
    """
    return {
        "op": aerospike._OP_STRING_TO_STRING,
        "bin": bin_name,
    }
