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

from ..string_helpers import NumericType, RegexFlags, StringPolicy, __generate_docstrings
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
    return locals()


def substr(bin_name: str, start: int, length: int | None = None, ctx: TypeCTX = None):
    """
    Create string ``substr`` operation from start. If length is :py:obj:`None`,
    the operation will continue to the end of the string.
    Negative start indexes count from the end of the string.

    Args:

        {bin_name}
        {start_get}
        length (int): Number of codepoints to return.
        {ctx}
    """
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
    return locals()


def contains(bin_name: str, needle: int, ctx: TypeCTX = None):
    """
    Create string ``contains`` operation that returns true if the bin contains needle.

    Args:

        {bin_name}
        {needle_get}
        {ctx}
    """
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
    return locals()


def to_integer(bin_name: str, ctx: TypeCTX = None):
    """
    Create string ``to_integer`` operation that parses the string as an unsigned 64-bit integer.
    Raises :exc:`~aerospike.exception.ParamError` if the bin cannot be parsed as an integer.

    Args:

        {bin_name}
        {ctx}
    """
    return locals()


def to_double(bin_name: str, ctx: TypeCTX = None):
    """
    Create string ``to_double`` operation that parses the string as a 64-bit float.
    Returns :exc:`~aerospike.exception.ParamError` if the bin cannot be parsed as a double.

    Args:

        {bin_name}
        {ctx}
    """
    return locals()


def byte_length(bin_name: str, ctx: TypeCTX = None):
    """
    Create string ``byte_length`` operation that returns the number of UTF-8 bytes in
    the string as an unsigned 64-bit integer. This differs from strlen for non-ASCII text.

    Args:

        {bin_name}
        {ctx}
    """
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
    return locals()


def is_upper(bin_name: str, ctx: TypeCTX = None):
    """
    Create string ``is_upper`` operation that returns true if every cased codepoint in
    the bin is uppercase.

    Args:

        {bin_name}
        {ctx}
    """
    return locals()


def is_lower(bin_name: str, ctx: TypeCTX = None):
    """
    Create string ``is_lower`` operation that returns true if every cased codepoint in
    the bin is lowercase.

    Args:

        {bin_name}
        {ctx}
    """
    return locals()


def to_blob(bin_name: str, ctx: TypeCTX = None):
    """
    Create string ``to_blob`` operation that returns the UTF-8 bytes of the string as
    a blob.

    Args:

        {bin_name}
        {ctx}
    """
    return locals()


def split(bin_name: str, separator: str, ctx: TypeCTX = None):
    """
    Create string ``split`` operation that splits by Unicode codepoint. Each codepoint
    becomes one string element in the returned list.

    Args:

        {bin_name}
        separator (str): The separator to split by.
        {ctx}
    """
    return locals()


def base64_decode(bin_name: str, ctx: TypeCTX = None):
    """
    Create string ``b64_decode`` operation that treats the bin as base64 text and
    returns the decoded bytes as a blob.

    Args:

        {bin_name}
        {ctx}
    """
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
    return locals()


def insert(policy: StringPolicy, bin_name: str, index: int, value: str, ctx: TypeCTX = None):
    """
    Create string ``insert`` operation that splices value into the bin at codepoint
    index. Negative indexes count from the end of the string.

    Args:

        {str_policy}
        {bin_name}
        index (int): Index of the codepoint to insert at.
        value (str): The value to insert.
        {ctx}
    """
    return locals()


def overwrite(policy: StringPolicy, bin_name: str, index: int, value: str, ctx: TypeCTX = None):
    """
    Create string ``overwrite`` operation that overwrites codepoints starting at index
    with value. The result may grow beyond the original length when value extends
    past the end.

    Args:

        {str_policy}
        {bin_name}
        index (int): Index of the codepoint to overwrite at.
        value (str): The value to overwrite.
        {ctx}
    """
    return locals()


def concat(policy: StringPolicy, bin_name: str, value: str, ctx: TypeCTX = None):
    """
    Create string ``concat`` operation that appends value to the bin.

    Args:

        {str_policy}
        {bin_name}
        value (str): The value to append.
        {ctx}
    """
    return locals()


def concat_list(policy: StringPolicy, bin_name: str, values: list[str], ctx: TypeCTX = None):
    """
    Create string ``concat`` operation that appends each string element in values to
    the bin in order.

    Args:

        {str_policy}
        {bin_name}
        values (str): The list of values to append.
        {ctx}
    """
    pass


def snip(policy: StringPolicy, bin_name: str, start: int, end: int | None = None, ctx: TypeCTX = None):
    """
    Create string ``snip`` operation that removes codepoints from start through the
    end of the string.

    Args:

        {str_policy}
        {bin_name}
        start (int): The index of the codepoint to remove from.
        end (int | None): The index of the codepoint to remove to. If set to :py:obj:`None`,
            remove from start through the end of the string.
        {ctx}
    """
    return locals()


def replace(policy: StringPolicy, bin_name: str, needle: str, replacement: str, ctx: TypeCTX = None):
    """
    Create string ``replace`` operation that replaces the first occurrence of needle
    with replacement.

    Args:

        {str_policy}
        {bin_name}
        {needle_to_replace}
        {replacement}
        {ctx}
    """
    return locals()


def replace_all(policy: StringPolicy, bin_name: str, needle: str, replacement: str, ctx: TypeCTX = None):
    """
    Create string ``replace_all`` operation that replaces every occurrence of needle
    with replacement.

    Args:

        {str_policy}
        {bin_name}
        {needle_to_replace}
        {replacement}
        {ctx}
    """
    return locals()


def upper(policy: StringPolicy, bin_name: str, ctx: TypeCTX = None):
    """
    Create string ``upper`` operation that uppercases the bin in place.

    Args:

        {str_policy}
        {bin_name}
        {ctx}
    """
    return locals()


def lower(policy: StringPolicy, bin_name: str, ctx: TypeCTX = None):
    """
    Create string ``lower`` operation that lowercases the bin in place.

    Args:

        {str_policy}
        {bin_name}
        {ctx}
    """
    return locals()


# TODO: read up how this works
def casefold(policy: StringPolicy, bin_name: str, ctx: TypeCTX = None):
    """
    Create string ``case_fold`` operation that applies locale-independent case folding
    (lowercase) to the bin. This is useful for normalized comparison keys.

    Args:

        {str_policy}
        {bin_name}
        {ctx}
    """
    return locals()


# TODO: read up how this works
def normalize_nfc(policy: StringPolicy, bin_name: str, ctx: TypeCTX = None):
    """
    Create string ``normalize_nfc`` operation that normalizes the bin to Unicode NFC.
    Already-normalized strings are unchanged.

    Args:

        {str_policy}
        {bin_name}
        {ctx}
    """
    return locals()


def trim_start(policy: StringPolicy, bin_name: str, ctx: TypeCTX = None):
    """
    Create string ``trim_start`` operation that removes whitespace from the start of
    the bin.

    Args:

        {str_policy}
        {bin_name}
        {ctx}
    """
    return locals()


def trim_end(policy: StringPolicy, bin_name: str, ctx: TypeCTX = None):
    """
    Create string ``trim_end`` operation that removes whitespace from the end of the
    bin.

    Args:

        {str_policy}
        {bin_name}
        {ctx}
    """
    return locals()


def trim(policy: StringPolicy, bin_name: str, ctx: TypeCTX = None):
    """
    Create string ``trim`` operation that removes whitespace from both ends of the bin.

    Args:

        {str_policy}
        {bin_name}
        {ctx}
    """
    return locals()


def pad_start(policy: StringPolicy, bin_name: str, target_length: int, pad_string: str, ctx: TypeCTX = None):
    """
    Create string ``pad_start`` operation that prepends ``pad_string`` repeatedly until
    the bin reaches ``target_length`` codepoints. No-op when the bin is already at or
    above the target length.

    Args:

        {str_policy}
        {bin_name}
        {target_length}
        {pad_string}
        {ctx}
    """
    return locals()


def pad_end(policy: StringPolicy, bin_name: str, target_length: int, pad_string: str, ctx: TypeCTX = None):
    """
    Create string ``pad_end`` operation that appends ``pad_string`` repeatedly until the
    bin reaches ``target_length`` codepoints. No-op when the bin is already at or
    above the target length.

    Args:

        {str_policy}
        {bin_name}
        {target_length}
        {pad_string}
        {ctx}
    """
    return locals()


def repeat(policy: StringPolicy, bin_name: str, count: int, ctx: TypeCTX = None):
    """
    Create string ``repeat`` operation that repeats the bin contents count times.

    Args:

        {str_policy}
        {bin_name}
        count (int): The number of times to repeat the string. Must be non-negative.
        {ctx}
    """
    return locals()

# TODO: regex flags enum


def regex_replace(
    policy: StringPolicy,
    bin_name: str,
    pattern: str,
    replacement: str,
    regex_flags: RegexFlags,
    ctx: TypeCTX = None
):
    """
    Create string ``regex_replace`` operation that replaces the first match of pattern
    with replacement. Pass :py:attr:`~aerospike_helpers.string_helpers.RegexFlags.GLOBAL` to replace every match.
    This server operation accepts regex flags but not string policy flags.

    Args:

        {str_policy}
        {bin_name}
        {pattern}
        {replacement}
        {regex_flags}
        {ctx}
    """
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
    return locals()


# These descriptions are shared across all the string operations


kwargs = {
    "bin_name": "bin_name (str): name of string bin.",
    "start_get": "start (int): Starting codepoint index.",
    "needle_get": "needle (int): the string to search for.",
    "pattern": "pattern (str): the regex pattern to match against.",
    "regex_flags": "regex_flags (:py:class:`~aerospike_helpers.string_helpers.RegexFlags`): The regex flags to use.",
    "str_policy": "policy (:py:class:`~aerospike_helpers.string_helpers.StringPolicy`): String policy.",
    "needle_to_replace": "needle (str): the string to replace.",
    "replacement": "replacement (str): the string to replace with.",
    "target_length": "target_length (int): the target length of the string.",
    "pad_string": "pad_string (str): the string to pad with.",
    "ctx": "ctx (list | None): Optional path into a string nested inside a list or map."
}
__generate_docstrings(sys.modules[__name__], kwargs)
