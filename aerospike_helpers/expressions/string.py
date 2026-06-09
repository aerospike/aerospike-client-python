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
String expressions contain expressions for reading and modifying strings. Most of
these operations are from the standard :mod:`String API <aerospike_helpers.operations.string_operations>`.

"""


import aerospike
from aerospike_helpers.expressions.resources import _BaseExpr
from aerospike_helpers.expressions.base import StrBin
from aerospike_helpers.operations import string_operations as str_ops
from ..string_helpers import RegexFlags, StringPolicy, NumericType, __generate_docstrings_for_all_func_members
import inspect
import sys


TypeBinName = _BaseExpr | str


class StrLen(_BaseExpr):

    _op = aerospike._AS_STRING_OP_STRLEN

    def __init__(self, bin: "TypeBinName"):
        """
        Args:

            {bin}

        Returns:

            The length of the string in the bin.
        """
        self._children = (
            bin if isinstance(bin, _BaseExpr) else StrBin(bin),
        )


class SubStr(_BaseExpr):

    _op = aerospike._AS_STRING_OP_SUBSTR

    def __init__(self, start: int, length: int | None, bin: "TypeBinName"):
        """
        Args:

            start: The starting index of the substring.
            length: The length of the substring.
            {bin}

        Returns:

            The substring of the string in the bin.
        """
        pass


class CharAt(_BaseExpr):
    _op = aerospike._AS_STRING_OP_CHAR_AT

    def __init__(self, index: int, bin: "TypeBinName"):
        """
        Args:

            index: the index of the codepoint to return.
            {bin}

        Returns:

            The codepoint at the index in the string in the bin.
        """
        pass


class Find(_BaseExpr):
    _op = aerospike._AS_STRING_OP_FIND

    def __init__(self, needle: str, occurrence: int, bin: "TypeBinName"):
        """
        Args:

            {needle_get}
            occurrence: the occurrence of the string to search for.
            {bin}

        Returns:

            The index of the occurrence of the string in the bin.
        """
        pass


class Contains(_BaseExpr):
    _op = aerospike._AS_STRING_OP_CONTAINS

    def __init__(self, needle: str, bin: "TypeBinName"):
        """
        Args:

            {needle_get}
            {bin}

        Returns:

            true if the string contains the string, false otherwise.
        """
        pass


class StartsWith(_BaseExpr):
    _op = aerospike._AS_STRING_OP_STARTS_WITH

    def __init__(self, prefix: str, bin: "TypeBinName"):
        """
        Args:

            prefix: the string to search for.
            {bin}

        Returns:

            true if the string starts with the string, false otherwise.
        """
        pass


class EndsWith(_BaseExpr):
    _op = aerospike._AS_STRING_OP_ENDS_WITH

    def __init__(self, suffix: str, bin: "TypeBinName"):
        """
        Args:

            suffix: the string to search for.
            {bin}

        Returns:

            true if the string ends with the string, false otherwise.
        """
        pass


class ToInteger(_BaseExpr):
    _op = aerospike._AS_STRING_OP_TO_INTEGER

    def __init__(self, bin: "TypeBinName"):
        """
        Args:

            {bin}

        Returns:

            The integer value of the string in the bin.
        """
        pass


class ToDouble(_BaseExpr):
    _op = aerospike._AS_STRING_OP_TO_DOUBLE

    def __init__(self, bin: "TypeBinName"):
        """
        Args:

            {bin}

        Returns:

            The double value of the string in the bin.
        """
        pass


class ByteLength(_BaseExpr):
    _op = aerospike._AS_STRING_OP_BYTE_LENGTH

    def __init__(self, bin: "TypeBinName"):
        """
        Args:

            {bin}

        Returns:

            The number of bytes in the string in the bin.
        """
        pass


class IsNumeric(_BaseExpr):
    _op = aerospike._AS_STRING_OP_IS_NUMERIC

    def __init__(self, bin: "TypeBinName", numeric_type: NumericType = NumericType.ANY):
        """
        Args:

            numeric_type: the numeric type to filter for.
            {bin}

        Returns:

            true if the string is a numeric value, false otherwise.
        """
        pass


class IsUpper(_BaseExpr):
    _op = aerospike._AS_STRING_OP_IS_UPPER

    def __init__(self, bin: "TypeBinName"):
        """
        Args:

            {bin}

        Returns:

            true if the string is uppercase, false otherwise.
        """
        pass


class IsLower(_BaseExpr):
    _op = aerospike._AS_STRING_OP_IS_LOWER

    def __init__(self, bin: "TypeBinName"):
        """
        Args:

            {bin}

        Returns:

            true if the string is lowercase, false otherwise.
        """
        pass


class ToBlob(_BaseExpr):
    _op = aerospike._AS_STRING_OP_TO_BLOB

    def __init__(self, bin: "TypeBinName"):
        """
        Args:

            {bin}

        Returns:

            The blob value of the string in the bin.
        """
        pass


# TODO: move optional args for the classes above.
class Split(_BaseExpr):
    _op = aerospike.AS_STRING_OP_SPLIT

    def __init__(self, bin: "TypeBinName", separator: str = " "):
        """
        Args:

            {bin}
            separator: The separator to split by.

        Returns:

            The list of strings in the bin.
        """
        pass


class Base64Decode(_BaseExpr):
    _op = aerospike._AS_STRING_OP_B64_DECODE

    def __init__(self, bin: "TypeBinName"):
        """
        Args:

            {bin}

        Returns:

            The blob value of the string in the bin.
        """
        pass


class RegexCompare(_BaseExpr):
    _op = aerospike._AS_STRING_OP_REGEX_COMPARE

    def __init__(self, pattern: str, bin: "TypeBinName", regex_flags: RegexFlags = RegexFlags.DEFAULT):
        """
        Args:

            {bin}
            {regex_flags}
            {pattern}

        Returns:

            true if the pattern matches, false otherwise.
        """
        pass


class Insert(_BaseExpr):
    _op = aerospike._AS_STRING_OP_INSERT

    def __init__(self, policy: StringPolicy, index: int, value: str, bin: "TypeBinName"):
        """
        Args:

            {str_policy}
            index: The index of the codepoint to insert at.
            value: The value to insert.
            {bin}

        Returns:

            The string in the bin with the value inserted.
        """
        pass


class Overwrite(_BaseExpr):
    _op = aerospike._AS_STRING_OP_OVERWRITE

    def __init__(self, policy: StringPolicy, index: int, value: str, bin: "TypeBinName"):
        """
        Args:

            {str_policy}
            index: The index of the codepoint to insert at.
            value: The value to insert.
            {bin}

        Returns:

            The string in the bin with the value overwritten.
        """
        pass


class Concat(_BaseExpr):
    _op = aerospike._AS_STRING_OP_CONCAT

    def __init__(self, policy: StringPolicy, value: str, bin: "TypeBinName"):
        """
        Args:

            {str_policy}
            value: The value to append.
            {bin}

        Returns:

            The string in the bin with the value appended.
        """
        pass


class ConcatList(_BaseExpr):
    _op = aerospike._AS_STRING_OP_CONCAT

    def __init__(self, policy: StringPolicy, values: list[str], bin: "TypeBinName"):
        """
        Args:

            {str_policy}
            values: an expression that evaluates to the list of values to append.
            {bin}

        Returns:

            The string in the bin with the values appended.
        """
        pass


class Snip(_BaseExpr):
    _op = aerospike._AS_STRING_OP_SNIP

    def __init__(self, policy: StringPolicy, start: int, end: int | None, bin: "TypeBinName"):
        """
        Args:

            {str_policy}
            start: The index of the codepoint to remove from.
            end: The index of the codepoint to remove to.
            {bin}

        Returns:

            The string in the bin with the value snipped.
        """
        pass


class Replace(_BaseExpr):
    _op = aerospike._AS_STRING_OP_REPLACE

    def __init__(self, policy: StringPolicy, needle: str, replacement: str, bin: "TypeBinName"):
        """
        Args:

            {str_policy}
            {needle_to_replace}
            {replacement}
            {bin}

        Returns:

            The string in the bin with the value replaced.
        """
        pass


class ReplaceAll(_BaseExpr):
    _op = aerospike._AS_STRING_OP_REPLACE_ALL

    def __init__(self, policy: StringPolicy, needle: str, replacement: str, bin: "TypeBinName"):
        """
        Args:

            {str_policy}
            {needle_to_replace}
            {replacement}
            {bin}

        Returns:

            The string in the bin with the value replaced.
        """
        pass


class Upper(_BaseExpr):
    _op = aerospike._AS_STRING_OP_UPPER

    def __init__(self, policy: StringPolicy, bin: "TypeBinName"):
        """
        Args:

            {str_policy}
            {bin}

        Returns:

            The string in the bin with the value uppercased.
        """
        pass


class Lower(_BaseExpr):
    _op = aerospike._AS_STRING_OP_LOWER

    def __init__(self, policy: StringPolicy, bin: "TypeBinName"):
        """
        Args:

            {str_policy}
            {bin}

        Returns:

            The string in the bin with the value lowercased.
        """
        pass


class CaseFold(_BaseExpr):
    _op = aerospike._AS_STRING_OP_CASE_FOLD

    def __init__(self, policy: StringPolicy, bin: "TypeBinName"):
        """
        Args:

            {str_policy}
            {bin}

        Returns:

            The string in the bin with the value case folded.
        """
        pass


class NormalizeNFC(_BaseExpr):
    _op = aerospike._AS_STRING_OP_NORMALIZE_NFC

    def __init__(self, policy: StringPolicy, bin: "TypeBinName"):
        """
        Args:

            {str_policy}
            {bin}

        Returns:

            The string in the bin with the value normalized.
        """
        pass


class TrimStart(_BaseExpr):
    _op = aerospike._AS_STRING_OP_TRIM_START

    def __init__(self, policy: StringPolicy, bin: "TypeBinName"):
        """
        Args:

            {str_policy}
            {bin}

        Returns:

            The string in the bin with the value trimmed.
        """
        pass


class TrimEnd(_BaseExpr):
    _op = aerospike._AS_STRING_OP_TRIM_END

    def __init__(self, policy: StringPolicy, bin: "TypeBinName"):
        """
        Args:

            {str_policy}
            {bin}

        Returns:

            The string in the bin with the value trimmed.
        """
        pass


class Trim(_BaseExpr):
    _op = aerospike._AS_STRING_OP_TRIM

    def __init__(self, policy: StringPolicy, bin: "TypeBinName"):
        """
        Args:

            {str_policy}
            {bin}

        Returns:

            The string in the bin with the value trimmed.
        """
        pass


class PadStart(_BaseExpr):
    _op = aerospike._AS_STRING_OP_PAD_START

    def __init__(self, policy: StringPolicy, target_length: int, pad_string: str, bin: "TypeBinName"):
        """
        Args:

            {str_policy}
            {target_length}
            {pad_string}
            {bin}

        Returns:

            The string in the bin with the value padded.
        """
        pass


class PadEnd(_BaseExpr):
    _op = aerospike._AS_STRING_OP_PAD_END

    def __init__(self, policy: StringPolicy, target_length: int, pad_string: str, bin: "TypeBinName"):
        """
        Args:

            {str_policy}
            {target_length}
            {pad_string}
            {bin}

        Returns:

            The string in the bin with the value padded.
        """
        pass


class Repeat(_BaseExpr):
    _op = aerospike._AS_STRING_OP_REPEAT

    def __init__(self, policy: StringPolicy, count: int, bin: "TypeBinName"):
        """
        Args:

            {str_policy}
            count: the number of times to repeat the string.
            {bin}

        Returns:

            The string in the bin with the value repeated.
        """
        pass


class RegexReplace(_BaseExpr):
    _op = aerospike.AS_STRING_OP_REGEX_REPLACE

    def __init__(
            self,
            policy: StringPolicy,
            pattern: str,
            replacement: int,
            regex_flags: RegexFlags,
            bin: "TypeBinName"
    ):
        """
        Args:

            {str_policy}
            {pattern}
            {replacement}
            {regex_flags}
            {bin}

        Returns:

            The string in the bin with the value replaced.
        """
        pass


class ToString(_BaseExpr):
    _op = aerospike._AS_EXP_CODE_CALL

    def __init__(self, bin: "TypeBinName"):
        """
        Args:

            {bin}

        Returns:

            The string in the bin with the value converted to a string.

        """
        pass


__exp_class_to_op_func = {
    StrLen: str_ops.strlen,
    SubStr: str_ops.substr,
    CharAt: str_ops.char_at,
    Find: str_ops.find,
    Contains: str_ops.contains,
    StartsWith: str_ops.starts_with,
    EndsWith: str_ops.ends_with,
    ToInteger: str_ops.to_integer,
    ToDouble: str_ops.to_double,
    ByteLength: str_ops.byte_length,
    IsNumeric: str_ops.is_numeric,
    IsUpper: str_ops.is_upper,
    IsLower: str_ops.is_lower,
    ToBlob: str_ops.to_blob,
    Split: str_ops.split,
    Base64Decode: str_ops.base64_decode,
    RegexCompare: str_ops.regex_compare,
    Insert: str_ops.insert,
    Overwrite: str_ops.overwrite,
    Concat: str_ops.concat,
    ConcatList: str_ops.concat_list,
    Snip: str_ops.snip,
    Replace: str_ops.replace,
    ReplaceAll: str_ops.replace_all,
    Upper: str_ops.upper,
    Lower: str_ops.lower,
    CaseFold: str_ops.casefold,
    NormalizeNFC: str_ops.normalize_nfc,
    TrimStart: str_ops.trim_start,
    TrimEnd: str_ops.trim_end,
    Trim: str_ops.trim,
    PadStart: str_ops.pad_start,
    PadEnd: str_ops.pad_end,
    Repeat: str_ops.repeat,
    RegexReplace: str_ops.regex_replace,
    ToString: str_ops.to_string
}

__this_module = sys.modules[__name__]
__all_classes = inspect.getmembers(__this_module, predicate=inspect.isclass)
kwargs = {
    "bin": "bin: A bin expression to apply this function to."
}

for _, cls_value in __all_classes:
    if cls_value.__module__ != __name__:
        continue

    __generate_docstrings_for_all_func_members(cls_value, kwargs)
    op_func = __exp_class_to_op_func[cls_value]
    cls_value.__doc__ = f"Create an expression that performs a {op_func} operation."
