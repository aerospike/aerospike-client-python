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
from aerospike_helpers.expressions.resources import _BaseExpr, _Keys
from aerospike_helpers.expressions.base import StrBin
from aerospike_helpers.operations import string_operations as str_ops
from ..string_helpers import RegexFlags, StringPolicy, NumericType
import inspect
import sys


TypeBinName = _BaseExpr | str


def _convert_bin_name_to_expr(bin: "TypeBinName"):
    return bin if isinstance(bin, _BaseExpr) else StrBin(bin)


class StrLen(_BaseExpr):

    _op = aerospike._OP_STRING_STRLEN

    def __init__(self, bin: "TypeBinName"):
        """
        Args:

            bin: A bin expression to apply this function to.

        Returns:

            The length of the string in the bin.
        """
        self._children = (
            _convert_bin_name_to_expr(bin),
        )


class SubStr(_BaseExpr):

    _op = aerospike._OP_STRING_SUBSTR

    def __init__(self, start: int, bin: "TypeBinName"):
        """
        Args:

            start: The starting index of the substring.
            bin: A bin expression to apply this function to.

        Returns:

            The substring of the string in the bin.
        """
        self._fixed = {
            aerospike._STR_EXP_START_KEY: start,
        }
        self._children = (
            _convert_bin_name_to_expr(bin),
        )


class SubStrRange(_BaseExpr):

    _op = aerospike._OP_STRING_SUBSTR_RANGE

    def __init__(self, start: int, end: int, bin: "TypeBinName"):
        """
        Args:

            start: Starting codepoint index, inclusive.
            end: Ending codepoint index, exclusive.
            bin: A bin expression to apply this function to.

        Returns:

            The substring of the string in the bin.
        """
        self._fixed = {
            aerospike._STR_EXP_START_KEY: start,
            aerospike._STR_EXP_END_KEY: end
        }
        self._children = (
            _convert_bin_name_to_expr(bin),
        )


class CharAt(_BaseExpr):
    _op = aerospike._OP_STRING_CHAR_AT

    def __init__(self, index: int, bin: "TypeBinName"):
        """
        Args:

            index: the index of the codepoint to return.
            bin: A bin expression to apply this function to.

        Returns:

            The codepoint at the index in the string in the bin.
        """
        self._fixed = {
            aerospike._STR_EXP_INDEX_KEY: index
        }
        self._children = (
            _convert_bin_name_to_expr(bin),
        )


class Find(_BaseExpr):
    _op = aerospike._OP_STRING_FIND

    def __init__(self, needle: str, occurrence: int, bin: "TypeBinName"):
        """
        Args:

            needle: the string to search for.
            occurrence: the occurrence of the string to search for.
            bin: A bin expression to apply this function to.

        Returns:

            The index of the occurrence of the string in the bin.
        """
        self._fixed = {
            aerospike._STR_EXP_NEEDLE_KEY: needle,
            aerospike._STR_EXP_OCCURRENCE_KEY: occurrence
        }
        self._children = (
            _convert_bin_name_to_expr(bin),
        )


class Contains(_BaseExpr):
    _op = aerospike._OP_STRING_CONTAINS

    def __init__(self, needle: str, bin: "TypeBinName"):
        """
        Args:

            needle: the string to search for.
            bin: A bin expression to apply this function to.

        Returns:

            true if the string contains the string, false otherwise.
        """
        self._fixed = {
            aerospike._STR_EXP_NEEDLE_KEY: needle,
        }
        self._children = (_convert_bin_name_to_expr(bin),)


class StartsWith(_BaseExpr):
    _op = aerospike._OP_STRING_STARTS_WITH

    def __init__(self, prefix: str, bin: "TypeBinName"):
        """
        Args:

            prefix: the string to search for.
            bin: A bin expression to apply this function to.

        Returns:

            true if the string starts with the string, false otherwise.
        """
        self._fixed = {
            aerospike._STR_EXP_PREFIX_KEY: prefix,
        }
        self._children = (_convert_bin_name_to_expr(bin),)


class EndsWith(_BaseExpr):
    _op = aerospike._OP_STRING_ENDS_WITH

    def __init__(self, suffix: str, bin: "TypeBinName"):
        """
        Args:

            suffix: the string to search for.
            bin: A bin expression to apply this function to.

        Returns:

            true if the string ends with the string, false otherwise.
        """
        self._fixed = {
            aerospike._STR_EXP_SUFFIX_KEY: suffix,
        }
        self._children = (_convert_bin_name_to_expr(bin),)


class ToInteger(_BaseExpr):
    _op = aerospike._OP_STRING_TO_INTEGER

    def __init__(self, bin: "TypeBinName"):
        """
        Args:

            bin: A bin expression to apply this function to.

        Returns:

            The integer value of the string in the bin.
        """
        self._children = (_convert_bin_name_to_expr(bin),)


class ToDouble(_BaseExpr):
    _op = aerospike._OP_STRING_TO_DOUBLE

    def __init__(self, bin: "TypeBinName"):
        """
        Args:

            bin: A bin expression to apply this function to.

        Returns:

            The double value of the string in the bin.
        """
        self._children = (_convert_bin_name_to_expr(bin),)


class ByteLength(_BaseExpr):
    _op = aerospike._OP_STRING_BYTE_LENGTH

    def __init__(self, bin: "TypeBinName"):
        """
        Args:

            bin: A bin expression to apply this function to.

        Returns:

            The number of bytes in the string in the bin.
        """
        self._children = (_convert_bin_name_to_expr(bin),)


class IsNumeric(_BaseExpr):
    _op = aerospike._OP_STRING_IS_NUMERIC

    def __init__(self, numeric_type: NumericType, bin: "TypeBinName"):
        """
        Args:

            numeric_type: the numeric type to filter for.
            bin: A bin expression to apply this function to.

        Returns:

            true if the string is a numeric value, false otherwise.
        """
        self._fixed = {
            aerospike._STR_EXP_NUMERIC_TYPE_KEY: numeric_type
        }
        self._children = (
            _convert_bin_name_to_expr(bin),
        )


class IsUpper(_BaseExpr):
    _op = aerospike._OP_STRING_IS_UPPER

    def __init__(self, bin: "TypeBinName"):
        """
        Args:

            bin: A bin expression to apply this function to.

        Returns:

            true if the string is uppercase, false otherwise.
        """
        self._children = (_convert_bin_name_to_expr(bin),)


class IsLower(_BaseExpr):
    _op = aerospike._OP_STRING_IS_LOWER

    def __init__(self, bin: "TypeBinName"):
        """
        Args:

            bin: A bin expression to apply this function to.

        Returns:

            true if the string is lowercase, false otherwise.
        """
        self._children = (_convert_bin_name_to_expr(bin),)


class ToBlob(_BaseExpr):
    _op = aerospike._OP_STRING_TO_BLOB

    def __init__(self, bin: "TypeBinName"):
        """
        Args:

            bin: A bin expression to apply this function to.

        Returns:

            The blob value of the string in the bin.
        """
        self._children = (_convert_bin_name_to_expr(bin),)


class Split(_BaseExpr):
    _op = aerospike._OP_STRING_SPLIT

    def __init__(self, bin: "TypeBinName"):
        """
        Args:

            bin: A bin expression to apply this function to.

        Returns:

            The list of strings in the bin.
        """
        self._children = (_convert_bin_name_to_expr(bin),)


class SplitSeparator(_BaseExpr):
    _op = aerospike._OP_STRING_SPLIT_SEPARATOR

    def __init__(self, separator: str, bin: "TypeBinName"):
        """
        Args:

            separator: The separator to split by.
            bin: A bin expression to apply this function to.

        Returns:

            The list of strings in the bin.
        """
        self._fixed = {
            aerospike._STR_EXP_SEPARATOR_KEY: separator
        }
        self._children = (_convert_bin_name_to_expr(bin),)


class Base64Decode(_BaseExpr):
    _op = aerospike._OP_STRING_B64_DECODE

    def __init__(self, bin: "TypeBinName"):
        """
        Args:

            bin: A bin expression to apply this function to.

        Returns:

            The blob value of the string in the bin.
        """
        self._children = (_convert_bin_name_to_expr(bin),)


class RegexCompare(_BaseExpr):
    _op = aerospike._OP_STRING_REGEX_COMPARE

    def __init__(self, pattern: str, bin: "TypeBinName", regex_flags: RegexFlags = RegexFlags.DEFAULT):
        """
        Args:

            bin: A bin expression to apply this function to.
            regex_flags: The regex flags to use.
            pattern: the regex pattern to match against.

        Returns:

            true if the pattern matches, false otherwise.
        """
        self._fixed = {
            aerospike._STR_EXP_PATTERN_KEY: pattern,
            aerospike._STR_EXP_REGEX_FLAGS_KEY: regex_flags
        }
        self._children = (_convert_bin_name_to_expr(bin),)


class _WriteOp(_BaseExpr):
    def __init__(self, policy: StringPolicy):
        self._fixed = {
            aerospike._STR_EXP_POLICY_KEY: policy
        }


class Insert(_WriteOp):
    _op = aerospike._OP_STRING_INSERT

    def __init__(self, policy: StringPolicy, index: int, value: str, bin: "TypeBinName"):
        """
        Args:

            policy: String policy.
            index: The index of the codepoint to insert at.
            value: The value to insert.
            bin: A bin expression to apply this function to.

        Returns:

            The string in the bin with the value inserted.
        """
        super().__init__(policy)
        self._fixed |= {
            aerospike._STR_EXP_INDEX_KEY: index,
            _Keys.VALUE_KEY: value
        }
        self._children = (_convert_bin_name_to_expr(bin),)


class Overwrite(_WriteOp):
    _op = aerospike._OP_STRING_OVERWRITE

    def __init__(self, policy: StringPolicy, index: int, value: str, bin: "TypeBinName"):
        """
        Args:

            policy: String policy.
            index: The index of the codepoint to insert at.
            value: The value to insert.
            bin: A bin expression to apply this function to.

        Returns:

            The string in the bin with the value overwritten.
        """
        super().__init__(policy)
        self._fixed |= {
            aerospike._STR_EXP_INDEX_KEY: index,
            _Keys.VALUE_KEY: value
        }
        self._children = (_convert_bin_name_to_expr(bin),)


class Append(_WriteOp):
    _op = aerospike._OP_STRING_APPEND

    def __init__(self, policy: StringPolicy, value: str, bin: "TypeBinName"):
        """
        Args:

            policy: String policy.
            value: The value to append.
            bin: A bin expression to apply this function to.

        Returns:

            The string in the bin with the value appended.
        """
        super().__init__(policy)
        self._fixed |= {
            _Keys.VALUE_KEY: value
        }
        self._children = (_convert_bin_name_to_expr(bin),)


class Prepend(_WriteOp):
    _op = aerospike._OP_STRING_PREPEND

    def __init__(self, policy: StringPolicy, value: str, bin: "TypeBinName"):
        """
        Args:

            policy: String policy.
            value: The value to prepend.
            bin: A bin expression to apply this function to.

        Returns:

            The string in the bin with the value prepended.
        """
        super().__init__(policy)
        self._fixed |= {
            _Keys.VALUE_KEY: value
        }
        self._children = (_convert_bin_name_to_expr(bin),)


class Concat(_WriteOp):
    _op = aerospike._OP_STRING_CONCAT

    def __init__(self, policy: StringPolicy, values: list[str], bin: "TypeBinName"):
        """
        Args:

            policy: String policy.
            values: an expression that evaluates to the list of values to append.
            bin: A bin expression to apply this function to.

        Returns:

            The string in the bin with the values appended.
        """
        super().__init__(policy)
        self._children = (values, _convert_bin_name_to_expr(bin),)


class Snip(_WriteOp):
    _op = aerospike._OP_STRING_SNIP

    def __init__(self, policy: StringPolicy, start: int, end: int, bin: "TypeBinName"):
        """
        Args:

            policy: String policy.
            start: First codepoint to remove, inclusive.
            end: One past the last codepoint to remove, exclusive.
            bin: A bin expression to apply this function to.

        Returns:

            The string in the bin with the value snipped.
        """
        super().__init__(policy)
        self._fixed |= {
            aerospike._STR_EXP_START_KEY: start,
            aerospike._STR_EXP_END_KEY: end
        }
        self._children = (_convert_bin_name_to_expr(bin),)


class Replace(_WriteOp):
    _op = aerospike._OP_STRING_REPLACE

    def __init__(self, policy: StringPolicy, needle: str, replacement: str, bin: "TypeBinName"):
        """
        Args:

            policy: String policy.
            needle: the string to replace.
            replacement: the string to replace with.
            bin: A bin expression to apply this function to.

        Returns:

            The string in the bin with the value replaced.
        """
        super().__init__(policy)
        self._fixed |= {
            aerospike._STR_EXP_NEEDLE_KEY: needle,
            aerospike._STR_EXP_REPLACEMENT_KEY: replacement
        }
        self._children = (_convert_bin_name_to_expr(bin),)


class ReplaceAll(_WriteOp):
    _op = aerospike._OP_STRING_REPLACE_ALL

    def __init__(self, policy: StringPolicy, needle: str, replacement: str, bin: "TypeBinName"):
        """
        Args:

            policy: String policy.
            needle: the string to replace.
            replacement: the string to replace with.
            bin: A bin expression to apply this function to.

        Returns:

            The string in the bin with the value replaced.
        """
        super().__init__(policy)
        self._fixed |= {
            aerospike._STR_EXP_NEEDLE_KEY: needle,
            aerospike._STR_EXP_REPLACEMENT_KEY: replacement
        }
        self._children = (_convert_bin_name_to_expr(bin),)


class Upper(_WriteOp):
    _op = aerospike._OP_STRING_UPPER

    def __init__(self, policy: StringPolicy, bin: "TypeBinName"):
        """
        Args:

            policy: String policy.
            bin: A bin expression to apply this function to.

        Returns:

            The string in the bin with the value uppercased.
        """
        super().__init__(policy)
        self._children = (_convert_bin_name_to_expr(bin),)


class Lower(_WriteOp):
    _op = aerospike._OP_STRING_LOWER

    def __init__(self, policy: StringPolicy, bin: "TypeBinName"):
        """
        Args:

            policy: String policy.
            bin: A bin expression to apply this function to.

        Returns:

            The string in the bin with the value lowercased.
        """
        super().__init__(policy)
        self._children = (_convert_bin_name_to_expr(bin),)


class CaseFold(_WriteOp):
    _op = aerospike._OP_STRING_CASE_FOLD

    def __init__(self, policy: StringPolicy, bin: "TypeBinName"):
        """
        Args:

            policy: String policy.
            bin: A bin expression to apply this function to.

        Returns:

            The string in the bin with the value case folded.
        """
        super().__init__(policy)
        self._children = (_convert_bin_name_to_expr(bin),)


class NormalizeNFC(_WriteOp):
    _op = aerospike._OP_STRING_NORMALIZE_NFC

    def __init__(self, policy: StringPolicy, bin: "TypeBinName"):
        """
        Args:

            policy: String policy.
            bin: A bin expression to apply this function to.

        Returns:

            The string in the bin with the value normalized.
        """
        super().__init__(policy)
        self._children = (_convert_bin_name_to_expr(bin),)


class TrimStart(_WriteOp):
    _op = aerospike._OP_STRING_TRIM_START

    def __init__(self, policy: StringPolicy, bin: "TypeBinName"):
        """
        Args:

            policy: String policy.
            bin: A bin expression to apply this function to.

        Returns:

            The string in the bin with the value trimmed.
        """
        super().__init__(policy)
        self._children = (_convert_bin_name_to_expr(bin),)


class TrimEnd(_WriteOp):
    _op = aerospike._OP_STRING_TRIM_END

    def __init__(self, policy: StringPolicy, bin: "TypeBinName"):
        """
        Args:

            policy: String policy.
            bin: A bin expression to apply this function to.

        Returns:

            The string in the bin with the value trimmed.
        """
        super().__init__(policy)
        self._children = (_convert_bin_name_to_expr(bin),)


class Trim(_WriteOp):
    _op = aerospike._OP_STRING_TRIM

    def __init__(self, policy: StringPolicy, bin: "TypeBinName"):
        """
        Args:

            policy: String policy.
            bin: A bin expression to apply this function to.

        Returns:

            The string in the bin with the value trimmed.
        """
        super().__init__(policy)
        self._children = (_convert_bin_name_to_expr(bin),)


class PadStart(_WriteOp):
    _op = aerospike._OP_STRING_PAD_START

    def __init__(self, policy: StringPolicy, target_length: int, pad_string: str, bin: "TypeBinName"):
        """
        Args:

            policy: String policy.
            target_length: the target length of the string.
            pad_string: the string to pad with.
            bin: A bin expression to apply this function to.

        Returns:

            The string in the bin with the value padded.
        """
        super().__init__(policy)
        self._fixed |= {
            aerospike._STR_EXP_TARGET_LENGTH_KEY: target_length,
            aerospike._STR_EXP_PAD_STRING_KEY: pad_string
        }
        self._children = (_convert_bin_name_to_expr(bin),)


class PadEnd(_WriteOp):
    _op = aerospike._OP_STRING_PAD_END

    def __init__(self, policy: StringPolicy, target_length: int, pad_string: str, bin: "TypeBinName"):
        """
        Args:

            policy: String policy.
            target_length: the target length of the string.
            pad_string: the string to pad with.
            bin: A bin expression to apply this function to.

        Returns:

            The string in the bin with the value padded.
        """
        super().__init__(policy)
        self._fixed |= {
            aerospike._STR_EXP_TARGET_LENGTH_KEY: target_length,
            aerospike._STR_EXP_PAD_STRING_KEY: pad_string
        }
        self._children = (_convert_bin_name_to_expr(bin),)


class Repeat(_WriteOp):
    _op = aerospike._OP_STRING_REPEAT

    def __init__(self, policy: StringPolicy, count: int, bin: "TypeBinName"):
        """
        Args:

            policy: String policy.
            count: the number of times to repeat the string.
            bin: A bin expression to apply this function to.

        Returns:

            The string in the bin with the value repeated.
        """
        super().__init__(policy)
        self._fixed |= {
            aerospike._STR_EXP_COUNT_KEY: count,
        }
        self._children = (_convert_bin_name_to_expr(bin),)


class RegexReplace(_WriteOp):
    _op = aerospike._OP_STRING_REGEX_REPLACE

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

            policy: No-op.
            pattern: the regex pattern to match against.
            replacement: the string to replace with.
            regex_flags: The regex flags to use.
            bin: A bin expression to apply this function to.

        Returns:

            The string in the bin with the value replaced.
        """
        super().__init__(policy)
        self._fixed |= {
            aerospike._STR_EXP_PATTERN_KEY: pattern,
            aerospike._STR_EXP_REPLACEMENT_KEY: replacement,
            aerospike._STR_EXP_REGEX_FLAGS_KEY: regex_flags
        }
        self._children = (_convert_bin_name_to_expr(bin),)


class ToString(_WriteOp):
    _op = aerospike._AS_EXP_CODE_CALL

    def __init__(self, bin: "TypeBinName"):
        """
        Args:

            bin: A bin expression to apply this function to.

        Returns:

            The string in the bin with the value converted to a string.

        """
        self._children = (_convert_bin_name_to_expr(bin),)


__exp_class_to_op_func = {
    StrLen: str_ops.strlen,
    SubStr: str_ops.substr,
    SubStrRange: str_ops.substr_range,
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
    SplitSeparator: str_ops.split_separator,
    Base64Decode: str_ops.base64_decode,
    RegexCompare: str_ops.regex_compare,
    Insert: str_ops.insert,
    Overwrite: str_ops.overwrite,
    Append: str_ops.append,
    Prepend: str_ops.prepend,
    Concat: str_ops.concat,
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

for _, _cls_value in __all_classes:
    if _cls_value.__module__ != __name__ or _cls_value == _WriteOp:
        continue

    _op_func = __exp_class_to_op_func[_cls_value]
    _cls_value.__doc__ = (
        "Create an expression that performs a "
        f":py:meth:`~{_op_func.__module__}.{_op_func.__qualname__}` operation."
    )
