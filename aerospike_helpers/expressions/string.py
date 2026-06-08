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
String expressions contain expressions for reading and modifying Lists. Most of
these operations are from the standard :mod:`List API <aerospike_helpers.operations.list_operations>`.

"""


from aerospike_helpers.expressions.resources import _BaseExpr
from ..string_helpers import RegexFlags, StringPolicy, NumericType, __generate_docstrings
import inspect
import sys


TypeBinName = _BaseExpr | str


# :py:meth:`~aerospike_helpers.operations.string_operations.strlen`
# TODO: inject docstring for each class. They all follow the same format.


class StrLen(_BaseExpr):
    def __init__(self, bin: "TypeBinName"):
        """
        Args:

            {bin}

        Returns:

            The length of the string in the bin.
        """
        pass


class SubStr(_BaseExpr):
    def __init__(self, start: int, length: int | None, bin: "TypeBinName"):
        """
        Args:

            {start}
            length: The length of the substring.
            {bin}

        Returns:

            The substring of the string in the bin.
        """
        pass


class CharAt(_BaseExpr):
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
    def __init__(self, bin: "TypeBinName"):
        """
        Args:

            {bin}

        Returns:

            The integer value of the string in the bin.
        """
        pass


class ToDouble(_BaseExpr):
    def __init__(self, bin: "TypeBinName"):
        """
        Args:

            {bin}

        Returns:

            The double value of the string in the bin.
        """
        pass


class ByteLength(_BaseExpr):
    def __init__(self, bin: "TypeBinName"):
        """
        Args:

            {bin}

        Returns:

            The number of bytes in the string in the bin.
        """
        pass


class IsNumeric(_BaseExpr):
    def __init__(self, numeric_type: NumericType, bin: "TypeBinName"):
        """
        Args:

            numeric_type: the numeric type to filter for.
            {bin}

        Returns:

            true if the string is a numeric value, false otherwise.
        """
        pass


class IsUpper(_BaseExpr):
    def __init__(self, bin: "TypeBinName"):
        """
        Args:

            {bin}

        Returns:

            true if the string is uppercase, false otherwise.
        """
        pass


class IsLower(_BaseExpr):
    def __init__(self, bin: "TypeBinName"):
        """
        Args:

            {bin}

        Returns:

            true if the string is lowercase, false otherwise.
        """
        pass


class ToBlob(_BaseExpr):
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
    def __init__(self, bin: "TypeBinName"):
        """
        Args:

            {bin}

        Returns:

            The blob value of the string in the bin.
        """
        pass


class RegexCompare(_BaseExpr):
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
    def __init__(self, bin: "TypeBinName"):
        """
        Args:

            {bin}

        Returns:

            The string in the bin with the value converted to a string.

        """
        pass


kwargs = {
    "bin": "bin: A bin expression to apply this function to.",
    "start_get": "start (int): The starting index of the substring.",
    "needle_get": "needle (int): the string to search for.",
    "pattern": "pattern (str): the regex pattern to match against.",
    "regex_flags": "regex_flags (:py:class:`~aerospike_helpers.string_helpers.RegexFlags`): The regex flags to use.",
    "str_policy": "policy (:py:class:`~aerospike_helpers.string_helpers.StringPolicy`): String policy.",
    "needle_to_replace": "needle (str): the string to replace.",
    "replacement": "replacement (str): the string to replace with.",
    "target_length": "target_length (int): the target length of the string.",
    "pad_string": "pad_string (str): the string to pad with.",
}


__this_module = sys.modules[__name__]
all_classes = inspect.getmembers(__this_module, predicate=inspect.isclass)
for _, cls_value in all_classes:
    __generate_docstrings(cls_value, kwargs)
