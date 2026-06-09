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
Shared classes for both string operations and string expressions.
"""

import inspect
from enum import IntEnum


class RegexFlags(IntEnum):
    """
    Regex flags for string regex operations. Use bitwise OR to combine flags.
    """

    #: Default. No flags set.
    DEFAULT = 0
    #: Case insensitive matching.
    CASE_INSENSITIVE = 1

    MULTILINE = 2
    """Treat input as a multi-line string. The ``^`` and ``$`` metacharacters match the
            start and end of any line, not just the start and end of the input."""

    #: The dot metacharacter matches line terminators.
    DOTALL = 4
    #: Treat only ``\n`` as a line terminator.
    UNIX_LINES = 8
    #: Replace all matches. Only applicable to :py:meth:`~aerospike_helpers.operations.string_operations.regex_replace`.
    GLOBAL = 16


class WriteFlags(IntEnum):
    """
    String operation policy write bit flags. Use bitwise OR to combine flags.
    """

    #: Default. Allow create or update.
    DEFAULT = 0

    NO_FAIL = 4
    """
    Do not raise an error if a modify operation cannot be applied because
            the target bin does not exist. The record is left unchanged.
    """


class NumericType(IntEnum):
    """
    Numeric type filter for :meth:`~aerospike_helpers.operations.string_operations.is_numeric`.
    """

    #: Match either an integer or a floating-point number.
    ANY = 0
    #: Match only integers.
    INT = 1
    #: Match only floating-point numbers.
    FLOAT = 2


class StringPolicy:
    """
    String operation policy.
    """

    def __init__(self, write_flags: WriteFlags):
        self.write_flags = write_flags


def __generate_docstrings_for_all_func_members(object, kwargs: dict):
    kwargs |= {
        "pattern": "pattern (str): the regex pattern to match against.",
        "regex_flags": "regex_flags (:py:class:`~aerospike_helpers.string_helpers.RegexFlags`): The regex flags to use.",
        "str_policy": "policy (:py:class:`~aerospike_helpers.string_helpers.StringPolicy`): String policy.",
        "needle_to_replace": "needle (str): the string to replace.",
        "replacement": "replacement (str): the string to replace with.",
        "target_length": "target_length (int): the target length of the string.",
        "pad_string": "pad_string (str): the string to pad with.",
        "needle_get": "needle (int): the string to search for.",
}
    functions = inspect.getmembers(object, predicate=inspect.isfunction)
    for _, function in functions:
        if function.__doc__ is None:
            continue
        function.__doc__ = function.__doc__.format(
            **kwargs
        )
