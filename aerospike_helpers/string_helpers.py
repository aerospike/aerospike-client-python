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

    DEFAULT = 0
    """
    Default. Does not suppress an in-operation execution failure.
    """

    NO_FAIL = 4
    """
    Suppress an operation failure with the bin unchanged.

    Does not suppress wrong-type errors.
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

    def __init__(self, write_flags: WriteFlags = WriteFlags.DEFAULT):
        self.write_flags = write_flags
