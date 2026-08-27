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

    CREATE_ONLY = 1
    """
    Create new values only. Valid only on:

    - :py:meth:`~aerospike_helpers.operations.string_operations.insert`
    - :py:meth:`~aerospike_helpers.operations.string_operations.overwrite`
    - :py:meth:`~aerospike_helpers.operations.string_operations.concat`
    - :py:meth:`~aerospike_helpers.operations.string_operations.append`
    - :py:meth:`~aerospike_helpers.operations.string_operations.prepend`
    - :py:meth:`~aerospike_helpers.operations.string_operations.pad_start`
    - :py:meth:`~aerospike_helpers.operations.string_operations.pad_end`
    - :py:meth:`~aerospike_helpers.operations.string_operations.repeat`

    and their corresponding expressions.

    Raises :py:exc:`~aerospike.exception.BinExistsError` if the bin already exists. Mutually exclusive with
    :py:attr:`~aerospike_helpers.string_helpers.WriteFlags.UPDATE_ONLY`. Invalid with a CDT context path.
    """

    UPDATE_ONLY = 2
    """
	 Update existing values only. Mutually exclusive with
	 :py:attr:`~aerospike_helpers.string_helpers.WriteFlags.CREATE_ONLY`.
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

    FLOAT = 2
    """
    Match only floating-point numbers. Stricter than parsing as a double:
    the string must contain a ``.`` followed by a digit, so ``"5"`` is false under
    this option, but true under :py:attr:`~aerospike_helpers.string_helpers.NumericType.ANY`.
    """


class StringPolicy:
    """
    String operation policy.
    """

    def __init__(self, write_flags: WriteFlags = WriteFlags.DEFAULT):
        self.write_flags = write_flags
