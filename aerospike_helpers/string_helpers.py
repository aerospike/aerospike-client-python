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
    Apply the operation only if the bin does not already exist.

    Against a live bin, the server returns :py:exc:`~aerospike.exception.BinExistsError`. This is valid only on the
    eight additive create ops:

    - :py:meth:`~aerospike_helpers.operations.string_operations.insert`
    - :py:meth:`~aerospike_helpers.operations.string_operations.overwrite`
    - :py:meth:`~aerospike_helpers.operations.string_operations.concat`
    - :py:meth:`~aerospike_helpers.operations.string_operations.append`
    - :py:meth:`~aerospike_helpers.operations.string_operations.prepend`
    - :py:meth:`~aerospike_helpers.operations.string_operations.pad_start`
    - :py:meth:`~aerospike_helpers.operations.string_operations.pad_end`
    - :py:meth:`~aerospike_helpers.operations.string_operations.repeat`

    and their corresponding expressions.

    #. On any other string modify operations, the server rejects it
       with :py:exc:`~aerospike.exception.InvalidRequest` via that op's flag mask.
    #. When this flag combined with :py:attr:`~aerospike_helpers.string_helpers.WriteFlags.UPDATE_ONLY`,
       :py:exc:`~aerospike.exception.InvalidRequest` is raised.
    #. When this flag is passed for a CDT context path, :py:exc:`~aerospike.exception.InvalidRequest` is raised.

    None of those three rejections is suppressible by :py:attr:`~aerospike_helpers.string_helpers.WriteFlags.NO_FAIL`:
    the server raises them while parsing the operation's arguments, upstream of every ``NO_FAIL`` test.
    """

    UPDATE_ONLY = 2
    """
    Apply the operation only to an existing bin, disabling bin creation.

    On a missing bin the operation is a silent no-op and the bin is not created.
    Valid on all string modify ops. Mutually exclusive with
    :py:attr:`~aerospike_helpers.string_helpers.WriteFlags.CREATE_ONLY`; combining the two raises
    :py:exc:`~aerospike.exception.InvalidRequest`.
    """

    NO_FAIL = 4
    """
    Do not raise an error when the modify itself cannot be applied.

    The operation becomes a silent success and the bin is left at its unmodified
    prior value. This flag does not suppress every failure.
    :py:exc:`~aerospike.exception.BinIncompatibleType` and ill-formed UTF-8 in the
    bin surface regardless of the flag, as do the argument-parsing rejections
    listed on :py:attr:`~aerospike_helpers.string_helpers.WriteFlags.CREATE_ONLY`.
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
