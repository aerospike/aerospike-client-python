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


def __generate_docstrings(object, kwargs: dict):
    functions = inspect.getmembers(object, predicate=inspect.isfunction)
    for _, function in functions:
        if function.__doc__ is None:
            continue
        function.__doc__ = function.__doc__.format(
            **kwargs
        )
