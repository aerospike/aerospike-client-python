from aerospike_helpers.expressions.resources import _BaseExpr
from aerospike_helpers.operations.string_operations import RegexFlags, StringPolicy


class StrLen(_BaseExpr):
    def __init__(self, src: str):
        pass


class SubStr(_BaseExpr):
    def __init__(self, start: int, length: int | None, src: str):
        pass


class CharAt(_BaseExpr):
    def __init__(self, index: int, src: str):
        pass


class Find(_BaseExpr):
    def __init__(self, needle: str, occurrence: int, src: str):
        pass


class Contains(_BaseExpr):
    def __init__(self, needle: str, src: str):
        pass


class StartsWith(_BaseExpr):
    def __init__(self, prefix: str, src: str):
        pass


class EndsWith(_BaseExpr):
    def __init__(self, suffix: str, src: str):
        pass


class ToInteger(_BaseExpr):
    def __init__(self, src: str):
        pass


class ToDouble(_BaseExpr):
    def __init__(self, src: str):
        pass


class ByteLength(_BaseExpr):
    def __init__(self, src: str):
        pass


class IsNumeric(_BaseExpr):
    def __init__(self, numeric_type: int, src: str):
        pass


class ToUpper(_BaseExpr):
    def __init__(self, src: str):
        pass


class ToLower(_BaseExpr):
    def __init__(self, src: str):
        pass


class ToBlob(_BaseExpr):
    def __init__(self, src: str):
        pass


class Split(_BaseExpr):
    def __init__(self, separator: str, src: str):
        pass


class Base64Decode(_BaseExpr):
    def __init__(self, src: str):
        pass


class RegexCompare(_BaseExpr):
    # TODO: set default flags
    def __init__(self, pattern: str, regex_flags: RegexFlags):
        pass


class Insert(_BaseExpr):
    def __init__(self, policy: StringPolicy, index: int, value: str, src: str):
        pass


class Overwrite(_BaseExpr):
    def __init__(self, policy: StringPolicy, index: int, value: str, src: str):
        pass


class Concat(_BaseExpr):
    def __init__(self, policy: StringPolicy, values: list[str], src: str):
        pass


class Snip(_BaseExpr):
    def __init__(self, policy: StringPolicy, start: int, end: int | None, src: str):
        pass


class Replace(_BaseExpr):
    def __init__(self, policy: StringPolicy, needle: str, replacement: str, src: str):
        pass


class ReplaceAll(_BaseExpr):
    def __init__(self, policy: StringPolicy, needle: str, replacement: str, src: str):
        pass


class Upper(_BaseExpr):
    def __init__(self, policy: StringPolicy, src: str):
        pass


class Lower(_BaseExpr):
    def __init__(self, policy: StringPolicy, src: str):
        pass


class CaseFold(_BaseExpr):
    def __init__(self, policy: StringPolicy, src: str):
        pass


class NormalizeNFC(_BaseExpr):
    def __init__(self, policy: StringPolicy, src: str):
        pass


class TrimStart(_BaseExpr):
    def __init__(self, policy: StringPolicy, src: str):
        pass


class TrimEnd(_BaseExpr):
    def __init__(self, policy: StringPolicy, src: str):
        pass


class Trim(_BaseExpr):
    def __init__(self, policy: StringPolicy, src: str):
        pass


class PadStart(_BaseExpr):
    def __init__(self, policy: StringPolicy, target_length: int, pad_string: str, src: str):
        pass


class PadEnd(_BaseExpr):
    def __init__(self, policy: StringPolicy, target_length: int, pad_string: str, src: str):
        pass


class Repeat(_BaseExpr):
    def __init__(self, policy: StringPolicy, count: int, src: str):
        pass


class RegexReplace(_BaseExpr):
    def __init__(self, policy: StringPolicy, pattern: str, replacement: int, regex_flags: RegexFlags, src: str):
        pass


class ToString(_BaseExpr):
    def __init__(self, src: str):
        pass
