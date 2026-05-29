from enum import IntEnum

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from aerospike_helpers.expressions.list import TypeCTX


def strlen(bin_name: str, ctx: TypeCTX = None):
    return locals()


def substr(bin_name: str, start: int, length: int | None = None, ctx: TypeCTX = None):
    return locals()


def char_at(bin_name: str, index: int, ctx: TypeCTX = None):
    return locals()


def find(bin_name: str, needle: int, occurrence: int | None = None, ctx: TypeCTX = None):
    return locals()


def contains(bin_name: str, index: int, ctx: TypeCTX = None):
    return locals()


def starts_with(bin_name: str, prefix: str, ctx: TypeCTX = None):
    return locals()


def ends_with(bin_name: str, suffix: str, ctx: TypeCTX = None):
    return locals()


def to_integer(bin_name: str, ctx: TypeCTX = None):
    return locals()


def to_double(bin_name: str, ctx: TypeCTX = None):
    return locals()


def byte_length(bin_name: str, ctx: TypeCTX = None):
    return locals()


def is_numeric(bin_name: str, numeric_type: int | None = None, ctx: TypeCTX = None):
    return locals()


def is_upper(bin_name: str, ctx: TypeCTX = None):
    return locals()


def is_lower(bin_name: str, ctx: TypeCTX = None):
    return locals()


def to_blob(bin_name: str, ctx: TypeCTX = None):
    return locals()


def split(bin_name: str, separator: str, ctx: TypeCTX = None):
    return locals()


def base64_decode(bin_name: str, ctx: TypeCTX = None):
    return locals()

# TODO: use enum for regex flags


def regex_compare(bin_name: str, pattern: str, regex_flags: int, ctx: TypeCTX = None):
    return locals()


class WriteFlags(IntEnum):
    DEFAULT = 0
    NO_FAIL = 4


class StringPolicy:
    def __init__(self, write_flags: WriteFlags):
        self.write_flags = write_flags


class RegexFlags(IntEnum):
    DEFAULT = 0
    CASE_INSENSITIVE = 1
    MULTILINE = 2
    DOTALL = 4
    UNIX_LINES = 8
    GLOBAL = 16


class NumericType(IntEnum):
    ANY = 0
    INT = 1
    FLOAT = 2


def insert(policy: StringPolicy, bin_name: str, index: int, value: str, ctx: TypeCTX = None):
    return locals()


def overwrite(policy: StringPolicy, bin_name: str, index: int, value: str, ctx: TypeCTX = None):
    return locals()


def concat(policy: StringPolicy, bin_name: str, value: str | list[str], ctx: TypeCTX = None):
    return locals()


def snip(policy: StringPolicy, bin_name: str, start: int, end: int | None = None, ctx: TypeCTX = None):
    return locals()


def replace(policy: StringPolicy, bin_name: str, needle: str, replacement: str, ctx: TypeCTX = None):
    return locals()


def replace_all(policy: StringPolicy, bin_name: str, needle: str, replacement: str, ctx: TypeCTX = None):
    return locals()


def upper(policy: StringPolicy, bin_name: str, ctx: TypeCTX = None):
    return locals()


def lower(policy: StringPolicy, bin_name: str, ctx: TypeCTX = None):
    return locals()


def casefold(policy: StringPolicy, bin_name: str, ctx: TypeCTX = None):
    return locals()


def normalize_nfc(policy: StringPolicy, bin_name: str, ctx: TypeCTX = None):
    return locals()


def trim_start(policy: StringPolicy, bin_name: str, ctx: TypeCTX = None):
    return locals()


def trim_end(policy: StringPolicy, bin_name: str, ctx: TypeCTX = None):
    return locals()


def trim(policy: StringPolicy, bin_name: str, ctx: TypeCTX = None):
    return locals()


def pad_start(policy: StringPolicy, bin_name: str, target_length: int, pad_string: str, ctx: TypeCTX = None):
    return locals()


def pad_end(policy: StringPolicy, bin_name: str, target_length: int, pad_string: str, ctx: TypeCTX = None):
    return locals()


def repeat(policy: StringPolicy, bin_name: str, count: int, ctx: TypeCTX = None):
    return locals()

# TODO: regex flags enum


def regex_replace(
        policy: StringPolicy,
        bin_name: str,
        pattern: str,
        replacement: str,
        regex_flags: int,
        ctx: TypeCTX = None
):
    return locals()


def to_string(bin_name: str):
    return locals()
