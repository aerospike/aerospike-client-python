from enum import IntEnum


def strlen(bin_name: str, ctx: list | None = None):
    pass


def substr(bin_name: str, start: int, length: int | None = None, ctx: list | None = None):
    pass


def char_at(bin_name: str, index: int, ctx: list | None = None):
    pass


def find(bin_name: str, needle: int, occurrence: int | None = None, ctx: list | None = None):
    pass


def contains(bin_name: str, index: int, ctx: list | None = None):
    pass


def starts_with(bin_name: str, prefix: str, ctx: list | None = None):
    pass


def ends_with(bin_name: str, suffix: str, ctx: list | None = None):
    pass


def to_integer(bin_name: str, ctx: list | None = None):
    pass


def to_double(bin_name: str, ctx: list | None = None):
    pass


def byte_length(bin_name: str, ctx: list | None = None):
    pass


def is_numeric(bin_name: str, numeric_type: int | None = None, ctx: list | None = None):
    pass


def is_upper(bin_name: str, ctx: list | None = None):
    pass


def is_lower(bin_name: str, ctx: list | None = None):
    pass


def to_blob(bin_name: str, ctx: list | None = None):
    pass


def split(bin_name: str, separator: str, ctx: list | None = None):
    pass


def base64_decode(bin_name: str, ctx: list | None = None):
    pass

# TODO: use enum for regex flags


def regex_compare(bin_name: str, pattern: str, regex_flags: int, ctx: list | None = None):
    pass


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


def insert(policy: StringPolicy, bin_name: str, index: int, value: str, ctx: list | None = None):
    pass


def overwrite(policy: StringPolicy, bin_name: str, index: int, value: str, ctx: list | None = None):
    pass


def concat(policy: StringPolicy, bin_name: str, value: str | list[str], ctx: list | None = None):
    pass


def snip(policy: StringPolicy, bin_name: str, start: int, end: int | None = None, ctx: list | None = None):
    pass


def replace(policy: StringPolicy, bin_name: str, needle: str, replacement: str, ctx: list | None = None):
    pass


def replace_all(policy: StringPolicy, bin_name: str, needle: str, replacement: str, ctx: list | None = None):
    pass


def upper(policy: StringPolicy, bin_name: str, ctx: list | None = None):
    pass


def lower(policy: StringPolicy, bin_name: str, ctx: list | None = None):
    pass


def casefold(policy: StringPolicy, bin_name: str, ctx: list | None = None):
    pass


def normalize_nfc(policy: StringPolicy, bin_name: str, ctx: list | None = None):
    pass


def trim_start(policy: StringPolicy, bin_name: str, ctx: list | None = None):
    pass


def trim_end(policy: StringPolicy, bin_name: str, ctx: list | None = None):
    pass


def trim(policy: StringPolicy, bin_name: str, ctx: list | None = None):
    pass


def pad_start(policy: StringPolicy, bin_name: str, target_length: int, pad_string: str, ctx: list | None = None):
    pass


def pad_end(policy: StringPolicy, bin_name: str, target_length: int, pad_string: str, ctx: list | None = None):
    pass


def repeat(policy: StringPolicy, bin_name: str, count: int, ctx: list | None = None):
    pass

# TODO: regex flags enum


def regex_replace(
        policy: StringPolicy,
        bin_name: str,
        pattern: str,
        replacement: str,
        regex_flags: int,
        ctx: list | None = None
):
    pass


def to_string(bin_name: str):
    pass
