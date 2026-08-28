import pytest
import base64

import aerospike
from aerospike_helpers.operations import string_operations as str_ops, operations, list_operations as list_ops
from aerospike_helpers.string_helpers import NumericType, StringPolicy, RegexFlags, WriteFlags
from aerospike import exception as e
from aerospike_helpers import cdt_ctx

from .conftest import expect_server_version_earlier_than_8_1_3_to_fail, TEST_NS, TEST_SET, TestBaseClass
from .string_helpers import *
KEY = (TEST_NS, TEST_SET, 1)


@expect_server_version_earlier_than_8_1_3_to_fail
class TestStringOperations:
    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection, expect_earlier_than_server_version_to_fail):
        try:
            self.as_connection.remove(KEY)
        except e.RecordNotFound:
            pass

        self.as_connection.put(
            key=KEY,
            bins=BINS,
        )

        yield

    root_level_and_nested_str = pytest.mark.parametrize(
        "bin_name, kwargs_with_ctx",
        [
            pytest.param(STR_BIN_NAME, {}, id="no_ctx_arg"),
            pytest.param(STR_BIN_NAME, {"ctx": None}, id="ctx_is_none"),
            pytest.param(
                NESTED_STR_BIN_NAME,
                {
                    "ctx": [
                        cdt_ctx.cdt_ctx_list_index(0)
                    ]
                },
                id="ctx_arg_list"
            )
        ]
    )

    @root_level_and_nested_str
    @pytest.mark.parametrize(
        "op_method, kwargs, expected_result",
        [
            (str_ops.strlen, {}, len(EXAMPLE_STR)),
            (str_ops.substr, {"start": START_IDX}, EXAMPLE_STR[START_IDX:]),
            (str_ops.substr_range, {"start": START_IDX, "end": START_IDX + 2}, EXAMPLE_STR[START_IDX:START_IDX + 2]),
            (str_ops.char_at, {"index": START_IDX}, EXAMPLE_STR[START_IDX]),
            (str_ops.char_at, {"index": -1}, EXAMPLE_STR[-1]),
            (str_ops.find, {"needle": NEEDLE}, 0),
            (str_ops.find, {"needle": NEEDLE, "occurrence": 1}, 0),
            (str_ops.find, {"needle": NEEDLE, "occurrence": 2}, 4),
            (str_ops.contains, {"needle": NEEDLE}, True),
            (str_ops.starts_with, {"prefix": NEEDLE}, True),
            (str_ops.starts_with, {"prefix": NOT_IN_EXAMPLE_STR}, False),
            (str_ops.ends_with, {"suffix": NEEDLE}, True),
            (str_ops.ends_with, {"suffix": NOT_IN_EXAMPLE_STR}, False),
            (str_ops.byte_length, {}, len(EXAMPLE_STR)),
            (str_ops.is_numeric, {}, False),
            (str_ops.is_upper, {}, False),
            (str_ops.is_lower, {}, True),
            (str_ops.to_blob, {}, bytes(EXAMPLE_STR, encoding="utf-8")),
            (str_ops.split, {}, list(EXAMPLE_STR)),
        ]
    )
    def test_string_read_op_on_str_value(self, op_method, bin_name: str, kwargs_with_ctx: dict, kwargs: dict, expected_result):
        ops = [
            op_method(bin_name=bin_name, **kwargs_with_ctx, **kwargs)
        ]
        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)
            assert bins[bin_name] == expected_result

    def test_find_not_found(self):
        ops = [
            str_ops.find(bin_name=STR_BIN_NAME, needle=NOT_IN_EXAMPLE_STR)
        ]
        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[STR_BIN_NAME] == -1

    def test_contains_not_found(self):
        ops = [
            str_ops.contains(bin_name=STR_BIN_NAME, needle=NOT_IN_EXAMPLE_STR)
        ]
        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[STR_BIN_NAME] is False

    def test_starts_with_returns_false(self):
        ops = [
            str_ops.starts_with(bin_name=STR_BIN_NAME, prefix=NOT_IN_EXAMPLE_STR)
        ]
        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[STR_BIN_NAME] is False

    def test_to_integer(self):
        ops = [
            str_ops.to_integer(bin_name=STR_WITH_INT_BIN_NAME)
        ]
        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[STR_WITH_INT_BIN_NAME] == int(STRING_WITH_INT)

    @pytest.mark.parametrize(
        "op",
        [
            str_ops.to_integer,
            str_ops.to_double
        ]
    )
    def test_to_numeric_fail(self, op):
        ops = [
            op(bin_name=STR_BIN_NAME)
        ]

        with pytest.raises(e.ServerError):
            self.as_connection.operate(KEY, ops)

    def test_to_double(self):
        ops = [
            str_ops.to_double(bin_name=STR_WITH_DOUBLE_BIN_NAME)
        ]
        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[STR_WITH_DOUBLE_BIN_NAME] == float(STRING_WITH_DOUBLE)

    def test_byte_length_for_multibyte_codepoint(self):
        ops = [
            str_ops.byte_length(bin_name=MULTIBYTE_CODEPOINT_BIN_NAME)
        ]
        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[MULTIBYTE_CODEPOINT_BIN_NAME] == len(BINS[MULTIBYTE_CODEPOINT_BIN_NAME].encode('utf-8'))

    @pytest.mark.parametrize(
        "bin_name, expected_result",
        [
            (STR_WITH_INT_BIN_NAME, True),
            (STR_WITH_DOUBLE_BIN_NAME, True),
        ]
    )
    def test_is_numeric(self, bin_name: str, expected_result: bool):
        ops = [
            str_ops.is_numeric(bin_name=bin_name)
        ]
        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[bin_name] is expected_result

    @pytest.mark.parametrize(
        "numeric_type, bin_name, expected_result",
        [
            # Positive test cases
            (NumericType.INT, STR_WITH_INT_BIN_NAME, True),
            (NumericType.FLOAT, STR_WITH_DOUBLE_BIN_NAME, True),
            # Negative test cases
            (NumericType.INT, STR_WITH_DOUBLE_BIN_NAME, False),
            (NumericType.FLOAT, STR_WITH_INT_BIN_NAME, False)
        ]
    )
    def test_numeric_type(self, numeric_type: NumericType, bin_name: str, expected_result: bool):
        ops = [
            str_ops.is_numeric(bin_name=bin_name, numeric_type=numeric_type)
        ]
        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[bin_name] is expected_result

    @pytest.mark.parametrize(
        "bin_name, expected_result",
        [
            (UPPERCASE_STR_BIN_NAME, True)
        ]
    )
    def test_is_upper(self, bin_name: str, expected_result: bool):
        ops = [
            str_ops.is_upper(bin_name=bin_name)
        ]
        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[bin_name] is expected_result

    @pytest.mark.parametrize(
        "bin_name, expected_result",
        [
            (UPPERCASE_STR_BIN_NAME, False)
        ]
    )
    def test_is_lower(self, bin_name: str, expected_result: bool):
        ops = [
            str_ops.is_lower(bin_name=bin_name)
        ]
        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[bin_name] is expected_result

    @pytest.mark.parametrize(
        "separator",
        [
            ".",
            ","
        ]
    )
    def test_split_with_separator(self, separator: str):
        ops = [
            str_ops.split_separator(bin_name=STR_WITH_DOUBLE_BIN_NAME, separator=separator)
        ]

        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            if separator == ".":
                assert bins[STR_WITH_DOUBLE_BIN_NAME] == STRING_WITH_DOUBLE.split(separator)
            else:
                assert bins[STR_WITH_DOUBLE_BIN_NAME] == [STRING_WITH_DOUBLE]

    def test_base64_decode(self):
        ops = [
            str_ops.base64_decode(bin_name=BASE64_ENCODED_BIN_NAME)
        ]
        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            expected_result = base64.b64decode(BASE64_ENCODED_STR)
            assert bins[BASE64_ENCODED_BIN_NAME] == bytearray(expected_result)

    @pytest.mark.parametrize(
        "pattern, expected_result",
        [
            (MULTIBYTE_CODEPOINT, True),
            ("π", False)
        ]
    )
    def test_regex_compare(self, pattern: str, expected_result: bool):
        ops = [
            str_ops.regex_compare(bin_name=MULTIBYTE_CODEPOINT_BIN_NAME, pattern=pattern)
        ]

        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[MULTIBYTE_CODEPOINT_BIN_NAME] is expected_result

    @pytest.mark.parametrize(
        "bin_name, expected_result",
        [
            (INT_BIN_NAME, str(BINS[INT_BIN_NAME])),
            (DOUBLE_BIN_NAME, str(BINS[INT_BIN_NAME])),
            (STR_BIN_NAME, BINS[STR_BIN_NAME]),
            (BLOB_BIN_NAME, bytes.decode(BINS[BLOB_BIN_NAME]))
        ]
    )
    def test_to_string(self, bin_name: str, expected_result: str):
        ops = [
            str_ops.to_string(bin_name=bin_name)
        ]

        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[bin_name] == expected_result

    # Write operations

    def add_read_op(self, ops, bin_name):
        if bin_name == NESTED_STR_BIN_NAME:
            op = list_ops.list_get_by_index(bin_name, 0, aerospike.LIST_RETURN_VALUE)
        else:
            op = operations.read(bin_name=bin_name)
        ops.append(op)

    @pytest.mark.parametrize(
        "op, kwargs, expected_value",
        [
            (str_ops.insert, {"index": 1, "value": NEEDLE}, EXAMPLE_STR[:1] + NEEDLE + EXAMPLE_STR[1:]),
            (str_ops.insert, {"index": -1, "value": NEEDLE}, EXAMPLE_STR[:-1] + NEEDLE + EXAMPLE_STR[-1:]),
            (str_ops.overwrite, {"index": 1, "value": SINGLE_CHAR}, EXAMPLE_STR[:1] + SINGLE_CHAR + EXAMPLE_STR[2:]),
            (str_ops.overwrite, {"index": 0, "value": EXAMPLE_STR + "a"}, EXAMPLE_STR + "a"),
            (str_ops.append, {"value": NEEDLE}, EXAMPLE_STR + NEEDLE),
            (str_ops.prepend, {"value": NEEDLE}, NEEDLE + EXAMPLE_STR),
            (str_ops.concat, {"value_list": [NEEDLE]}, EXAMPLE_STR + NEEDLE),
            (str_ops.concat, {"value_list": [NEEDLE, NEEDLE]}, EXAMPLE_STR + NEEDLE * 2),
            (str_ops.snip, {"start": START_IDX, "end": len(EXAMPLE_STR) - 1}, EXAMPLE_STR[:START_IDX] + EXAMPLE_STR[-1]),
            (str_ops.replace, {"needle": NEEDLE, "replacement": SINGLE_CHAR}, EXAMPLE_STR.replace(NEEDLE, SINGLE_CHAR, 1)),
            (str_ops.replace_all, {"needle": NEEDLE, "replacement": SINGLE_CHAR}, EXAMPLE_STR.replace(NEEDLE, SINGLE_CHAR)),
            (str_ops.upper, {}, EXAMPLE_STR.upper()),
            (str_ops.pad_start, {"pad_string": PAD_STRING, "target_length": len(EXAMPLE_STR) + 2}, 2 * PAD_STRING + EXAMPLE_STR),
            (str_ops.pad_start, {"pad_string": PAD_STRING, "target_length": len(EXAMPLE_STR)}, EXAMPLE_STR),
            (str_ops.pad_start, {"pad_string": PAD_STRING, "target_length": len(EXAMPLE_STR) - 1}, EXAMPLE_STR),
            (str_ops.pad_end, {"pad_string": PAD_STRING, "target_length": len(EXAMPLE_STR) + 2}, EXAMPLE_STR + 2 * PAD_STRING),
            (str_ops.pad_end, {"pad_string": PAD_STRING, "target_length": len(EXAMPLE_STR)}, EXAMPLE_STR),
            (str_ops.pad_end, {"pad_string": PAD_STRING, "target_length": len(EXAMPLE_STR) - 1}, EXAMPLE_STR),
            (str_ops.repeat, {"count": 1}, EXAMPLE_STR),
            (str_ops.repeat, {"count": 2}, EXAMPLE_STR * 2)
        ]
    )
    @root_level_and_nested_str
    @kwargs_policy
    def test_string_write_op_on_str_value(self, op, expected_value: str, kwargs: dict, kwargs_policy: dict, bin_name: str, kwargs_with_ctx: dict):
        ops = [
            op(bin_name=bin_name, **kwargs, **kwargs_policy, **kwargs_with_ctx)
        ]
        self.add_read_op(ops, bin_name)

        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[bin_name] == expected_value

    def test_concat_with_non_str_in_list(self):
        ops = [
            str_ops.concat(bin_name=STR_BIN_NAME, value_list=[1])
        ]

        with pytest.raises(e.ServerError):
            self.as_connection.operate(KEY, ops)

    @pytest.mark.parametrize(
        "bin_name, expected_result",
        [
            (UPPERCASE_STR_BIN_NAME, UPPERCASE_STR.lower()),
            (MULTIBYTE_CODEPOINT_BIN_NAME, MULTIBYTE_CODEPOINT)
        ]
    )
    @kwargs_policy
    def test_lower(self, kwargs_policy: dict, bin_name: str, expected_result: str):
        ops = [
            str_ops.lower(bin_name=bin_name, **kwargs_policy)
        ]
        self.add_read_op(ops, bin_name)

        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[bin_name] == expected_result

    @kwargs_policy
    def test_casefold(self, kwargs_policy: dict):
        ops = [
            str_ops.casefold(bin_name=MULTIBYTE_CODEPOINT_BIN_NAME, **kwargs_policy)
        ]
        self.add_read_op(ops, MULTIBYTE_CODEPOINT_BIN_NAME)

        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[MULTIBYTE_CODEPOINT_BIN_NAME] == MULTIBYTE_CODEPOINT.casefold()

    @kwargs_policy
    def test_normalize_nfc(self, kwargs_policy):
        ops = [
            str_ops.normalize_nfc(bin_name=MULTIBYTE_CODEPOINT_BIN_NAME, **kwargs_policy)
        ]
        self.add_read_op(ops, MULTIBYTE_CODEPOINT_BIN_NAME)

        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[MULTIBYTE_CODEPOINT_BIN_NAME] == NORMALIZED_CODEPOINT

    @kwargs_policy
    def test_trim_start(self, kwargs_policy):
        ops = [
            str_ops.trim_start(bin_name=SURROUNDING_WHITESPACE_BIN_NAME, **kwargs_policy)
        ]
        self.add_read_op(ops, SURROUNDING_WHITESPACE_BIN_NAME)

        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[SURROUNDING_WHITESPACE_BIN_NAME] == EXAMPLE_STR_WITH_SURROUNDING_WHITESPACE[1:]

    @kwargs_policy
    def test_trim_end(self, kwargs_policy):
        ops = [
            str_ops.trim_end(bin_name=SURROUNDING_WHITESPACE_BIN_NAME, **kwargs_policy)
        ]
        self.add_read_op(ops, SURROUNDING_WHITESPACE_BIN_NAME)

        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[SURROUNDING_WHITESPACE_BIN_NAME] == EXAMPLE_STR_WITH_SURROUNDING_WHITESPACE[:-1]


    @kwargs_policy
    def test_trim(self, kwargs_policy):
        ops = [
            str_ops.trim(bin_name=SURROUNDING_WHITESPACE_BIN_NAME, **kwargs_policy)
        ]
        self.add_read_op(ops, SURROUNDING_WHITESPACE_BIN_NAME)

        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[SURROUNDING_WHITESPACE_BIN_NAME] == EXAMPLE_STR_WITH_SURROUNDING_WHITESPACE[1:-1]

    @kwargs_policy
    @root_level_and_nested_str
    def test_regex_replace(self, kwargs_policy: dict, bin_name: str, kwargs_with_ctx: dict):
        PATTERN = "asdf"
        ops = [
            str_ops.regex_replace(bin_name=bin_name, pattern=PATTERN, replacement=NEW_STR, **kwargs_policy, **kwargs_with_ctx)
        ]
        self.add_read_op(ops, bin_name)

        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[bin_name] == NEW_STR + "asdf"

    @pytest.mark.parametrize(
        "bin_name, regex_flags, pattern, expected_results",
        [
            (STR_BIN_NAME, RegexFlags.CASE_INSENSITIVE, "ASDF", NEW_STR + "asdf"),
            (MULTILINE_STR_BIN_NAME, RegexFlags.MULTILINE, "^a$", NEW_STR + "\na"),
            (MULTILINE_STR_BIN_NAME, RegexFlags.DOTALL, ".*", NEW_STR),
            # Carriage return should be ignored
            (MULTILINE_STR_WITH_CR_BIN_NAME, RegexFlags.MULTILINE | RegexFlags.UNIX_LINES, "^.*$", NEW_STR),
            (STR_BIN_NAME, RegexFlags.GLOBAL, "asdf", NEW_STR * 2)
        ]
    )
    def test_regex_flags(self, bin_name: str, regex_flags: RegexFlags, pattern: str, expected_results: str):
        ops = [
            str_ops.regex_replace(bin_name=bin_name, pattern=pattern, replacement=NEW_STR, regex_flags=regex_flags)
        ]
        self.add_read_op(ops, bin_name)

        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[bin_name] == expected_results

    def test_string_policy_create_only(self):
        policy = StringPolicy(write_flags=WriteFlags.CREATE_ONLY)
        ops = [
            str_ops.insert(bin_name=STR_BIN_NAME, index=0, value="a", policy=policy)
        ]

        if (TestBaseClass.major_ver, TestBaseClass.minor_ver, TestBaseClass.patch_ver) < (8, 1, 3):
            expected_exc = e.InvalidRequest
        else:
            expected_exc = e.BinExistsError

        with pytest.raises(expected_exc):
            self.as_connection.operate(KEY, ops)

    def test_string_policy_update_only(self):
        policy = StringPolicy(write_flags=WriteFlags.UPDATE_ONLY)
        ops = [
            str_ops.insert(bin_name="aaaa", index=0, value="a", policy=policy)
        ]
        self.as_connection.operate(KEY, ops)

        _, _, bins = self.as_connection.get(KEY)
        assert "aaaa" not in bins

    def test_string_policy_no_fail(self):
        policy = StringPolicy(write_flags=WriteFlags.NO_FAIL)
        ops = [
            str_ops.repeat(bin_name=STR_BIN_NAME, count=-1, policy=policy)
        ]
        self.add_read_op(ops, INT_BIN_NAME)

        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[INT_BIN_NAME] == BINS[INT_BIN_NAME]

    @pytest.mark.parametrize(
        "op, kwargs, creates_bin",
        [
            # Positive
            (str_ops.append, {"value": NEEDLE}, True),
            (str_ops.prepend, {"value": NEEDLE}, True),
            (str_ops.concat, {"value_list": [NEEDLE]}, True),
            (str_ops.overwrite, {"index": 0, "value": NEEDLE}, True),
            (str_ops.insert, {"index": 0, "value": NEEDLE}, True),
            (str_ops.pad_start, {"target_length": 4, "pad_string": NEEDLE}, True),
            (str_ops.pad_end, {"target_length": 4, "pad_string": NEEDLE}, True),
            (str_ops.repeat, {"count": 2}, True),
            (str_ops.repeat, {"count": 2}, True),
            # Negative
            (str_ops.snip, {"start": 0, "end": 1}, False),
            (str_ops.replace, {"needle": "a", "replacement": "b"}, False),
            (str_ops.replace_all, {"needle": "a", "replacement": "b"}, False),
            (str_ops.upper, {}, False),
            (str_ops.lower, {}, False),
            (str_ops.casefold, {}, False),
            (str_ops.normalize_nfc, {}, False),
            (str_ops.trim_start, {}, False),
            (str_ops.trim_end, {}, False),
            (str_ops.trim, {}, False),
            (str_ops.regex_replace, {"pattern": "a", "replacement": "b"}, False),
            (str_ops.to_string, {}, False),
        ]
    )
    def test_string_ops_on_nonexistent_bin(self, op, kwargs: dict, creates_bin: bool):
        ops = [
            op(bin_name=NON_EXISTENT_BIN_NAME, **kwargs),
            operations.read(NON_EXISTENT_BIN_NAME)
        ]

        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            if creates_bin is False:
                assert bins[NON_EXISTENT_BIN_NAME] is None
                return

            if op == str_ops.repeat:
                assert bins[NON_EXISTENT_BIN_NAME] == ""
            else:
                assert bins[NON_EXISTENT_BIN_NAME] == NEEDLE
