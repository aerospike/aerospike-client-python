import pytest
import base64

import aerospike
from aerospike_helpers.operations import string_operations as str_ops, operations, list_operations as list_ops
from aerospike_helpers.string_helpers import NumericType, StringPolicy, RegexFlags
from aerospike import exception as e
from aerospike_helpers import cdt_ctx

from .conftest import expect_server_version_earlier_than_8_1_3_to_fail
from .test_base_class import TestBaseClass
from .string_helpers import *


class TestStringOperations:
    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection, expect_earlier_than_server_version_to_fail):
        self.as_connection.put(
            key=KEY,
            bins=BINS
        )

        yield

        self.as_connection.remove(KEY)

    # TODO: ctx can also be None.
    root_level_and_nested_str = pytest.mark.parametrize(
        "bin_name, kwargs_with_ctx",
        [
            pytest.param(STR_BIN_NAME, {}, id="no_ctx_arg"),
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
    @expect_server_version_earlier_than_8_1_3_to_fail
    def test_strlen(self, bin_name: str, kwargs_with_ctx: dict):
        ops = [
            str_ops.strlen(bin_name=bin_name, **kwargs_with_ctx)
        ]
        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)
            assert bins[bin_name] == len(EXAMPLE_STR)

    @pytest.mark.parametrize(
        "end_kwargs",
        [
            {},
            {
                "end": None
            },
            {
                "end": 2
            }
        ]
    )
    @root_level_and_nested_str
    @expect_server_version_earlier_than_8_1_3_to_fail
    def test_substr(self, end_kwargs: dict, bin_name: str, kwargs_with_ctx: dict):
        kwargs_with_ctx = kwargs_with_ctx | end_kwargs
        ops = [
            str_ops.substr(bin_name=bin_name, start=START_IDX, **kwargs_with_ctx)
        ]
        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            if "end" not in end_kwargs or end_kwargs["end"] is None:
                assert bins[bin_name] == EXAMPLE_STR[START_IDX:]
            else:
                end = end_kwargs["end"]
                assert bins[bin_name] == EXAMPLE_STR[START_IDX:(START_IDX + end)]

    @pytest.mark.parametrize(
        "index",
        [
            START_IDX,
            -1
        ]
    )
    @root_level_and_nested_str
    @expect_server_version_earlier_than_8_1_3_to_fail
    def test_char_at(self, index: int, bin_name: str, kwargs_with_ctx: dict):
        ops = [
            str_ops.char_at(bin_name=bin_name, index=index, **kwargs_with_ctx)
        ]
        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[bin_name] == EXAMPLE_STR[index]

    @pytest.mark.parametrize(
        "occurrence_kwargs, expected_idx",
        [
            ({}, 0),
            ({"occurrence": 1}, 0),
            ({"occurrence": 2}, 4)
        ]
    )
    @root_level_and_nested_str
    @expect_server_version_earlier_than_8_1_3_to_fail
    def test_find(self, occurrence_kwargs: dict, expected_idx: int, bin_name: str, kwargs_with_ctx: dict):
        kwargs_with_ctx = kwargs_with_ctx | occurrence_kwargs
        ops = [
            str_ops.find(bin_name=bin_name, needle=NEEDLE, **kwargs_with_ctx)
        ]
        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[bin_name] == expected_idx

    @expect_server_version_earlier_than_8_1_3_to_fail
    def test_find_not_found(self):
        ops = [
            str_ops.find(bin_name=STR_BIN_NAME, needle=NOT_IN_EXAMPLE_STR)
        ]
        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[STR_BIN_NAME] == -1

    @root_level_and_nested_str
    @expect_server_version_earlier_than_8_1_3_to_fail
    def test_contains(self, bin_name: str, kwargs_with_ctx: dict):
        ops = [
            str_ops.contains(bin_name=bin_name, needle=NEEDLE, **kwargs_with_ctx)
        ]
        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[bin_name] is True

    @expect_server_version_earlier_than_8_1_3_to_fail
    def test_contains_not_found(self):
        ops = [
            str_ops.contains(bin_name=STR_BIN_NAME, needle=NOT_IN_EXAMPLE_STR)
        ]
        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[STR_BIN_NAME] is False

    @root_level_and_nested_str
    @expect_server_version_earlier_than_8_1_3_to_fail
    def test_starts_with(self, bin_name: str, kwargs_with_ctx: dict):
        ops = [
            str_ops.starts_with(bin_name=bin_name, prefix=NEEDLE, **kwargs_with_ctx)
        ]
        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[bin_name] is True

    @expect_server_version_earlier_than_8_1_3_to_fail
    def test_starts_with_returns_false(self):
        ops = [
            str_ops.starts_with(bin_name=STR_BIN_NAME, prefix=NOT_IN_EXAMPLE_STR)
        ]
        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[STR_BIN_NAME] is False

    @root_level_and_nested_str
    @expect_server_version_earlier_than_8_1_3_to_fail
    def test_ends_with(self, bin_name: str, kwargs_with_ctx: dict):
        ops = [
            str_ops.ends_with(bin_name=bin_name, suffix=NEEDLE, **kwargs_with_ctx)
        ]
        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[bin_name] is True

    @expect_server_version_earlier_than_8_1_3_to_fail
    def test_ends_with_returns_false(self):
        ops = [
            str_ops.ends_with(bin_name=STR_BIN_NAME, suffix=NOT_IN_EXAMPLE_STR)
        ]
        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[STR_BIN_NAME] is False

    @expect_server_version_earlier_than_8_1_3_to_fail
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
    @expect_server_version_earlier_than_8_1_3_to_fail
    def test_to_numeric_fail(self, op):
        ops = [
            op(bin_name=STR_BIN_NAME)
        ]

        if (TestBaseClass.major_ver, TestBaseClass.minor_ver, TestBaseClass.patch_ver) >= (8, 1, 3):
            expected_exc = e.OpNotApplicable
        else:
            expected_exc = e.InvalidRequest

        with pytest.raises(expected_exc):
            self.as_connection.operate(KEY, ops)

    @expect_server_version_earlier_than_8_1_3_to_fail
    def test_to_double(self):
        ops = [
            str_ops.to_double(bin_name=STR_WITH_DOUBLE_BIN_NAME)
        ]
        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[STR_WITH_DOUBLE_BIN_NAME] == float(STRING_WITH_DOUBLE)

    # TODO: add case for multi-byte unicode codepoints
    @root_level_and_nested_str
    @expect_server_version_earlier_than_8_1_3_to_fail
    def test_byte_length(self, bin_name: str, kwargs_with_ctx: dict):
        ops = [
            str_ops.byte_length(bin_name=bin_name, **kwargs_with_ctx)
        ]
        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[bin_name] == len(EXAMPLE_STR)

    @pytest.mark.parametrize(
        "bin_name, expected_result",
        [
            (STR_BIN_NAME, False),
            (STR_WITH_INT_BIN_NAME, True),
            (STR_WITH_DOUBLE_BIN_NAME, True),
        ]
    )
    @expect_server_version_earlier_than_8_1_3_to_fail
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
    @expect_server_version_earlier_than_8_1_3_to_fail
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
            (STR_BIN_NAME, False),
            (UPPERCASE_STR_BIN_NAME, True)
        ]
    )
    @expect_server_version_earlier_than_8_1_3_to_fail
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
            (STR_BIN_NAME, True),
            (UPPERCASE_STR_BIN_NAME, False)
        ]
    )
    @expect_server_version_earlier_than_8_1_3_to_fail
    def test_is_lower(self, bin_name: str, expected_result: bool):
        ops = [
            str_ops.is_lower(bin_name=bin_name)
        ]
        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[bin_name] is expected_result

    @root_level_and_nested_str
    @expect_server_version_earlier_than_8_1_3_to_fail
    def test_to_blob(self, bin_name: str, kwargs_with_ctx: dict):
        ops = [
            str_ops.to_blob(bin_name=bin_name, **kwargs_with_ctx)
        ]
        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[bin_name] == bytes(EXAMPLE_STR, encoding="utf-8")

    @root_level_and_nested_str
    @expect_server_version_earlier_than_8_1_3_to_fail
    def test_split(self, bin_name: str, kwargs_with_ctx: dict):
        ops = [
            str_ops.split(bin_name=bin_name, **kwargs_with_ctx)
        ]
        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[bin_name] == list(EXAMPLE_STR)

    @pytest.mark.parametrize(
        "separator",
        [
            ".",
            ","
        ]
    )
    @expect_server_version_earlier_than_8_1_3_to_fail
    def test_split_with_separator(self, separator: str):
        ops = [
            str_ops.split(bin_name=STR_WITH_DOUBLE_BIN_NAME, separator=separator)
        ]

        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            if separator == ".":
                assert bins[STR_WITH_DOUBLE_BIN_NAME] == STRING_WITH_DOUBLE.split(separator)
            else:
                assert bins[STR_WITH_DOUBLE_BIN_NAME] == [STRING_WITH_DOUBLE]

    @expect_server_version_earlier_than_8_1_3_to_fail
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
    @expect_server_version_earlier_than_8_1_3_to_fail
    def test_regex_compare(self, pattern: str, expected_result: bool):
        ops = [
            str_ops.regex_compare(bin_name=MULTIBYTE_CODEPOINT_BIN_NAME, pattern=pattern)
        ]

        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[MULTIBYTE_CODEPOINT_BIN_NAME] is expected_result

    # Write operations

    def add_read_op(self, ops, bin_name):
        if bin_name == NESTED_STR_BIN_NAME:
            op = list_ops.list_get_by_index(bin_name, 0, aerospike.LIST_RETURN_VALUE)
        else:
            op = operations.read(bin_name=bin_name)
        ops.append(op)

    @pytest.mark.parametrize(
        "index, expected_value",
        [
            (1, EXAMPLE_STR[:1] + NEEDLE + EXAMPLE_STR[1:]),
            (-1, EXAMPLE_STR[:-1] + NEEDLE + EXAMPLE_STR[-1:])
        ]
    )
    @root_level_and_nested_str
    @kwargs_policy
    @expect_server_version_earlier_than_8_1_3_to_fail
    def test_insert(self, index: int, expected_value: str, kwargs_policy: dict, bin_name: str, kwargs_with_ctx: dict):
        ops = [
            str_ops.insert(bin_name=bin_name, index=index, value=NEEDLE, **kwargs_policy, **kwargs_with_ctx)
        ]
        self.add_read_op(ops, bin_name)

        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[bin_name] == expected_value

    @pytest.mark.parametrize(
        "index, expected_value",
        [
            (1, EXAMPLE_STR[:1] + SINGLE_CHAR + EXAMPLE_STR[2:]),
        ]
    )
    @root_level_and_nested_str
    @kwargs_policy
    @expect_server_version_earlier_than_8_1_3_to_fail
    def test_overwrite_single_char(self, index: int, expected_value: str, kwargs_policy: dict, bin_name: str, kwargs_with_ctx: dict):
        ops = [
            str_ops.overwrite(bin_name=bin_name, index=index, value=SINGLE_CHAR, **kwargs_policy, **kwargs_with_ctx)
        ]
        self.add_read_op(ops, bin_name)

        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[bin_name] == expected_value

    @expect_server_version_earlier_than_8_1_3_to_fail
    def test_overwrite_past_string_length(self):
        NEW_STR = EXAMPLE_STR + "a"
        ops = [
            str_ops.overwrite(None, bin_name=STR_BIN_NAME, index=0, value=NEW_STR)
        ]
        self.add_read_op(ops, STR_BIN_NAME)


        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[STR_BIN_NAME] == NEW_STR

    @root_level_and_nested_str
    @kwargs_policy
    @expect_server_version_earlier_than_8_1_3_to_fail
    def test_append(self, kwargs_policy: dict, bin_name: str, kwargs_with_ctx: dict):
        ops = [
            str_ops.append(bin_name=bin_name, value=NEEDLE, **kwargs_policy, **kwargs_with_ctx)
        ]
        self.add_read_op(ops, bin_name)

        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[bin_name] == EXAMPLE_STR + NEEDLE

    @root_level_and_nested_str
    @kwargs_policy
    @expect_server_version_earlier_than_8_1_3_to_fail
    def test_prepend(self, kwargs_policy: dict, bin_name: str, kwargs_with_ctx: dict):
        ops = [
            str_ops.prepend(bin_name=bin_name, value=NEEDLE, **kwargs_policy, **kwargs_with_ctx)
        ]
        self.add_read_op(ops, bin_name)

        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[bin_name] == NEEDLE + EXAMPLE_STR

    @pytest.mark.parametrize(
        "value_list",
        [
            [NEEDLE],
            [NEEDLE, NEEDLE]
        ]
    )
    @root_level_and_nested_str
    @kwargs_policy
    @expect_server_version_earlier_than_8_1_3_to_fail
    def test_concat(self, value_list: list[str], kwargs_policy: dict, bin_name: str, kwargs_with_ctx: dict):
        ops = [
            str_ops.concat(bin_name=bin_name, value_list=value_list, **kwargs_policy, **kwargs_with_ctx)
        ]
        self.add_read_op(ops, bin_name)

        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[bin_name] == EXAMPLE_STR + "".join(value_list)

    def test_concat_with_non_str_in_list(self):
        ops = [
            str_ops.concat(bin_name=STR_BIN_NAME, value_list=[1])
        ]

        with pytest.raises(e.ServerError):
            _, _, bins = self.as_connection.operate(KEY, ops)

    @pytest.mark.parametrize(
        "end_kwargs",
        [
            {},
            {"end": None},
            {"end": len(EXAMPLE_STR) - 1}
        ]
    )
    @root_level_and_nested_str
    @kwargs_policy
    @pytest.mark.skip("Test case with end omitted or set to None fails. Raised this with rest of client team. TODO")
    @expect_server_version_earlier_than_8_1_3_to_fail
    def test_snip(self, end_kwargs, kwargs_policy: dict, bin_name: str, kwargs_with_ctx: dict):

        START_IDX = 1
        ops = [
            str_ops.snip(bin_name=bin_name, start=START_IDX, **end_kwargs, **kwargs_policy, **kwargs_with_ctx)
        ]
        self.add_read_op(ops, bin_name)

        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            if "end" not in end_kwargs or end_kwargs["end"] is None:
                assert bins[bin_name] == EXAMPLE_STR[:START_IDX]
            else:
                assert bins[bin_name] == EXAMPLE_STR[:START_IDX] + EXAMPLE_STR[-1]

    @root_level_and_nested_str
    @kwargs_policy
    @expect_server_version_earlier_than_8_1_3_to_fail
    def test_replace(self, kwargs_policy: dict, bin_name: str, kwargs_with_ctx: dict):
        ops = [
            str_ops.replace(bin_name=bin_name, needle=NEEDLE, replacement=SINGLE_CHAR, **kwargs_policy, **kwargs_with_ctx)
        ]
        self.add_read_op(ops, bin_name)

        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[bin_name] == EXAMPLE_STR.replace(NEEDLE, SINGLE_CHAR, 1)

    @root_level_and_nested_str
    @kwargs_policy
    @expect_server_version_earlier_than_8_1_3_to_fail
    def test_replace_all(self, kwargs_policy: dict, bin_name: str, kwargs_with_ctx: dict):
        ops = [
            str_ops.replace_all(bin_name=bin_name, needle=NEEDLE, replacement=SINGLE_CHAR, **kwargs_policy, **kwargs_with_ctx)
        ]
        self.add_read_op(ops, bin_name)

        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[bin_name] == EXAMPLE_STR.replace(NEEDLE, SINGLE_CHAR)

    @root_level_and_nested_str
    @kwargs_policy
    @expect_server_version_earlier_than_8_1_3_to_fail
    def test_upper(self, kwargs_policy: dict, bin_name: str, kwargs_with_ctx: dict):
        ops = [
            str_ops.upper(bin_name=bin_name, **kwargs_policy, **kwargs_with_ctx)
        ]
        self.add_read_op(ops, bin_name)

        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[bin_name] == EXAMPLE_STR.upper()

    @kwargs_policy
    @expect_server_version_earlier_than_8_1_3_to_fail
    def test_lower(self, kwargs_policy: dict):
        ops = [
            str_ops.lower(bin_name=UPPERCASE_STR_BIN_NAME, **kwargs_policy)
        ]
        self.add_read_op(ops, UPPERCASE_STR_BIN_NAME)

        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[UPPERCASE_STR_BIN_NAME] == UPPERCASE_STR.lower()

    # TODO: add test case showing this char cannot be converted to ss with .lower()
    @kwargs_policy
    @expect_server_version_earlier_than_8_1_3_to_fail
    def test_casefold(self, kwargs_policy: dict):
        ops = [
            str_ops.casefold(bin_name=MULTIBYTE_CODEPOINT_BIN_NAME, **kwargs_policy)
        ]
        self.add_read_op(ops, MULTIBYTE_CODEPOINT_BIN_NAME)

        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[MULTIBYTE_CODEPOINT_BIN_NAME] == MULTIBYTE_CODEPOINT.casefold()

    @kwargs_policy
    @expect_server_version_earlier_than_8_1_3_to_fail
    def test_normalize_nfc(self, kwargs_policy):
        ops = [
            str_ops.normalize_nfc(bin_name=MULTIBYTE_CODEPOINT_BIN_NAME, **kwargs_policy)
        ]
        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[MULTIBYTE_CODEPOINT_BIN_NAME] == NORMALIZED_CODEPOINT

    @kwargs_policy
    @expect_server_version_earlier_than_8_1_3_to_fail
    def test_trim_start(self, kwargs_policy):
        ops = [
            str_ops.trim_start(bin_name=SURROUNDING_WHITESPACE_BIN_NAME, **kwargs_policy)
        ]
        self.add_read_op(ops, SURROUNDING_WHITESPACE_BIN_NAME)

        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[SURROUNDING_WHITESPACE_BIN_NAME] == EXAMPLE_STR_WITH_SURROUNDING_WHITESPACE[1:]

    @kwargs_policy
    @expect_server_version_earlier_than_8_1_3_to_fail
    def test_trim_end(self, kwargs_policy):
        ops = [
            str_ops.trim_end(bin_name=SURROUNDING_WHITESPACE_BIN_NAME, **kwargs_policy)
        ]
        self.add_read_op(ops, SURROUNDING_WHITESPACE_BIN_NAME)

        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[SURROUNDING_WHITESPACE_BIN_NAME] == EXAMPLE_STR_WITH_SURROUNDING_WHITESPACE[:-1]


    @kwargs_policy
    @expect_server_version_earlier_than_8_1_3_to_fail
    def test_trim(self, kwargs_policy):
        ops = [
            str_ops.trim(bin_name=SURROUNDING_WHITESPACE_BIN_NAME, **kwargs_policy)
        ]
        self.add_read_op(ops, SURROUNDING_WHITESPACE_BIN_NAME)

        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[SURROUNDING_WHITESPACE_BIN_NAME] == EXAMPLE_STR_WITH_SURROUNDING_WHITESPACE[1:-1]

    # TODO: add no-op test case
    @kwargs_policy
    @root_level_and_nested_str
    @pytest.mark.parametrize(
        "target_length, expected_results",
        [
            (len(EXAMPLE_STR) + 2, 2 * PAD_STRING + EXAMPLE_STR),
            (len(EXAMPLE_STR), EXAMPLE_STR),
            (len(EXAMPLE_STR) - 1, EXAMPLE_STR)
        ]
    )
    @expect_server_version_earlier_than_8_1_3_to_fail
    def test_pad_start(self, target_length: int, expected_results: str, kwargs_policy: dict, bin_name: str, kwargs_with_ctx: dict):
        ops = [
            str_ops.pad_start(bin_name=bin_name, pad_string=PAD_STRING, target_length=target_length, **kwargs_policy, **kwargs_with_ctx)
        ]
        self.add_read_op(ops, bin_name)

        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[bin_name] == expected_results

    @kwargs_policy
    @root_level_and_nested_str
    @pytest.mark.parametrize(
        "target_length, expected_results",
        [
            (len(EXAMPLE_STR) + 2, EXAMPLE_STR + 2 * PAD_STRING),
            (len(EXAMPLE_STR), EXAMPLE_STR),
            (len(EXAMPLE_STR) - 1, EXAMPLE_STR)
        ]
    )
    @expect_server_version_earlier_than_8_1_3_to_fail
    def test_pad_end(self, target_length: int, expected_results: str, kwargs_policy: dict, bin_name: str, kwargs_with_ctx: dict):
        ops = [
            str_ops.pad_end(bin_name=bin_name, pad_string=PAD_STRING, target_length=target_length, **kwargs_policy, **kwargs_with_ctx)
        ]
        self.add_read_op(ops, bin_name)

        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[bin_name] == expected_results

    @kwargs_policy
    @root_level_and_nested_str
    @pytest.mark.parametrize(
        "count",
        [
            1,
            2,
        ]
    )
    @expect_server_version_earlier_than_8_1_3_to_fail
    def test_repeat(self, count: int, kwargs_policy: dict, bin_name: str, kwargs_with_ctx: dict):
        ops = [
            str_ops.repeat(bin_name=bin_name, count=count, **kwargs_policy, **kwargs_with_ctx)
        ]
        self.add_read_op(ops, bin_name)

        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[bin_name] == EXAMPLE_STR * count

    @kwargs_policy
    @root_level_and_nested_str
    @expect_server_version_earlier_than_8_1_3_to_fail
    def test_regex_replace(self, kwargs_policy: dict, bin_name: str, kwargs_with_ctx: dict):
        PATTERN = "asdf"
        NEW_STR = "1234"
        ops = [
            str_ops.regex_replace(bin_name=bin_name, pattern=PATTERN, replacement=NEW_STR, **kwargs_policy, **kwargs_with_ctx)
        ]
        self.add_read_op(ops, bin_name)

        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[bin_name] == NEW_STR + "asdf"
