import pytest
import base64

from aerospike_helpers.expressions import string as str_expr
from aerospike_helpers.operations import expression_operations as expr_ops
from aerospike_helpers.operations import operations
from aerospike_helpers.string_helpers import NumericType, RegexFlags
from aerospike import exception as e

from .test_base_class import TestBaseClass
from .string_helpers import *
from .conftest import expect_server_version_earlier_than_8_1_3_to_fail


# TODO: verify that subclassing is correct behavior
class TestExpressions(TestBaseClass):
    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection, expect_earlier_than_server_version_to_fail):
        self.as_connection.put(
            key=KEY,
            bins=BINS
        )

        yield

    # TODO: should use hashmap that maps bin names to values.
    @pytest.mark.parametrize(
        "expr, expected_result",
        [
            (str_expr.StrLen(bin=STR_BIN_NAME), len(EXAMPLE_STR)),
            (str_expr.SubStr(start=START_IDX, end=None, bin=STR_BIN_NAME), EXAMPLE_STR[START_IDX:]),
            (str_expr.SubStr(start=START_IDX, end=START_IDX + 2, bin=STR_BIN_NAME), EXAMPLE_STR[START_IDX:(START_IDX + 2)]),
            (str_expr.CharAt(index=START_IDX, bin=STR_BIN_NAME), EXAMPLE_STR[START_IDX]),
            (str_expr.CharAt(index=-1, bin=STR_BIN_NAME), EXAMPLE_STR[-1]),
            (str_expr.Find(needle=NEEDLE, occurrence=1, bin=STR_BIN_NAME), EXAMPLE_STR.find(NEEDLE)),
            (str_expr.Find(needle=NEEDLE, occurrence=2, bin=STR_BIN_NAME), 4),
            (str_expr.Find(needle=NOT_IN_EXAMPLE_STR, occurrence=1, bin=STR_BIN_NAME), -1),
            (str_expr.Contains(needle=NEEDLE, bin=STR_BIN_NAME), True),
            (str_expr.Contains(needle=NOT_IN_EXAMPLE_STR, bin=STR_BIN_NAME), False),
            (str_expr.StartsWith(prefix=NEEDLE, bin=STR_BIN_NAME), True),
            (str_expr.StartsWith(prefix=NOT_IN_EXAMPLE_STR, bin=STR_BIN_NAME), False),
            (str_expr.EndsWith(suffix=NEEDLE, bin=STR_BIN_NAME), True),
            (str_expr.EndsWith(suffix=NOT_IN_EXAMPLE_STR, bin=STR_BIN_NAME), False),
            (str_expr.ToInteger(bin=STR_WITH_INT_BIN_NAME), int(STRING_WITH_INT)),
            (str_expr.ToDouble(bin=STR_WITH_DOUBLE_BIN_NAME), float(STRING_WITH_DOUBLE)),
            (str_expr.ByteLength(bin=STR_BIN_NAME), len(EXAMPLE_STR)),
            (str_expr.IsNumeric(bin=STR_BIN_NAME), False),
            (str_expr.IsNumeric(bin=STR_WITH_INT_BIN_NAME), True),
            (str_expr.IsNumeric(bin=STR_WITH_INT_BIN_NAME, numeric_type=NumericType.INT), True),
            (str_expr.IsNumeric(bin=STR_WITH_DOUBLE_BIN_NAME, numeric_type=NumericType.FLOAT), True),
            (str_expr.IsNumeric(bin=STR_WITH_DOUBLE_BIN_NAME, numeric_type=NumericType.INT), False),
            (str_expr.IsNumeric(bin=STR_WITH_INT_BIN_NAME, numeric_type=NumericType.FLOAT), False),
            (str_expr.IsUpper(bin=STR_BIN_NAME), False),
            (str_expr.IsUpper(bin=UPPERCASE_STR_BIN_NAME), True),
            (str_expr.IsLower(bin=STR_BIN_NAME), True),
            (str_expr.IsLower(bin=UPPERCASE_STR_BIN_NAME), False),
            (str_expr.ToBlob(bin=STR_BIN_NAME), bytes(EXAMPLE_STR, encoding="utf-8")),
            (str_expr.Split(bin=STR_BIN_NAME), list(EXAMPLE_STR)),
            (str_expr.Split(bin=STR_WITH_DOUBLE_BIN_NAME, separator='.'), STRING_WITH_DOUBLE.split('.')),
            (str_expr.Split(bin=STR_WITH_DOUBLE_BIN_NAME, separator=','), [STRING_WITH_DOUBLE]),
            (
                str_expr.Base64Decode(bin=BASE64_ENCODED_BIN_NAME),
                bytearray(base64.b64decode(BASE64_ENCODED_STR))
            ),
            (
                str_expr.RegexCompare(pattern=MULTIBYTE_CODEPOINT, regex_flags=RegexFlags.DEFAULT, bin=MULTIBYTE_CODEPOINT_BIN_NAME),
                True
            ),
            (
                str_expr.RegexCompare(pattern="π", regex_flags=RegexFlags.DEFAULT, bin=MULTIBYTE_CODEPOINT_BIN_NAME),
                False
            )
        ]
    )
    @expect_server_version_earlier_than_8_1_3_to_fail
    def test_reading_str_bins(self, expr, expected_result):
        compiled_expr = expr.compile()
        ops = [
            expr_ops.expression_read(STR_BIN_NAME, compiled_expr)
        ]
        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[STR_BIN_NAME] == expected_result

    @pytest.mark.parametrize(
        "expr",
        [
            str_expr.ToInteger(bin=STR_BIN_NAME),
            str_expr.ToDouble(bin=STR_BIN_NAME)
        ]
    )
    def test_expression_read_fail(self, expr):
        compiled_expr = expr.compile()
        ops = [
            expr_ops.expression_read(STR_BIN_NAME, compiled_expr)
        ]
        if (TestBaseClass.major_ver, TestBaseClass.minor_ver, TestBaseClass.patch_ver) >= (8, 1, 3):
            expected_exc = e.OpNotApplicable
        else:
            expected_exc = e.InvalidRequest

        with pytest.raises(expected_exc):
            self.as_connection.operate(KEY, ops)

    # TODO: need to reuse StringPolicy parameters

    @pytest.mark.parametrize(
        "expr, expected_result",
        [
            # TODO: maybe have a place to share expected results for both string ops and exprs.
            (
                str_expr.Insert(policy=None, index=1, value=NEEDLE, bin=STR_BIN_NAME),
                EXAMPLE_STR[:1] + NEEDLE + EXAMPLE_STR[1:]
            ),
            (
                str_expr.Insert(policy=None, index=-1, value=NEEDLE, bin=STR_BIN_NAME),
                EXAMPLE_STR[:-1] + NEEDLE + EXAMPLE_STR[-1:]
            ),
            (
                str_expr.Overwrite(policy=None, index=1, value=SINGLE_CHAR, bin=STR_BIN_NAME),
                EXAMPLE_STR[:1] + SINGLE_CHAR + EXAMPLE_STR[2:]
            ),
            (
                str_expr.Overwrite(policy=None, index=0, value=EXAMPLE_STR + "a", bin=STR_BIN_NAME),
                EXAMPLE_STR + "a"
            ),
            # TODO: add test case for append and prepend.
            (
                str_expr.Concat(policy=None, values=[NEEDLE], bin=STR_BIN_NAME),
                EXAMPLE_STR + NEEDLE
            ),
            (
                str_expr.Concat(policy=None, values=[NEEDLE, NEEDLE], bin=STR_BIN_NAME),
                EXAMPLE_STR + NEEDLE + NEEDLE
            ),
#            (
#                str_expr.Snip(policy=None, start=START_IDX, bin=STR_BIN_NAME),
#                EXAMPLE_STR[:START_IDX]
#            ),
#            (
#                str_expr.Snip(policy=None, start=START_IDX, end=None, bin=STR_BIN_NAME),
#                EXAMPLE_STR[:START_IDX]
#            ),
            (
                str_expr.Snip(policy=None, start=START_IDX, end=len(EXAMPLE_STR) - 1, bin=STR_BIN_NAME),
                EXAMPLE_STR[:START_IDX] + EXAMPLE_STR[-1]
            ),
            (
                str_expr.Replace(policy=None, needle=NEEDLE, replacement=SINGLE_CHAR, bin=STR_BIN_NAME),
                EXAMPLE_STR.replace(NEEDLE, SINGLE_CHAR, 1)
            ),
            (
                str_expr.ReplaceAll(policy=None, needle=NEEDLE, replacement=SINGLE_CHAR, bin=STR_BIN_NAME),
                EXAMPLE_STR.replace(NEEDLE, SINGLE_CHAR)
            ),
            (
                str_expr.Upper(policy=None, bin=STR_BIN_NAME),
                EXAMPLE_STR.upper()
            ),
            (
                str_expr.Lower(policy=None, bin=UPPERCASE_STR_BIN_NAME),
                UPPERCASE_STR.lower()
            ),
            (
                str_expr.CaseFold(policy=None, bin=MULTIBYTE_CODEPOINT_BIN_NAME),
                # TODO: dynamically get expected result
                "ss"
            ),
            # TODO: this test case needs to be corrected
            # (
            #     str_expr.NormalizeNFC(policy=None, bin=MULTIBYTE_CODEPOINT_BIN_NAME),
            #     "ss"
            # ),
            (
                str_expr.TrimStart(policy=None, bin=SURROUNDING_WHITESPACE_BIN_NAME),
                EXAMPLE_STR_WITH_SURROUNDING_WHITESPACE[1:]
            ),
            (
                str_expr.TrimEnd(policy=None, bin=SURROUNDING_WHITESPACE_BIN_NAME),
                EXAMPLE_STR_WITH_SURROUNDING_WHITESPACE[:-1]
            ),
            (
                str_expr.Trim(policy=None, bin=SURROUNDING_WHITESPACE_BIN_NAME),
                EXAMPLE_STR_WITH_SURROUNDING_WHITESPACE[1:-1]
            ),
            (
                str_expr.PadStart(policy=None, target_length=len(EXAMPLE_STR) + 2, pad_string=PAD_STRING, bin=STR_BIN_NAME),
                2 * PAD_STRING + EXAMPLE_STR
            ),
            (
                str_expr.PadStart(policy=None, target_length=len(EXAMPLE_STR), pad_string=PAD_STRING, bin=STR_BIN_NAME),
                EXAMPLE_STR
            ),
            (
                str_expr.PadStart(policy=None, target_length=len(EXAMPLE_STR) - 1, pad_string=PAD_STRING, bin=STR_BIN_NAME),
                EXAMPLE_STR
            ),
            (
                str_expr.PadEnd(policy=None, target_length=len(EXAMPLE_STR) + 2, pad_string=PAD_STRING, bin=STR_BIN_NAME),
                EXAMPLE_STR + 2 * PAD_STRING
            ),
            (
                str_expr.PadEnd(policy=None, target_length=len(EXAMPLE_STR), pad_string=PAD_STRING, bin=STR_BIN_NAME),
                EXAMPLE_STR
            ),
            (
                str_expr.PadEnd(policy=None, target_length=len(EXAMPLE_STR) - 1, pad_string=PAD_STRING, bin=STR_BIN_NAME),
                EXAMPLE_STR
            ),
            (
                str_expr.Repeat(policy=None, count=1, bin=STR_BIN_NAME),
                EXAMPLE_STR
            ),
            (
                str_expr.Repeat(policy=None, count=2, bin=STR_BIN_NAME),
                EXAMPLE_STR * 2
            ),
            (
                str_expr.RegexReplace(policy=None, pattern="asdf", replacement="1234", regex_flags=RegexFlags.DEFAULT, bin=STR_BIN_NAME),
                "1234asdf"
            ),
        ]
    )
    @expect_server_version_earlier_than_8_1_3_to_fail
    def test_expression_write(self, expr, expected_result):
        compiled_expr = expr.compile()
        ops = [
            expr_ops.expression_write(STR_BIN_NAME, compiled_expr),
            operations.read(STR_BIN_NAME)
        ]
        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(KEY, ops)

            assert bins[STR_BIN_NAME] == expected_result
