import pytest
import base64

from aerospike_helpers.operations import string_operations as str_ops
from aerospike_helpers.string_helpers import NumericType, StringPolicy, WriteFlags
from aerospike import exception as e
from aerospike_helpers import cdt_ctx
from .conftest import KEYS


KEY = KEYS[0]

SINGLE_CHAR = "z"
STR_BIN_NAME = "str"
UPPERCASE_STR_BIN_NAME = "uppercase_str"
NESTED_STR_BIN_NAME = "nested_str"
STR_WITH_INT_BIN_NAME = "str_with_int"
STR_WITH_DOUBLE_BIN_NAME = "str_with_double"
MULTIBYTE_CODEPOINT_BIN_NAME = "multibyte"
BASE64_ENCODED_BIN_NAME = "base64_enc"

NEEDLE = "asdf"
EXAMPLE_STR = NEEDLE * 2
UPPERCASE_STR = EXAMPLE_STR.upper()
NOT_IN_EXAMPLE_STR = STRING_WITH_INT = "1"
STRING_WITH_DOUBLE = "2.3"
MULTIBYTE_CODEPOINT = "ñ"
BASE64_ENCODED_STR = "YXNkZgo="

START_IDX = 1


class TestStringOperations:
    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        self.as_connection.put(
            key=KEY,
            bins={
                STR_BIN_NAME: EXAMPLE_STR,
                STR_WITH_INT_BIN_NAME: STRING_WITH_INT,
                NESTED_STR_BIN_NAME: [EXAMPLE_STR],
                UPPERCASE_STR_BIN_NAME: UPPERCASE_STR,

            }
        )

        yield

        self.as_connection.remove(KEY)

    pytestmark = [
        pytest.mark.parametrize(
            "bin_name, kwargs_with_ctx",
            [
                pytest.param(STR_BIN_NAME, {}),
                pytest.param(
                    NESTED_STR_BIN_NAME,
                    {
                        "ctx": [
                            cdt_ctx.cdt_ctx_list_index(0)
                        ]
                    }
                )
            ]
        )
    ]

    def test_strlen(self, bin_name: str, kwargs_with_ctx: dict):
        ops = [
            str_ops.strlen(bin_name=bin_name, **kwargs_with_ctx)
        ]
        _, _, bins = self.as_connection.operate(KEY, ops)
        assert bins[bin_name] == len(EXAMPLE_STR)

    @pytest.mark.parametrize(
        "length_kwargs",
        [
            {},
            {
                "length": None
            },
            {
                "length": 2
            }
        ]
    )
    def test_substr(self, length_kwargs: dict, bin_name: str, kwargs_with_ctx: dict):
        kwargs_with_ctx |= length_kwargs
        ops = [
            str_ops.substr(bin_name=bin_name, start=START_IDX, **kwargs_with_ctx)
        ]
        _, _, bins = self.as_connection.operate(KEY, ops)

        if "length" not in length_kwargs or length_kwargs["length"] is None:
            assert bins[bin_name] == len(EXAMPLE_STR[START_IDX:])
        else:
            length = length_kwargs["length"]
            assert bins[bin_name] == len(EXAMPLE_STR[START_IDX:length])

    @pytest.mark.parametrize(
        "index",
        [
            START_IDX,
            -1
        ]
    )
    def test_char_at(self, index: int, bin_name: str, kwargs_with_ctx: dict):
        ops = [
            str_ops.char_at(bin_name=bin_name, index=index, **kwargs_with_ctx)
        ]
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
    def test_find(self, occurrence_kwargs: dict, expected_idx: int, bin_name: str, kwargs_with_ctx: dict):
        kwargs_with_ctx |= occurrence_kwargs
        ops = [
            str_ops.find(bin_name=bin_name, needle=NEEDLE, **kwargs_with_ctx)
        ]
        _, _, bins = self.as_connection.operate(KEY, ops)

        assert bins[bin_name] == expected_idx

    def test_find_not_found(self):
        ops = [
            str_ops.find(bin_name=STR_BIN_NAME, needle=NOT_IN_EXAMPLE_STR)
        ]
        _, _, bins = self.as_connection.operate(KEY, ops)

        assert bins[STR_BIN_NAME] == -1

    def test_contains(self, bin_name: str, kwargs_with_ctx: dict):
        ops = [
            str_ops.contains(bin_name=bin_name, needle=NEEDLE, **kwargs_with_ctx)
        ]
        _, _, bins = self.as_connection.operate(KEY, ops)

        assert bins[STR_BIN_NAME] is True

    def test_contains_not_found(self):
        ops = [
            str_ops.contains(bin_name=STR_BIN_NAME, needle=NOT_IN_EXAMPLE_STR)
        ]
        _, _, bins = self.as_connection.operate(KEY, ops)

        assert bins[STR_BIN_NAME] is False

    def test_starts_with(self, bin_name: str, kwargs_with_ctx: dict):
        ops = [
            str_ops.starts_with(bin_name=bin_name, prefix=NEEDLE, **kwargs_with_ctx)
        ]
        _, _, bins = self.as_connection.operate(KEY, ops)

        assert bins[STR_BIN_NAME] is True

    def test_starts_with_returns_false(self):
        ops = [
            str_ops.starts_with(bin_name=STR_BIN_NAME, prefix=NOT_IN_EXAMPLE_STR)
        ]
        _, _, bins = self.as_connection.operate(KEY, ops)

        assert bins[STR_BIN_NAME] is False

    def test_ends_with(self, bin_name: str, kwargs_with_ctx: dict):
        ops = [
            str_ops.ends_with(bin_name=bin_name, suffix=NEEDLE, **kwargs_with_ctx)
        ]
        _, _, bins = self.as_connection.operate(KEY, ops)

        assert bins[STR_BIN_NAME] is True

    def test_ends_with_returns_false(self):
        ops = [
            str_ops.ends_with(bin_name=STR_BIN_NAME, suffix=NOT_IN_EXAMPLE_STR)
        ]
        _, _, bins = self.as_connection.operate(KEY, ops)

        assert bins[STR_BIN_NAME] is False

    def test_to_integer(self):
        ops = [
            str_ops.to_integer(bin_name=STR_WITH_INT_BIN_NAME)
        ]
        _, _, bins = self.as_connection.operate(KEY, ops)

        assert bins[STR_BIN_NAME] == int(STRING_WITH_INT)

    def test_to_integer_fail(self):
        ops = [
            str_ops.to_integer(bin_name=STR_BIN_NAME)
        ]
        with pytest.raises(e.ParamError):
            self.as_connection.operate(KEY, ops)

    def test_to_double(self):
        ops = [
            str_ops.to_double(bin_name=STR_WITH_DOUBLE_BIN_NAME)
        ]
        _, _, bins = self.as_connection.operate(KEY, ops)

        assert bins[STR_BIN_NAME] == float(STRING_WITH_DOUBLE)

    def test_to_double_fail(self):
        ops = [
            str_ops.to_double(bin_name=STR_BIN_NAME)
        ]
        with pytest.raises(e.ParamError):
            self.as_connection.operate(KEY, ops)

    # TODO: add case for multi-byte unicode codepoints
    def test_byte_length(self, bin_name: str, kwargs_with_ctx: dict):
        ops = [
            str_ops.byte_length(bin_name=bin_name, **kwargs_with_ctx)
        ]
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
    def test_is_numeric(self, bin_name: str, expected_result: bool):
        ops = [
            str_ops.is_numeric(bin_name=bin_name)
        ]
        _, _, bins = self.as_connection.operate(KEY, ops)

        assert bins[bin_name] is expected_result

    @pytest.mark.parametrize(
        "numeric_type, bin_name, expected_result",
        [
            # Positive test cases
            (NumericType.INT, STR_WITH_INT_BIN_NAME, True),
            (NumericType.FLOAT, STR_WITH_DOUBLE_BIN_NAME, True)
            # Negative test cases
            (NumericType.INT, STR_WITH_DOUBLE_BIN_NAME, False),
            (NumericType.FLOAT, STR_WITH_INT_BIN_NAME, False)
        ]
    )
    def test_numeric_type(self, numeric_type: NumericType, bin_name: str, expected_result: bool):
        ops = [
            str_ops.is_numeric(bin_name=bin_name, numeric_type=numeric_type)
        ]
        _, _, bins = self.as_connection.operate(KEY, ops)

        assert bins[bin_name] is expected_result

    @pytest.mark.parametrize(
        "bin_name, expected_result",
        [
            (STR_BIN_NAME, False)
            (UPPERCASE_STR_BIN_NAME, True)
        ]
    )
    def test_is_upper(self, bin_name: str, expected_result: bool):
        ops = [
            str_ops.is_upper(bin_name=bin_name)
        ]
        _, _, bins = self.as_connection.operate(KEY, ops)

        assert bins[bin_name] is expected_result

    @pytest.mark.parametrize(
        "bin_name, expected_result",
        [
            (STR_BIN_NAME, True)
            (UPPERCASE_STR_BIN_NAME, False)
        ]
    )
    def test_is_lower(self, bin_name: str, expected_result: bool):
        ops = [
            str_ops.is_lower(bin_name=bin_name)
        ]
        _, _, bins = self.as_connection.operate(KEY, ops)

        assert bins[bin_name] is expected_result

    def test_to_blob(self, bin_name: str, kwargs_with_ctx: dict):
        ops = [
            str_ops.to_blob(bin_name=bin_name)
        ]
        _, _, bins = self.as_connection.operate(KEY, ops)

        assert bins[bin_name] == EXAMPLE_STR

    def test_split(self):
        ops = [
            str_ops.split(bin_name=STR_BIN_NAME)
        ]
        _, _, bins = self.as_connection.operate(KEY, ops)

        assert bins[STR_BIN_NAME] == list(EXAMPLE_STR)

    def test_split_with_separator(self):
        ops = [
            str_ops.split(bin_name=STR_WITH_DOUBLE_BIN_NAME, separator=".")
        ]
        _, _, bins = self.as_connection.operate(KEY, ops)

        assert bins[STR_WITH_DOUBLE_BIN_NAME] == STRING_WITH_DOUBLE.split('.')

    def test_base64_decode(self):
        ops = [
            str_ops.base64_decode(bin_name=BASE64_ENCODED_BIN_NAME, separator=".")
        ]
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
        _, _, bins = self.as_connection.operate(KEY, ops)

        assert bins[MULTIBYTE_CODEPOINT_BIN_NAME] is expected_result

    # Write operations

    kwargs_policy = pytest.mark.parametrize(
        "kwargs_policy",
        [
            {},
            {"policy": None},
            {"policy": StringPolicy()}
        ]
    )

    @pytest.mark.parametrize(
        "index, expected_value",
        [
            (1, EXAMPLE_STR[:1] + NEEDLE + EXAMPLE_STR[1:]),
            (-1, EXAMPLE_STR[:-1])
        ]
    )
    @kwargs_policy
    def test_insert(self, index: int, expected_value: str, kwargs_policy: dict, bin_name: str, kwargs_with_ctx: dict):
        ops = [
            str_ops.insert(bin_name=bin_name, index=index, value=NEEDLE, **kwargs_policy)
        ]
        _, _, bins = self.as_connection.operate(KEY, ops)

        assert bins[bin_name] == expected_value

    @pytest.mark.parametrize(
        "index, expected_value",
        [
            (1, EXAMPLE_STR[:1] + SINGLE_CHAR + EXAMPLE_STR[2:]),
            (len(EXAMPLE_STR), SINGLE_CHAR + EXAMPLE_STR[1:])
        ]
    )
    @kwargs_policy
    def test_overwrite_single_char(self, index: int, expected_value: str, kwargs_policy: dict, bin_name: str, kwargs_with_ctx: dict):
        ops = [
            str_ops.overwrite(bin_name=bin_name, index=index, value=SINGLE_CHAR, **kwargs_policy, **kwargs_with_ctx)
        ]
        _, _, bins = self.as_connection.operate(KEY, ops)

        assert bins[bin_name] == expected_value

    @kwargs_policy
    def test_overwrite_past_string_length(self):
        NEW_STR = EXAMPLE_STR + "a"
        ops = [
            str_ops.overwrite(None, bin_name=STR_BIN_NAME, index=0, value=NEW_STR)
        ]
        _, _, bins = self.as_connection.operate(KEY, ops)

        assert bins[STR_BIN_NAME] == NEW_STR

    @kwargs_policy
    def test_concat(self, kwargs_policy: dict, bin_name: str, kwargs_with_ctx: dict):
        ops = [
            str_ops.concat(bin_name=bin_name, value=NEEDLE, **kwargs_policy, **kwargs_with_ctx)
        ]
        _, _, bins = self.as_connection.operate(KEY, ops)

        assert bins[bin_name] == EXAMPLE_STR + NEEDLE

    @pytest.mark.parametrize(
        "values",
        [
            [NEEDLE],
            [NEEDLE, NEEDLE]
        ]
    )
    @kwargs_policy
    def test_concat_list(self, values: list[str], kwargs_policy: dict, bin_name: str, kwargs_with_ctx: dict):
        ops = [
            str_ops.concat_list(bin_name=bin_name, values=values, **kwargs_policy, **kwargs_with_ctx)
        ]
        _, _, bins = self.as_connection.operate(KEY, ops)

        assert bins[bin_name] == EXAMPLE_STR + str.join(values)

    @pytest.mark.parametrize(
        "end_kwargs",
        [
            {},
            {"end": None},
            {"end": len(EXAMPLE_STR) - 2}
        ]
    )
    @kwargs_policy
    def test_snip(self, end_kwargs, kwargs_policy: dict, bin_name: str, kwargs_with_ctx: dict):

        START_IDX = 1
        ops = [
            str_ops.snip(bin_name=bin_name, start=START_IDX, **end_kwargs, **kwargs_policy, **kwargs_with_ctx)
        ]
        _, _, bins = self.as_connection.operate(KEY, ops)

        if "end" not in end_kwargs or end_kwargs["end"] is None:
            assert bins[bin_name] == EXAMPLE_STR[:START_IDX]
        else:
            assert bins[bin_name] == EXAMPLE_STR[:START_IDX] + EXAMPLE_STR[-1]

    @kwargs_policy
    def test_replace(self, kwargs_policy: dict, bin_name: str, kwargs_with_ctx: dict):
        ops = [
            str_ops.replace(bin_name=bin_name, needle=NEEDLE, replacement=SINGLE_CHAR, **kwargs_policy, **kwargs_with_ctx)
        ]
        _, _, bins = self.as_connection.operate(KEY, ops)

        assert bins[bin_name] == EXAMPLE_STR.replace(old=NEEDLE, new=SINGLE_CHAR, count=1)

    @kwargs_policy
    def test_replace_all(self, kwargs_policy: dict, bin_name: str, kwargs_with_ctx: dict):
        ops = [
            str_ops.replace_all(bin_name=bin_name, needle=NEEDLE, replacement=SINGLE_CHAR, **kwargs_policy, **kwargs_with_ctx)
        ]
        _, _, bins = self.as_connection.operate(KEY, ops)

        assert bins[bin_name] == EXAMPLE_STR.replace(old=NEEDLE, new=SINGLE_CHAR)

    @kwargs_policy
    def test_upper(self, kwargs_policy: dict, bin_name: str, kwargs_with_ctx: dict):
        ops = [
            str_ops.replace_all(bin_name=bin_name, needle=NEEDLE, replacement=SINGLE_CHAR, **kwargs_policy, **kwargs_with_ctx)
        ]
        _, _, bins = self.as_connection.operate(KEY, ops)

        assert bins[bin_name] == EXAMPLE_STR.replace(old=NEEDLE, new=SINGLE_CHAR)
