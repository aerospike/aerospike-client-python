import pytest
from aerospike_helpers.operations import string_operations as str_ops
from aerospike_helpers import cdt_ctx
from .conftest import KEYS


KEY = KEYS[0]

STR_BIN_NAME = "str"
NESTED_STR_BIN_NAME = "nested_str"
STR_WITH_INT_BIN_NAME = "str_with_int"

NEEDLE = "asdf"
EXAMPLE_STR = NEEDLE * 2
NOT_IN_EXAMPLE_STR = STRING_WITH_INT = "1"
START_IDX = 1


class TestStringOperations:
    @pytest.fixture(autouse=True, scope="class")
    def setup(self, as_connection):
        self.as_connection.put(
            key=KEY,
            bins={
                STR_BIN_NAME: EXAMPLE_STR,
                STR_WITH_INT_BIN_NAME: STRING_WITH_INT,
                NESTED_STR_BIN_NAME: [EXAMPLE_STR]
            }
        )
        yield
        self.as_connection.remove(KEY)

    ctx_param = pytest.mark.parametrize(
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

        assert bins[STR_BIN_NAME] == 1
