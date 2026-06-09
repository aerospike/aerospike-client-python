import pytest
from aerospike_helpers.operations import string_operations as str_ops
from aerospike_helpers import cdt_ctx
from .conftest import KEYS


KEY = KEYS[0]
STR_BIN_NAME = "str"
NESTED_STR_BIN_NAME = "nested_str"
EXAMPLE_STR = "asdf"
START_IDX = 1

class TestStringOperations:
    @pytest.fixture(autouse=True, scope="class")
    def setup(self, as_connection):
        self.as_connection.put(
            key=KEY,
            bins={
                STR_BIN_NAME: EXAMPLE_STR,
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
            str_ops.char_at(bin_name=STR_BIN_NAME, index=index, **kwargs_with_ctx)
        ]
        _, _, bins = self.as_connection.operate(KEY, ops)

        assert bins[bin_name] == len(EXAMPLE_STR[index])
