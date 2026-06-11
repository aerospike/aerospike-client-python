import pytest

from aerospike_helpers.expressions import string as str_expr
from aerospike_helpers.operations import expression_operations as expr_ops

from .test_base_class import TestBaseClass
from .conftest import KEYS
from .string_helpers import *


# TODO: verify that subclassing is correct behavior
class TestExpressions(TestBaseClass):
    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        self.as_connection.put(
            key=KEY,
            bins=BINS
        )

        yield

    @pytest.mark.parametrize(
        "expr, expected_result",
        [
            (str_expr.StrLen(STR_BIN_NAME), len(EXAMPLE_STR))
        ]
    )
    def test_reading_str_bins(self, expr, expected_result):
        compiled_expr = expr.compile()
        ops = [
            expr_ops.expression_read(STR_BIN_NAME, compiled_expr)
        ]
        _, _, bins = self.as_connection.operate(KEY, ops)

        assert bins[STR_BIN_NAME] == expected_result
