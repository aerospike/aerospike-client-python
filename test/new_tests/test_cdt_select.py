import pytest

import aerospike
from aerospike_helpers.operations import operations
from aerospike_helpers.expressions.base import GE, VarBuiltInMap
from aerospike_helpers import cdt_ctx
from aerospike import exception as e

class TestCDTSelectOperations:
    MAP_BIN_NAME = "map_bin"
    LIST_BIN_NAME = "list_bin"
    BINS_FOR_CDT_SELECT_TEST = {
        MAP_BIN_NAME: {
            "a": 1,
            "ab": {
            "bb": 12
            },
            "b": 2
        },
        LIST_BIN_NAME: [
            {
                "a": 1,
                "ab": {
                    "aa": 11,
                    "ab": 13,
                    "bb": 12
                },
                "b": 2
            },
            {
                "c": 3,
                "cd": {
                    "cc": 9
                },
                "d": 4
            }
        ]
    }
    @pytest.fixture(autouse=True)
    def insert_record(self):
        self.key = ("test", "demo", 1)
        self.as_connection.put(self.key, bins=self.BINS_FOR_CDT_SELECT_TEST)
        yield
        self.as_connection.remove(self.key)

    @pytest.mark.parametrize(
        # TODO: ids
        "op, expected_bins",
        [
            (
                operations.cdt_select(
                    name=LIST_BIN_NAME,
                    ctx=[
                        cdt_ctx.cdt_ctx_all(),
                    ]
                ),
                {
                    LIST_BIN_NAME: BINS_FOR_CDT_SELECT_TEST[LIST_BIN_NAME]
                }
            ),
            (
                operations.cdt_select(
                    name=MAP_BIN_NAME,
                    ctx=[
                        cdt_ctx.cdt_ctx_all(),
                    ]
                ),
                {
                    MAP_BIN_NAME: BINS_FOR_CDT_SELECT_TEST[MAP_BIN_NAME]
                }
            ),
            (
                operations.cdt_select(
                    name=LIST_BIN_NAME,
                    ctx=[
                        cdt_ctx.cdt_ctx_all(),
                        cdt_ctx.cdt_ctx_all()
                    ]
                ),
                {
                    LIST_BIN_NAME: [
                        1,
                        {
                            "aa": 11,
                            "ab": 13,
                            "bb": 12
                        },
                        2,
                        3,
                        {
                            "cc": 9
                        },
                        4
                    ]
                }
            )
        ]
    )
    def test_cdt_select_basic_functionality(self, op, expected_bins):
        ops = [
            op
        ]
        _, _, bins = self.as_connection.operate(self.key, ops)
        assert bins == expected_bins

    @pytest.mark.parametrize(
        "flags", [
            aerospike.CDT_SELECT_TREE,
            # TODO: combine?
            aerospike.CDT_SELECT_LEAF_LIST_VALUE,
            # TODO: bad naming?
            aerospike.CDT_SELECT_LEAF_MAP_VALUE,
            aerospike.CDT_SELECT_LEAF_MAP_KEY,
            aerospike.CDT_SELECT_NO_FAIL
        ]
    )
    def test_cdt_select_flags(self, flags, expected_bins):
        ops = [
            operations.cdt_select(
                # TODO: not done
                flags=flags
            )
        ]
        _, _, bins = self.as_connection.operate(self.key, ops)
        assert bins == expected_bins

    @pytest.mark.parametrize(
        "op, context",
        [
            (
                operations.cdt_select(
                    name=MAP_BIN_NAME,
                    ctx=[],
                ),
                # TODO: vague
                pytest.raises(e.AerospikeError)
            ),
        ]
    )
    def test_cdt_select_negative_cases(self, op, context):
        ops = [
            op
        ]
        with context:
            self.as_connection.operate(self.key, ops)

    def test_cdt_apply(self):
        expr = GE(
            VarBuiltInMap(aerospike.EXP_BUILTIN_VALUE),
            15
        ).compile()
        ops = [
            operations.cdt_apply(
                name=self.LIST_BIN_NAME,
                ctx=[
                    cdt_ctx.cdt_ctx_all(),
                ],
                expr=expr
            )
        ]
        self.as_connection.operate(self.key, ops)
