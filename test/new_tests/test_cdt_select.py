import pytest

import aerospike
from aerospike_helpers.operations import operations
from aerospike_helpers.expressions.resources import ResultType
from aerospike_helpers.expressions.base import GE, Eq, LoopVarStr, LoopVarFloat
from aerospike_helpers.expressions.arithmetic import Sub
from aerospike_helpers import cdt_ctx
from aerospike import exception as e
from contextlib import nullcontext
from .test_base_class import TestBaseClass
import copy


@pytest.fixture(scope="class", autouse=True)
def setup_class(as_connection, request):
    if (TestBaseClass.major_ver, TestBaseClass.minor_ver, TestBaseClass.patch_ver) >= (8, 1, 1):
        request.cls.expected_context_for_pos_tests = nullcontext()
    else:
        # InvalidRequest, BinIncompatibleTypes are exceptions that have been raised
        request.cls.expected_context_for_pos_tests = pytest.raises(e.ServerError)


class TestCDTSelectOperations:
    MAP_BIN_NAME = "map_bin"
    LIST_BIN_NAME = "list_bin"
    MAP_OF_NESTED_MAPS_BIN_NAME = "map_of_maps_bin"
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
        ],
        MAP_OF_NESTED_MAPS_BIN_NAME: {
            "Day1": {
                "book": 14.990000,
                "ferry": 5.000000,
            },
            "Day2": {
                "food": 34.000000,
                "game": 12.990000,
            },
            "Day3": {
                "plants": 19.990000,
                "stickers": 2.000000
            }
        }
    }
    @pytest.fixture(autouse=True)
    def insert_record(self):
        self.key = ("test", "demo", 1)
        self.as_connection.put(self.key, bins=self.BINS_FOR_CDT_SELECT_TEST)
        yield
        self.as_connection.remove(self.key)

    EXPR_ON_DIFFERENT_ITERATED_TYPE = Eq(LoopVarStr(aerospike.EXP_LOOPVAR_VALUE), "a").compile()

    @pytest.mark.parametrize(
        # TODO: ids
        "op,expected_bins",
        [
            pytest.param(
                operations.select_by_path(
                    name=LIST_BIN_NAME,
                    ctx=[
                        cdt_ctx.cdt_ctx_all_children(),
                    ],
                    flags=aerospike.CDT_SELECT_VALUES
                ),
                {
                    LIST_BIN_NAME: BINS_FOR_CDT_SELECT_TEST[LIST_BIN_NAME]
                },
                id="select_all_children_once_in_list"
            ),
            pytest.param(
                operations.select_by_path(
                    name=MAP_BIN_NAME,
                    ctx=[
                        cdt_ctx.cdt_ctx_all_children(),
                    ],
                    flags=aerospike.CDT_SELECT_VALUES
                ),
                {
                    MAP_BIN_NAME: list(BINS_FOR_CDT_SELECT_TEST[MAP_BIN_NAME].values())
                },
                id="select_all_children_once_in_map"
            ),
            pytest.param(
                operations.select_by_path(
                    name=LIST_BIN_NAME,
                    ctx=[
                        cdt_ctx.cdt_ctx_all_children(),
                        cdt_ctx.cdt_ctx_all_children()
                    ],
                    flags=aerospike.CDT_SELECT_VALUES
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
                },
                id="select_all_children_twice_in_list"
            ),
            pytest.param(
                operations.select_by_path(
                    name=MAP_BIN_NAME,
                    ctx=[
                        cdt_ctx.cdt_ctx_all_children(),
                        cdt_ctx.cdt_ctx_all_children_with_filter(expression=EXPR_ON_DIFFERENT_ITERATED_TYPE)
                    ],
                    flags=aerospike.CDT_SELECT_VALUES | aerospike.CDT_SELECT_NO_FAIL
                ),
                {
                    MAP_BIN_NAME: []
                },
                id="cdt_select_no_fail"
            )
        ]
    )
    def test_cdt_select_basic_functionality(self, op, expected_bins):
        ops = [
            op
        ]
        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(self.key, ops)
            assert bins == expected_bins

    FILTER_EXPR = GE(
        LoopVarFloat(aerospike.EXP_LOOPVAR_VALUE),
        20.0
    ).compile()

    def test_cdt_select_with_filter(self):
        ops = [
            operations.select_by_path(
                name=self.MAP_OF_NESTED_MAPS_BIN_NAME,
                ctx=[
                    cdt_ctx.cdt_ctx_all_children(),
                    cdt_ctx.cdt_ctx_all_children_with_filter(expression=self.FILTER_EXPR)
                ],
                flags=aerospike.CDT_SELECT_VALUES
            )
        ]
        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(self.key, ops)

            assert bins[self.MAP_OF_NESTED_MAPS_BIN_NAME] == [
                self.BINS_FOR_CDT_SELECT_TEST[self.MAP_OF_NESTED_MAPS_BIN_NAME]["Day2"]["food"]
            ]

    @pytest.mark.parametrize("flags", [
        aerospike.CDT_MODIFY_NO_FAIL,
        aerospike.CDT_MODIFY_DEFAULT,
    ])
    def test_cdt_modify(self, flags):
        mod_expr = Sub(LoopVarFloat(aerospike.EXP_LOOPVAR_VALUE), 5.0).compile()
        ops = [
            operations.modify_by_path(
                name=self.MAP_OF_NESTED_MAPS_BIN_NAME,
                ctx=[
                    cdt_ctx.cdt_ctx_all_children(),
                    cdt_ctx.cdt_ctx_all_children()
                ],
                expr=mod_expr,
                flags=flags
            ),
            operations.select_by_path(
                name=self.MAP_OF_NESTED_MAPS_BIN_NAME,
                ctx=[
                    cdt_ctx.cdt_ctx_all_children(),
                    cdt_ctx.cdt_ctx_all_children()
                ],
                flags=aerospike.CDT_SELECT_VALUES
            ),
        ]
        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(self.key, ops)

            expected_results = [x - 5.0 for x in [14.990000, 5.0000, 34.000000, 12.990000, 19.990000, 2.000000]]
            assert bins[self.MAP_OF_NESTED_MAPS_BIN_NAME] == expected_results


    # Test cdt select flags

    def test_cdt_select_flag_matching_tree(self):
        ops = [
            operations.select_by_path(
                name=self.MAP_OF_NESTED_MAPS_BIN_NAME,
                ctx=[
                    cdt_ctx.cdt_ctx_all_children(),
                    cdt_ctx.cdt_ctx_all_children_with_filter(expression=self.FILTER_EXPR)
                ],
                flags=aerospike.CDT_SELECT_MATCHING_TREE
            )
        ]

        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(self.key, ops)

            expected_bin_value = copy.deepcopy(self.BINS_FOR_CDT_SELECT_TEST[self.MAP_OF_NESTED_MAPS_BIN_NAME])

            # Remove all nodes that are filtered out (dict key-value pairs)
            expected_bin_value["Day1"].clear()
            del expected_bin_value["Day2"]["game"]
            expected_bin_value["Day3"].clear()

            assert bins == {self.MAP_OF_NESTED_MAPS_BIN_NAME: expected_bin_value}

    def test_cdt_select_flag_map_keys(self):
        ops = [
            operations.select_by_path(
                name=self.MAP_OF_NESTED_MAPS_BIN_NAME,
                ctx=[
                    cdt_ctx.cdt_ctx_all_children(),
                    cdt_ctx.cdt_ctx_all_children()
                ],
                flags=aerospike.CDT_SELECT_MAP_KEYS
            )
        ]

        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(self.key, ops)
            assert bins == {self.MAP_OF_NESTED_MAPS_BIN_NAME: ["book", "ferry", "food", "game", "plants", "stickers"]}

    # TODO: set default for BUILTIN

    @pytest.mark.parametrize(
        "op, context",
        [
            pytest.param(
                operations.select_by_path(
                    name=MAP_BIN_NAME,
                    ctx=[],
                    flags=aerospike.CDT_SELECT_VALUES
                ),
                # TODO: vague
                pytest.raises(e.AerospikeError),
                id="empty_ctx"
            ),
            pytest.param(
                operations.select_by_path(
                    name=MAP_BIN_NAME,
                    ctx=[
                        cdt_ctx.cdt_ctx_all_children(),
                        cdt_ctx.cdt_ctx_all_children_with_filter(expression=EXPR_ON_DIFFERENT_ITERATED_TYPE)
                    ],
                    flags=aerospike.CDT_SELECT_VALUES
                ),
                pytest.raises(e.AerospikeError),
                id="iterate_on_unexpected_type"
            )
        ]
    )
    def test_cdt_select_negative_cases(self, op, context):
        ops = [
            op
        ]
        with context:
            self.as_connection.operate(self.key, ops)
