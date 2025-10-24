import pytest

import aerospike
from aerospike_helpers.operations import operations
from aerospike_helpers.expressions.resources import ResultType
from aerospike_helpers.expressions.base import GE, Eq, LoopVarStr, LoopVarFloat, LoopVarInt, LoopVarMap, LoopVarList, ModifyByPath, SelectByPath, MapBin
from aerospike_helpers.expressions.map import MapGetByKey
from aerospike_helpers.expressions.list import ListSize
from aerospike_helpers.expressions.arithmetic import Sub
from aerospike_helpers.operations import expression_operations as expr_ops
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
    NESTED_LIST_BIN_NAME = "list_of_lists"

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
        NESTED_LIST_BIN_NAME: [
            [1, 2, 3],
            [4, 5],
            [6]
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
        "op,expected_bins",
        [
            pytest.param(
                operations.select_by_path(
                    bin_name=LIST_BIN_NAME,
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
                    bin_name=MAP_BIN_NAME,
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
                    bin_name=LIST_BIN_NAME,
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
                    bin_name=MAP_BIN_NAME,
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
                bin_name=self.MAP_OF_NESTED_MAPS_BIN_NAME,
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

    @pytest.mark.parametrize(
        "filter_expr, expected_bin_value",
        [
            pytest.param(
                GE(LoopVarInt(aerospike.EXP_LOOPVAR_VALUE), 2),
                # Should filter out 1
                [2]
            ),
            # At the first level below root, filter out all maps where "bb" does not have
            # an int value greater than 10
            pytest.param(
                GE(
                    expr0=MapGetByKey(
                        ctx=None,
                        return_type=aerospike.MAP_RETURN_VALUE,
                        value_type=ResultType.INTEGER,
                        key="bb",
                        bin=LoopVarMap(aerospike.EXP_LOOPVAR_VALUE)
                    ),
                    expr1=10
                ),
                [BINS_FOR_CDT_SELECT_TEST[MAP_BIN_NAME]["ab"]]
            )
        ]
    )
    def test_exp_loopvar_int_and_map(self, filter_expr, expected_bin_value):
        ops = [
            operations.select_by_path(
                bin_name=self.MAP_BIN_NAME,
                ctx=[
                    cdt_ctx.cdt_ctx_all_children_with_filter(expression=filter_expr.compile())
                ],
                flags=aerospike.CDT_SELECT_VALUES | aerospike.CDT_SELECT_NO_FAIL
            )
        ]
        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(self.key, ops)
            assert bins[self.MAP_BIN_NAME] == expected_bin_value

    LIST_SIZE_GE_TWO_EXPR = GE(ListSize(ctx=None, bin=LoopVarList(aerospike.CDT_SELECT_VALUES)), 2)

    def test_exp_loopvar_list(self):
        ops = [
            operations.select_by_path(
                bin_name=self.NESTED_LIST_BIN_NAME,
                ctx=[
                    cdt_ctx.cdt_ctx_all_children_with_filter(expression=self.LIST_SIZE_GE_TWO_EXPR.compile())
                ],
                flags=aerospike.CDT_SELECT_VALUES
            )
        ]
        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(self.key, ops)
            assert bins[self.NESTED_LIST_BIN_NAME] == [
                [1, 2, 3],
                [4, 5]
            ]


    MOD_EXPR = Sub(LoopVarFloat(aerospike.EXP_LOOPVAR_VALUE), 5.0).compile()
    # Expected results
    SECOND_LEVEL_INTEGERS_MINUS_FIVE = [x - 5.0 for x in [14.990000, 5.0000, 34.000000, 12.990000, 19.990000, 2.000000]]

    @pytest.mark.parametrize("flags", [
        aerospike.CDT_MODIFY_NO_FAIL,
        aerospike.CDT_MODIFY_DEFAULT,
    ])
    def test_cdt_modify(self, flags):
        ops = [
            operations.modify_by_path(
                bin_name=self.MAP_OF_NESTED_MAPS_BIN_NAME,
                ctx=[
                    cdt_ctx.cdt_ctx_all_children(),
                    cdt_ctx.cdt_ctx_all_children()
                ],
                expr=self.MOD_EXPR,
                flags=flags
            ),
            operations.select_by_path(
                bin_name=self.MAP_OF_NESTED_MAPS_BIN_NAME,
                ctx=[
                    cdt_ctx.cdt_ctx_all_children(),
                    cdt_ctx.cdt_ctx_all_children()
                ],
                flags=aerospike.CDT_SELECT_VALUES
            ),
        ]
        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(self.key, ops)

            assert bins[self.MAP_OF_NESTED_MAPS_BIN_NAME] == self.SECOND_LEVEL_INTEGERS_MINUS_FIVE


    # Test cdt select flags

    def test_cdt_select_flag_matching_tree(self):
        ops = [
            operations.select_by_path(
                bin_name=self.MAP_OF_NESTED_MAPS_BIN_NAME,
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
                bin_name=self.MAP_OF_NESTED_MAPS_BIN_NAME,
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

    def test_neg_iterate_on_unexpected_type(self):
        op = operations.select_by_path(
            bin_name=self.MAP_BIN_NAME,
            ctx=[
                cdt_ctx.cdt_ctx_all_children(),
                cdt_ctx.cdt_ctx_all_children_with_filter(expression=self.EXPR_ON_DIFFERENT_ITERATED_TYPE)
            ],
            flags=aerospike.CDT_SELECT_VALUES
        )
        ops = [
            op
        ]
        with pytest.raises(e.AerospikeError):
            self.as_connection.operate(self.key, ops)

    @pytest.mark.parametrize("ctx_list, expected_context", [
        (None, pytest.raises(e.ParamError)),
        ([], pytest.raises(e.InvalidRequest))
    ])
    @pytest.mark.parametrize(
        "op_method, op_kwargs", [
            pytest.param(
                operations.select_by_path,
                {
                    "name": MAP_BIN_NAME,
                    "flags": aerospike.CDT_SELECT_VALUES
                }
            ),
            pytest.param(
                operations.modify_by_path,
                {
                    "name": MAP_BIN_NAME,
                    "expr": MOD_EXPR,
                    "flags": aerospike.CDT_MODIFY_DEFAULT
                }
            ),
        ]
    )
    def test_neg_invalid_ctx(self, ctx_list, expected_context, op_method, op_kwargs):
        ops = [
            op_method(ctx=ctx_list, **op_kwargs)
        ]
        with expected_context:
            self.as_connection.operate(self.key, ops)

    def test_exp_select_by_path(self):
        ctx=[
            cdt_ctx.cdt_ctx_all_children(),
            cdt_ctx.cdt_ctx_all_children()
        ]

        bin_expr=MapBin(bin=self.MAP_OF_NESTED_MAPS_BIN_NAME)
        modify_expr = ModifyByPath(ctx=ctx, return_type=ResultType.MAP, mod_exp=self.MOD_EXPR, flags=aerospike.CDT_MODIFY_DEFAULT, bin=bin_expr).compile()
        select_expr = SelectByPath(ctx=ctx, return_type=ResultType.LIST, flags=aerospike.EXP_LOOPVAR_VALUE, bin=bin_expr).compile()
        ops = [
            expr_ops.expression_write(bin_name=self.MAP_OF_NESTED_MAPS_BIN_NAME, expression=modify_expr),
            expr_ops.expression_read(bin_name=self.MAP_OF_NESTED_MAPS_BIN_NAME, expression=select_expr)

        ]
        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(self.key, ops)
            assert bins[self.MAP_OF_NESTED_MAPS_BIN_NAME] == self.SECOND_LEVEL_INTEGERS_MINUS_FIVE

    MAP_KEY_FILTER_EXPR = Eq(LoopVarStr(aerospike.EXP_LOOPVAR_KEY), "book").compile()

    def test_loopvar_id_map_key(self):
        ops = [
            operations.select_by_path(
                bin_name=self.MAP_OF_NESTED_MAPS_BIN_NAME,
                ctx=[
                    cdt_ctx.cdt_ctx_all_children(),
                    cdt_ctx.cdt_ctx_all_children_with_filter(expression=self.MAP_KEY_FILTER_EXPR)
                ],
                flags=aerospike.CDT_SELECT_MATCHING_TREE
            )
        ]

        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(self.key, ops)

            expected_bin_value = copy.deepcopy(self.BINS_FOR_CDT_SELECT_TEST[self.MAP_OF_NESTED_MAPS_BIN_NAME])

            # Remove all nodes that are filtered out by dict key
            del expected_bin_value["Day1"]["ferry"]
            expected_bin_value["Day2"].clear()
            expected_bin_value["Day3"].clear()

            assert bins == {self.MAP_OF_NESTED_MAPS_BIN_NAME: expected_bin_value}

    LIST_INDEX_FILTER_EXPR = Eq(LoopVarInt(aerospike.EXP_LOOPVAR_INDEX), 0).compile()

    def test_loopvar_id_list_index(self):
        ops = [
            operations.select_by_path(
                bin_name=self.LIST_BIN_NAME,
                ctx=[
                    cdt_ctx.cdt_ctx_all_children_with_filter(expression=self.LIST_INDEX_FILTER_EXPR)
                ],
                flags=aerospike.CDT_SELECT_MATCHING_TREE
            )
        ]

        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(self.key, ops)
            # Return the same list, but with all list elements except at index 0 removed
            assert bins == {self.LIST_BIN_NAME: [self.BINS_FOR_CDT_SELECT_TEST[self.LIST_BIN_NAME][0]]}
