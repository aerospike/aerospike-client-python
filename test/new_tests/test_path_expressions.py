import pytest

import aerospike
from aerospike_helpers.operations import operations
from aerospike_helpers.operations import hll_operations as hll_ops
from aerospike_helpers.operations import map_operations
from aerospike_helpers.expressions.resources import ResultType
from aerospike_helpers.expressions.base import GE, Eq, LoopVarStr, LoopVarFloat, LoopVarInt, LoopVarMap, LoopVarList, ModifyByPath, SelectByPath, MapBin, LoopVarBool, LoopVarBlob, ResultRemove, LoopVarGeoJson, LoopVarNil, CmpGeo, LoopVarHLL, HLLBin
from aerospike_helpers.expressions.map import MapGetByKey, MapPut
from aerospike_helpers.expressions.list import ListSize
from aerospike_helpers.expressions.arithmetic import Sub
from aerospike_helpers.expressions import hll
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


class TestPathExprOperations:
    MAP_BIN_NAME = "map_bin"
    LIST_BIN_NAME = "list_bin"
    MAP_OF_NESTED_MAPS_BIN_NAME = "map_of_maps_bin"
    NESTED_LIST_BIN_NAME = "list_of_lists"
    MAP_WITH_GEOJSON_BIN_NAME = "map_w_geo_bin"

    GEOJSON_VALUE = aerospike.geojson('{"type": "Point", "coordinates": [-80.604333, 28.608389]}')
    RECORD_BINS = {
        MAP_BIN_NAME: {
            "a": 1,
            "ab": {
            "bb": 12
            },
            "b": 2,
            "c": True,
            "d": b'123',
            "e": None,
        },
        MAP_WITH_GEOJSON_BIN_NAME: {
            "f": GEOJSON_VALUE
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
        self.as_connection.put(self.key, bins=self.RECORD_BINS)
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
                    flags=aerospike.EXP_PATH_SELECT_VALUE
                ),
                {
                    LIST_BIN_NAME: RECORD_BINS[LIST_BIN_NAME]
                },
                id="select_all_children_once_in_list"
            ),
            pytest.param(
                operations.select_by_path(
                    bin_name=MAP_BIN_NAME,
                    ctx=[
                        cdt_ctx.cdt_ctx_all_children(),
                    ],
                    flags=aerospike.EXP_PATH_SELECT_VALUE
                ),
                {
                    MAP_BIN_NAME: list(RECORD_BINS[MAP_BIN_NAME].values())
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
                    flags=aerospike.EXP_PATH_SELECT_VALUE
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
                    flags=aerospike.EXP_PATH_SELECT_VALUE | aerospike.EXP_PATH_SELECT_NO_FAIL
                ),
                {
                    MAP_BIN_NAME: []
                },
                id="exp_path_no_fail"
            )
        ]
    )
    def test_select_by_path_operation(self, op, expected_bins):
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

    def test_select_by_path_operation_with_filter(self):
        ops = [
            operations.select_by_path(
                bin_name=self.MAP_OF_NESTED_MAPS_BIN_NAME,
                ctx=[
                    cdt_ctx.cdt_ctx_all_children(),
                    cdt_ctx.cdt_ctx_all_children_with_filter(expression=self.FILTER_EXPR)
                ],
                flags=aerospike.EXP_PATH_SELECT_VALUE
            )
        ]
        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(self.key, ops)

            assert bins[self.MAP_OF_NESTED_MAPS_BIN_NAME] == [
                self.RECORD_BINS[self.MAP_OF_NESTED_MAPS_BIN_NAME]["Day2"]["food"]
            ]

    @pytest.mark.parametrize(
        "filter_expr, expected_bin_value",
        [
            pytest.param(
                GE(LoopVarInt(aerospike.EXP_LOOPVAR_VALUE), 2),
                # Should filter out 1
                [2],
                # Without an id, it's harder to run this test case individually
                # LoopVarInt isn't printed to stdout
                id="LoopVarInt"
            ),
            # At the first level below root, only return maps that have a key "bb" with value >= 10
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
                [RECORD_BINS[MAP_BIN_NAME]["ab"]],
                id="LoopVarMap"
            ),
            pytest.param(
                Eq(LoopVarBool(aerospike.EXP_LOOPVAR_VALUE), True),
                [True],
                id="LoopVarBool"
            ),
            pytest.param(
                Eq(LoopVarBlob(aerospike.EXP_LOOPVAR_VALUE), b'123'),
                [bytearray(b'123')],
                id="LoopVarBlob"
            ),
            pytest.param(
                Eq(LoopVarNil(aerospike.EXP_LOOPVAR_VALUE), None),
                [None],
                id="LoopVarNil"
            )
        ]
    )
    def test_exp_loopvar_types(self, filter_expr, expected_bin_value):
        ops = [
            operations.select_by_path(
                bin_name=self.MAP_BIN_NAME,
                ctx=[
                    cdt_ctx.cdt_ctx_all_children_with_filter(expression=filter_expr.compile())
                ],
                flags=aerospike.EXP_PATH_SELECT_VALUE | aerospike.EXP_PATH_SELECT_NO_FAIL
            )
        ]
        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(self.key, ops)
            assert bins[self.MAP_BIN_NAME] == expected_bin_value

    def test_exp_loopvar_geojson(self):
        rectangle = aerospike.GeoJSON({'type': "Polygon",
                         'coordinates': [
                          [[-80.590000, 28.60000],
                           [-80.590000, 28.61800],
                           [-80.620000, 28.61800],
                           [-80.620000, 28.60000],
                           [-80.590000, 28.60000]]]})

        # Check if point is within rect region
        filter_expr = CmpGeo(LoopVarGeoJson(aerospike.EXP_LOOPVAR_VALUE), rectangle)
        ops = [
            operations.select_by_path(
                bin_name=self.MAP_WITH_GEOJSON_BIN_NAME,
                ctx=[
                    cdt_ctx.cdt_ctx_all_children_with_filter(expression=filter_expr.compile())
                ],
                flags=aerospike.EXP_PATH_SELECT_VALUE
            )
        ]
        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(self.key, ops)
            assert bins[self.MAP_WITH_GEOJSON_BIN_NAME][0].geo_data == self.GEOJSON_VALUE.geo_data

    LIST_SIZE_GE_TWO_EXPR = GE(ListSize(ctx=None, bin=LoopVarList(aerospike.EXP_PATH_SELECT_VALUE)), 2)

    def test_exp_loopvar_list(self):
        ops = [
            operations.select_by_path(
                bin_name=self.NESTED_LIST_BIN_NAME,
                ctx=[
                    cdt_ctx.cdt_ctx_all_children_with_filter(expression=self.LIST_SIZE_GE_TWO_EXPR.compile())
                ],
                flags=aerospike.EXP_PATH_SELECT_VALUE
            )
        ]
        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(self.key, ops)
            assert bins[self.NESTED_LIST_BIN_NAME] == [
                [1, 2, 3],
                [4, 5]
            ]

    MAP_WITH_HLL_BIN_NAME = "map_w_hll_bin"

    @pytest.fixture
    def setup_hll_bin(self):
        ops = [
            # Insert root level HLL bin
            # Using a second operation to move the hll value into a map doesn't work...
            hll_ops.hll_add(bin_name=self.MAP_WITH_HLL_BIN_NAME, values=[i for i in range(5000)], index_bit_count=4, mh_bit_count=4),
        ]
        self.as_connection.operate(self.key, ops)

        _, _, bins = self.as_connection.get(self.key)
        self.expected_hll_value = bins[self.MAP_WITH_HLL_BIN_NAME]

        map_with_hll_value = {
            "a": bins[self.MAP_WITH_HLL_BIN_NAME]
        }
        self.as_connection.put(self.key, bins={self.MAP_WITH_HLL_BIN_NAME: map_with_hll_value})

        yield

        self.as_connection.remove_bin(self.key, list=[self.MAP_WITH_HLL_BIN_NAME])

    def test_exp_loopvar_hll(self, setup_hll_bin):
        # HLL bin value should always be returned
        filter_expr = GE(hll.HLLGetCount(bin=LoopVarHLL(var_id=aerospike.EXP_LOOPVAR_VALUE)), 0).compile()
        ops = [
            operations.select_by_path(
                bin_name=self.MAP_WITH_HLL_BIN_NAME,
                ctx=[
                    cdt_ctx.cdt_ctx_all_children_with_filter(filter_expr)
                ],
                flags=aerospike.EXP_PATH_SELECT_VALUE
            )
        ]
        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(self.key, ops)
            assert bins[self.MAP_WITH_HLL_BIN_NAME] == [self.expected_hll_value]

    SUBTRACT_FIVE_FROM_ITERATED_FLOAT_EXPR = Sub(LoopVarFloat(aerospike.EXP_LOOPVAR_VALUE), 5.0).compile()
    # Expected results
    SECOND_LEVEL_INTEGERS_MINUS_FIVE = [x - 5.0 for x in [14.990000, 5.0000, 34.000000, 12.990000, 19.990000, 2.000000]]

    # This operate command will pass with either flag set, but we are just checking the API by using it
    @pytest.mark.parametrize("flags", [
        aerospike.EXP_PATH_MODIFY_NO_FAIL,
        aerospike.EXP_PATH_MODIFY_DEFAULT,
    ])
    def test_modify_by_path_operation(self, flags):
        ops = [
            operations.modify_by_path(
                bin_name=self.MAP_OF_NESTED_MAPS_BIN_NAME,
                ctx=[
                    cdt_ctx.cdt_ctx_all_children(),
                    cdt_ctx.cdt_ctx_all_children()
                ],
                expr=self.SUBTRACT_FIVE_FROM_ITERATED_FLOAT_EXPR,
                flags=flags
            ),
            operations.select_by_path(
                bin_name=self.MAP_OF_NESTED_MAPS_BIN_NAME,
                ctx=[
                    cdt_ctx.cdt_ctx_all_children(),
                    cdt_ctx.cdt_ctx_all_children()
                ],
                flags=aerospike.EXP_PATH_SELECT_VALUE
            ),
        ]
        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(self.key, ops)

            assert bins[self.MAP_OF_NESTED_MAPS_BIN_NAME] == self.SECOND_LEVEL_INTEGERS_MINUS_FIVE


    # Test path expression select flags

    def test_exp_path_flag_matching_tree(self):
        ops = [
            operations.select_by_path(
                bin_name=self.MAP_OF_NESTED_MAPS_BIN_NAME,
                ctx=[
                    cdt_ctx.cdt_ctx_all_children(),
                    cdt_ctx.cdt_ctx_all_children_with_filter(expression=self.FILTER_EXPR)
                ],
                flags=aerospike.EXP_PATH_SELECT_MATCHING_TREE
            )
        ]

        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(self.key, ops)

            expected_bin_value = copy.deepcopy(self.RECORD_BINS[self.MAP_OF_NESTED_MAPS_BIN_NAME])
            # Remove all nodes that are filtered out (dict key-value pairs)
            expected_bin_value["Day1"].clear()
            del expected_bin_value["Day2"]["game"]
            expected_bin_value["Day3"].clear()

            assert bins == {self.MAP_OF_NESTED_MAPS_BIN_NAME: expected_bin_value}

    @pytest.mark.parametrize(
        "flags, expected_bin_value", [
            pytest.param(
                aerospike.EXP_PATH_SELECT_MAP_KEY,
                ["book", "ferry", "food", "game", "plants", "stickers"]
            ),
            pytest.param(
                aerospike.EXP_PATH_SELECT_MAP_KEY_VALUE,
                [
                    "book",
                    14.990000,
                    "ferry",
                    5.000000,
                    "food",
                    34.000000,
                    "game",
                    12.990000,
                    "plants",
                    19.990000,
                    "stickers",
                    2.000000
                ]
            )
        ]
    )
    def test_exp_path_flag_map(self, flags, expected_bin_value):
        ops = [
            operations.select_by_path(
                bin_name=self.MAP_OF_NESTED_MAPS_BIN_NAME,
                ctx=[
                    cdt_ctx.cdt_ctx_all_children(),
                    cdt_ctx.cdt_ctx_all_children()
                ],
                flags=flags
            )
        ]

        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(self.key, ops)
            assert bins == {self.MAP_OF_NESTED_MAPS_BIN_NAME: expected_bin_value}

    def test_cdt_ctx_all_children_with_filter_with_invalid_expr(self):
        op = operations.select_by_path(
            bin_name=self.MAP_BIN_NAME,
            ctx=[
                cdt_ctx.cdt_ctx_all_children_with_filter(expression=1)
            ],
            flags=aerospike.EXP_PATH_SELECT_VALUE
        )
        ops = [
            op
        ]
        with pytest.raises(e.ParamError):
            self.as_connection.operate(self.key, ops)

    def test_neg_iterate_on_unexpected_type(self):
        op = operations.select_by_path(
            bin_name=self.MAP_BIN_NAME,
            ctx=[
                cdt_ctx.cdt_ctx_all_children(),
                cdt_ctx.cdt_ctx_all_children_with_filter(expression=self.EXPR_ON_DIFFERENT_ITERATED_TYPE)
            ],
            flags=aerospike.EXP_PATH_SELECT_VALUE
        )
        ops = [
            op
        ]
        with pytest.raises(e.AerospikeError):
            self.as_connection.operate(self.key, ops)

    @pytest.mark.parametrize("ctx_list, expected_context", [
        (None, pytest.raises(e.ParamError)),
        ([], pytest.raises(e.ParamError))
    ])
    @pytest.mark.parametrize(
        "op_method, op_kwargs", [
            pytest.param(
                operations.select_by_path,
                {
                    "bin_name": MAP_BIN_NAME,
                    "flags": aerospike.EXP_PATH_SELECT_VALUE
                }
            ),
            pytest.param(
                operations.modify_by_path,
                {
                    "bin_name": MAP_BIN_NAME,
                    "expr": SUBTRACT_FIVE_FROM_ITERATED_FLOAT_EXPR,
                    "flags": aerospike.EXP_PATH_MODIFY_DEFAULT
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

    def test_select_by_path_expression(self):
        ctx=[
            cdt_ctx.cdt_ctx_all_children(),
            cdt_ctx.cdt_ctx_all_children()
        ]

        bin_expr=MapBin(bin=self.MAP_OF_NESTED_MAPS_BIN_NAME)
        modify_expr = ModifyByPath(ctx=ctx, value_type=ResultType.MAP, mod_exp=self.SUBTRACT_FIVE_FROM_ITERATED_FLOAT_EXPR, flags=aerospike.EXP_PATH_MODIFY_DEFAULT, bin=bin_expr).compile()
        select_expr = SelectByPath(ctx=ctx, value_type=ResultType.LIST, flags=aerospike.EXP_PATH_SELECT_VALUE, bin=bin_expr).compile()
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
                flags=aerospike.EXP_PATH_SELECT_MATCHING_TREE
            )
        ]

        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(self.key, ops)

            expected_bin_value = copy.deepcopy(self.RECORD_BINS[self.MAP_OF_NESTED_MAPS_BIN_NAME])
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
                flags=aerospike.EXP_PATH_SELECT_MATCHING_TREE
            )
        ]

        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(self.key, ops)
            # Return the same list, but with all list elements except at index 0 removed
            assert bins == {self.LIST_BIN_NAME: [self.RECORD_BINS[self.LIST_BIN_NAME][0]]}

    def test_expr_result_remove(self):
        ops = [
            operations.modify_by_path(
                bin_name=self.MAP_OF_NESTED_MAPS_BIN_NAME,
                ctx=[
                    cdt_ctx.cdt_ctx_all_children(),
                    cdt_ctx.cdt_ctx_all_children()
                ],
                expr=ResultRemove().compile(),
                flags=aerospike.EXP_PATH_MODIFY_DEFAULT
            )
        ]

        with self.expected_context_for_pos_tests:
            self.as_connection.operate(self.key, ops)

            _, _, bins = self.as_connection.get(self.key)
            assert bins[self.MAP_OF_NESTED_MAPS_BIN_NAME] == {
                "Day1": {
                },
                "Day2": {
                },
                "Day3": {
                }
            }
