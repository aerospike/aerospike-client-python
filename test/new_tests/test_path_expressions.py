import pytest

import aerospike
from aerospike_helpers.operations import operations
from aerospike_helpers.operations import hll_operations as hll_ops
from aerospike_helpers.expressions.resources import ResultType
from aerospike_helpers.expressions.base import GE, Eq, LoopVarStr, LoopVarFloat, LoopVarInt, LoopVarMap, LoopVarList, ModifyByPath, SelectByPath, MapBin, LoopVarBool, LoopVarBlob, ResultRemove, LoopVarGeoJson, LoopVarNil, CmpGeo, LoopVarHLL, LE, Val
from aerospike_helpers.expressions.map import MapGetByKey, MapGetKeys, MapGetValues
from aerospike_helpers.expressions.list import ListSize, InList
from aerospike_helpers.expressions.arithmetic import Sub
from aerospike_helpers.expressions import hll
from aerospike_helpers.operations import expression_operations as expr_ops
from aerospike_helpers import cdt_ctx
from aerospike import exception as e
from .test_base_class import TestBaseClass
import copy



class TestPathExprOperations:
    expect_server_version_earlier_than_8_1_1_to_fail = pytest.mark.parametrize(
        "expect_earlier_than_server_version_to_fail",
        [
            (8, 1, 1)
        ],
        indirect=True
    )

    expect_server_version_earlier_than_8_1_2_to_fail = pytest.mark.parametrize(
        "expect_earlier_than_server_version_to_fail",
        [
            (8, 1, 2)
        ],
        indirect=True
    )

    MAP_BIN_NAME = "map_bin"
    LIST_BIN_NAME = "list_bin"
    MAP_OF_NESTED_MAPS_BIN_NAME = "map_of_maps_bin"
    NESTED_LIST_BIN_NAME = "list_of_lists"
    MAP_WITH_GEOJSON_BIN_NAME = "map_w_geo_bin"

    # For testing InList
    LIST_OF_INTS_BIN_NAME = "list_of_ints"
    SECOND_LIST_OF_INTS_BIN_NAME = "list_of_ints2"

    GEOJSON_VALUE = aerospike.geojson('{"type": "Point", "coordinates": [-80.604333, 28.608389]}')
    # TODO: moving this and other test data to conftest may be helpful
    RECORD_BINS = {
        LIST_OF_INTS_BIN_NAME: [1, 2, 3],
        SECOND_LIST_OF_INTS_BIN_NAME: [3],
        MAP_BIN_NAME: {
            "a": 1,
            "ab": {
            "bb": 12
            },
            "b": 2,
            "c": True,
            "d": b'123',
            "e": None,
            "f": 3,
            "g": 4,
            1: 5
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
    def insert_record(self, expect_earlier_than_server_version_to_fail):
        self.key = ("test", "demo", 1)
        self.as_connection.put(self.key, bins=self.RECORD_BINS)
        yield
        self.as_connection.remove(self.key)

    EXPR_ON_DIFFERENT_ITERATED_TYPE = Eq(LoopVarStr(aerospike.EXP_LOOPVAR_VALUE), "a").compile()

    @pytest.mark.parametrize(
        "bin_name, op, expected_bin_value",
        [
            pytest.param(
                LIST_BIN_NAME,
                operations.select_by_path(
                    bin_name=LIST_BIN_NAME,
                    ctx=[
                        cdt_ctx.cdt_ctx_all_children(),
                    ],
                    flags=aerospike.EXP_PATH_SELECT_VALUE
                ),
                RECORD_BINS[LIST_BIN_NAME],
                id="select_all_children_once_in_list"
            ),
            pytest.param(
                LIST_BIN_NAME,
                operations.select_by_path(
                    bin_name=LIST_BIN_NAME,
                    ctx=[
                        cdt_ctx.cdt_ctx_all_children(),
                        cdt_ctx.cdt_ctx_all_children()
                    ],
                    flags=aerospike.EXP_PATH_SELECT_VALUE
                ),
                [
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
                ],
                id="select_all_children_twice_in_list"
            )
        ]
    )
    @expect_server_version_earlier_than_8_1_1_to_fail
    def test_select_by_path_operation_returning_list_values(self, bin_name, op, expected_bin_value: list):
        ops = [
            op
        ]
        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(self.key, ops)
            assert bins[bin_name] == expected_bin_value

    def convert_dict_to_hashable_in_list(self, list):
        for i, list_elem in enumerate(list):
            if isinstance(list_elem, dict):
                list[i] = frozenset(list_elem.items())

    @pytest.mark.parametrize(
        "op, expected_bin_value",
        [
            pytest.param(
                operations.select_by_path(
                    bin_name=MAP_BIN_NAME,
                    ctx=[
                        cdt_ctx.cdt_ctx_all_children(),
                    ],
                    flags=aerospike.EXP_PATH_SELECT_VALUE
                ),
                list(RECORD_BINS[MAP_BIN_NAME].values()),
                id="select_all_children_once_in_map"
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
                [],
                id="exp_path_no_fail"
            )
        ]
    )
    @expect_server_version_earlier_than_8_1_1_to_fail
    def test_select_by_path_operation_returning_map_values(self, op, expected_bin_value: list):
        ops = [
            op
        ]
        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(self.key, ops)

            # Order of selected map entries doesn't matter.
            # But sorting a list / creating a set containing a non-hashable type (i.e dict) will fail.
            # One of the map values is a dictionary which is not hashable,
            # so we have to convert it to a frozenset to represent itself
            self.convert_dict_to_hashable_in_list(bins[self.MAP_BIN_NAME])
            self.convert_dict_to_hashable_in_list(expected_bin_value)
            assert set(bins[self.MAP_BIN_NAME]) == set(expected_bin_value)

    FILTER_EXPR = GE(
        LoopVarFloat(aerospike.EXP_LOOPVAR_VALUE),
        20.0
    ).compile()

    @expect_server_version_earlier_than_8_1_1_to_fail
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
                [value for value in RECORD_BINS[MAP_BIN_NAME].values() if type(value) == int and value >= 2],
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
    @expect_server_version_earlier_than_8_1_1_to_fail
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
            assert sorted(bins[self.MAP_BIN_NAME]) == sorted(expected_bin_value)

    @expect_server_version_earlier_than_8_1_1_to_fail
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

    @expect_server_version_earlier_than_8_1_1_to_fail
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

    @expect_server_version_earlier_than_8_1_1_to_fail
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
    @expect_server_version_earlier_than_8_1_1_to_fail
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

    @expect_server_version_earlier_than_8_1_1_to_fail
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
                # TODO: this test shouldn't rely on the insertion order of the map entries
                # in the test setup. But don't have time to fix this
                ["book", "ferry", "food", "game", "plants", "stickers"]
            ),
            pytest.param(
                # TODO: see TODO for above test case.
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
    @expect_server_version_earlier_than_8_1_1_to_fail
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

    @expect_server_version_earlier_than_8_1_1_to_fail
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
            assert sorted(bins[self.MAP_OF_NESTED_MAPS_BIN_NAME]) == sorted(self.SECOND_LEVEL_INTEGERS_MINUS_FIVE)

    MAP_KEY_FILTER_EXPR = Eq(LoopVarStr(aerospike.EXP_LOOPVAR_KEY), "book").compile()

    @expect_server_version_earlier_than_8_1_1_to_fail
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

    @expect_server_version_earlier_than_8_1_1_to_fail
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

    @expect_server_version_earlier_than_8_1_1_to_fail
    def test_expr_result_remove(self):
        with pytest.warns(DeprecationWarning):
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

    @pytest.mark.parametrize(
        "map_keys",
        [
            # Base case: return nothing
            [],
            # One key
            ["a"],
            # Multiple keys
            ["a", "b"],
            # Keys with different types
            ["a", 1]
        ]
    )
    @expect_server_version_earlier_than_8_1_2_to_fail
    def test_cdt_ctx_map_get_matching_keys(self, map_keys):
        ops = [
            operations.select_by_path(
                bin_name=self.MAP_BIN_NAME,
                ctx=[
                    cdt_ctx.cdt_ctx_map_keys_in(map_keys)
                ],
                flags=aerospike.EXP_PATH_SELECT_MAP_VALUE
            )
        ]
        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(self.key, ops)
            # Assuming that order of map entries returned doesn't matter
            assert sorted(bins[self.MAP_BIN_NAME]) == sorted([self.RECORD_BINS[self.MAP_BIN_NAME][key] for key in map_keys])

    @expect_server_version_earlier_than_8_1_2_to_fail
    def test_cdt_ctx_map_get_matching_and_nonmatching_keys(self):
        ops = [
            operations.select_by_path(
                bin_name=self.MAP_BIN_NAME,
                ctx=[
                    cdt_ctx.cdt_ctx_map_keys_in(["a", "z"])
                ],
                flags=aerospike.EXP_PATH_SELECT_MAP_VALUE
            )
        ]
        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(self.key, ops)
            # Map key z should be ignored
            assert bins[self.MAP_BIN_NAME] == [self.RECORD_BINS[self.MAP_BIN_NAME]["a"]]

    @pytest.mark.parametrize(
        "map_keys",
        [
            # One key
            ["z"],
            # Multiple keys
            ["z", "zz"],
        ]
    )
    @expect_server_version_earlier_than_8_1_2_to_fail
    def test_cdt_ctx_map_get_only_nonmatching_keys(self, map_keys):
        ops = [
            operations.select_by_path(
                bin_name=self.MAP_BIN_NAME,
                ctx=[
                    cdt_ctx.cdt_ctx_map_keys_in(map_keys)
                ],
                flags=aerospike.EXP_PATH_SELECT_MAP_VALUE
            )
        ]
        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(self.key, ops)
            assert bins[self.MAP_BIN_NAME] == []

    @expect_server_version_earlier_than_8_1_2_to_fail
    def test_cdt_ctx_map_get_keys_in_and_filter(self):
        filter_expr = GE(LoopVarInt(aerospike.EXP_LOOPVAR_VALUE), 2).compile()
        ops = [
            operations.select_by_path(
                bin_name=self.MAP_BIN_NAME,
                ctx=[
                    cdt_ctx.cdt_ctx_map_keys_in(["a", "b"]),
                    cdt_ctx.cdt_ctx_and_filter(filter_expr)
                ],
                flags=aerospike.EXP_PATH_SELECT_MAP_VALUE
            )
        ]
        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(self.key, ops)
            # Map key "a" should be filtered out
            assert bins[self.MAP_BIN_NAME] == [self.RECORD_BINS[self.MAP_BIN_NAME]["b"]]

    def test_cdt_ctx_map_get_keys_in_with_chained_and_filters(self):
        GE_expr = GE(LoopVarInt(aerospike.EXP_LOOPVAR_VALUE), 2).compile()
        LE_expr = LE(LoopVarInt(aerospike.EXP_LOOPVAR_VALUE), 3).compile()
        ops = [
            operations.select_by_path(
                bin_name=self.MAP_BIN_NAME,
                ctx=[
                    cdt_ctx.cdt_ctx_map_keys_in(["a", "b", "f", "g"]),
                    cdt_ctx.cdt_ctx_and_filter(GE_expr),
                    cdt_ctx.cdt_ctx_and_filter(LE_expr)
                ],
                flags=aerospike.EXP_PATH_SELECT_MAP_VALUE
            )
        ]
        with pytest.raises(e.InvalidRequest):
            self.as_connection.operate(self.key, ops)

    def test_cdt_ctx_all_children_with_filter_then_and_filter(self):
        if (TestBaseClass.major_ver, TestBaseClass.minor_ver, TestBaseClass.patch_ver) < (8, 1, 2):
            pytest.skip("Server versions < 8.1.2 will not return an invalid request error."
                        "We consider this undefined behavior")

        filter_expr = GE(LoopVarInt(aerospike.EXP_LOOPVAR_VALUE), 2).compile()
        ops = [
            operations.select_by_path(
                bin_name=self.MAP_BIN_NAME,
                ctx=[
                    cdt_ctx.cdt_ctx_all_children_with_filter(filter_expr),
                    cdt_ctx.cdt_ctx_and_filter(filter_expr)
                ],
                flags=aerospike.EXP_PATH_SELECT_MAP_VALUE | aerospike.EXP_PATH_SELECT_NO_FAIL
            )
        ]
        with pytest.raises(e.InvalidRequest):
            self.as_connection.operate(self.key, ops)

    def test_cdt_ctx_map_get_keys_in_nonlist(self):
        ops = [
            operations.select_by_path(
                bin_name=self.MAP_BIN_NAME,
                ctx=[
                    cdt_ctx.cdt_ctx_map_keys_in(4),
                ],
                flags=aerospike.EXP_PATH_SELECT_MAP_VALUE
            )
        ]
        with pytest.raises(e.ParamError):
            self.as_connection.operate(self.key, ops)

    # I believe this raises InvalidRequest for any server version
    def test_cdt_ctx_and_filter_taking_in_expr_evaluating_to_non_bool(self):
        non_bool_expr = MapBin(self.MAP_BIN_NAME).compile()
        ops = [
            operations.select_by_path(
                bin_name=self.MAP_BIN_NAME,
                ctx=[
                    cdt_ctx.cdt_ctx_map_keys_in(["a", "b"]),
                    cdt_ctx.cdt_ctx_and_filter(non_bool_expr)
                ],
                flags=aerospike.EXP_PATH_SELECT_MAP_VALUE
            )
        ]
        with pytest.raises(e.InvalidRequest):
            self.as_connection.operate(self.key, ops)

    @pytest.mark.parametrize(
        "filter_expr, expected_results",
        [
            pytest.param(
                InList(
                    LoopVarInt(aerospike.EXP_LOOPVAR_VALUE),
                    Val([1, 2, 3])
                ),
                # Only the third list element is in [3]
                [3]
            ),
            pytest.param(
                InList(
                    LoopVarInt(aerospike.EXP_LOOPVAR_VALUE),
                    SECOND_LIST_OF_INTS_BIN_NAME
                ),
                # Only the third list element is in SECOND_LIST_OF_INTS_BIN_NAME
                [3]
            ),
        ]
    )
    @expect_server_version_earlier_than_8_1_2_to_fail
    def test_expr_in_list(self, filter_expr, expected_results):
        filter_expr = filter_expr.compile()
        ctx = [
            cdt_ctx.cdt_ctx_all_children_with_filter(filter_expr)
        ]
        ops = [
            operations.select_by_path(self.LIST_OF_INTS_BIN_NAME, ctx, aerospike.EXP_PATH_SELECT_LIST_VALUE)
        ]
        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(self.key, ops)
            assert bins[self.LIST_OF_INTS_BIN_NAME] == expected_results

    def test_expr_in_map_instead_of_list(self):
        filter_expr = InList(LoopVarInt(aerospike.EXP_LOOPVAR_VALUE), self.MAP_BIN_NAME).compile()
        ctx = [
            cdt_ctx.cdt_ctx_all_children_with_filter(filter_expr)
        ]
        ops = [
            operations.select_by_path(self.LIST_OF_INTS_BIN_NAME, ctx, aerospike.EXP_PATH_SELECT_LIST_VALUE)
        ]
        with pytest.raises(e.InvalidRequest):
            self.as_connection.operate(self.key, ops)

    @expect_server_version_earlier_than_8_1_2_to_fail
    def test_expr_map_get_keys(self):
        expr = MapGetKeys(self.MAP_BIN_NAME).compile()
        ops = [
            expr_ops.expression_read(self.MAP_BIN_NAME, expr)
        ]
        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(self.key, ops)
            assert set(bins[self.MAP_BIN_NAME]) == set(self.RECORD_BINS[self.MAP_BIN_NAME].keys())

    @expect_server_version_earlier_than_8_1_2_to_fail
    def test_expr_map_get_values(self):
        expr = MapGetValues(self.MAP_BIN_NAME).compile()
        ops = [
            expr_ops.expression_read(self.MAP_BIN_NAME, expr)
        ]
        with self.expected_context_for_pos_tests:
            _, _, bins = self.as_connection.operate(self.key, ops)

            self.convert_dict_to_hashable_in_list(bins[self.MAP_BIN_NAME])

            expected_bin_value = list(self.RECORD_BINS[self.MAP_BIN_NAME].values())
            self.convert_dict_to_hashable_in_list(expected_bin_value)

            assert len(expected_bin_value) == len(set(expected_bin_value))
            assert set(bins[self.MAP_BIN_NAME]) == set(expected_bin_value)

    @pytest.mark.parametrize(
        "map_expr_api",
        [
            MapGetKeys,
            MapGetValues
        ]
    )
    def test_expr_map_get_keys_or_values_on_non_map(self, map_expr_api):
        expr = map_expr_api(self.LIST_BIN_NAME).compile()
        ops = [
            expr_ops.expression_read(self.MAP_BIN_NAME, expr)
        ]
        with pytest.raises(e.ServerError):
            self.as_connection.operate(self.key, ops)
