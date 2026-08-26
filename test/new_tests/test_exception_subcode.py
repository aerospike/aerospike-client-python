import pytest
from .conftest import TEST_NS, TEST_SET, BIN_NAME
import aerospike
from aerospike import exception as e
from aerospike_helpers.operations import list_operations as list_ops
from aerospike_helpers import expressions as expr
from aerospike_helpers.batch.records import BatchRecords, Write
from .test_base_class import TestBaseClass
from . import as_errors


KEY = (TEST_NS, TEST_SET, 1)
KEY2 = (TEST_NS, TEST_SET, 2)
OPS = [
    list_ops.list_get_by_index(BIN_NAME, index=99, return_type=aerospike.LIST_RETURN_VALUE)
]
ERROR_DETAIL_VERBOSITY_SETTING = "error_detail_verbosity"


class TestExceptionSubcode:
    @pytest.mark.parametrize(
        "constant",
        [
            aerospike.SUB_PARAM_TTL_INVALID,
            aerospike.SUB_PARAM_BITS_OFFSET_OUT_OF_RANGE,
            aerospike.SUB_PARAM_BITS_SIZE_OUT_OF_RANGE,
            aerospike.SUB_PARAM_BITS_RESIZE_EXCEEDED,
            aerospike.SUB_PARAM_BIN_COUNT_TOO_LARGE,
            aerospike.SUB_UNAVAIL_INITIAL_BALANCE_UNRESOLVED,
            aerospike.SUB_UNAVAIL_REPLICA_UNAVAILABLE,
            aerospike.SUB_UNSUPP_FEAT_MRT_REQUIRES_STRONG_CONSISTENCY,
            aerospike.SUB_UNSUPP_FEAT_GENERIC,
            aerospike.SUB_BIN_NOT_FOUND_HLL_CANNOT_CREATE_WITH_OP,
            aerospike.SUB_BIN_NAME_COUNT_TOO_LARGE,
            aerospike.SUB_FORBID_XDR_FILTER_BLOCKED,
            aerospike.SUB_FORBID_SET_COUNT_STOP_WRITES,
            aerospike.SUB_FORBID_SET_SIZE_STOP_WRITES,
            aerospike.SUB_FORBID_CLOCK_SKEW_STOP_WRITES,
            aerospike.SUB_FORBID_REPLACE_CONFLICT_RESOLVING,
            aerospike.SUB_FORBID_TRUNCATED,
            aerospike.SUB_FORBID_MASKING_POLICY_BLOCKED,
            aerospike.SUB_FORBID_DURABILITY_VIOLATION,
            aerospike.SUB_FORBID_MASKING_ROLE_VIOLATION,
            aerospike.SUB_OPNOT_CDT_INDEX_OUT_OF_BOUNDS,
            aerospike.SUB_OPNOT_CDT_RANK_OUT_OF_BOUNDS,
            aerospike.SUB_OPNOT_CDT_BOUNDED_LIST_OVERFLOW,
            aerospike.SUB_OPNOT_HLL_INDEX_BITS_UNSET,
            aerospike.SUB_OPNOT_HLL_CANNOT_REDUCE_INDEX_BITS,
            aerospike.SUB_OPNOT_HLL_CANNOT_REDUCE_MINHASH_BITS,
            aerospike.SUB_OPNOT_HLL_CANNOT_FOLD_MINHASH,
            aerospike.SUB_OPNOT_HLL_FOLD_INDEX_BITS_TOO_LARGE,
            aerospike.SUB_OPNOT_HLL_INTERSECT_MINHASH_MISMATCH,
            aerospike.SUB_OPNOT_STRING_CONVERSION_FAILED,
            aerospike.SUB_OPNOT_STRING_UTF8_INVALID,
            aerospike.SUB_OPNOT_STRING_B64_INVALID
        ]
    )
    def test_subcode_constants(self, constant):
        assert type(constant) == int

    # TODO: need to reuse fixture in conftest.py using indirect params to set num of records
    @pytest.fixture()
    def setup(self, as_connection):
        self.as_connection.put(KEY, bins={BIN_NAME: []})
        self.as_connection.put(KEY2, bins={BIN_NAME: []})
        yield
        self.as_connection.batch_remove(keys=[KEY, KEY2])

    @pytest.mark.parametrize(
        "policy_w_verbosity_setting",
        [
            {},
            {ERROR_DETAIL_VERBOSITY_SETTING: aerospike.ERROR_DETAIL_NONE},
            {ERROR_DETAIL_VERBOSITY_SETTING: aerospike.ERROR_DETAIL_SUBCODE},
            {ERROR_DETAIL_VERBOSITY_SETTING: aerospike.ERROR_DETAIL_MESSAGE},
        ]
    )
    @pytest.mark.parametrize(
        "set_in_client_config",
        [False, True]
    )
    @pytest.mark.usefixtures("setup")
    def test_error_verbosity_levels(self, policy_w_verbosity_setting: dict, set_in_client_config: bool):
        if set_in_client_config:
            config = {
                "policies": {
                    "operate": policy_w_verbosity_setting
                }
            }
            self.as_connection = TestBaseClass.get_new_connection(config)

        with pytest.raises(e.OpNotApplicable) as excinfo:
            cmd_policy = {}
            if not set_in_client_config:
                cmd_policy |= policy_w_verbosity_setting

            self.as_connection.operate(KEY, OPS, policy=cmd_policy)

        # Make sure there's no regression with the parent error code
        assert excinfo.value.code == as_errors.AEROSPIKE_ERR_OP_NOT_APPLICABLE

        subcode_should_be_zero = (
            ERROR_DETAIL_VERBOSITY_SETTING not in policy_w_verbosity_setting
            or
            policy_w_verbosity_setting[ERROR_DETAIL_VERBOSITY_SETTING] == aerospike.ERROR_DETAIL_NONE
            or
            # If running against a unsupported version, we expect subcode to always return 0
            # (and no undefined behavior)
            (TestBaseClass.major_ver, TestBaseClass.minor_ver, TestBaseClass.patch_ver) < (8, 1, 3)
        )
        if subcode_should_be_zero:
            assert excinfo.value.subcode == 0
        else:
            assert excinfo.value.subcode > 0

    @pytest.mark.usefixtures("setup")
    @pytest.mark.parametrize(
        "brs",
        [
            [
                Write(KEY, ops=OPS),
            ],
            [
                Write(KEY, ops=OPS),
                Write(KEY2, ops=OPS),
            ]
        ]
    )
    def test_batch_write_return_error_details(self, brs):
        brs = BatchRecords(
            brs
        )
        self.as_connection.batch_write(brs, policy_batch={ERROR_DETAIL_VERBOSITY_SETTING: aerospike.ERROR_DETAIL_MESSAGE})
        for br in brs.batch_records:
            assert isinstance(br.message, str)
            if (TestBaseClass.major_ver, TestBaseClass.minor_ver, TestBaseClass.patch_ver) < (8, 1, 3):
                assert br.subcode == 0
            else:
                assert br.subcode > 0

    @pytest.mark.usefixtures("setup")
    @pytest.mark.parametrize(
        "keys",
        [
            [KEY],
            [KEY, KEY2]
        ]
    )
    def test_batch_operate_return_error_details(self, keys):
        brs = self.as_connection.batch_operate(
            keys, OPS, policy_batch={ERROR_DETAIL_VERBOSITY_SETTING: aerospike.ERROR_DETAIL_MESSAGE})

        for br in brs.batch_records:
            assert isinstance(br.message, str)
            if (TestBaseClass.major_ver, TestBaseClass.minor_ver, TestBaseClass.patch_ver) < (8, 1, 3):
                assert br.subcode == 0
            else:
                assert br.subcode > 0

    @pytest.mark.usefixtures("setup")
    def test_error_detail_exp_trace(self):
        if (TestBaseClass.major_ver, TestBaseClass.minor_ver, TestBaseClass.patch_ver) < (8, 1, 3):
            pytest.skip("Expression tracing only supported in server 8.1.3 or higher")

        policy = {
            ERROR_DETAIL_VERBOSITY_SETTING: aerospike.ERROR_DETAIL_EXP_TRACE,
            "expressions": expr.GE(expr.Abs(expr.Val("a")), 1).compile()
        }
        with pytest.raises(e.InvalidRequest) as excinfo:
            self.as_connection.get(KEY, policy=policy)

        assert "; exp_trace={" in excinfo.value.msg
        print(excinfo.value.msg)

    def test_invalid_verbosity(self):
        policy = {
            ERROR_DETAIL_VERBOSITY_SETTING: 4
        }
        with pytest.raises(e.ServerError):
            self.as_connection.operate(KEY, OPS, policy=policy)
