import pytest
from .conftest import TEST_NS, TEST_SET, BIN_NAME, DYN_CONFIG_PATH, KeysValue
import aerospike
from aerospike import exception as e
from aerospike_helpers.operations import list_operations as list_ops, operations
from aerospike_helpers import expressions as expr
from aerospike_helpers.batch.records import BatchRecords, Read
from .test_base_class import TestBaseClass
from . import as_errors


OPS = [
    list_ops.list_get_by_index(BIN_NAME, index=99, return_type=aerospike.LIST_RETURN_VALUE)
]
ERROR_DETAIL_VERBOSITY_SETTING = "error_detail_verbosity"


@pytest.mark.parametrize(
    "constant",
    [
        aerospike.SUB_PARAM_TTL_INVALID,
        aerospike.SUB_PARAM_BITS_OFFSET_OUT_OF_RANGE,
        aerospike.SUB_PARAM_BITS_SIZE_OUT_OF_RANGE,
        aerospike.SUB_PARAM_BITS_RESIZE_EXCEEDED,
        aerospike.SUB_PARAM_BIN_COUNT_TOO_LARGE,
        aerospike.SUB_PARAM_STRING_CTX_MALFORMED,
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
        aerospike.SUB_OPNOT_STRING_REGEX_LIMIT_EXCEEDED,
        aerospike.SUB_OPNOT_STRING_B64_INVALID
    ]
)
def test_subcode_constants(constant):
    assert type(constant) == int

def assert_subcode(subcode: int, verbosity_level: int):
    server_version = (TestBaseClass.major_ver, TestBaseClass.minor_ver, TestBaseClass.patch_ver)
    if server_version < (8, 1, 3) or verbosity_level == aerospike.ERROR_DETAIL_NONE:
        assert subcode == 0
    else:
        assert subcode > 0

@pytest.mark.parametrize(
    "insert_records",
    [{"record_count": 2, "make_set_unique": False, "bins": {BIN_NAME: []}}],
    indirect=True
)
@pytest.mark.usefixtures("insert_records")
class TestExceptionSubcode:
    verbosity_levels = [
        aerospike.ERROR_DETAIL_NONE,
        aerospike.ERROR_DETAIL_SUBCODE,
        aerospike.ERROR_DETAIL_MESSAGE,
        aerospike.ERROR_DETAIL_EXP_TRACE,
    ]

    @pytest.mark.parametrize(
        "as_connection",
        [
            KeysValue(["policies", "operate", ERROR_DETAIL_VERBOSITY_SETTING], verbosity_level)
            for verbosity_level in verbosity_levels
        ],
        indirect=True
    )
    def test_error_verbosity_levels_from_client_config(self):
        with pytest.raises(e.OpNotApplicable) as excinfo:
            self.as_connection.operate(self.keys[0], OPS)

        # Make sure there's no regression with the parent error code
        assert excinfo.value.code == as_errors.AEROSPIKE_ERR_OP_NOT_APPLICABLE

        policies = self.as_connection.get_policies()
        assert_subcode(excinfo.value.subcode, policies["operate"][ERROR_DETAIL_VERBOSITY_SETTING])

    @pytest.mark.parametrize(
        "as_connection",
        [
            KeysValue(["policies", "operate", ERROR_DETAIL_VERBOSITY_SETTING], aerospike.ERROR_DETAIL_NONE)
        ],
        indirect=True
    )
    @pytest.mark.parametrize(
        "verbosity_level",
        verbosity_levels
    )
    def test_error_verbosity_levels_from_command_policy(self, verbosity_level):
        with pytest.raises(e.OpNotApplicable) as excinfo:
            self.as_connection.operate(self.keys[0], OPS, policy={ERROR_DETAIL_VERBOSITY_SETTING: verbosity_level})

        # Make sure there's no regression with the parent error code
        assert excinfo.value.code == as_errors.AEROSPIKE_ERR_OP_NOT_APPLICABLE

        assert_subcode(excinfo.value.subcode, verbosity_level)

    def test_batch_record_message_field_is_none_when_batch_succeeds(self):
        brs = BatchRecords(
            [
                Read(self.keys[0], ops=[
                    operations.read(BIN_NAME)
                ])
            ]
        )
        self.as_connection.batch_write(brs, policy_batch={ERROR_DETAIL_VERBOSITY_SETTING: aerospike.ERROR_DETAIL_MESSAGE})
        for br in brs.batch_records:
            assert br.message is None
            assert br.subcode == 0

    @pytest.mark.parametrize("key_count", [1, 2])
    def test_batch_write_return_error_details(self, key_count):
        brs = BatchRecords(
            [Read(key, ops=OPS) for key in self.keys[:key_count]]
        )
        self.as_connection.batch_write(brs, policy_batch={ERROR_DETAIL_VERBOSITY_SETTING: aerospike.ERROR_DETAIL_MESSAGE})
        for br in brs.batch_records:
            assert isinstance(br.message, str)
            assert_subcode(br.subcode, aerospike.ERROR_DETAIL_MESSAGE)

    @pytest.mark.parametrize("key_count", [1, 2])
    def test_batch_operate_return_error_details(self, key_count):
        brs = self.as_connection.batch_operate(
            self.keys[:key_count], OPS, policy_batch={ERROR_DETAIL_VERBOSITY_SETTING: aerospike.ERROR_DETAIL_MESSAGE})

        for br in brs.batch_records:
            assert isinstance(br.message, str)
            assert_subcode(br.subcode, aerospike.ERROR_DETAIL_MESSAGE)

    @pytest.mark.parametrize(
        "verbosity_level",
        [
            aerospike.ERROR_DETAIL_EXP_TRACE,
            # Test that an invalid verbosity level gets clamped
            4
        ]
    )
    def test_error_detail_exp_trace(self, verbosity_level):
        if (TestBaseClass.major_ver, TestBaseClass.minor_ver, TestBaseClass.patch_ver) < (8, 1, 3):
            pytest.skip("Expression tracing only supported in server 8.1.3 or higher")

        policy = {
            ERROR_DETAIL_VERBOSITY_SETTING: verbosity_level,
            "expressions": expr.GE(expr.Abs(expr.Val("a")), 1).compile()
        }
        with pytest.raises(e.InvalidRequest) as excinfo:
            self.as_connection.get(self.keys[0], policy=policy)

        assert "; exp_trace={" in excinfo.value.msg
        print(excinfo.value.msg)

    @pytest.mark.parametrize(
        "api_method, kwargs",
        [
            (
                aerospike.Client.get,
                {}
            ),
            (
                aerospike.Client.put,
                {"bins": {"a": 1}}
            )
        ]
    )
    def test_dyn_config(self, api_method, kwargs):
        if (TestBaseClass.major_ver, TestBaseClass.minor_ver, TestBaseClass.patch_ver) < (8, 1, 3):
            pytest.skip("Expression tracing only supported in server 8.1.3 or higher")

        config = TestBaseClass.get_connection_config()
        provider = aerospike.ConfigProvider(DYN_CONFIG_PATH)
        config["config_provider"] = provider

        client = aerospike.client(config)

        policy = {
            "expressions": expr.GE(expr.Abs(expr.Val("a")), 1).compile()
        }
        with pytest.raises(e.InvalidRequest) as excinfo:
            api_method(client, key=self.keys[0], **kwargs, policy=policy)

        assert "; exp_trace={" in excinfo.value.msg
        print(excinfo.value.msg)

        client.close()
