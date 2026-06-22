import pytest
from .conftest import KEYS, BIN_NAME
import aerospike
from aerospike import exception as e
from aerospike_helpers.operations import list_operations as list_ops
from .test_base_class import TestBaseClass
from . import as_errors


KEY = KEYS[0]
OPS = [
    list_ops.list_get_by_index(BIN_NAME, index=99, return_type=aerospike.LIST_RETURN_VALUE)
]
ERROR_DETAIL_VERBOSITY_SETTING = "error_detail_verbosity"


class TestExceptionSubcode:
    # TODO: need to reuse fixture in conftest.py using indirect params to set num of records
    @pytest.fixture(autouse=True)
    def setup(self, as_connection):
        self.as_connection.put(KEY, bins={BIN_NAME: []})
        yield
        self.as_connection.remove(KEY)

    @pytest.mark.parametrize(
        "policy_w_verbosity_setting",
        [
            {},
            {ERROR_DETAIL_VERBOSITY_SETTING: 0},
            {ERROR_DETAIL_VERBOSITY_SETTING: 1},
            {ERROR_DETAIL_VERBOSITY_SETTING: 2},
        ]
    )
    @pytest.mark.parametrize(
        "set_in_client_config",
        [False, True]
    )
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

            self.as_connection.operate(KEYS[0], OPS, policy=cmd_policy)

        # Make sure there's no regression with the parent error code
        assert excinfo.value.code == as_errors.AEROSPIKE_ERR_OP_NOT_APPLICABLE

        subcode_should_be_zero = (
            ERROR_DETAIL_VERBOSITY_SETTING not in policy_w_verbosity_setting
            or
            policy_w_verbosity_setting[ERROR_DETAIL_VERBOSITY_SETTING] == 0
            or
            # If running against a unsupported version, we expect subcode to always return 0
            # (and no undefined behavior)
            (TestBaseClass.major_ver, TestBaseClass.minor_ver, TestBaseClass.patch_ver) < (8, 1, 3)
        )
        if subcode_should_be_zero:
            assert excinfo.value.subcode == 0
        else:
            assert excinfo.value.subcode > 0

        EXPECTED_SUBCODE_IN_MESSAGE = "subcode="
        if excinfo.value.subcode == 0:
            assert EXPECTED_SUBCODE_IN_MESSAGE not in excinfo.value.msg
        elif policy_w_verbosity_setting[ERROR_DETAIL_VERBOSITY_SETTING] == 1:
            assert EXPECTED_SUBCODE_IN_MESSAGE in excinfo.value.msg
        else:
            # There should be a message before the subcode
            SUBCODE_IN_QUOTES = "({}".format(EXPECTED_SUBCODE_IN_MESSAGE)
            assert SUBCODE_IN_QUOTES in excinfo.value.msg

    def test_invalid_verbosity(self):
        policy = {
            ERROR_DETAIL_VERBOSITY_SETTING: 3
        }
        with pytest.raises(e.ServerError):
            self.as_connection.operate(KEYS[0], OPS, policy=policy)
