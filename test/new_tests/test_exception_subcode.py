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
        "policy",
        [
            {},
            {ERROR_DETAIL_VERBOSITY_SETTING: 0},
            {ERROR_DETAIL_VERBOSITY_SETTING: 1},
            {ERROR_DETAIL_VERBOSITY_SETTING: 2},
        ]
    )
    def test_minimum_error_verbosity(self, policy: dict):
        with pytest.raises(e.OpNotApplicable) as excinfo:
            self.as_connection.operate(KEYS[0], OPS, policy=policy)

        # Make sure there's no regression with the parent error code
        assert excinfo.value.code == as_errors.AEROSPIKE_ERR_OP_NOT_APPLICABLE

        err_verbosity_is_zero = (
            ERROR_DETAIL_VERBOSITY_SETTING not in policy
            or
            policy[ERROR_DETAIL_VERBOSITY_SETTING] == 0
            or
            # If running against a unsupported version, we expect subcode to always return 0
            # (and no undefined behavior)
            (TestBaseClass.major_ver, TestBaseClass.minor_ver, TestBaseClass.patch_ver) < (8, 1, 3)
        )
        if err_verbosity_is_zero:
            assert excinfo.value.subcode == 0
        else:
            assert excinfo.value.subcode > 0

        SUBCODE_IN_MESSAGE = "subcode="
        if err_verbosity_is_zero:
            assert SUBCODE_IN_MESSAGE not in excinfo.value.msg
        elif policy[ERROR_DETAIL_VERBOSITY_SETTING] == 1:
            # Make sure there's no regression with the server error message
            # with lower verbosity
            assert SUBCODE_IN_MESSAGE in excinfo.value.msg
        else:
            # There should be a message before the subcode
            SUBCODE_IN_QUOTES = "({}".format(SUBCODE_IN_MESSAGE)
            assert SUBCODE_IN_QUOTES in excinfo.value.msg

    def test_invalid_verbosity(self):
        policy = {
            ERROR_DETAIL_VERBOSITY_SETTING: 3
        }
        with pytest.raises(e.ServerError):
            self.as_connection.operate(KEYS[0], OPS, policy=policy)
