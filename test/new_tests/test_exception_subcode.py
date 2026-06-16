import pytest
from .conftest import KEYS
from aerospike import exception as e
from aerospike_helpers.operations import list_operations as list_ops
from . import as_errors


KEY = KEYS[0]
OPS = [
    list_ops.list_get_by_index(99)
]
ERROR_DETAIL_VERBOSITY_SETTING = "error_detail_verbosity"


class TestExceptionSubcode:
    # TODO: need to reuse fixture in conftest.py using indirect params to set num of records
    def setup(self):
        self.as_connection.put(KEY, bins={"a": []})
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
        with pytest.raises(e.InvalidRequest) as excinfo:
            self.as_connection.operate(KEYS[0], OPS, policy=policy)

        # Make sure there's no regression with the parent error code
        assert excinfo.value.code == as_errors.AEROSPIKE_ERR_REQUEST_INVALID
        if policy[ERROR_DETAIL_VERBOSITY_SETTING] == 0:
            assert excinfo.value.subcode == 0
        else:
            assert excinfo.value.subcode > 0

        SUBCODE_IN_MESSAGE = "subcode="
        if policy[ERROR_DETAIL_VERBOSITY_SETTING] == 0:
            assert SUBCODE_IN_MESSAGE not in excinfo.value.msg
        elif policy[ERROR_DETAIL_VERBOSITY_SETTING] == 1:
            # Make sure there's no regression with the server error message
            # with lower verbosity
            assert SUBCODE_IN_MESSAGE in excinfo.value.msg
        else:
            # There should be a message before the subcode
            SUBCODE_IN_QUOTES = "({})".format(SUBCODE_IN_MESSAGE)
            assert SUBCODE_IN_QUOTES in excinfo.value.msg
