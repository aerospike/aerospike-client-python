import aerospike
from aerospike import exception as e
import pytest
from contextlib import nullcontext
from typing import Optional


@pytest.mark.usefixtures("as_connection")
class TestMRT:
    @pytest.mark.parametrize(
        "kwargs, context, err_msg",
        [
            ({}, nullcontext(), None),
            ({"reads_capacity": 256, "writes_capacity": 256}, nullcontext(), None),
            (
                {"reads_capacity": 256, "writes_capacity": 256, "invalid_arg": 1},
                pytest.raises((TypeError)), "function takes at most 2 keyword arguments (3 given)"
            ),
            (
                {"reads_capacity": 256},
                pytest.raises((TypeError)), "Both reads_capacity and writes_capacity must be specified"
            ),
            (
                {"writes_capacity": 256},
                pytest.raises((TypeError)), "Both reads_capacity and writes_capacity must be specified"
            ),
            (
                {"reads_capacity": "256", "writes_capacity": 256},
                pytest.raises((TypeError)), "reads_capacity must be an integer"
            ),
            (
                {"reads_capacity": 256, "writes_capacity": "256"},
                pytest.raises((TypeError)), "writes_capacity must be an integer"
            ),
            # Only need to test codepath once for uint32_t conversion helper function
            (
                {"reads_capacity": 2**32, "writes_capacity": 256},
                pytest.raises((ValueError)), "reads_capacity is too large for an unsigned 32-bit integer"
            )
        ]
    )
    def test_transaction_class(self, kwargs: dict, context, err_msg: Optional[str]):
        with context as excinfo:
            mrt = aerospike.Transaction(**kwargs)
        if type(context) == nullcontext:
            mrt_id = mrt.id()
            assert type(mrt_id) == int
        else:
            assert str(excinfo.value) == err_msg

    # Even though this is an unlikely use case, this should not cause problems.
    def test_transaction_reinit(self):
        mrt = aerospike.Transaction()
        mrt.__init__()

    # Don't need to test abort() for invalid args since it shares the same codepath as commit() for input
    # validation
    @pytest.mark.parametrize(
        "args",
        [
            [],
            ["string"]
        ]
    )
    def test_mrt_invalid_args(self, args: list):
        with pytest.raises(TypeError):
            self.as_connection.commit(*args)

    def test_invalid_txn_in_policy(self):
        policy = {"txn": True}
        key = ("test", "demo", 1)
        with pytest.raises(e.ParamError) as excinfo:
            self.as_connection.get(key, policy)
        assert excinfo.value.msg == "txn is not of type aerospike.Transaction"
