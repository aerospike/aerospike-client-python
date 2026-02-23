import aerospike
from aerospike import exception as e
import pytest
from contextlib import nullcontext
from typing import Callable


@pytest.mark.usefixtures("as_connection")
class TestMRTAPI:
    @pytest.mark.parametrize(
        "kwargs, context",
        [
            ({}, nullcontext()),
            ({"reads_capacity": 256, "writes_capacity": 256}, nullcontext()),
            (
                {"reads_capacity": 256, "writes_capacity": 256, "invalid_arg": 1},
                pytest.raises((TypeError))
            ),
            (
                {"reads_capacity": "256", "writes_capacity": 256},
                pytest.raises((TypeError))
            ),
            (
                {"reads_capacity": 256, "writes_capacity": "256"},
                pytest.raises((TypeError))
            ),
            # Only need to test codepath once for uint32_t conversion helper function
            (
                {"reads_capacity": 2**32, "writes_capacity": 256},
                # Linux x64's unsigned long is 8 bytes long at most
                # but Windows x64's unsigned long is 4 bytes long at most
                # Python in Windows x64 will throw an internal error (OverflowError) when trying to convert a Python
                # int that is larger than 4 bytes into an unsigned long.
                # That error doesn't happen in Linux for that same scenario, so we throw our own error
                pytest.raises((ValueError, OverflowError))
            )
        ],
    )
    def test_transaction_class(self, kwargs: dict, context):
        with context as excinfo:
            mrt = aerospike.Transaction(**kwargs)
        if type(context) == nullcontext:
            # Can we read these fields
            assert type(mrt.id) == int
            assert type(mrt.timeout) == int
            assert type(mrt.state) == int
            assert type(mrt.in_doubt) == bool
            # Can we change the timeout?
            mrt.timeout = 10
        else:
            # Just use kwargs to id the test case
            if kwargs == {"reads_capacity": 2**32, "writes_capacity": 256} and excinfo.type == OverflowError:
                # Internal Python error thrown in Windows
                assert str(excinfo.value) == "Python int too large to convert to C uint32_t"
            elif kwargs == {"reads_capacity": 256, "writes_capacity": 256, "invalid_arg": 1}:
                assert str(excinfo.value) == "function takes at most 2 keyword arguments (3 given)"
            elif kwargs == {"reads_capacity": '256', "writes_capacity": 256}:
                assert str(excinfo.value) == "reads_capacity must be an uint32_t integer"
            elif kwargs == {"reads_capacity": 256, "writes_capacity": '256'}:
                assert str(excinfo.value) == "writes_capacity must be an uint32_t integer"



    # Even though this is an unlikely use case, this should not cause problems.
    def test_transaction_reinit(self):
        mrt = aerospike.Transaction()
        # Create a new transaction object using the same Python class instance
        mrt.__init__()

    @pytest.mark.parametrize(
        "args",
        [
            [],
            ["string"]
        ]
    )
    @pytest.mark.parametrize(
        "api_call",
        [
            aerospike.Client.commit,
            aerospike.Client.abort
        ]
    )
    def test_mrt_invalid_args(self, args: list, api_call: Callable):
        with pytest.raises(TypeError):
            api_call(self.as_connection, *args)

    def test_invalid_txn_in_policy(self):
        policy = {"txn": True}
        key = ("test", "demo", 1)
        with pytest.raises(e.ParamError) as excinfo:
            self.as_connection.get(key, policy)
        assert excinfo.value.msg == "txn is not of type aerospike.Transaction"
