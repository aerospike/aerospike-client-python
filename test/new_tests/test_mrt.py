import aerospike
from aerospike import exception as e
import pytest
from contextlib import nullcontext
from .conftest import TestBaseClass
from typing import Optional
# from aerospike.Client import abort, commit


@pytest.mark.usefixtures("as_connection")
class TestMRT:
    @pytest.fixture
    def requires_server_mrt_support(self, as_connection):
        if TestBaseClass.enterprise_in_use() is False or (TestBaseClass.major_ver, TestBaseClass.minor_ver) < (8, 0):
            pytest.skip()

    @pytest.mark.parametrize(
        "kwargs, context, err_msg",
        [
            ({}, nullcontext(), None),
            ({"reads_capacity": 256, "writes_capacity": 256}, nullcontext(), None),
            (
                {"reads_capacity": 256},
                pytest.raises((TypeError)), "Both reads capacity and writes capacity must be specified"
            ),
            (
                {"writes_capacity": 256},
                pytest.raises((TypeError)), "Both reads capacity and writes capacity must be specified"
            ),
            (
                {"reads_capacity": "256", "writes_capacity": 256},
                pytest.raises((TypeError)), "Reads capacity must be an integer"
            ),
            (
                {"reads_capacity": 256, "writes_capacity": "256"},
                pytest.raises((TypeError)), "Writes capacity must be an integer"
            ),
        ]
    )
    def test_transaction(self, kwargs: dict, context, err_msg: Optional[str]):
        with context as excinfo:
            mrt = aerospike.Transaction(**kwargs)
        if err_msg is None:
            mrt_id = mrt.id()
            assert type(mrt_id) == int
        else:
            assert str(excinfo.value) == err_msg

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

    def test_commit(self, requires_server_mrt_support):
        mrt = aerospike.Transaction()
        self.as_connection.commit(mrt)

    def test_commit_fail(self, requires_server_mrt_support):
        mrt = aerospike.Transaction()
        self.as_connection.commit(mrt)
        with pytest.raises(e.RollAlreadyAttempted):
            self.as_connection.commit(mrt)

    def test_abort(self, requires_server_mrt_support):
        mrt = aerospike.Transaction()
        self.as_connection.abort(mrt)

    def test_abort_fail(self, requires_server_mrt_support):
        mrt = aerospike.Transaction()
        self.as_connection.abort(mrt)
        with pytest.raises(e.RollAlreadyAttempted):
            self.as_connection.abort(mrt)

    # TODO: global config and transaction level config have different codepaths (for now)
    def test_basic_usage(self, requires_server_mrt_support):
        mrt = aerospike.Transaction()

        policy = {
            "txn": mrt
        }
        # TODO: reuse fixture from another test
        key = ("test", "demo", 1)
        bins = {"a": 1}
        key2 = ("test", "demo", 2)
        self.as_connection.put(key, bins, policy)
        self.as_connection.put(key2, bins, policy)
        self.as_connection.commit(mrt)

        # Did it work?
        for key in [key, key2]:
            _, _, bins = self.as_connection.get(key)
            assert bins["a"] == 1
