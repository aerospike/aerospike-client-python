import aerospike
from aerospike import exception as e
import pytest
from contextlib import nullcontext
from .conftest import TestBaseClass
# from aerospike.Client import abort, commit


@pytest.mark.usefixtures("as_connection")
class TestMRT:
    @pytest.fixture(scope="class", autouse=True)
    def setup_class(self, as_connection):
        if (TestBaseClass.major_ver, TestBaseClass.minor_ver) < (8, 0):
            pytest.skip()

    @pytest.mark.parametrize(
        "args, context",
        [
            ({}, nullcontext),
            ({"reads_capacity": 256, "writes_capacity": 256}, nullcontext),
            ({"reads_capacity": 256}, pytest.raises((TypeError))),
            ({"writes_capacity": 256}, pytest.raises((TypeError))),
            ({"reads_capacity": "256", "writes_capacity": 256}, pytest.raises((TypeError))),
            ({"reads_capacity": 256, "writes_capacity": "256"}, pytest.raises((TypeError))),
        ]
    )
    def test_transaction(self, kwargs: list, context):
        with context:
            mrt = aerospike.Transaction(**kwargs)
        if context != nullcontext:
            id = mrt.id()
            assert type(id) == int

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

    def test_commit(self):
        mrt = aerospike.Transaction()
        self.as_connection.commit(mrt)

    def test_commit_fail(self):
        mrt = aerospike.Transaction()
        self.as_connection.commit(mrt)
        with pytest.raises(e.RollAlreadyAttempted):
            self.as_connection.commit(mrt)

    def test_abort(self):
        mrt = aerospike.Transaction()
        self.as_connection.abort(mrt)

    def test_abort_fail(self):
        mrt = aerospike.Transaction()
        self.as_connection.abort(mrt)
        with pytest.raises(e.RollAlreadyAttempted):
            self.as_connection.abort(mrt)

    # TODO: global config and transaction level config have different codepaths (for now)
    def test_basic_usage(self):
        mrt = aerospike.Transaction()

        policy = {
            "txn": mrt
        }
        # TODO: reuse fixture from another test
        key = ("test", "demo", 1)
        key2 = ("test", "demo", 2)
        self.as_connection.put(key, policy)
        self.as_connection.get(key2, policy)

        self.as_connection.commit(mrt)
