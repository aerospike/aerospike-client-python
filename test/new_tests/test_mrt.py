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

    @pytest.fixture
    def cleanup_records_before_test(self, request, as_connection):
        self.keys = []
        NUM_RECORDS = 5
        for i in range(NUM_RECORDS):
            key = ("test", "demo", i)
            self.keys.append(key)

        # Remove records before adding them because I don't trust that other tests have cleaned up properly
        for key in self.keys:
            try:
                as_connection.remove(key)
            except e.RecordNotFound:
                pass

        def teardown():
            for key in self.keys:
                try:
                    as_connection.remove(key)
                except e.RecordNotFound:
                    pass

        request.addfinalizer(teardown)

    # TODO: global config and transaction level config have different codepaths (for now)
    def test_commit(self, requires_server_mrt_support, cleanup_records_before_test):
        mrt = aerospike.Transaction()
        policy = {
            "txn": mrt
        }
        for i in range(len(self.keys)):
            self.as_connection.put(self.keys[i], {"a": i}, policy)
        self.as_connection.commit(mrt)

        # Did it work?
        for i, key in enumerate(self.keys):
            _, _, bins = self.as_connection.get(key)
            assert bins == {"a": i}

    def test_commit_more_than_once(self):
        mrt = aerospike.Transaction()
        policy = {
            "txn": mrt
        }
        self.as_connection.put(self.keys[0], {"a": 0}, policy)
        self.as_connection.commit(mrt)
        with pytest.raises(e.RollAlreadyAttempted):
            self.as_connection.commit(mrt)

    @pytest.mark.parametrize("more_than_once", [False, True])
    def test_abort(self, requires_server_mrt_support, cleanup_records_before_test, more_than_once: bool):
        mrt = aerospike.Transaction()
        policy = {
            "txn": mrt
        }
        self.as_connection.put(self.keys[0], {"a": 0}, policy)
        self.as_connection.abort(mrt)
        if more_than_once is False:
            # Test that transaction didn't go through
            _, meta = self.as_connection.exists(self.keys[0])
            assert meta is None
        else:
            with pytest.raises(e.RollAlreadyAttempted):
                self.as_connection.abort(mrt)
