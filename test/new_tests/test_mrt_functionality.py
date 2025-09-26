import pytest
from aerospike import exception as e
import aerospike
from .test_base_class import TestBaseClass
import time


class TestMRTBasicFunctionality:
    def setup_class(cls):
        cls.keys = []
        NUM_RECORDS = 2
        for i in range(NUM_RECORDS):
            key = ("test", "demo", i)
            cls.keys.append(key)
        cls.bin_name = "a"

    @pytest.fixture(autouse=True)
    def insert_or_update_records(self, as_connection):
        if (TestBaseClass.major_ver, TestBaseClass.minor_ver) < (8, 0):
            pytest.skip("MRT is only supported in server version 8.0 or higher")
        if TestBaseClass.strong_consistency_enabled is False:
            pytest.skip("Strong consistency is not enabled")

        for i, key in enumerate(self.keys):
            self.as_connection.put(key, {self.bin_name: i})

    # Test case 1: Execute a simple MRT with multiple SRTs(Read and Write) in any sequence (P3)
    # Validate that all operations complete successfully.
    def test_commit_api_and_functionality(self):
        mrt = aerospike.Transaction()
        policy = {
            "txn": mrt
        }
        self.as_connection.put(self.keys[0], {self.bin_name: 1}, policy=policy)
        # Reads in an MRT should read the intermediate values of the MRT
        _, _, bins = self.as_connection.get(self.keys[0], policy)
        # Check that original value was overwritten
        assert bins == {self.bin_name: 1}
        self.as_connection.put(self.keys[1], {self.bin_name: 2}, policy=policy)

        retval = self.as_connection.commit(transaction=mrt)
        assert retval == aerospike.COMMIT_OK

        # Were the writes committed?
        for i in range(len(self.keys)):
            _, _, bins = self.as_connection.get(self.keys[i])
            assert bins == {self.bin_name: i + 1}

    def test_timeout_mrt(self):
        mrt = aerospike.Transaction()
        mrt.timeout = 1
        policy = {
            "txn": mrt
        }
        self.as_connection.put(self.keys[0], {self.bin_name: 1}, policy=policy)
        time.sleep(3)
        # Server should indicate that MRT has expired
        with pytest.raises(e.AerospikeError):
            self.as_connection.put(self.keys[1], {self.bin_name: 2}, policy=policy)

        # Cleanup MRT on server side before continuing to run test
        self.as_connection.abort(mrt)

    # Test case 57: "Execute the MRT. Before issuing commit, give abort request using abort API" (P1)
    def test_abort_api_and_functionality(self):
        mrt = aerospike.Transaction()
        policy = {
            "txn": mrt
        }
        self.as_connection.put(self.keys[0], {self.bin_name: 1}, policy=policy)
        # Should return intermediate overwritten value from MRT
        _, _, bins = self.as_connection.get(self.keys[0], policy)
        assert bins == {self.bin_name: 1}
        self.as_connection.put(self.keys[1], {self.bin_name: 2}, policy=policy)

        retval = self.as_connection.abort(transaction=mrt)
        assert retval == aerospike.ABORT_OK

        # Test that MRT didn't go through
        # i.e write commands were rolled back
        for i in range(len(self.keys)):
            _, _, bins = self.as_connection.get(self.keys[i])
            assert bins == {self.bin_name: i}

    def test_commit_fail(self):
        mrt = aerospike.Transaction()
        policy = {
            "txn": mrt
        }
        self.as_connection.put(self.keys[0], {self.bin_name: 1}, policy=policy)
        self.as_connection.abort(mrt)
        with pytest.raises(e.TransactionAlreadyAborted):
            self.as_connection.commit(mrt)

    # Test case 10: Issue abort after issung commit. (P1)
    def test_abort_fail(self):
        mrt = aerospike.Transaction()
        policy = {
            "txn": mrt
        }
        self.as_connection.put(self.keys[0], {self.bin_name: 1}, policy=policy)
        self.as_connection.commit(mrt)
        with pytest.raises(e.TransactionAlreadyCommitted):
            self.as_connection.abort(mrt)
