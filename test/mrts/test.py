import pytest
from aerospike import exception as e
import aerospike


# We don't include this in the new_tests/ suite because this requires strong consistency
# We can't detect if the server has strong consistency or not
class TestMRTBasicFunctionality:
    @pytest.fixture(scope="class", autouse=True)
    def set_record_keys(request):
        request.cls.keys = []
        NUM_RECORDS = 2
        for i in range(NUM_RECORDS):
            key = ("test", "demo", i)
            request.cls.keys.append(key)
        request.cls.bin_name = "a"

    @pytest.fixture(autouse=True)
    def insert_or_update_records(self):
        config = {"hosts": [("127.0.0.1", 3000)]}
        self.as_connection = aerospike.client(config)
        for i, key in enumerate(self.keys):
            try:
                self.as_connection.put(key, {self.bin_name: i})
            except e.RecordNotFound:
                pass

    # Test case 1: Execute a simple MRT with multiple SRTs(Read and Write) in any sequence (P3)
    # Validate that all operations complete successfully.
    def test_commit_api_and_functionality(self):
        mrt = aerospike.Transaction()
        policy = {
            "txn": mrt
        }
        self.as_connection.put(self.keys[0], {self.bin_name: 1}, policy)
        # Reads in an MRT should read the intermediate values of the MRT
        _, _, bins = self.as_connection.get(self.keys[0], policy)
        # Check that original value was overwritten
        assert bins == {self.bin_name: 1}
        self.as_connection.put(self.keys[1], {self.bin_name: 2}, policy)

        retval = self.as_connection.commit(transaction=mrt)
        assert retval is None

        # Were the writes committed?
        for i in range(len(self.keys)):
            _, _, bins = self.as_connection.get(self.keys[i])
            assert bins == {self.bin_name: i + 1}

    # Test case 57: "Execute the MRT. Before issuing commit, give abort request using abort API" (P1)
    def test_abort_api_and_functionality(self):
        mrt = aerospike.Transaction()
        policy = {
            "txn": mrt
        }
        self.as_connection.put(self.keys[0], {self.bin_name: 1}, policy)
        # Should return intermediate overwritten value from MRT
        self.as_connection.get(self.keys[0])
        self.as_connection.put(self.keys[1], {self.bin_name: 2}, policy)
        retval = self.as_connection.abort(transaction=mrt)
        assert retval is None

        # Test that MRT didn't go through
        for i in range(len(self.keys)):
            _, _, bins = self.as_connection.get(self.keys[i])
            # Write transaction was rolled back
            assert bins == {self.bin_name: i}

    # Test case 10: Issue abort after issung commit. (P1)
    # Don't need to test negative case for commit()
    # Both Python client's commit() and abort() share the same code path for handling exceptions
    def test_abort_fail(self):
        mrt = aerospike.Transaction()
        policy = {
            "txn": mrt
        }
        self.as_connection.put(self.keys[0], {self.bin_name: 1}, policy)
        self.as_connection.commit(mrt)
        with pytest.raises(e.RollAlreadyAttempted):
            self.as_connection.abort(mrt)
