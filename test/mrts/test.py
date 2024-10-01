import os
import pytest
import docker
from aerospike import exception as e
import aerospike
import base64

def setup_server():
    # Enable SC

    # Run server
    pass

@pytest.fixture
def insert_records(self, request):
    # TODO: not optimized. We should only do this once
    self.keys = []
    NUM_RECORDS = 2
    for i in range(NUM_RECORDS):
        key = ("test", "demo", i)
        self.keys.append(key)

    for i, key in enumerate(self.keys):
        try:
            self.as_connection.put(key, {"a": i})
        except e.RecordNotFound:
            pass

    # For now, we must clean up to avoid interfering with future tests after MRTs
    # But every test should set up known values instead of relying on the server being empty
    def remove_records():
        for key in self.keys:
            try:
                self.as_connection.remove(key)
            except e.RecordNotFound:
                pass

    request.addfinalizer(remove_records)

# Test case 1: Execute a simple MRT with multiple SRTs(Read and Write) in any sequence (P3)
# Validate that all operations complete successfully.
# TODO: global config and transaction level config have different codepaths (for now)
def test_commit_api_and_functionality(self, insert_records):
    mrt = aerospike.Transaction()
    policy = {
        "txn": mrt
    }
    self.as_connection.put(self.keys[0], {"a": 1}, policy)
    # Reads in an MRT should read the intermediate values of the MRT
    _, _, bins = self.as_connection.get(self.keys[0], policy)
    # Check that original value was overwritten
    assert bins == {"a": 1}
    self.as_connection.put(self.keys[1], {"a": 2}, policy)

    retval = self.as_connection.commit(transaction=mrt)
    assert retval is None

    # Were the writes committed?
    for i in range(len(self.keys)):
        _, _, bins = self.as_connection.get(self.keys[i])
        assert bins == {"a": i + 1}

# Test case 57: "Execute the MRT. Before issuing commit, give abort request using abort API" (P1)
def test_abort_api_and_functionality(self,
                                        insert_records):
    mrt = aerospike.Transaction()
    policy = {
        "txn": mrt
    }
    self.as_connection.put(self.keys[0], {"a": 1}, policy)
    # Should return intermediate overwritten value from MRT
    self.as_connection.get(self.keys[0])
    self.as_connection.put(self.keys[1], {"a": 2}, policy)
    retval = self.as_connection.abort(transaction=mrt)
    assert retval is None

    # Test that MRT didn't go through
    for i in range(len(self.keys)):
        _, _, bins = self.as_connection.get(self.keys[i])
        # Write transaction was rolled back
        assert bins == {"a": i}

# Test case 10: Issue abort after issung commit. (P1)
# Don't need to test negative case for commit()
# Both Python client's commit() and abort() share the same code path for handling exceptions
def test_abort_fail(self, insert_records):
    mrt = aerospike.Transaction()
    policy = {
        "txn": mrt
    }
    self.as_connection.put(self.keys[0], {"a": 1}, policy)
    self.as_connection.commit(mrt)
    with pytest.raises(e.RollAlreadyAttempted):
        self.as_connection.abort(mrt)
