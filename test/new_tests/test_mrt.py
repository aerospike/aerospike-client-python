import aerospike
from aerospike import exception as e
import pytest
# from aerospike.Client import abort, commit


@pytest.mark.usefixtures("as_connection")
class TestMRT:
    def test_transaction(self):
        mrt = aerospike.Transaction()
        id = mrt.id()
        assert type(id) == int

    @pytest.mark.parametrize(
        "args",
        [
            [],
            ["invalid_type"],
        ],
        # "api_method",
        # [
        #     commit,
        #     abort
        # ]
    )
    def test_mrt_invalid_arg(self, args):
        self.as_connection.commit()

    def test_commit(self):
        mrt = aerospike.Transaction()
        self.as_connection.commit(mrt)

    def test_commit_fail(self):
        mrt = aerospike.Transaction()
        self.as_connection.commit(mrt)
        with pytest.raises(e.AerospikeError):
            self.as_connection.commit(mrt)

    def test_abort(self):
        mrt = aerospike.Transaction()
        self.as_connection.abort(mrt)

    def test_abort_fail(self):
        mrt = aerospike.Transaction()
        self.as_connection.abort(mrt)
        self.as_connection.abort(mrt)
