from aerospike import exception as e
import aerospike
import pytest

# This isn't a correctness test. It's only for code coverage purposes
# and to make sure the API is aligned with the documentation
class TestGetPartitionID:
    def test_basic_usage(self):
        digest = aerospike.calc_digest("test", "demo", 1)
        part_id = aerospike.get_partition_id(digest)
        assert type(part_id) == int

    @pytest.mark.parametrize(
        "digest, expected_exception",
        [
            # Digests must be exactly 20 bytes long
            (bytearray([0] * 21), e.ParamError),
            (bytearray([0] * 19), e.ParamError),
            # Does not accept strings
            ("1" * 20, TypeError)
        ]
    )
    def test_invalid_digest(self, digest, expected_exception):
        with pytest.raises(expected_exception):
            aerospike.get_partition_id(digest)
