from .conftest import KEYS
import pytest

from aerospike_helpers.operations import bitwise_operations, map_operations, list_operations, hll_operations
import aerospike
from aerospike import exception as e

@pytest.mark.usefixtures("as_connection")
class TestInvalidOptions:
    @pytest.mark.parametrize(
        "op",
        [
            # Use fixture with correct bins
            bitwise_operations.bit_lshift(bin_name="bitwise", bit_offset=0, bit_size=2, shift=1, policy={"bit_write_flags": -1}),
            # TODO: aerospike.BIT_WRITE_PARTIAL <= x < aerospike.BIT_WRITE_PARTIAL * 2 may be valid
            bitwise_operations.bit_resize(bin_name="bitwise", byte_size=1, resize_flags=aerospike.BIT_RESIZE_SHRINK_ONLY * 2),
            # TODO: same issue as above
            map_operations.map_set_policy(bin_name="map", policy={"map_write_flags": aerospike.MAP_WRITE_PARTIAL * 2}),
            list_operations.list_append(bin_name="list", value=1, policy={"list_order": aerospike.LIST_ORDERED + 1}),
            list_operations.list_append(bin_name="list", value=1, policy={"write_flags": aerospike.LIST_WRITE_PARTIAL * 2}),
            hll_operations.hll_add(bin_name="hll", values=[1], policy={"flags": aerospike.HLL_WRITE_ALLOW_FOLD * 2}),
        ]
    )
    def test_invalid_enum_values_emits_warning(self, op):
        ops = [
            op
        ]
        try:
            with pytest.warns(DeprecationWarning):
                self.as_connection.operate(KEYS[0], ops)
        except e.ServerError:
            pass
