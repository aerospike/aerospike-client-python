from .conftest import TEST_NS, TEST_SET
import pytest

from aerospike_helpers.operations import (
    bitwise_operations,
    map_operations,
    list_operations,
    hll_operations,
)
import aerospike
from aerospike import exception as e


KEY = (TEST_NS, TEST_SET, 1)


@pytest.mark.usefixtures("as_connection")
class TestInvalidOptions:
    @pytest.mark.parametrize(
        "op",
        [
            bitwise_operations.bit_lshift(
                bin_name="bitwise",
                bit_offset=0,
                bit_size=2,
                shift=1,
                policy={"bit_write_flags": -1},
            ),
            bitwise_operations.bit_resize(
                bin_name="bitwise",
                byte_size=1,
                resize_flags=aerospike.BIT_RESIZE_SHRINK_ONLY * 2,
            ),
            map_operations.map_put(
                bin_name="map",
                key=1,
                value=1,
                map_policy={"map_order": aerospike.MAP_KEY_VALUE_ORDERED + 1},
            ),
            list_operations.list_append(
                bin_name="list",
                value=1,
                policy={"list_order": aerospike.LIST_ORDERED + 1},
            ),
            list_operations.list_append(
                bin_name="list",
                value=1,
                policy={"write_flags": aerospike.LIST_WRITE_PARTIAL * 2},
            ),
            hll_operations.hll_add(
                bin_name="hll",
                values=[1],
                policy={"flags": aerospike.HLL_WRITE_ALLOW_FOLD * 2},
            ),
        ],
    )
    def test_invalid_enum_values_emits_warning(self, op):
        ops = [op]
        try:
            with pytest.warns(DeprecationWarning):
                self.as_connection.operate(KEY, ops)
        # We only care about the client printing the DeprecationWarning; this is not an end to end test
        except e.ServerError:
            pass
