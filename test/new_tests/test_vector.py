# -*- coding: utf-8 -*-
import struct
from enum import IntEnum

import pytest

import aerospike
from aerospike import exception as e
from aerospike_helpers import Vector

# ---------------------------------------------------------------------------
# Unit tests - no server required. Covers Vector's construction-time
# validation (dimension bounds, NaN/Inf, malformed from_bytes() input).
# ---------------------------------------------------------------------------


class TestVectorUnit(object):
    def test_element_type_is_int_enum(self):
        assert issubclass(Vector.ElementType, IntEnum)
        assert Vector.ElementType.FLOAT32 == 0x03
        vector = Vector.of_float32([0.1])
        assert vector.element_type is Vector.ElementType.FLOAT32

    def test_query_order_constants_are_int_enums(self):
        assert issubclass(aerospike.QueryOrderByType, IntEnum)
        assert issubclass(aerospike.QueryOrder, IntEnum)
        assert issubclass(aerospike.QueryOrderByFlags, IntEnum)
        assert aerospike.QueryOrderByType.INTEGER == 1
        assert aerospike.QueryOrderByType.DOUBLE == 2
        assert aerospike.QueryOrderByType.STRING == 3
        assert aerospike.QueryOrderByType.BYTES == 4
        assert aerospike.QueryOrder.ASCENDING == 0
        assert aerospike.QueryOrder.DESCENDING == 1
        assert aerospike.QueryOrderByFlags.DEFAULT == 0
        assert aerospike.QueryOrderByFlags.CASE_INSENSITIVE == 1
        assert not hasattr(aerospike, "QUERY_ORDER_BY_DOUBLE")
        assert not hasattr(aerospike, "QUERY_ORDER_ASCENDING")
        assert not hasattr(aerospike, "QUERY_ORDER_BY_CASE_INSENSITIVE")

    def test_of_float32(self):
        v = Vector.of_float32([0.1, 0.2, 0.3, 0.4])
        assert v.element_type == Vector.ElementType.FLOAT32
        assert v.dimensions == 4
        assert list(v.value) == pytest.approx([0.1, 0.2, 0.3, 0.4], abs=1e-6)

    def test_of_int32(self):
        v = Vector.of_int32([1, -2, 3, -4, 5])
        assert v.element_type == Vector.ElementType.INT32
        assert v.dimensions == 5
        assert list(v.value) == [1, -2, 3, -4, 5]

    def test_of_float64(self):
        v = Vector.of_float64([1.5, 2.5])
        assert v.element_type == Vector.ElementType.FLOAT64
        assert v.dimensions == 2
        assert list(v.value) == pytest.approx([1.5, 2.5])

    def test_of_float16_raw_bit_patterns(self):
        # No value conversion is performed - raw bit patterns pass through unchanged.
        raw = [0x3C00, 0x4000]  # fp16 bit patterns for 1.0, 2.0
        v = Vector.of_float16(raw)
        assert v.element_type == Vector.ElementType.FLOAT16
        assert list(v.value) == raw

    def test_wire_format_header(self):
        v = Vector.of_float32([0.1, 0.2, 0.3, 0.4])
        # 6-byte header: version(1) + element_type(1) + dimensions(4)
        assert bytes(v)[0] == Vector.VERSION
        assert bytes(v)[1] == Vector.ElementType.FLOAT32
        assert bytes(v)[2:6] == (4).to_bytes(4, "little")
        assert len(bytes(v)) == 6 + 4 * 4  # header + 4 float32 elements

    def test_element_bytes_excludes_header(self):
        v = Vector.of_float32([0.1, 0.2, 0.3, 0.4])
        assert v.element_bytes() == bytes(v)[6:]
        assert len(v.element_bytes()) == len(bytes(v)) - 6

    def test_from_bytes_round_trip(self):
        v = Vector.of_float32([0.1, 0.2, 0.3, 0.4])
        v2 = Vector.from_bytes(bytes(v))
        assert bytes(v2) == bytes(v)
        assert v2.element_type == v.element_type
        assert v2.dimensions == v.dimensions
        assert list(v2.value) == pytest.approx(list(v.value))

    def test_single_positional_bytes_arg_decodes(self):
        # This is the calling convention the C extension's decode path uses
        # (create_class_instance_from_module() calls Vector(py_bytes)).
        v = Vector.of_float32([0.1, 0.2, 0.3, 0.4])
        v2 = Vector(bytes(v))
        assert bytes(v2) == bytes(v)

    def test_unknown_element_type_raises(self):
        with pytest.raises(ValueError):
            Vector(0xFF, [1, 2, 3])

    def test_zero_dimensions_raises(self):
        with pytest.raises(ValueError):
            Vector.of_float32([])

    def test_too_many_dimensions_raises(self):
        max_dims = Vector._MAX_ELEMENTS_BYTES // 4  # FLOAT32 is 4 bytes/element
        with pytest.raises(ValueError):
            Vector.of_float32([0.0] * (max_dims + 1))

    @pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
    def test_nan_inf_rejected_float32(self, bad):
        with pytest.raises(ValueError):
            Vector.of_float32([0.1, bad, 0.3])

    @pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
    def test_nan_inf_rejected_float64(self, bad):
        with pytest.raises(ValueError):
            Vector.of_float64([0.1, bad])

    def test_from_bytes_too_short_header_raises(self):
        with pytest.raises(ValueError):
            Vector.from_bytes(b"\x01\x03\x04\x00")  # < 6 bytes

    def test_from_bytes_unknown_element_type_raises(self):
        raw = struct.pack("<BBI", Vector.VERSION, 0xFF, 1) + b"\x00\x00\x00\x00"
        with pytest.raises(ValueError):
            Vector.from_bytes(raw)

    def test_from_bytes_unsupported_version_raises(self):
        raw = struct.pack("<BBI", Vector.VERSION + 1, Vector.ElementType.FLOAT32, 1) + b"\x00\x00\x00\x00"
        with pytest.raises(ValueError, match="Unsupported Vector version"):
            Vector.from_bytes(raw)

    def test_from_bytes_zero_dimensions_raises(self):
        raw = struct.pack("<BBI", Vector.VERSION, Vector.ElementType.FLOAT32, 0)
        with pytest.raises(ValueError, match="dimensions"):
            Vector.from_bytes(raw)

    def test_from_bytes_truncated_body_raises(self):
        v = Vector.of_float32([0.1, 0.2, 0.3, 0.4])
        with pytest.raises(ValueError):
            Vector.from_bytes(bytes(v)[:-2])  # chop off part of the last element

    def test_repr_is_not_indistinguishable_from_bytes(self):
        v = Vector.of_float32([0.1, 0.2])
        assert repr(v) != bytes(v).__repr__()
        assert "Vector(" in repr(v)
        assert repr(v) == str(v)

    def test_is_bytes_subclass(self):
        v = Vector.of_float32([0.1, 0.2])
        assert isinstance(v, bytes)


# ---------------------------------------------------------------------------
# Integration tests - require a running server with the Vector particle type
# (Vector Phase 1 milestone 1).
# ---------------------------------------------------------------------------


class TestVectorIntegration(object):
    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        self.test_ns = "test"
        self.test_set = "test_vector"
        self.test_keys = []

        def teardown():
            for key in self.test_keys:
                try:
                    as_connection.remove(key)
                except aerospike.exception.RecordNotFound:
                    pass

        request.addfinalizer(teardown)

    def _put_get(self, i, vector):
        key = (self.test_ns, self.test_set, i)
        self.test_keys.append(key)
        self.as_connection.put(key, {"embedding": vector})
        _, _, bins = self.as_connection.get(key)
        return bins["embedding"]

    @pytest.mark.parametrize(
        "factory,values",
        [
            ("of_float32", [0.1, 0.2, 0.3, 0.4]),
            ("of_int32", [1, -2, 3, -4, 5]),
            ("of_float64", [1.5, -2.5, 3.5]),
            ("of_float16", [0x3C00, 0x4000, 0xB800]),
        ],
    )
    def test_put_get_round_trip_every_element_type(self, factory, values):
        vector = getattr(Vector, factory)(values)
        result = self._put_get(f"rt-{factory}", vector)

        assert isinstance(result, Vector)
        assert bytes(result) == bytes(vector)
        assert result.element_type == vector.element_type
        assert result.dimensions == vector.dimensions
        assert list(result.value) == list(vector.value)

    def test_vector_bin_type_constant(self):
        assert aerospike.AS_BYTES_VECTOR == 16

    @pytest.mark.parametrize(
        "bin_value",
        [
            [Vector.of_float32([0.1, 0.2, 0.3])],
            {"a": Vector.of_float32([0.5, 0.6])},
            {"a": [Vector.of_float32([0.7])]},
        ],
        ids=["list", "map", "list-in-map"],
    )
    def test_vector_nested_in_collection_is_rejected(self, bin_value):
        key = (self.test_ns, self.test_set, "nested")
        self.test_keys.append(key)
        with pytest.raises(e.ParamError):
            self.as_connection.put(key, {"vectors": bin_value})
