# -*- coding: utf-8 -*-
import pickle
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

    def test_of_float16(self):
        v = Vector.of_float16([1.0, 2.0, -0.5, 0.1])
        assert v.element_type == Vector.ElementType.FLOAT16
        assert v.dimensions == 4
        assert v.value.typecode == "f"
        assert list(v.value) == [1.0, 2.0, -0.5, 0.0999755859375]
        assert list(v.float16_bits()) == [0x3C00, 0x4000, 0xB800, 0x2E66]

    @pytest.mark.parametrize(
        "value, bit_pattern",
        [
            # Halfway between 1.0 and the next binary16 value rounds to the even 1.0.
            (1 + 2**-11, 0x3C00),
            # Halfway between the next two binary16 values rounds up to the even one.
            (1 + 3 * 2**-11, 0x3C02),
            # Rounding straight from binary64 gives 0x3C01, but this narrows to the
            # binary32 value 1 + 2**-11 first, which is a tie.
            (1 + 2**-11 + 2**-40, 0x3C00),
            (65504.0, 0x7BFF),
            (65519.99, 0x7BFF),
            (-65504.0, 0xFBFF),
            (6e-8, 0x0001),
        ],
    )
    def test_of_float16_rounds_through_float32(self, value, bit_pattern):
        assert list(Vector.of_float16([value]).float16_bits()) == [bit_pattern]

    @pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf"), 65520.0, -65520.0, 1e300])
    def test_nan_inf_rejected_float16(self, bad):
        with pytest.raises(ValueError, match=r"^Vector elements must be finite \(NaN/Infinity not allowed\)$"):
            Vector.of_float16([0.1, bad])

    def test_of_float16_bits(self):
        # No value conversion is performed - raw bit patterns pass through unchanged.
        raw = [0x3C00, 0x4000]  # binary16 bit patterns for 1.0, 2.0
        v = Vector.of_float16_bits(raw)
        assert v.element_type == Vector.ElementType.FLOAT16
        assert v.float16_bits().typecode == "H"
        assert list(v.float16_bits()) == raw
        assert list(v.value) == [1.0, 2.0]

    def test_float16_bits_requires_float16(self):
        with pytest.raises(ValueError, match=r"^float16_bits\(\) requires a FLOAT16 vector; got FLOAT32$"):
            Vector.of_float32([0.1]).float16_bits()

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

    def test_pickle_round_trip(self):
        # pickle rebuilds a bytes subclass by calling the class with the raw bytes.
        v = Vector.of_float32([0.1, 0.2])
        v2 = pickle.loads(pickle.dumps(v))
        assert bytes(v2) == bytes(v)
        assert v2.element_type == v.element_type
        assert v2.dimensions == v.dimensions

    def test_constructor_rejects_elements(self):
        with pytest.raises(
            TypeError,
            match=r"^Vector\(\) takes the raw wire bytes; use a Vector\.of_\*\(\) factory to construct a vector "
            r"from elements$",
        ):
            Vector([0.1, 0.2])

    def test_constructor_rejects_element_type(self):
        with pytest.raises(TypeError, match=r"takes 2 positional arguments but 3 were given$"):
            Vector(Vector.ElementType.FLOAT32, [0.1, 0.2])

    def test_zero_dimensions_raises(self):
        with pytest.raises(ValueError, match=r"^Vector dimensions must be in \[1, 65536\] for this element type; got 0$"):
            Vector.of_float32([])

    def test_too_many_dimensions_raises(self):
        max_dims = Vector._MAX_ELEMENTS_BYTES // 4  # FLOAT32 is 4 bytes/element
        with pytest.raises(ValueError, match=rf"^Vector dimensions must be in \[1, {max_dims}\] for this element type; got {max_dims + 1}$"):
            Vector.of_float32([0.0] * (max_dims + 1))

    @pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
    def test_nan_inf_rejected_float32(self, bad):
        with pytest.raises(ValueError, match=r"^Vector elements must be finite \(NaN/Infinity not allowed\)$"):
            Vector.of_float32([0.1, bad, 0.3])

    @pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
    def test_nan_inf_rejected_float64(self, bad):
        with pytest.raises(ValueError, match=r"^Vector elements must be finite \(NaN/Infinity not allowed\)$"):
            Vector.of_float64([0.1, bad])

    def test_from_bytes_too_short_header_raises(self):
        with pytest.raises(ValueError, match=r"^Vector buffer too short: missing 6-byte header$"):
            Vector.from_bytes(b"\x01\x03\x04\x00")  # < 6 bytes

    def test_from_bytes_unknown_element_type_raises(self):
        raw = struct.pack("<BBI", Vector.VERSION, 0xFF, 1) + b"\x00\x00\x00\x00"
        with pytest.raises(ValueError, match=r"^Unknown Vector element_type in wire data: 255$"):
            Vector.from_bytes(raw)

    def test_from_bytes_unsupported_version_raises(self):
        raw = struct.pack("<BBI", Vector.VERSION + 1, Vector.ElementType.FLOAT32, 1) + b"\x00\x00\x00\x00"
        with pytest.raises(ValueError, match=rf"^Unsupported Vector version in wire data: {Vector.VERSION + 1}$"):
            Vector.from_bytes(raw)

    def test_from_bytes_zero_dimensions_raises(self):
        raw = struct.pack("<BBI", Vector.VERSION, Vector.ElementType.FLOAT32, 0)
        with pytest.raises(
            ValueError, match=r"^Vector dimensions in wire data must be in \[1, 65536\] for this element type; got 0$"
        ):
            Vector.from_bytes(raw)

    def test_from_bytes_truncated_body_raises(self):
        v = Vector.of_float32([0.1, 0.2, 0.3, 0.4])
        with pytest.raises(ValueError, match=r"^Vector buffer too short for declared dimensions: need 22, got 20$"):
            Vector.from_bytes(bytes(v)[:-2])  # chop off part of the last element

    def test_repr_is_not_indistinguishable_from_bytes(self):
        v = Vector.of_float32([0.1, 0.2])
        assert repr(v) != bytes(v).__repr__()
        assert "Vector(" in repr(v)
        assert repr(v) == str(v)

    def test_is_bytes_subclass(self):
        v = Vector.of_float32([0.1, 0.2])
        assert isinstance(v, bytes)

    def test_vector_bin_type_constant(self):
        assert aerospike.AS_BYTES_VECTOR == 16


# ---------------------------------------------------------------------------
# Integration tests - require server version 8.2.1 or later for vectors.
# ---------------------------------------------------------------------------


class TestVectorIntegration(object):
    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection, skip_if_server_older_than_8_2_1):
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
            ("of_float16", [1.0, 2.0, -0.5]),
            ("of_float16_bits", [0x3C00, 0x4000, 0xB800]),
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
        with pytest.raises(e.InvalidRequest):
            self.as_connection.put(key, {"vectors": bin_value})
