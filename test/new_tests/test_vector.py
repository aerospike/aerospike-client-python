# -*- coding: utf-8 -*-
import copy
import math
import pickle
import struct
import sys
from enum import IntEnum
from typing import Optional

import pytest

import aerospike
from aerospike import exception as e
from aerospike_helpers import Vector
from aerospike_helpers.batch import records as br
from aerospike_helpers.operations import list_operations, map_operations, operations

from .as_status_codes import AerospikeStatus
from .conftest import wait_for_job_completion

# ---------------------------------------------------------------------------
# Unit tests - no server required. Covers Vector's construction-time
# validation (dimension bounds, NaN/Inf, malformed from_bytes() input).
# ---------------------------------------------------------------------------

# Each factory's largest dimension count: 256 KiB of elements.
MAX_DIMENSIONS = [
    ("of_float16", 131072),
    ("of_float16_bits", 131072),
    ("of_int32", 65536),
    ("of_float32", 65536),
    ("of_float64", 32768),
]


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

    @staticmethod
    def _finite_float16_array(np):
        every_float16 = np.arange(0x10000, dtype=np.uint16).view(np.float16)
        return every_float16[np.isfinite(every_float16)]

    # CLIENT-5845 test plan: PV-03
    # NumPy and PyTorch aren't test dependencies, so these run only where they are installed.
    def test_numpy_arrays_round_trip_bit_for_bit(self):
        np = pytest.importorskip("numpy")
        source = self._finite_float16_array(np)
        vector = Vector.of_float16(source)
        assert vector.element_bytes() == source.tobytes()
        assert np.array_equal(np.frombuffer(vector.float16_bits(), dtype=np.uint16), source.view(np.uint16))
        restored = np.array(vector.value, dtype=np.float32).astype(np.float16)
        assert np.array_equal(restored.view(np.uint16), source.view(np.uint16))

        for factory, dtype in (("of_float32", np.float32), ("of_float64", np.float64)):
            array = np.array([-0.0, 0.1, -2.5, 1e-30], dtype=dtype)
            assert getattr(Vector, factory)(array).element_bytes() == array.astype(f"<{array.dtype.char}").tobytes()

    # CLIENT-5845 test plan: PV-03
    def test_torch_float16_tensor_round_trips_bit_for_bit(self):
        np = pytest.importorskip("numpy")
        torch = pytest.importorskip("torch")
        source = self._finite_float16_array(np)
        assert Vector.of_float16(torch.from_numpy(source)).element_bytes() == source.tobytes()

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

    # CLIENT-5845 test plan: PV-04
    @pytest.mark.parametrize("factory, max_dims", MAX_DIMENSIONS)
    def test_dimension_bounds_accepted(self, factory, max_dims):
        for dimensions in (1, max_dims):
            assert getattr(Vector, factory)([0] * dimensions).dimensions == dimensions

    # CLIENT-5845 test plan: PV-04
    @pytest.mark.parametrize("factory, max_dims", MAX_DIMENSIONS)
    def test_dimensions_out_of_bounds_raise(self, factory, max_dims):
        for dimensions in (0, max_dims + 1):
            with pytest.raises(
                ValueError,
                match=rf"^Vector dimensions must be in \[1, {max_dims}\] for this element type; got {dimensions}$",
            ):
                getattr(Vector, factory)([0] * dimensions)

    @pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
    def test_nan_inf_rejected_float32(self, bad):
        with pytest.raises(ValueError, match=r"^Vector elements must be finite \(NaN/Infinity not allowed\)$"):
            Vector.of_float32([0.1, bad, 0.3])

    @pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf"), 10**400])
    def test_nan_inf_rejected_float64(self, bad):
        with pytest.raises(ValueError, match=r"^Vector elements must be finite \(NaN/Infinity not allowed\)$"):
            Vector.of_float64([0.1, bad])

    # CLIENT-5845 test plan: PV-05
    # -0.0, the smallest subnormal and the largest finite value of each type.
    @pytest.mark.parametrize(
        "factory, fmt, values",
        [
            ("of_float16", "<3e", [-0.0, 2**-24, 65504.0]),
            ("of_float32", "<3f", [-0.0, 2**-149, 3.4028234663852886e38]),
            ("of_float64", "<3d", [-0.0, 5e-324, sys.float_info.max]),
        ],
    )
    def test_finite_boundary_values_stored_verbatim(self, factory, fmt, values):
        assert getattr(Vector, factory)(values).element_bytes() == struct.pack(fmt, *values)

    @pytest.mark.parametrize("bad", [1e39, -1e39, 10**400])
    def test_overflowing_float32_rejected(self, bad):
        with pytest.raises(ValueError, match=r"^Vector elements must be finite \(NaN/Infinity not allowed\)$"):
            Vector.of_float32([0.1, bad])

    def test_overflowing_int_rejected_float16(self):
        with pytest.raises(ValueError, match=r"^Vector elements must be finite \(NaN/Infinity not allowed\)$"):
            Vector.of_float16([0.1, 10**400])

    @pytest.mark.parametrize(
        "factory, element_type",
        [("of_float16", "FLOAT16"), ("of_float32", "FLOAT32"), ("of_float64", "FLOAT64")],
    )
    @pytest.mark.parametrize("bad", ["0.5", None, b"\x00", 1j])
    def test_non_real_rejected(self, factory, element_type, bad):
        with pytest.raises(
            TypeError, match=rf"^Vector {element_type} elements must be real numbers; got {type(bad).__name__}$"
        ):
            getattr(Vector, factory)([0.1, bad])

    def test_float_factories_accept_ints(self):
        assert list(Vector.of_float32([1, 2]).value) == [1.0, 2.0]
        assert list(Vector.of_float64([1, 2]).value) == [1.0, 2.0]
        assert list(Vector.of_float16([1, 2]).value) == [1.0, 2.0]

    # CLIENT-5845 test plan: PV-06
    @pytest.mark.parametrize("bad", [1.5, 1.0, "1", None])
    def test_non_integer_rejected_int32(self, bad):
        with pytest.raises(TypeError, match=rf"^Vector INT32 elements must be integers; got {type(bad).__name__}$"):
            Vector.of_int32([1, bad])

    # CLIENT-5845 test plan: PV-06
    @pytest.mark.parametrize("bad", [2**31, -(2**31) - 1, 2**64])
    def test_out_of_range_rejected_int32(self, bad):
        with pytest.raises(
            ValueError, match=rf"^Vector INT32 elements must be in \[-2147483648, 2147483647\]; got {bad}$"
        ):
            Vector.of_int32([1, bad])

    # CLIENT-5845 test plan: PV-06
    def test_int32_bounds_accepted(self):
        assert list(Vector.of_int32([-(2**31), 2**31 - 1]).value) == [-(2**31), 2**31 - 1]

    # CLIENT-5845 test plan: PV-06
    @pytest.mark.parametrize("bad", [1.0, "1"])
    def test_non_integer_rejected_float16_bits(self, bad):
        with pytest.raises(
            TypeError, match=rf"^Vector FLOAT16 bit patterns must be integers; got {type(bad).__name__}$"
        ):
            Vector.of_float16_bits([0x3C00, bad])

    # CLIENT-5845 test plan: PV-06
    @pytest.mark.parametrize("bad", [-1, 0x10000])
    def test_out_of_range_rejected_float16_bits(self, bad):
        with pytest.raises(ValueError, match=rf"^Vector FLOAT16 bit patterns must be in \[0, 65535\]; got {bad}$"):
            Vector.of_float16_bits([0x3C00, bad])

    # +infinity, -infinity, a quiet NaN, and a NaN with the sign bit set.
    @pytest.mark.parametrize("bad", [0x7C00, 0xFC00, 0x7E00, 0xFE01])
    def test_nan_inf_rejected_float16_bits(self, bad):
        with pytest.raises(ValueError, match=r"^Vector elements must be finite \(NaN/Infinity not allowed\)$"):
            Vector.of_float16_bits([0x3C00, bad])

    def test_float16_bits_bounds_accepted(self):
        # 0x7BFF is the largest finite binary16 value and 0xFBFF its negation.
        assert list(Vector.of_float16_bits([0x0000, 0x7BFF, 0xFBFF]).float16_bits()) == [0x0000, 0x7BFF, 0xFBFF]

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

    # CLIENT-5845 test plan: PV-09
    # pickle and copy rebuild a bytes subclass by calling the class with the raw bytes.
    @pytest.mark.parametrize(
        "round_trip",
        [lambda v: pickle.loads(pickle.dumps(v)), copy.copy, copy.deepcopy],
        ids=["pickle", "copy", "deepcopy"],
    )
    def test_pickle_and_copy_round_trip(self, round_trip):
        v = Vector.of_float32([0.1, 0.2])
        v2 = round_trip(v)
        assert type(v2) is Vector
        assert bytes(v2) == bytes(v)
        assert v2.element_type == v.element_type
        assert v2.dimensions == v.dimensions

    # CLIENT-5845 test plan: PV-09
    def test_repr_is_not_indistinguishable_from_bytes(self):
        v = Vector.of_float32([0.1, 0.2])
        assert repr(v) != bytes(v).__repr__()
        assert "Vector(" in repr(v)
        assert repr(v) == str(v)

    # CLIENT-5845 test plan: PV-09
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

    def _key(self, name):
        key = (self.test_ns, self.test_set, name)
        self.test_keys.append(key)
        return key

    @staticmethod
    def _assert_same_vector(result, expected):
        assert isinstance(result, Vector)
        assert bytes(result) == bytes(expected)

    def test_operate_write_and_read(self):
        key = self._key("operate")
        vector = Vector.of_float32([0.1, 0.2, 0.3])
        _, _, bins = self.as_connection.operate(
            key, [operations.write("embedding", vector), operations.read("embedding")]
        )
        self._assert_same_vector(bins["embedding"], vector)

    def test_batch_write_and_batch_read(self):
        vectors = [Vector.of_float32([0.1, 0.2]), Vector.of_int32([3, -4]), Vector.of_float16([0.5, 1.5])]
        keys = [self._key(f"batch-{i}") for i in range(len(vectors))]
        writes = br.BatchRecords(
            [br.Write(key, [operations.write("embedding", vector)]) for key, vector in zip(keys, vectors)]
        )
        written = self.as_connection.batch_write(writes)
        assert [record.result for record in written.batch_records] == [0] * len(vectors)

        read = self.as_connection.batch_read(keys, ["embedding"])
        for record, vector in zip(read.batch_records, vectors):
            assert record.result == 0
            self._assert_same_vector(record.record[2]["embedding"], vector)

    # CLIENT-5845 test plan: PB-04
    def test_batch_operate_writes_vector(self):
        keys = [self._key(f"batch-operate-{i}") for i in range(2)]
        vector = Vector.of_float64([0.5, -0.5])
        written = self.as_connection.batch_operate(keys, [operations.write("embedding", vector)])
        assert [record.result for record in written.batch_records] == [0] * len(keys)
        for key in keys:
            self._assert_same_vector(self.as_connection.get(key)[2]["embedding"], vector)

    # CLIENT-5845 test plan: PB-04
    @pytest.mark.parametrize("command, job_module", [("query", aerospike.JOB_QUERY), ("scan", aerospike.JOB_SCAN)])
    def test_background_write_stores_vector(self, command, job_module):
        set_name = f"test_vector_background_{command}"
        keys = [(self.test_ns, set_name, i) for i in range(3)]
        self.test_keys.extend(keys)
        for key in keys:
            self.as_connection.put(key, {"id": key[2]})

        vector = Vector.of_float16([0.5, 1.5])
        job = getattr(self.as_connection, command)(self.test_ns, set_name)
        job.add_ops([operations.write("embedding", vector)])
        job_id = job.execute_background()
        wait_for_job_completion(self.as_connection, job_id, job_module=job_module, time_limit_secs=30)
        assert self.as_connection.job_info(job_id, job_module)["status"] == aerospike.JOB_STATUS_COMPLETED

        for key in keys:
            self._assert_same_vector(self.as_connection.get(key)[2]["embedding"], vector)

    # CLIENT-5845 test plan: PB-05
    @pytest.mark.parametrize("command", ["select", "batch_operate", "query", "query_foreach", "scan", "scan_foreach"])
    def test_read_path_returns_vector(self, command):
        set_name = "test_vector_read_paths"
        key = (self.test_ns, set_name, "read-path")
        self.test_keys.append(key)
        vector = Vector.of_int32([7, -8, 9])
        self.as_connection.put(key, {"embedding": vector})

        if command == "select":
            records = [self.as_connection.select(key, ["embedding"])]
        elif command == "batch_operate":
            batch = self.as_connection.batch_operate([key], [operations.read("embedding")])
            records = [batch_record.record for batch_record in batch.batch_records]
        elif command in ("query", "scan"):
            command_method = getattr(self.as_connection, command)
            records = command_method(self.test_ns, set_name).results()
        else:
            command_method = getattr(self.as_connection, command.removesuffix("_foreach"))
            records = []
            command_method(self.test_ns, set_name).foreach(records.append)

        assert len(records) == 1
        self._assert_same_vector(records[0][2]["embedding"], vector)

    # CLIENT-5845 test plan: PB-06
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
        self.as_connection.put(key, {"vectors": [1, 2]})
        with pytest.raises(e.InvalidRequest):
            self.as_connection.put(key, {"vectors": bin_value})
        _, _, bins = self.as_connection.get(key)
        assert bins == {"vectors": [1, 2]}

    # CLIENT-5845 test plan: PB-06
    @pytest.mark.parametrize(
        "op",
        [
            list_operations.list_append("l", Vector.of_float32([0.5])),
            list_operations.list_append_items("l", [Vector.of_float32([0.5])]),
            list_operations.list_insert("l", 0, Vector.of_float32([0.5])),
            list_operations.list_set("l", 0, Vector.of_float32([0.5])),
            map_operations.map_put("m", "b", Vector.of_float32([0.5])),
            map_operations.map_put_items("m", {"b": Vector.of_float32([0.5])}),
            map_operations.map_put("m", Vector.of_float32([0.5]), 1),
        ],
        ids=["list_append", "list_append_items", "list_insert", "list_set", "map_put", "map_put_items",
             "map_put_vector_key"],
    )
    def test_vector_in_cdt_operation_is_rejected(self, op):
        key = self._key("cdt-op")
        self.as_connection.put(key, {"l": [1, 2], "m": {"a": 1}})
        with pytest.raises(e.InvalidRequest):
            self.as_connection.operate(key, [op])
        _, _, bins = self.as_connection.get(key)
        assert bins == {"l": [1, 2], "m": {"a": 1}}

    # CLIENT-5845 test plan: PB-06
    # The C client only accepts plain blobs as map keys, so the map is rejected before it is sent.
    def test_vector_as_map_key_is_rejected_by_the_client(self):
        key = self._key("vector-map-key")
        with pytest.raises(e.ClientError) as excinfo:
            self.as_connection.put(key, {"m": {Vector.of_float32([0.5]): 1}})
        assert excinfo.value.msg == "Failed to convert Python dictionary to a C client as_map."

    # CLIENT-5845 test plan: PB-07
    @pytest.mark.parametrize(
        "command, value",
        [("append", "x"), ("append", b"\x00"), ("prepend", "x"), ("prepend", b"\x00"), ("increment", 1)],
    )
    def test_modifying_vector_bin_raises_bin_incompatible_type(self, command, value):
        key = self._key("modify")
        vector = Vector.of_float32([0.5, 1.5])
        self.as_connection.put(key, {"embedding": vector})
        command_method = getattr(self.as_connection, command)
        with pytest.raises(e.BinIncompatibleType):
            command_method(key, "embedding", value)
        self._assert_same_vector(self.as_connection.get(key)[2]["embedding"], vector)

    # CLIENT-5845 test plan: PB-08
    # bytes.__new__() skips the client's checks, so these show that the client sends a malformed vector
    # unchanged and surfaces the server's error detail. aerospike-tests-python's VW corpus covers the
    # server's validation itself.
    @pytest.mark.parametrize(
        "raw, expected_detail",
        [
            (
                struct.pack("<BBI2f", 1, Vector.ElementType.FLOAT32, 2, 1.0, math.nan),
                "vector: elements must be finite (no NaN or infinity)",
            ),
            (
                bytes(Vector.of_float32([1, 2, 3]))[:-1],
                "vector: value size 17 does not match 18 (3 dimensions of 4 bytes, little-endian)",
            ),
            (b"", "vector: value size 0 smaller than 6-byte header"),
            (
                struct.pack("<BBI2f", 2, Vector.ElementType.FLOAT32, 2, 1.0, 2.0),
                "vector: unsupported format version 2 (expected 1)",
            ),
            (struct.pack("<BBI2f", 1, 5, 2, 1.0, 2.0), "vector: invalid element type 5"),
        ],
        ids=["nan-element", "one-byte-short", "empty", "version-2", "unknown-element-type"],
    )
    def test_server_rejects_malformed_vector_with_error_detail(self, raw, expected_detail):
        key = self._key("malformed")
        original = Vector.of_float32([9.0, 8.0])
        self.as_connection.put(key, {"embedding": original})

        policy = {"error_detail_verbosity": aerospike.ERROR_DETAIL_MESSAGE}
        with pytest.raises(e.InvalidRequest) as excinfo:
            self.as_connection.put(key, {"embedding": bytes.__new__(Vector, raw)}, policy=policy)
        # The C client prefixes the server's detail with the node address.
        assert excinfo.value.msg.endswith(f" {expected_detail}")
        self._assert_same_vector(self.as_connection.get(key)[2]["embedding"], original)

    def _write_mixed_batch(self, policy_batch: Optional[dict] = None) -> br.BatchRecord:
        keys = [self._key(f"batch-mixed-{i}") for i in range(3)]
        original = Vector.of_float32([9.0, 8.0])
        self.as_connection.put(keys[1], {"embedding": original})
        good = Vector.of_float32([1.0, 2.0])
        nan_vector = bytes.__new__(Vector, struct.pack("<BBI2f", 1, Vector.ElementType.FLOAT32, 2, 1.0, math.nan))
        writes = br.BatchRecords(
            [
                br.Write(keys[0], [operations.write("embedding", good)]),
                br.Write(keys[1], [operations.write("embedding", nan_vector)]),
                br.Write(keys[2], [operations.write("embedding", good)]),
            ]
        )
        written = self.as_connection.batch_write(writes, policy_batch=policy_batch)

        assert written.result == AerospikeStatus.AEROSPIKE_BATCH_FAILED
        assert [record.result for record in written.batch_records] == [
            AerospikeStatus.AEROSPIKE_OK,
            AerospikeStatus.AEROSPIKE_ERR_REQUEST_INVALID,
            AerospikeStatus.AEROSPIKE_OK,
        ]
        for key in (keys[0], keys[2]):
            self._assert_same_vector(self.as_connection.get(key)[2]["embedding"], good)
        self._assert_same_vector(self.as_connection.get(keys[1])[2]["embedding"], original)
        return written.batch_records[1]

    # CLIENT-5845 test plan: PB-09
    def test_batch_write_with_invalid_vector_writes_the_valid_records(self):
        invalid = self._write_mixed_batch({"error_detail_verbosity": aerospike.ERROR_DETAIL_MESSAGE})
        # The C client prefixes the server's detail with the node address.
        assert invalid.message.endswith(" vector: elements must be finite (no NaN or infinity)")

    # CLIENT-5845 test plan: PB-09
    def test_batch_write_invalid_vector_default_message_names_its_result(self):
        invalid = self._write_mixed_batch()
        assert invalid.message.endswith(" AEROSPIKE_ERR_REQUEST_INVALID")

    @pytest.fixture
    def vector_udf(self):
        self.as_connection.udf_put("vector_udf.lua", 0)
        yield "vector_udf"
        self.as_connection.udf_remove("vector_udf.lua")

    def test_apply_returns_vector(self, vector_udf):
        key = self._key("apply-read")
        vector = Vector.of_float64([1.5, -2.5])
        self.as_connection.put(key, {"embedding": vector})
        result = self.as_connection.apply(key, vector_udf, "read_bin", ["embedding"])
        self._assert_same_vector(result, vector)

    def test_apply_writes_vector_argument(self, vector_udf):
        key = self._key("apply-write")
        vector = Vector.of_float32([0.25, 0.5])
        self.as_connection.apply(key, vector_udf, "write_bin", ["embedding", vector])
        _, _, bins = self.as_connection.get(key)
        self._assert_same_vector(bins["embedding"], vector)

    def test_batch_apply_returns_vectors(self, vector_udf):
        vectors = [Vector.of_float32([0.1, 0.2]), Vector.of_float32([0.3, 0.4])]
        keys = [self._key(f"batch-apply-{i}") for i in range(len(vectors))]
        for key, vector in zip(keys, vectors):
            self.as_connection.put(key, {"embedding": vector})

        results = self.as_connection.batch_apply(keys, vector_udf, "read_bin", ["embedding"])
        for record, vector in zip(results.batch_records, vectors):
            assert record.result == 0
            self._assert_same_vector(record.record[2]["SUCCESS"], vector)

    @pytest.mark.parametrize(
        "command", ["apply", "batch_apply", "batch_write", "query_aggregate", "query_aggregate_foreach"]
    )
    def test_invalid_vector_bytes_from_server_raise_client_error(self, vector_udf, command):
        key = self._key("invalid-from-udf")
        self.as_connection.put(key, {"a": 1})
        with pytest.raises(e.ClientError) as excinfo:
            if command == "apply":
                self.as_connection.apply(key, vector_udf, "truncated_vector", [])
            elif command == "batch_apply":
                self.as_connection.batch_apply([key], vector_udf, "truncated_vector", [])
            elif command == "batch_write":
                self.as_connection.batch_write(br.BatchRecords([br.Apply(key, vector_udf, "truncated_vector", [])]))
            else:
                query = self.as_connection.query(self.test_ns, self.test_set)
                query.apply(vector_udf, "truncated_vectors")
                if command == "query_aggregate":
                    query.results()
                else:
                    query.foreach(lambda _: None)
        assert excinfo.value.msg == "Unable to deserialize bytes as Vector: Vector buffer too short: missing 6-byte header"

    # The server validates stored vectors, so make the constructor fail instead.
    # See test_hll_conversion_failure_raises_client_error in test_hll.py.
    @pytest.mark.parametrize(
        "command",
        ["get", "operate", "batch_read", "batch_operate", "batch_write", "query", "query_foreach", "scan",
         "scan_foreach"],
    )
    def test_vector_conversion_failure_raises_client_error(self, monkeypatch, command):
        key = self._key("conversion-failure")
        self.as_connection.put(key, {"embedding": Vector.of_float32([0.5, 1.5])})

        def failing_new(cls, raw):
            raise ValueError("Vector constructor failed")

        monkeypatch.setattr(Vector, "__new__", failing_new)
        with pytest.raises(e.ClientError) as excinfo:
            if command == "get":
                self.as_connection.get(key)
            elif command == "operate":
                self.as_connection.operate(key, [operations.read("embedding")])
            elif command == "batch_read":
                self.as_connection.batch_read([key])
            elif command == "batch_operate":
                self.as_connection.batch_operate([key], [operations.read("embedding")])
            elif command == "batch_write":
                self.as_connection.batch_write(br.BatchRecords([br.Read(key, [operations.read("embedding")])]))
            elif command == "query":
                self.as_connection.query(self.test_ns, self.test_set).results()
            elif command == "query_foreach":
                self.as_connection.query(self.test_ns, self.test_set).foreach(lambda _: None)
            elif command == "scan":
                self.as_connection.scan(self.test_ns, self.test_set).results()
            else:
                self.as_connection.scan(self.test_ns, self.test_set).foreach(lambda _: None)
        assert excinfo.value.msg == "Unable to deserialize bytes as Vector: Vector constructor failed"
