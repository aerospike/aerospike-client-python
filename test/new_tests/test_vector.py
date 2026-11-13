# -*- coding: utf-8 -*-
import copy
import math
import pickle
import struct
import sys
from enum import IntEnum
from typing import Optional

import numpy as np
import pytest

import aerospike
from aerospike import exception as e
from aerospike_helpers import Vector, cdt_ctx
from aerospike_helpers.batch import records as br
from aerospike_helpers.expressions import VectorBin
from aerospike_helpers.expressions import vector as vector_exp
from aerospike_helpers.operations import expression_operations, list_operations, map_operations, operations

from .as_status_codes import AerospikeStatus
from .conftest import wait_for_job_completion
from .test_base_class import TestBaseClass

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
        assert issubclass(aerospike.Query.BinDataType, IntEnum)
        assert issubclass(aerospike.Query.Order, IntEnum)
        assert issubclass(aerospike.QueryOrderByFlags, IntEnum)
        assert aerospike.Query.BinDataType.INTEGER == 1
        assert aerospike.Query.BinDataType.DOUBLE == 2
        assert aerospike.Query.BinDataType.STRING == 3
        assert aerospike.Query.BinDataType.BYTES == 4
        assert aerospike.Query.Order.ASC == 0
        assert aerospike.Query.Order.DESC == 1
        assert aerospike.QueryOrderByFlags.DEFAULT == 0
        assert aerospike.QueryOrderByFlags.CASE_INSENSITIVE == 1
        assert not hasattr(aerospike, "QUERY_ORDER_BY_DOUBLE")
        assert not hasattr(aerospike, "QUERY_ORDER_ASCENDING")
        assert not hasattr(aerospike, "QUERY_ORDER_BY_CASE_INSENSITIVE")
        assert not hasattr(aerospike, "QueryOrderByType")
        assert not hasattr(aerospike, "QueryOrder")
        assert not hasattr(aerospike, "BinDataType")
        assert not hasattr(aerospike, "Order")

    def test_query_order_constants_pickle_by_query_path(self):
        assert aerospike.Query.Order.__module__ == "aerospike"
        assert aerospike.Query.Order.__qualname__ == "Query.Order"
        pickled = pickle.dumps(aerospike.Query.BinDataType.DOUBLE)
        assert pickle.loads(pickled) is aerospike.Query.BinDataType.DOUBLE

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
    def _finite_float16_array() -> np.ndarray:
        every_float16 = np.arange(0x10000, dtype=np.uint16).view(np.float16)
        return every_float16[np.isfinite(every_float16)]

    # CLIENT-5845 test plan: PV-03
    def test_numpy_arrays_round_trip_bit_for_bit(self):
        source = self._finite_float16_array()
        vector = Vector.of_float16(source)
        assert vector.element_bytes() == source.tobytes()
        assert np.array_equal(np.frombuffer(vector.float16_bits(), dtype=np.uint16), source.view(np.uint16))
        restored = np.array(vector.value, dtype=np.float32).astype(np.float16)
        assert np.array_equal(restored.view(np.uint16), source.view(np.uint16))

        for factory, dtype in (("of_float32", np.float32), ("of_float64", np.float64)):
            array = np.array([-0.0, 0.1, -2.5, 1e-30], dtype=dtype)
            assert getattr(Vector, factory)(array).element_bytes() == array.astype(f"<{array.dtype.char}").tobytes()

    # CLIENT-5845 test plan: PV-03
    # PyTorch isn't in test/requirements.txt, so this runs only where it is installed.
    def test_torch_float16_tensor_round_trips_bit_for_bit(self):
        torch = pytest.importorskip("torch")
        source = self._finite_float16_array()
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

    # CLIENT-5845 test plan: PV-06
    # +infinity, -infinity, a quiet NaN, and a NaN with the sign bit set.
    @pytest.mark.parametrize("bad", [0x7C00, 0xFC00, 0x7E00, 0xFE01])
    def test_nan_inf_rejected_float16_bits(self, bad):
        with pytest.raises(ValueError, match=r"^Vector elements must be finite \(NaN/Infinity not allowed\)$"):
            Vector.of_float16_bits([0x3C00, bad])

    # CLIENT-5845 test plan: PV-06
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

EVERY_ELEMENT_TYPE = [
    ("of_float32", [0.1, 0.2, 0.3, 0.4]),
    ("of_int32", [1, -2, 3, -4, 5]),
    ("of_float64", [1.5, -2.5, 3.5]),
    ("of_float16", [1.0, 2.0, -0.5]),
    ("of_float16_bits", [0x3C00, 0x4000, 0xB800]),
]

# A FLOAT32 vector with a NaN element, which the client rejects, for sending through bytes.__new__().
NAN_FLOAT32_VECTOR = struct.pack("<BBI2f", 1, Vector.ElementType.FLOAT32, 2, 1.0, math.nan)


def tombstone_count(client: aerospike.Client) -> int:
    count = 0
    for _, response in client.info_all("namespace/test").values():
        stats = dict(item.split("=", 1) for item in response.strip().split(";"))
        count += int(stats["tombstones"])
    return count


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

    @pytest.mark.parametrize("factory,values", EVERY_ELEMENT_TYPE)
    def test_put_get_round_trip_every_element_type(self, factory, values):
        vector = getattr(Vector, factory)(values)
        result = self._put_get(f"rt-{factory}", vector)

        assert isinstance(result, Vector)
        assert bytes(result) == bytes(vector)
        assert result.element_type == vector.element_type
        assert result.dimensions == vector.dimensions
        assert list(result.value) == list(vector.value)

    # CLIENT-5845 test plan: PB-03
    @pytest.mark.parametrize("factory, max_dims", MAX_DIMENSIONS)
    def test_put_get_max_dimensions(self, factory, max_dims):
        vector = getattr(Vector, factory)([i % 100 for i in range(max_dims)])
        result = self._put_get(f"max-{factory}", vector)
        assert isinstance(result, Vector)
        assert result.dimensions == max_dims
        assert bytes(result) == bytes(vector)

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
            list_operations.list_append("l", Vector.of_float32([0.5]), ctx=[cdt_ctx.cdt_ctx_list_index(0)]),
            map_operations.map_put("m", "c", Vector.of_float32([0.5]), ctx=[cdt_ctx.cdt_ctx_map_key("a")]),
        ],
        ids=["list_append", "list_append_items", "list_insert", "list_set", "map_put", "map_put_items",
             "map_put_vector_key", "list_append_ctx", "map_put_ctx"],
    )
    def test_vector_in_cdt_operation_is_rejected(self, op):
        key = self._key("cdt-op")
        bins = {"l": [[1], 2], "m": {"a": {"b": 1}}}
        self.as_connection.put(key, bins)
        with pytest.raises(e.InvalidRequest):
            self.as_connection.operate(key, [op])
        assert self.as_connection.get(key)[2] == bins

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
            (NAN_FLOAT32_VECTOR, "vector: elements must be finite (no NaN or infinity)"),
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
        nan_vector = bytes.__new__(Vector, NAN_FLOAT32_VECTOR)
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

    # CLIENT-5845 test plan: PB-10
    # Each replica keeps its own tombstone, so the count rises by the effective replication factor.
    @pytest.mark.parametrize("command", ["write_none", "remove"])
    def test_durable_delete_leaves_tombstone(self, command):
        if not TestBaseClass.enterprise_in_use():
            pytest.skip("Durable deletes need an Enterprise server")
        key = self._key(f"durable-delete-{command}")
        self.as_connection.put(key, {"embedding": Vector.of_float32([0.5, 1.5])})
        stats = self.as_connection.info_random_node("namespace/test").strip().split(";")
        replicas = int(dict(item.split("=", 1) for item in stats)["effective_replication_factor"])
        before = tombstone_count(self.as_connection)

        policy = {"durable_delete": True}
        if command == "write_none":
            self.as_connection.operate(key, [operations.write("embedding", None)], policy=policy)
        else:
            self.as_connection.remove(key, policy=policy)

        with pytest.raises(e.RecordNotFound):
            self.as_connection.get(key)
        assert tombstone_count(self.as_connection) - before == replicas

    @pytest.fixture
    def vector_udf(self):
        self.as_connection.udf_put("vector_udf.lua", 0)
        yield "vector_udf"
        self.as_connection.udf_remove("vector_udf.lua")

    # CLIENT-5845 test plan: PC-05
    @pytest.mark.parametrize("factory, values", EVERY_ELEMENT_TYPE)
    def test_apply_returns_vector(self, vector_udf, factory, values):
        key = self._key("apply-read")
        vector = getattr(Vector, factory)(values)
        self.as_connection.put(key, {"embedding": vector})
        result = self.as_connection.apply(key, vector_udf, "read_bin", ["embedding"])
        self._assert_same_vector(result, vector)
        assert result.element_type == vector.element_type

    # CLIENT-5845 test plan: PC-05, PC-06
    @pytest.mark.parametrize("factory, values", EVERY_ELEMENT_TYPE)
    def test_apply_writes_vector_argument(self, vector_udf, factory, values):
        key = self._key("apply-write")
        vector = getattr(Vector, factory)(values)
        assert self.as_connection.apply(key, vector_udf, "bytes_type", [vector]) == aerospike.AS_BYTES_VECTOR
        self.as_connection.apply(key, vector_udf, "write_bin", ["embedding", vector])
        _, _, bins = self.as_connection.get(key)
        self._assert_same_vector(bins["embedding"], vector)
        assert bins["embedding"].element_type == vector.element_type

    def test_batch_apply_returns_vectors(self, vector_udf):
        vectors = [Vector.of_float32([0.1, 0.2]), Vector.of_float32([0.3, 0.4])]
        keys = [self._key(f"batch-apply-{i}") for i in range(len(vectors))]
        for key, vector in zip(keys, vectors):
            self.as_connection.put(key, {"embedding": vector})

        results = self.as_connection.batch_apply(keys, vector_udf, "read_bin", ["embedding"])
        for record, vector in zip(results.batch_records, vectors):
            assert record.result == 0
            self._assert_same_vector(record.record[2]["SUCCESS"], vector)

    def _aggregate(self, udf_module: str, function: str, vector: Vector) -> list:
        set_name = "test_vector_stream"
        key = (self.test_ns, set_name, function)
        self.test_keys.append(key)
        self.as_connection.put(key, {"embedding": vector})
        query = self.as_connection.query(self.test_ns, set_name)
        query.apply(udf_module, function)
        return query.results()

    # CLIENT-5845 test plan: PC-06
    @pytest.mark.parametrize("function", ["vector_map", "vector_reduce", "vector_aggregate"])
    def test_stream_udf_returns_vector(self, vector_udf, function):
        vector = Vector.of_float16([0.5, 1.5])
        results = self._aggregate(vector_udf, function, vector)
        if function == "vector_aggregate":
            assert len(results) == 1
            results = results[0]
        assert len(results) == 1
        self._assert_same_vector(results[0], vector)

    # CLIENT-5845 test plan: PC-06
    def test_client_side_stream_stage_sees_vector_type(self, vector_udf):
        vector = Vector.of_int32([7, -8])
        results = self._aggregate(vector_udf, "vector_client_map", vector)
        assert len(results) == 1
        assert results[0]["type"] == aerospike.AS_BYTES_VECTOR
        self._assert_same_vector(results[0]["value"], vector)

    # CLIENT-5845 test plan: PC-08
    def test_user_serializer_does_not_intercept_vector(self):
        key = self._key("serializer")
        vector = Vector.of_float32([0.5, 1.5])
        aerospike.set_serializer(lambda value: b"serialized")
        aerospike.set_deserializer(lambda raw: "deserialized")
        try:
            self.as_connection.put(key, {"embedding": vector}, serializer=aerospike.SERIALIZER_USER)
            _, _, bins = self.as_connection.get(key)
        finally:
            aerospike.unset_serializers()
        self._assert_same_vector(bins["embedding"], vector)

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


class TestVectorOlderServer(object):
    @pytest.fixture(autouse=True)
    def setup(self, as_connection, skip_if_server_8_2_1_or_later):
        self.keys = []
        yield
        for key in self.keys:
            as_connection.remove(key)

    def _put_get(self, name: str, vector: Vector) -> Vector:
        key = ("test", "test_vector_older_server", name)
        self.keys.append(key)
        self.as_connection.put(key, {"embedding": vector})
        return self.as_connection.get(key)[2]["embedding"]

    # CLIENT-5845 test plan: PC-03
    @pytest.mark.parametrize("factory, values", EVERY_ELEMENT_TYPE)
    def test_vector_stored_as_blob(self, factory, values):
        vector = getattr(Vector, factory)(values)
        result = self._put_get(factory, vector)
        assert isinstance(result, Vector)
        assert bytes(result) == bytes(vector)

    # CLIENT-5845 test plan: PC-03
    # Before 8.2.1, a vector is a blob subtype, so the server doesn't validate it.
    def test_vector_not_validated(self):
        nan_vector = bytes.__new__(Vector, NAN_FLOAT32_VECTOR)
        result = self._put_get("nan", nan_vector)
        assert isinstance(result, Vector)
        assert bytes(result) == NAN_FLOAT32_VECTOR


class TestVectorTransaction(object):
    @pytest.fixture(autouse=True)
    def setup(self, as_connection, skip_if_server_older_than_8_2_1):
        if not TestBaseClass.strong_consistency_enabled:
            pytest.skip("Transactions need a strong consistency namespace")
        self.keys = [("test", "test_vector_txn", i) for i in range(2)]
        self.original = Vector.of_float32([9.0, 8.0])
        for key in self.keys:
            as_connection.put(key, {"embedding": self.original})
        yield
        for key in self.keys:
            as_connection.remove(key)

    def _stored(self) -> list[bytes]:
        return [bytes(self.as_connection.get(key)[2]["embedding"]) for key in self.keys]

    # CLIENT-5845 test plan: PC-10
    def test_vector_writes_visible_on_commit(self):
        vector = Vector.of_float32([1.0, 2.0])
        txn = aerospike.Transaction()
        policy = {"txn": txn}
        for key in self.keys:
            self.as_connection.put(key, {"embedding": vector}, policy=policy)
        dist = vector_exp.VectorDistance(
            vector_exp.VectorDistanceMetric.EUCLIDEAN_SQUARED, vector, VectorBin("embedding")
        ).compile()
        _, _, bins = self.as_connection.operate(
            self.keys[0], [expression_operations.expression_read("dist", dist)], policy=policy
        )
        assert bins["dist"] == 0.0
        assert self._stored() == [bytes(self.original)] * len(self.keys)

        assert self.as_connection.commit(transaction=txn) == aerospike.COMMIT_OK
        assert self._stored() == [bytes(vector)] * len(self.keys)

    # CLIENT-5845 test plan: PC-10
    def test_vector_writes_undone_on_abort(self):
        txn = aerospike.Transaction()
        for key in self.keys:
            self.as_connection.put(key, {"embedding": Vector.of_float32([1.0, 2.0])}, policy={"txn": txn})
        assert self.as_connection.abort(transaction=txn) == aerospike.ABORT_OK
        assert self._stored() == [bytes(self.original)] * len(self.keys)

    # CLIENT-5845 test plan: PC-10
    def test_invalid_vector_does_not_break_the_transaction(self):
        vector = Vector.of_float32([1.0, 2.0])
        txn = aerospike.Transaction()
        self.as_connection.put(self.keys[0], {"embedding": vector}, policy={"txn": txn})
        with pytest.raises(e.InvalidRequest):
            self.as_connection.put(
                self.keys[1], {"embedding": bytes.__new__(Vector, NAN_FLOAT32_VECTOR)}, policy={"txn": txn}
            )
        assert self.as_connection.commit(transaction=txn) == aerospike.COMMIT_OK
        assert self._stored() == [bytes(vector), bytes(self.original)]
