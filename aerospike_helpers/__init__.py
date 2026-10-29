##########################################################################
# Copyright 2013-2026 Aerospike, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
##########################################################################

class HyperLogLog(bytes):
    """
    Represents a HyperLogLog value. This can be returned from or sent to the server.

    .. testcode::

        from aerospike_helpers.operations import hll_operations
        import aerospike

        client = aerospike.client({'hosts': [('localhost', 3000)]})

        BIN_NAME="hll"
        ops = [
            hll_operations.hll_init(BIN_NAME, index_bit_count=4, mh_bit_count=4)
        ]
        keyTuple = ("test", "demo", 1)
        client.operate(keyTuple, ops)
        _, _, bins = client.get(keyTuple)
        print(bins[BIN_NAME])

        client.put(keyTuple, bins)
        _, _, bins = client.get(keyTuple)
        print(bins[BIN_NAME])

        # Cleanup
        client.remove(keyTuple)

    .. testoutput::

        HyperLogLog(...)
        HyperLogLog(...)
    """
    def __new__(cls, o) -> "HyperLogLog":
        return super().__new__(cls, o)

    # We need to implement repr() and str() ourselves
    # Otherwise, this class will inherit these methods from bytes
    # making it indistinguishable from bytes objects when printed
    def __repr__(self) -> str:
        bytes_str = super().__repr__()
        return f"{self.__class__.__name__}({bytes_str})"

    def __str__(self) -> str:
        return self.__repr__()


import math as _math
import struct as _struct
from array import array as _array
from enum import IntEnum as _IntEnum
from typing import Iterable as _Iterable


class Vector(bytes):
    """
    Represents a vector bin value used for vector similarity search.

    Wraps the raw wire-format bytes (a 6-byte header followed by a
    contiguous little-endian element array). The header is the version (1 byte),
    the element type (1 byte), and the number of dimensions (4 bytes, little-endian).

    A vector can only be stored as a top-level bin value. The server rejects a write
    that puts a vector in a list or map, at any depth, with
    :exc:`~aerospike.exception.InvalidRequest`.

    .. note:: Requires server version >= 8.2.1

    Construct with one of the ``Vector.of_*()`` factories::

        from aerospike_helpers import Vector

        v = Vector.of_float32([0.1, 0.2, 0.3, 0.4])

        # Python has no float16 type, so of_float16() rounds each value to binary16,
        # and float16_values() reads the elements back as floats.
        v16 = Vector.of_float16([0.1, 0.2])
        v16.float16_values()  # [0.0999755859375, 0.199951171875]

    .. testcode::

        from aerospike_helpers import Vector
        import aerospike

        client = aerospike.client({'hosts': [('localhost', 3000)]})

        keyTuple = ("test", "demo", "vec1")
        client.put(keyTuple, {"embedding": Vector.of_float32([0.1, 0.2, 0.3, 0.4])})

        _, _, bins = client.get(keyTuple)
        embedding = bins["embedding"]
        print(embedding.element_type == Vector.ElementType.FLOAT32)
        print(list(embedding.value))

        client.remove(keyTuple)

    .. testoutput::

        True
        [0.10000000149011612, 0.20000000298023224, 0.30000001192092896, 0.4000000059604645]

    .. seealso::
        :meth:`~aerospike.Query.order_by` for using :class:`Vector` bins in a Top-K
        (nearest-neighbor) vector similarity search query.
    """

    VERSION = 1

    class ElementType(_IntEnum):
        FLOAT16 = 0x01
        INT32 = 0x02
        FLOAT32 = 0x03  # default
        FLOAT64 = 0x04
        # No BIN/Hamming type - confirmed absent from the real C client.

    _STRUCT_FMT = {
        ElementType.FLOAT16: "<H",  # raw binary16 bit pattern; of_float16() converts values
        ElementType.INT32: "<i",
        ElementType.FLOAT32: "<f",
        ElementType.FLOAT64: "<d",
    }
    _HEADER_FMT = "<BBI"
    _HEADER_SIZE = 6
    _MAX_ELEMENTS_BYTES = 1 << 18  # AS_VECTOR_VALUE_MAX_ELEMENTS_BYTES
    _NON_FINITE_ERROR = "Vector elements must be finite (NaN/Infinity not allowed)"

    # Declared here (rather than only assigned in __new__/from_bytes) so that
    # mypy/stubtest can see these instance attributes exist on the class.
    _element_type: ElementType
    _dimensions: int

    def __new__(
        cls,
        element_type_or_bytes: ElementType | bytes | bytearray,
        elements=None,
    ) -> "Vector":
        # Two calling conventions:
        #   Vector(element_type: ElementType, elements: Sequence)  - normal construction
        #   Vector(raw_bytes: bytes)                        - internal decode path,
        #       used by the C extension when deserializing a stored Vector bin
        #       (mirrors create_class_instance_from_module()'s single-positional-arg
        #       calling convention).
        if isinstance(element_type_or_bytes, (bytes, bytearray)):
            if elements is not None:
                raise TypeError("Vector() takes no elements when given raw bytes")
            return cls.from_bytes(bytes(element_type_or_bytes))

        element_type = element_type_or_bytes
        fmt = cls._STRUCT_FMT.get(element_type)
        if fmt is None:
            raise ValueError(f"Unknown Vector element_type: {element_type}")

        elements = list(elements)
        dimensions = len(elements)
        elem_size = _struct.calcsize(fmt)
        max_dims = cls._MAX_ELEMENTS_BYTES // elem_size

        if not (1 <= dimensions <= max_dims):
            raise ValueError(
                f"Vector dimensions must be in [1, {max_dims}] for this element "
                f"type; got {dimensions}"
            )

        if element_type in (cls.ElementType.FLOAT32, cls.ElementType.FLOAT64):
            for e in elements:
                if e != e or e in (float("inf"), float("-inf")):  # NaN/Inf check
                    raise ValueError(cls._NON_FINITE_ERROR)

        header = _struct.pack(cls._HEADER_FMT, cls.VERSION, element_type,
                               dimensions)
        body = _struct.pack(f"<{dimensions}{fmt[1]}", *elements)
        self = super().__new__(cls, header + body)
        self._element_type = cls.ElementType(element_type)
        self._dimensions = dimensions
        return self

    @classmethod
    def of_float16(cls, values: _Iterable[float]) -> "Vector":
        """Construct a FLOAT16 vector from float values.

        Each value is narrowed to IEEE 754 binary32 and then rounded to binary16 with
        round-ties-to-even. Narrowing to binary32 first matters: rounding a Python
        :class:`float` (binary64) straight to binary16 can produce different bits.
        Use :meth:`of_float16_bits` to pass binary16 bit patterns through unchanged.

        :raises ValueError: a value is NaN or infinite, or its magnitude rounds to
            infinity in binary16 (65520 or above).
        """
        float32_values = _array("f", values)
        for value in float32_values:
            if not _math.isfinite(value):
                raise ValueError(cls._NON_FINITE_ERROR)

        dimensions = len(float32_values)
        try:
            packed = _struct.pack(f"<{dimensions}e", *float32_values)
        except OverflowError:
            raise ValueError(cls._NON_FINITE_ERROR) from None

        bit_patterns = _struct.unpack(f"<{dimensions}H", packed)
        return cls.of_float16_bits(bit_patterns)

    @classmethod
    def of_float16_bits(cls, bit_patterns: _Iterable[int]) -> "Vector":
        """Construct a FLOAT16 vector from raw IEEE 754 binary16 bit patterns, for
        exact interchange. No value conversion is performed. Use :meth:`of_float16`
        to construct one from float values."""
        return cls(cls.ElementType.FLOAT16, bit_patterns)

    @classmethod
    def of_int32(cls, data: _Iterable[int]) -> "Vector":
        return cls(cls.ElementType.INT32, data)

    @classmethod
    def of_float32(cls, data: _Iterable[float]) -> "Vector":
        return cls(cls.ElementType.FLOAT32, data)

    @classmethod
    def of_float64(cls, data: _Iterable[float]) -> "Vector":
        return cls(cls.ElementType.FLOAT64, data)

    @property
    def element_type(self) -> ElementType:
        return self._element_type

    @property
    def dimensions(self) -> int:
        return self._dimensions

    @property
    def value(self) -> _array:
        """Decoded element array (typed, per element_type). A FLOAT16 vector's
        elements are its raw binary16 bit patterns; use :meth:`float16_values` to
        read them as floats."""
        fmt = self._STRUCT_FMT[self._element_type]
        return _array(
            fmt[1],
            _struct.unpack_from(f"<{self._dimensions}{fmt[1]}", self,
                                 offset=self._HEADER_SIZE),
        )

    # Alias for readability.
    elements = value

    def float16_values(self) -> list[float]:
        """Read a FLOAT16 vector's elements as floats. Reverse of :meth:`of_float16`.
        Every binary16 value is exactly representable as a :class:`float`.

        :raises ValueError: the vector's element type is not FLOAT16.
        """
        if self._element_type != self.ElementType.FLOAT16:
            raise ValueError(
                f"float16_values() requires a FLOAT16 vector; got {self._element_type.name}")
        values = _struct.unpack_from(f"<{self._dimensions}e", self,
                                     offset=self._HEADER_SIZE)
        return list(values)

    def element_bytes(self) -> bytes:
        """
        Element data only, no 6-byte header. ``as_exp_vector_dist()``'s query-vector
        argument expects the *complete* serialized vector (header + elements);
        this accessor exists for callers who need the headerless form for other
        reasons.
        """
        return bytes(self[self._HEADER_SIZE:])

    @classmethod
    def from_bytes(cls, raw: bytes) -> "Vector":
        """
        Reverse of the wire encode. Validates header size, version, element-type
        code, and dimensions before trusting the payload.
        """
        if len(raw) < cls._HEADER_SIZE:
            raise ValueError("Vector buffer too short: missing 6-byte header")
        version, element_type, dimensions = _struct.unpack_from(
            cls._HEADER_FMT, raw, 0)
        if version != cls.VERSION:
            raise ValueError(
                f"Unsupported Vector version in wire data: {version}")
        fmt = cls._STRUCT_FMT.get(element_type)
        if fmt is None:
            raise ValueError(
                f"Unknown Vector element_type in wire data: {element_type}")
        max_dims = cls._MAX_ELEMENTS_BYTES // _struct.calcsize(fmt)
        if not (1 <= dimensions <= max_dims):
            raise ValueError(
                f"Vector dimensions in wire data must be in [1, {max_dims}] for "
                f"this element type; got {dimensions}"
            )
        expected_len = cls._HEADER_SIZE + dimensions * _struct.calcsize(fmt)
        if len(raw) < expected_len:
            raise ValueError(
                f"Vector buffer too short for declared dimensions: need "
                f"{expected_len}, got {len(raw)}"
            )
        self = bytes.__new__(cls, raw[:expected_len])
        self._element_type = cls.ElementType(element_type)
        self._dimensions = dimensions
        return self

    def __repr__(self) -> str:
        return (f"{self.__class__.__name__}(element_type={self._element_type}, "
                f"dimensions={self._dimensions}, elements={list(self.value)})")

    __str__ = __repr__
