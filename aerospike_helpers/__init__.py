##########################################################################
# Copyright 2013-2021 Aerospike, Inc.
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


import struct as _struct
from array import array as _array


class Vector(bytes):
    """
    Represents a vector bin value used for vector similarity search.

    Wraps the raw wire-format bytes (an 8-byte header followed by a
    contiguous little-endian element array), the same way :class:`HyperLogLog`
    wraps its own opaque byte buffer.

    Construct with one of the ``Vector.of_*()`` factories::

        from aerospike_helpers import Vector

        v = Vector.of_float32([0.1, 0.2, 0.3, 0.4])

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

    class ElementType:
        FLOAT16 = 0x01
        INT32 = 0x02
        FLOAT32 = 0x03  # default
        FLOAT64 = 0x04
        # No BIN/Hamming type - confirmed absent from the real C client.

    _STRUCT_FMT = {
        ElementType.FLOAT16: "<H",  # raw fp16 bit pattern, no value conversion
        ElementType.INT32: "<i",
        ElementType.FLOAT32: "<f",
        ElementType.FLOAT64: "<d",
    }
    _HEADER_FMT = "<BBIH"
    _HEADER_SIZE = 8
    _MAX_ELEMENTS_BYTES = 1 << 18  # AS_VECTOR_VALUE_MAX_ELEMENTS_BYTES

    # Declared here (rather than only assigned in __new__/from_bytes) so that
    # mypy/stubtest can see these instance attributes exist on the class.
    _element_type: int
    _dimensions: int

    def __new__(cls, element_type_or_bytes, elements=None) -> "Vector":
        # Two calling conventions:
        #   Vector(element_type: int, elements: Sequence)  - normal construction
        #   Vector(raw_bytes: bytes)                        - internal decode path,
        #       used by the C extension when deserializing a stored Vector bin
        #       (mirrors create_class_instance_from_module()'s single-positional-arg
        #       calling convention).
        if elements is None and isinstance(element_type_or_bytes,
                                            (bytes, bytearray)):
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
                    raise ValueError(
                        "Vector elements must be finite (NaN/Infinity not allowed)"
                    )

        header = _struct.pack(cls._HEADER_FMT, cls.VERSION, element_type,
                               dimensions, 0)
        body = _struct.pack(f"<{dimensions}{fmt[1]}", *elements)
        self = super().__new__(cls, header + body)
        self._element_type = element_type
        self._dimensions = dimensions
        return self

    @classmethod
    def of_float16(cls, raw_bit_patterns) -> "Vector":
        """Construct a FLOAT16 vector from raw fp16 bit patterns (no value
        conversion is performed; each element must already be the encoded
        16-bit IEEE-754-half bit pattern)."""
        return cls(cls.ElementType.FLOAT16, raw_bit_patterns)

    @classmethod
    def of_int32(cls, data) -> "Vector":
        return cls(cls.ElementType.INT32, data)

    @classmethod
    def of_float32(cls, data) -> "Vector":
        return cls(cls.ElementType.FLOAT32, data)

    @classmethod
    def of_float64(cls, data) -> "Vector":
        return cls(cls.ElementType.FLOAT64, data)

    @property
    def element_type(self) -> int:
        return self._element_type

    @property
    def dimensions(self) -> int:
        return self._dimensions

    @property
    def value(self) -> _array:
        """Decoded element array (typed, per element_type)."""
        fmt = self._STRUCT_FMT[self._element_type]
        return _array(
            fmt[1],
            _struct.unpack_from(f"<{self._dimensions}{fmt[1]}", self,
                                 self._HEADER_SIZE),
        )

    # Alias matching the design doc / cross-client naming.
    elements = value

    def element_bytes(self) -> bytes:
        """
        Element data only, no 8-byte header. ``as_exp_vector_dist()``'s query-vector
        argument expects the *complete* serialized vector (header + elements);
        this accessor exists for callers who need the headerless form for other
        reasons.
        """
        return bytes(self[self._HEADER_SIZE:])

    @classmethod
    def from_bytes(cls, raw: bytes) -> "Vector":
        """
        Reverse of the wire encode. Validates header size, element-type code,
        and dimensions-vs-length before trusting the payload.
        """
        if len(raw) < cls._HEADER_SIZE:
            raise ValueError("Vector buffer too short: missing 8-byte header")
        version, element_type, dimensions, _reserved = _struct.unpack_from(
            cls._HEADER_FMT, raw, 0)
        fmt = cls._STRUCT_FMT.get(element_type)
        if fmt is None:
            raise ValueError(
                f"Unknown Vector element_type in wire data: {element_type}")
        expected_len = cls._HEADER_SIZE + dimensions * _struct.calcsize(fmt)
        if len(raw) < expected_len:
            raise ValueError(
                f"Vector buffer too short for declared dimensions: need "
                f"{expected_len}, got {len(raw)}"
            )
        self = bytes.__new__(cls, raw[:expected_len])
        self._element_type = element_type
        self._dimensions = dimensions
        return self

    def __repr__(self) -> str:
        return (f"{self.__class__.__name__}(element_type={self._element_type}, "
                f"dimensions={self._dimensions}, elements={list(self.value)})")

    __str__ = __repr__
