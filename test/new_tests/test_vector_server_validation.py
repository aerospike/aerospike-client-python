# -*- coding: utf-8 -*-
import math
import struct

import pytest

import aerospike
from aerospike import exception as e
from aerospike_helpers import Vector

# Each value below is built byte by byte and sent through bytes.__new__(Vector, raw), which skips the
# client's checks, so the server's own validation decides whether it is stored. Each case is labelled
# by hand with the first check it fails, in the order the server applies them. The corpus is ported
# from QE-901's VW golden corpus (v2_sdk_vector/vector_wire.py in aerospike-tests-python), without the
# entries for the older 8-byte header.

HEADER_SIZE = 6
VERSION = 1
FLOAT16, INT32, FLOAT32, FLOAT64 = (
    Vector.ElementType.FLOAT16,
    Vector.ElementType.INT32,
    Vector.ElementType.FLOAT32,
    Vector.ElementType.FLOAT64,
)
ELEMENT_TYPES = (FLOAT16, INT32, FLOAT32, FLOAT64)
ELEMENT_SIZE = {FLOAT16: 2, INT32: 4, FLOAT32: 4, FLOAT64: 8}
ELEMENT_FORMAT = {FLOAT16: "e", INT32: "i", FLOAT32: "f", FLOAT64: "d"}
# Element data is capped at 256 KiB, so the dimension limit depends on the element type.
MAX_ELEMENT_BYTES = 1 << 18

OK = "ok"
SHORT = "short"
BAD_VERSION = "version"
BAD_ELEMENT_TYPE = "element_type"
NO_DIMENSIONS = "no_dimensions"
TOO_MANY_DIMENSIONS = "too_many_dimensions"
BAD_SIZE = "size"
NON_FINITE = "non_finite"


def max_dimensions(element_type: Vector.ElementType) -> int:
    return MAX_ELEMENT_BYTES // ELEMENT_SIZE[element_type]


# element_type is an int so that invalid element type codes can be encoded.
def header(element_type: int, dimensions: int, version: int = VERSION) -> bytes:
    return struct.pack("<BBI", version, element_type, dimensions)


def pack_elements(element_type: Vector.ElementType, values: list[float]) -> bytes:
    return struct.pack(f"<{len(values)}{ELEMENT_FORMAT[element_type]}", *values)


def encode(element_type: Vector.ElementType, values: list[float]) -> bytes:
    return header(element_type, len(values)) + pack_elements(element_type, values)


def sample_values(element_type: Vector.ElementType, dimensions: int) -> list[float]:
    if element_type == INT32:
        return [(i * 7) % 2001 - 1000 for i in range(dimensions)]
    return [((i * 7) % 41 - 20) / 2 for i in range(dimensions)]


# Returns pytest.param() values, each of (raw, check).
def build_corpus() -> list:
    corpus = []

    def add(name: str, raw: bytes, check: str) -> None:
        corpus.append(pytest.param(raw, check, id=name))

    for et in ELEMENT_TYPES:
        for dims in (1, 3, 4, 5, 8):
            add(f"valid-{et.name}-dims{dims}", encode(et, sample_values(et, dims)), OK)
        add(f"valid-{et.name}-dims-max", encode(et, sample_values(et, max_dimensions(et))), OK)

    largest = {FLOAT16: 65504.0, FLOAT32: 3.4028234663852886e38, FLOAT64: 1.7976931348623157e308}
    smallest_subnormal = {FLOAT16: 5.960464477539063e-08, FLOAT32: 1.401298464324817e-45, FLOAT64: 5e-324}
    for et in (FLOAT16, FLOAT32, FLOAT64):
        add(f"boundary-{et.name}-largest-finite", encode(et, [largest[et], -largest[et]]), OK)
        add(f"boundary-{et.name}-subnormal", encode(et, [smallest_subnormal[et], -smallest_subnormal[et]]), OK)
        add(f"boundary-{et.name}-negative-zero", encode(et, [-0.0, 0.0]), OK)
    add("boundary-INT32-min-max", encode(INT32, [-(2**31), 2**31 - 1]), OK)

    for et in (FLOAT32, FLOAT64):
        for label, bad in (("nan", math.nan), ("pos-inf", math.inf), ("neg-inf", -math.inf)):
            add(f"nonfinite-{et.name}-{label}", encode(et, [1.0, bad, 2.0]), NON_FINITE)
        add(f"nonfinite-{et.name}-nan-last-element", encode(et, [1.0] * 15 + [math.nan]), NON_FINITE)
    for label, bits in (("nan", 0x7E00), ("pos-inf", 0x7C00), ("neg-inf", 0xFC00)):
        add(f"nonfinite-FLOAT16-raw-{label}", header(FLOAT16, 3) + struct.pack("<3H", 0x3C00, bits, 0x4000),
            NON_FINITE)

    add("short-empty", b"", SHORT)
    add("short-one-below-header", header(FLOAT32, 1)[:HEADER_SIZE - 1], SHORT)
    add("header-only-zero-dims", header(FLOAT32, 0), NO_DIMENSIONS)
    add("zero-dims-with-payload", header(FLOAT32, 0) + pack_elements(FLOAT32, [1.0]), NO_DIMENSIONS)

    for version in (0, 2, 255):
        add(f"version-{version}", header(FLOAT32, 3, version=version) + pack_elements(FLOAT32, [1, 2, 3]),
            BAD_VERSION)
    add("version-checked-before-dims", header(FLOAT32, 0, version=2), BAD_VERSION)

    for et in (0, 5, 255):
        add(f"element-type-{et}", header(et, 3) + bytes(12), BAD_ELEMENT_TYPE)

    for et in ELEMENT_TYPES:
        add(f"dims-max-plus-one-{et.name}", header(et, max_dimensions(et) + 1) + bytes(8), TOO_MANY_DIMENSIONS)
    add("dims-0xffffffff", header(FLOAT32, 0xFFFFFFFF), TOO_MANY_DIMENSIONS)

    add("size-one-element-short", header(FLOAT32, 4) + pack_elements(FLOAT32, [1, 2, 3]), BAD_SIZE)
    add("size-one-element-extra", header(FLOAT32, 2) + pack_elements(FLOAT32, [1, 2, 3]), BAD_SIZE)
    add("size-one-byte-short", encode(FLOAT32, [1, 2, 3])[:-1], BAD_SIZE)
    add("size-one-byte-extra", encode(FLOAT32, [1, 2, 3]) + b"\x00", BAD_SIZE)
    add("size-float32-data-labelled-float64", header(FLOAT64, 3) + pack_elements(FLOAT32, [1, 2, 3]), BAD_SIZE)
    # The older 8-byte header, as a client that predates the 6-byte format sends it.
    add("size-8-byte-header-layout", header(FLOAT32, 3) + b"\x00\x00" + pack_elements(FLOAT32, [1, 2, 3]),
        BAD_SIZE)

    return corpus


def expected_message(raw: bytes, check: str) -> str:
    """The error detail the server sets for each check, from particle_vector.c."""
    if check == SHORT:
        return f"vector: value size {len(raw)} smaller than {HEADER_SIZE}-byte header"
    version, element_type, dimensions = struct.unpack_from("<BBI", raw)
    if check == BAD_VERSION:
        return f"vector: unsupported format version {version} (expected {VERSION})"
    if check == BAD_ELEMENT_TYPE:
        return f"vector: invalid element type {element_type}"
    if check == NO_DIMENSIONS:
        return "vector: must have at least 1 dimension"
    element_type = Vector.ElementType(element_type)
    if check == TOO_MANY_DIMENSIONS:
        return (
            f"vector: {dimensions} dimensions exceed max {max_dimensions(element_type)} "
            f"for element type {int(element_type)}"
        )
    element_size = ELEMENT_SIZE[element_type]
    if check == BAD_SIZE:
        return (
            f"vector: value size {len(raw)} does not match {HEADER_SIZE + dimensions * element_size} "
            f"({dimensions} dimensions of {element_size} bytes, little-endian)"
        )
    assert check == NON_FINITE
    return "vector: elements must be finite (no NaN or infinity)"


ERROR_DETAIL_POLICY = {"error_detail_verbosity": aerospike.ERROR_DETAIL_MESSAGE}


class TestVectorServerValidation:
    @pytest.fixture(autouse=True)
    def setup(self, as_connection, skip_if_server_older_than_8_2_1):
        self.key = ("test", "test_vector_validation", "corpus")
        self.original = Vector.of_float32([9.0, 8.0])
        as_connection.put(self.key, {"embedding": self.original})
        yield
        as_connection.remove(self.key)

    @pytest.mark.parametrize("raw, check", build_corpus())
    def test_server_validates_vector(self, raw, check):
        vector = bytes.__new__(Vector, raw)
        if check == OK:
            self.as_connection.put(self.key, {"embedding": vector}, policy=ERROR_DETAIL_POLICY)
            _, _, bins = self.as_connection.get(self.key)
            assert isinstance(bins["embedding"], Vector)
            assert bytes(bins["embedding"]) == raw
            return

        with pytest.raises(e.InvalidRequest) as excinfo:
            self.as_connection.put(self.key, {"embedding": vector}, policy=ERROR_DETAIL_POLICY)
        # The C client prefixes the server's detail with the node address.
        assert excinfo.value.msg.endswith(f" {expected_message(raw, check)}")

        _, _, bins = self.as_connection.get(self.key)
        assert bytes(bins["embedding"]) == bytes(self.original)
