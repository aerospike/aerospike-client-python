'''
Bitwise expressions contain bit read and modify expressions.

Example::

    import aerospike_helpers.expressions as exp
    # Let blob bin "c" == bytearray([3] * 5).
    # Count set bits starting at 3rd byte in bin "c" to get count of 6.
    expr = exp.BitCount(16, 8 * 3, exp.BlobBin("c")).compile()
'''

from itertools import chain
from typing import List, Optional, Tuple, Union, Dict, Any
import aerospike
from aerospike_helpers import cdt_ctx
from aerospike_helpers.expressions.base import *
from aerospike_helpers.expressions.base import _GenericExpr

########################
# Bit Modify Expressions
########################

TypeBitValue = Union[bytes, bytearray]


class BitResize(BaseExpr):
    """Create an expression that performs a bit_resize operation."""
    op = aerospike.OP_BIT_RESIZE

    def __init__(self, policy: TypePolicy, byte_size: int, flags: int, bin: TypeBinName):
        """ Create an expression that performs a bit_resize operation.

            Args:
                policy (TypePolicy): An optional aerospike bit policy.
                byte_size (int): Number of bytes the resulting blob should occupy.
                flags (int): One or a combination of bit resize flags.
                bin (TypeBinName): Blob bin name or blob value expression.

            :return: Blob value expression of resized blob bin.
        
            Example::

                # Blob bin "c" == bytearray([1] * 5).
                # Resize blob bin "c" from the front so that the returned value is bytearray([0] * 5 + [1] * 5).
                expr = BitResize(None, 10, aerospike.BIT_RESIZE_FROM_FRONT, BlobBin("c")).compile()
        """        
        self.children = (
            byte_size,
            _GenericExpr(ExprOp._AS_EXP_BIT_FLAGS, 0, {VALUE_KEY: policy['bit_write_flags']} if policy is not None and 'bit_write_flags' in policy else {VALUE_KEY: 0}),
            _GenericExpr(ExprOp._AS_EXP_BIT_FLAGS, 0, {VALUE_KEY: flags} if flags is not None else {VALUE_KEY: 0}),
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )


class BitInsert(BaseExpr):
    """Create an expression that performs a bit_insert operation."""
    op = aerospike.OP_BIT_INSERT

    def __init__(self, policy: TypePolicy, byte_offset: int, value: TypeBitValue, bin: TypeBinName):
        """ Create an expression that performs a bit_insert operation.

            Args:
                policy (TypePolicy): An optional aerospike bit policy.
                byte_offset (int): Integer byte index of where to insert the value.
                value (TypeBitValue): A bytes value or blob value expression to insert.
                bin (TypeBinName): Blob bin name or blob value expression.

            :return: Resulting blob containing the inserted bytes.
        
            Example::

                # Let blob bin "c" == bytearray([1] * 5).
                # Insert 3 so that returned value is bytearray([1, 3, 1, 1, 1, 1]).
                expr = BitInsert(None, 1, bytearray([3]), BlobBin("c")).compile()
        """        
        self.children = (
            byte_offset,
            value,
            _GenericExpr(ExprOp._AS_EXP_BIT_FLAGS, 0, {VALUE_KEY: policy['bit_write_flags']} if policy is not None and 'bit_write_flags' in policy else {VALUE_KEY: 0}),
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )


class BitRemove(BaseExpr):
    """Create an expression that performs a bit_remove operation."""
    op = aerospike.OP_BIT_REMOVE

    def __init__(self, policy: TypePolicy, byte_offset: int, byte_size: int, bin: TypeBinName):
        """ Create an expression that performs a bit_remove operation.

            Args:
                policy (TypePolicy): An optional aerospike bit policy.
                byte_offset (int): Byte index of where to start removing from.
                byte_size (int): Number of bytes to remove.
                bin (TypeBinName): Blob bin name or blob value expression.

            :return: Resulting blob containing the remaining bytes.
        
            Example::

                # Let blob bin "c" == bytearray([1] * 5).
                # Remove 1 element so that the returned value is bytearray([1] * 4).
                expr = BitRemove(None, 1, 1, BlobBin("c")).compile()
        """        
        self.children = (
            byte_offset,
            byte_size,
            _GenericExpr(ExprOp._AS_EXP_BIT_FLAGS, 0, {VALUE_KEY: policy['bit_write_flags']} if policy is not None and 'bit_write_flags' in policy else {VALUE_KEY: 0}),
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )


class BitSet(BaseExpr):
    """Create an expression that performs a bit_set operation."""
    op = aerospike.OP_BIT_SET

    def __init__(self, policy: TypePolicy, bit_offset: int, bit_size: int, value: TypeBitValue, bin: TypeBinName):
        """ Create an expression that performs a bit_set operation.

            Args:
                policy (TypePolicy): An optional aerospike bit policy.
                bit_offset (int): Bit index of where to start overwriting.
                bit_size (int): Number of bits to overwrite.
                value (TypeBitValue): Bytes value or blob expression containing bytes to write.
                bin (TypeBinName): Blob bin name or blob expression.

            :return: Resulting blob expression with the bits overwritten.
        
            Example::

                # Let blob bin "c" == bytearray([0] * 5).
                # Set bit at offset 7 with size 1 bits to 1 to make the returned value bytearray([1, 0, 0, 0, 0]).
                expr = BitSet(None, 7, 1, bytearray([255]), BlobBin("c")).compile()
        """        
        self.children = (
            bit_offset,
            bit_size,
            value,
            _GenericExpr(ExprOp._AS_EXP_BIT_FLAGS, 0, {VALUE_KEY: policy['bit_write_flags']} if policy is not None and 'bit_write_flags' in policy else {VALUE_KEY: 0}),
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )


class BitOr(BaseExpr):
    """Create an expression that performs a bit_or operation."""
    op = aerospike.OP_BIT_OR

    def __init__(self, policy: TypePolicy, bit_offset: int, bit_size: int, value: TypeBitValue, bin: TypeBinName):
        """ Create an expression that performs a bit_or operation.

            Args:
                policy (TypePolicy): An optional aerospike bit policy.
                bit_offset (int): Bit index of where to start operation.
                bit_size (int): Number of bits to be operated on.
                value (TypeBitValue): Bytes value or blob expression containing bytes to use in operation.
                bin (TypeBinName): Blob bin name or blob expression.

            :return: Resulting blob with the bits operated on.
        
            Example::

                # Let blob bin "c" == bytearray([1] * 5).
                # bitwise Or `8` with the first byte of blob bin c so that the returned value is bytearray([9, 1, 1, 1, 1]).
                expr = BitOr(None, 0, 8, bytearray([8]), BlobBin("c")).compile()
        """        
        self.children = (
            bit_offset,
            bit_size,
            value,
            _GenericExpr(ExprOp._AS_EXP_BIT_FLAGS, 0, {VALUE_KEY: policy['bit_write_flags']} if policy is not None and 'bit_write_flags' in policy else {VALUE_KEY: 0}),
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )


class BitXor(BaseExpr):
    """Create an expression that performs a bit_xor operation."""
    op = aerospike.OP_BIT_XOR

    def __init__(self, policy: TypePolicy, bit_offset: int, bit_size: int, value: TypeBitValue, bin: TypeBinName):
        """ Create an expression that performs a bit_xor operation.

            Args:
                policy (TypePolicy): An optional aerospike bit policy.
                bit_offset (int): Bit index of where to start operation.
                bit_size (int): Number of bits to be operated on.
                value (TypeBitValue): Bytes value or blob expression containing bytes to use in operation.
                bin (TypeBinName): Blob bin name or blob expression.

            :return: Resulting blob with the bits operated on.
        
            Example::

                # Let blob bin "c" == bytearray([1] * 5).
                # bitwise Xor `1` with the first byte of blob bin c so that the returned value is bytearray([0, 1, 1, 1, 1]).
                expr = BitXor(None, 0, 8, bytearray([1]), BlobBin("c")).compile()
        """        
        self.children = (
            bit_offset,
            bit_size,
            value,
            _GenericExpr(ExprOp._AS_EXP_BIT_FLAGS, 0, {VALUE_KEY: policy['bit_write_flags']} if policy is not None and 'bit_write_flags' in policy else {VALUE_KEY: 0}),
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )


class BitAnd(BaseExpr):
    """Create an expression that performs a bit_and operation."""
    op = aerospike.OP_BIT_AND

    def __init__(self, policy: TypePolicy, bit_offset: int, bit_size: int, value: TypeBitValue, bin: TypeBinName):
        """ Create an expression that performs a bit_and operation.

            Args:
                policy (TypePolicy): An optional aerospike bit policy.
                bit_offset (int): Bit index of where to start operation.
                bit_size (int): Number of bits to be operated on.
                value (TypeBitValue): Bytes value or blob expression containing bytes to use in operation.
                bin (TypeBinName): Blob bin name or blob expression.

            :return: Resulting blob with the bits operated on.
        
            Example::

                # Let blob bin "c" == bytearray([1] * 5).
                # bitwise and `0` with the first byte of blob bin c so that the returned value is bytearray([0, 5, 5, 5, 5]).
                expr = BitAnd(None, 0, 8, bytearray([0]), BlobBin("c")).compile()
        """        
        self.children = (
            bit_offset,
            bit_size,
            value,
            _GenericExpr(ExprOp._AS_EXP_BIT_FLAGS, 0, {VALUE_KEY: policy['bit_write_flags']} if policy is not None and 'bit_write_flags' in policy else {VALUE_KEY: 0}),
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )


class BitNot(BaseExpr):
    """Create an expression that performs a bit_not operation."""
    op = aerospike.OP_BIT_NOT

    def __init__(self, policy: TypePolicy, bit_offset: int, bit_size: int, bin: TypeBinName):
        """ Create an expression that performs a bit_not operation.

            Args:
                policy (TypePolicy): An optional aerospike bit policy.
                bit_offset (int): Bit index of where to start operation.
                bit_size (int): Number of bits to be operated on.
                bin (TypeBinName): Blob bin name or blob expression.

            :return: Resulting blob with the bits operated on.
        
            Example::

                # Let blob bin "c" == bytearray([255] * 5).
                # bitwise, not, all of "c" to get bytearray([254] * 5).
                expr = BitNot(None, 0, 40, BlobBin("c")).compile()
        """        
        self.children = (
            bit_offset,
            bit_size,
            _GenericExpr(ExprOp._AS_EXP_BIT_FLAGS, 0, {VALUE_KEY: policy['bit_write_flags']} if policy is not None and 'bit_write_flags' in policy else {VALUE_KEY: 0}),
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )


class BitLeftShift(BaseExpr):
    """Create an expression that performs a bit_lshift operation."""
    op = aerospike.OP_BIT_LSHIFT

    def __init__(self, policy: TypePolicy, bit_offset: int, bit_size: int, shift: int, bin: TypeBinName):
        """ Create an expression that performs a bit_lshift operation.

            Args:
                policy (TypePolicy): An optional aerospike bit policy.
                bit_offset (int): Bit index of where to start operation.
                bit_size (int): Number of bits to be operated on.
                shift (int): Number of bits to shift by.
                bin (TypeBinName): Blob bin name or blob expression.

            :return: Resulting blob with the bits operated on.
        
            Example::

                # Let blob bin "c" == bytearray([1] * 5).
                # Bit left shift the first byte of bin "c" to get bytearray([8, 1, 1, 1, 1]).
                expr = BitLeftShift(None, 0, 8, 3, BlobBin("c")).compile()
        """        
        self.children = (
            bit_offset,
            bit_size,
            shift,
            _GenericExpr(ExprOp._AS_EXP_BIT_FLAGS, 0, {VALUE_KEY: policy['bit_write_flags']} if policy is not None and 'bit_write_flags' in policy else {VALUE_KEY: 0}),
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )


class BitRightShift(BaseExpr):
    """Create an expression that performs a bit_rshift operation."""
    op = aerospike.OP_BIT_RSHIFT

    def __init__(self, policy: TypePolicy, bit_offset: int, bit_size: int, shift: int, bin: TypeBinName):
        """ Create an expression that performs a bit_rshift operation.

            Args:
                policy (TypePolicy): An optional aerospike bit policy.
                bit_offset (int): Bit index of where to start operation.
                bit_size (int): Number of bits to be operated on.
                shift (int): Number of bits to shift by.
                bin (TypeBinName): Blob bin name or blob expression.

            :return: Resulting blob with the bits operated on.
        
            Example::

                # Let blob bin "c" == bytearray([8] * 5).
                # Bit left shift the first byte of bin "c" to get bytearray([4, 8, 8, 8, 8]).
                expr = BitRightShift(None, 0, 8, 1, BlobBin("c")).compile()
        """        
        self.children = (
            bit_offset,
            bit_size,
            shift,
            _GenericExpr(ExprOp._AS_EXP_BIT_FLAGS, 0, {VALUE_KEY: policy['bit_write_flags']} if policy is not None and 'bit_write_flags' in policy else {VALUE_KEY: 0}),
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )


class BitAdd(BaseExpr):
    """Create an expression that performs a bit_add operation."""
    op = aerospike.OP_BIT_ADD

    def __init__(self, policy: TypePolicy, bit_offset: int, bit_size: int, value: int, action: int, bin: TypeBinName):
        """ Create an expression that performs a bit_add operation.
            Note: integers are stored big-endian.

            Args:
                policy (TypePolicy): An optional aerospike bit policy.
                bit_offset (int): Bit index of where to start operation.
                bit_size (int): Number of bits to be operated on.
                value (int): Integer value or expression for value to add.
                action (int): An aerospike bit overflow action.
                bin (TypeBinName): Blob bin name or blob expression.

            :return: resulting blob with the bits operated on.
        
            Example::

                # Let blob bin "c" == bytearray([1] * 5).
                # Bit add the second byte of bin "c" to get bytearray([1, 2, 1, 1, 1])
                expr = BitAdd(None, 8, 8, 1, aerospike.BIT_OVERFLOW_FAIL).compile()
        """        
        self.children = (
            bit_offset,
            bit_size,
            value,
            _GenericExpr(ExprOp._AS_EXP_BIT_FLAGS, 0, {VALUE_KEY: policy['bit_write_flags']} if policy is not None and 'bit_write_flags' in policy else {VALUE_KEY: 0}),
            _GenericExpr(ExprOp._AS_EXP_BIT_FLAGS, 0, {VALUE_KEY: action} if action is not None else {VALUE_KEY: 0}),
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )


class BitSubtract(BaseExpr):
    """ Create an expression that performs a bit_subtract operation.
        Note: integers are stored big-endian.
    """
    op = aerospike.OP_BIT_SUBTRACT

    def __init__(self, policy: TypePolicy, bit_offset: int, bit_size: int, value: int, action: int, bin: TypeBinName):
        """ Create an expression that performs a bit_subtract operation.
            Note: integers are stored big-endian.

            Args:
                policy (TypePolicy): An optional aerospike bit policy.
                bit_offset (int): Bit index of where to start operation.
                bit_size (int): Number of bits to be operated on.
                value (int): Integer value or expression for value to add.
                action (int): An aerospike bit overflow action.
                bin (TypeBinName): Blob bin name or blob expression.

            :return: resulting blob with the bits operated on.
        
            Example::

                # Let blob bin "c" == bytearray([1] * 5).
                # Bit subtract the second byte of bin "c" to get bytearray([1, 0, 1, 1, 1])
                expr = BitSubtract(None, 8, 8, 1, aerospike.BIT_OVERFLOW_FAIL).compile()
        """        
        self.children = (
            bit_offset,
            bit_size,
            value,
            _GenericExpr(ExprOp._AS_EXP_BIT_FLAGS, 0, {VALUE_KEY: policy['bit_write_flags']} if policy is not None and 'bit_write_flags' in policy else {VALUE_KEY: 0}),
            _GenericExpr(ExprOp._AS_EXP_BIT_FLAGS, 0, {VALUE_KEY: action} if action is not None else {VALUE_KEY: 0}),
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )


class BitSetInt(BaseExpr):
    """ Create an expression that performs a bit_set_int operation.
        Note: integers are stored big-endian.
    """
    op = aerospike.OP_BIT_SET_INT

    def __init__(self, policy: TypePolicy, bit_offset: int, bit_size: int, value: int, bin: TypeBinName):
        """ Create an expression that performs a bit_set_int operation.
            Note: integers are stored big-endian.

            Args:
                policy (TypePolicy): An optional aerospike bit policy.
                bit_offset (int): Bit index of where to start writing.
                bit_size (int): Number of bits to overwrite.
                value (int): Integer value or integer expression containing value to write.
                bin (TypeBinName): Blob bin name or blob expression.

            :return: Resulting blob expression with the bits overwritten.
        
            Example::

                # Let blob bin "c" == bytearray([0] * 5).
                # Set bit at offset 7 with size 1 bytes to 1 to make the returned value bytearray([1, 0, 0, 0, 0]).
                expr = BitSetInt(None, 7, 1, 1, BlobBin("c")).compile()
        """        
        self.children = (
            bit_offset,
            bit_size,
            value,
            _GenericExpr(ExprOp._AS_EXP_BIT_FLAGS, 0, {VALUE_KEY: policy['bit_write_flags']} if policy is not None and 'bit_write_flags' in policy else {VALUE_KEY: 0}),
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )


######################
# Bit Read Expressions
######################


class BitGet(BaseExpr):
    """Create an expression that performs a bit_get operation."""
    op = aerospike.OP_BIT_GET

    def __init__(self, bit_offset: int, bit_size: int, bin: TypeBinName):
        """ Create an expression that performs a bit_get operation.

            Args:
                bit_offset (int): Bit index of where to start reading.
                bit_size (int): Number of bits to get.
                bin (TypeBinName): Blob bin name or blob expression.

            :return: Blob, bit_size bits rounded up to the nearest byte size.
        
            Example::

                # Let blob bin "c" == bytearray([1, 2, 3, 4, 5).
                # Get 2 from bin "c".
                expr = BitGet(8, 8, BlobBin("c")).compile()
        """        
        self.children = (
            bit_offset,
            bit_size,
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )


class BitCount(BaseExpr):
    """Create an expression that performs a bit_count operation."""
    op = aerospike.OP_BIT_COUNT

    def __init__(self, bit_offset: int, bit_size: int, bin: TypeBinName):
        """ Create an expression that performs a bit_count operation.

            Args:
                bit_offset (int): Bit index of where to start reading.
                bit_size (int): Number of bits to count.
                bin (TypeBinName): Blob bin name or blob expression.

            :return: Blob, bit_size bits rounded up to the nearest byte size.
        
            Example::

                # Let blob bin "c" == bytearray([3] * 5).
                # Count set bits starting at 3rd byte in bin "c" to get count of 6.
                expr = BitCount(16, 8 * 3, BlobBin("c")).compile()
        """        
        self.children = (
            bit_offset,
            bit_size,
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )


class BitLeftScan(BaseExpr):
    """Create an expression that performs a bit_lscan operation."""
    op = aerospike.OP_BIT_LSCAN

    def __init__(self, bit_offset: int, bit_size: int, value: Union[ExpTrue, ExpFalse], bin: TypeBinName):
        """ Create an expression that performs a bit_lscan operation.

            Args:
                bit_offset (int): Bit index of where to start reading.
                bit_size (int): Number of bits to read.
                value Union[ExpTrue, ExpFalse]: Bit value to check for, ExpTrue for 1 or ExpFalse for 0.
                bin (TypeBinName): Blob bin name or blob expression.

            :return: Index of the left most bit starting from bit_offset set to value. Returns -1 if not found.
        
            Example::

                # Let blob bin "c" == bytearray([3] * 5).
                # Scan the first byte of bin "c" for the first bit set to 1. (should get 6)
                expr = BitLeftScan(0, 8, True, BlobBin("c")).compile()
        """        
        self.children = (
            bit_offset,
            bit_size,
            value,
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )


class BitRightScan(BaseExpr):
    """Create an expression that performs a bit_rscan operation."""
    op = aerospike.OP_BIT_RSCAN

    def __init__(self, bit_offset: int, bit_size: int, value: Union[ExpTrue, ExpFalse], bin: TypeBinName):
        """ Create an expression that performs a bit_rscan operation.

            Args:
                bit_offset (int): Bit index of where to start reading.
                bit_size (int): Number of bits to read.
                value Union[ExpTrue, ExpFalse]: Bit value to check for, ExpTrue for 1 or ExpFalse for 0.
                bin (TypeBinName): Blob bin name or blob expression.

            :return: Index of the right most bit starting from bit_offset set to value. Returns -1 if not found.
        
            Example::

                # Let blob bin "c" == bytearray([3] * 5).
                # Scan the first byte of bin "c" for the right most bit set to 1. (should get 7)
                expr = BitRightScan(0, 8, True, BlobBin("c")).compile()
        """        
        self.children = (
            bit_offset,
            bit_size,
            value,
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )


class BitGetInt(BaseExpr):
    """Create an expression that performs a bit_get_int operation."""
    op = aerospike.OP_BIT_GET_INT

    def __init__(self, bit_offset: int, bit_size: int, sign: Union[ExpTrue, ExpFalse], bin: TypeBinName):
        """ Create an expression that performs a bit_get_int operation.

            Args:
                bit_offset (int): Bit index of where to start reading.
                bit_size (int): Number of bits to get.
                sign Union[ExpTrue, ExpFalse]: ExpTrue for signed, ExpFalse for unsigned.
                bin (TypeBinName): Blob bin name or blob expression.

            :return: Integer expression.
        
            Example::

                # Let blob bin "c" == bytearray([1, 2, 3, 4, 5).
                # Get 2 as an integer from bin "c".
                expr = BitGetInt(8, 8, True, BlobBin("c")).compile()
        """        
        self.children = (
            bit_offset,
            bit_size,
            0 if not sign or isinstance(sign, ExpFalse) else 1,
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )