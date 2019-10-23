'''
Helper functions to create bit operation dictionary arguments for
the operate and operate_ordered methods of the aerospike client.
'''
import aerospike

BIN_KEY = "bin"
BYTE_SIZE_KEY = "byte_size"
BYTE_OFFSET_KEY = "byte_offset"
BIT_OFFSET_KEY = "bit_offset"
BIT_SIZE_KEY = "bit_size"
VALUE_BYTE_SIZE_KEY = "value_byte_size"
VALUE_KEY = "value"
COUNT_KEY = "count"
INDEX_KEY = "index"
KEY_KEY = "key"
OFFSET_KEY = "offset"
OP_KEY = "op"
POLICY_KEY = "policy"
RESIZE_FLAGS_KEY = "resize_flags"
SIGN_KEY = "sign"
ACTION_KEY = "action"


def bit_resize(bin_name, byte_size, policy=None, resize_flags=aerospike.BIT_RESIZE_DEFAULT):
    """Creates a bit_resize_operation to be used with operate or operate_ordered.

    Change the size of a bytes bin stored in a record on the Aerospike Server.

    Args:
        bin_name (str): The name of the bin containing the map.
        byte_size (int): The new size of the bytes.
        policy (dict, optional): The bit_policy policy dictionary. See: See :ref:`aerospike_bit_policies`. default: None
        resize_flags (int, optional): Flags modifying the behavior of the resize.
            This should be constructed by bitwise or'ing together any of the values: `aerospike.BIT_RESIZE_DEFAULT`, `aerospike.BIT_RESIZE_FROM_FRONT`
            `aerospike.BIT_RESIZE_GROW_ONLY`, `aerospike.BIT_RESIZE_SHRINK_ONLY` . e.g. `aerospike.BIT_RESIZE_GROW_ONLY | aerospike.BIT_RESIZE_FROM_FRONT`

    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    return {
        OP_KEY: aerospike.OP_BIT_RESIZE,
        BIN_KEY: bin_name,
        POLICY_KEY: policy,
        RESIZE_FLAGS_KEY: resize_flags,
        BYTE_SIZE_KEY: byte_size
    }


def bit_remove(bin_name, byte_offset, byte_size, policy=None):
    """Creates a bit_remove_operation to be used with operate or operate_ordered.

    Remove bytes from bitmap at byte_offset for byte_size.

    Args:
        bin_name (str): The name of the bin containing the map.
        byte_offset (int): Position of bytes to be removed.
        byte_size (int): How many bytes to remove.
        policy (dict, optional): The bit_policy policy dictionary. See: See :ref:`aerospike_bit_policies`. default: None

    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    return {
        OP_KEY: aerospike.OP_BIT_REMOVE,
        BIN_KEY: bin_name,
        POLICY_KEY: policy,
        BYTE_OFFSET_KEY: byte_offset,
        BYTE_SIZE_KEY: byte_size
    }


def bit_set(bin_name, bit_offset, bit_size, value_byte_size, value, policy=None):
    """Creates a bit_set_operation to be used with operate or operate_ordered.

    Set the value on a bitmap at bit_offset for bit_size in a record on the Aerospike Server.

    Args:
        bin_name (str): The name of the bin containing the map.
        bit_offset (int): The offset where the bits will be set.
        bit_size (int): How many bits of value to write.
        value_byte_size (int): Size of value in bytes.
        value (bytes/byte array): The value to be set.
        policy (dict, optional): The bit_policy policy dictionary. See: See :ref:`aerospike_bit_policies`. default: None

    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    return {
        OP_KEY: aerospike.OP_BIT_SET,
        BIN_KEY: bin_name,
        POLICY_KEY: policy,
        BIT_OFFSET_KEY: bit_offset,
        BIT_SIZE_KEY: bit_size,
        VALUE_BYTE_SIZE_KEY: value_byte_size,
        VALUE_KEY: value
    }


def bit_count(bin_name, bit_offset, bit_size):
    """Creates a bit_count_operation to be used with operate or operate_ordered.

    Server returns an integer count of all set bits starting at bit_offset for bit_size bits.

    Args:
        bin_name (str): The name of the bin containing the map.
        bit_offset (int): The offset where the set bits will begin being counted.
        bit_size (int): How many bits will be considered for counting.

    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    return {
        OP_KEY: aerospike.OP_BIT_COUNT,
        BIN_KEY: bin_name,
        BIT_OFFSET_KEY: bit_offset,
        BIT_SIZE_KEY: bit_size
    }


def bit_add(bin_name, bit_offset, bit_size, value, sign, action, policy=None):
    """Creates a bit_add_operation to be used with operate or operate_ordered.

    Creates a bit add operation. Server adds value to the bin at bit_offset for bit_size.
    bit_size must <= 64. If Sign is true value will be treated as a signed number.
    If an underflow or overflow occurs, as_bit_overflow_action is used. Server returns nothing.

    Args:
        bin_name (str): The name of the bin containing the map.
        bit_offset (int): The offset where the bits will be added.
        bit_size (int): How many bits of value to add.
        value (int): The value to be added.
        sign (bool): True: treat value as signed, False: treat value as unsigned.
        action (aerospike.constant): Action taken if an overflow/underflow occurs.
        policy (dict, optional): The bit_policy policy dictionary. See: See :ref:`aerospike_bit_policies`. default: None

    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    return {
        OP_KEY: aerospike.OP_BIT_ADD,
        BIN_KEY: bin_name,
        POLICY_KEY: policy,
        BIT_OFFSET_KEY: bit_offset,
        BIT_SIZE_KEY: bit_size,
        VALUE_KEY: value,
        SIGN_KEY: sign,
        ACTION_KEY: action
    }


def bit_and(bin_name, bit_offset, bit_size, value_byte_size, value, policy=None):
    """Creates a bit_and_operation to be used with operate or operate_ordered.

    Creates a bit and operation. Server performs an and op with value and bitmap in bin
    at bit_offset for bit_size. Server returns nothing.

    Args:
        bin_name (str): The name of the bin containing the map.
        bit_offset (int): The offset where the bits will be modified.
        bit_size (int): How many bits of value to and.
        value_byte_size (int): Length of value in bytes.
        value (bytes/byte array): Bytes to be used in and operation.
        policy (dict, optional): The bit_policy policy dictionary. See: See :ref:`aerospike_bit_policies`. default: None

    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    return {
        OP_KEY: aerospike.OP_BIT_AND,
        BIN_KEY: bin_name,
        POLICY_KEY: policy,
        BIT_OFFSET_KEY: bit_offset,
        BIT_SIZE_KEY: bit_size,
        VALUE_BYTE_SIZE_KEY: value_byte_size,
        VALUE_KEY: value
    }


def bit_get(bin_name, bit_offset, bit_size):
    """Creates a bit_get_operation to be used with operate or operate_ordered.

    Server returns bits from bitmap starting at bit_offset for bit_size.

    Args:
        bin_name (str): The name of the bin containing the map.
        bit_offset (int): The offset where the bits will start being read.
        bit_size (int): How many bits to get.

    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    return {
        OP_KEY: aerospike.OP_BIT_GET,
        BIN_KEY: bin_name,
        BIT_OFFSET_KEY: bit_offset,
        BIT_SIZE_KEY: bit_size
    }


def bit_get_int(bin_name, bit_offset, bit_size, sign):
    """Creates a bit_get_int_operation to be used with operate or operate_ordered.

    Server returns an integer formed from the bits read
    from bitmap starting at bit_offset for bit_size.

    Args:
        bin_name (str): The name of the bin containing the map.
        bit_offset (int): The offset where the bits will start being read.
        bit_size (int): How many bits to get.
        sign (bool): True: Treat read value as signed. False: treat read value as unsigned.

    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    return {
        OP_KEY: aerospike.OP_BIT_GET_INT,
        BIN_KEY: bin_name,
        BIT_OFFSET_KEY: bit_offset,
        BIT_SIZE_KEY: bit_size,
        SIGN_KEY: sign
    }


def bit_insert(bin_name, byte_offset, value_byte_size, value, policy=None):
    """Creates a bit_insert_operation to be used with operate or operate_ordered.

    Server inserts the bytes from value into the bitmap at byte_offset.
    No value is returned.

    Args:
        bin_name (str): The name of the bin containing the map.
        byte_offset (int): The offset where the bytes will be inserted.
        value_byte_size (int): Size of value in bytes.
        value (bytes/byte array) The value to be inserted.
        policy (dict, optional): The bit_policy policy dictionary. See: See :ref:`aerospike_bit_policies`. default: None


    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    return {
        OP_KEY: aerospike.OP_BIT_INSERT,
        BIN_KEY: bin_name,
        BYTE_OFFSET_KEY: byte_offset,
        VALUE_BYTE_SIZE_KEY: value_byte_size,
        VALUE_KEY: value,
        POLICY_KEY: policy
    }


def bit_lscan(bin_name, bit_offset, bit_size, value):
    """Creates a bit_lscan_operation to be used with operate or operate_ordered.

    Server returns an integer representing the bit offset of the first occurence
    of the specified value bit. Starts scanning at bit_offset for bit_size. Returns
    -1 if value not found.

    Args:
        bin_name (str): The name of the bin containing the map.
        bit_offset (int): The offset where the bits will start being scanned.
        bit_size (int): How many bits to scan.
        value (bool): True: look for 1, False: look for 0.

    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    return {
        OP_KEY: aerospike.OP_BIT_LSCAN,
        BIN_KEY: bin_name,
        BIT_OFFSET_KEY: bit_offset,
        BIT_SIZE_KEY: bit_size,
        VALUE_KEY: value
    }


def bit_lshift(bin_name, bit_offset, bit_size, shift, policy=None):
    """Creates a bit_lshift_operation to be used with operate or operate_ordered.

    Server left shifts bitmap starting at bit_offset for bit_size by shift bits.
    No value is returned.

    Args:
        bin_name (str): The name of the bin containing the map.
        bit_offset (int): The offset where the bits will start being shifted.
        bit_size (int): The number of bits that will be shifted by shift places.
        shift (int): How many bits to shift by.
        policy (dict, optional): The bit_policy policy dictionary. See: See :ref:`aerospike_bit_policies`. default: None

    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    return {
        OP_KEY: aerospike.OP_BIT_LSHIFT,
        BIN_KEY: bin_name,
        BIT_OFFSET_KEY: bit_offset,
        BIT_SIZE_KEY: bit_size,
        VALUE_KEY: shift,
        POLICY_KEY: policy
    }


def bit_not(bin_name, bit_offset, bit_size, policy=None):
    """Creates a bit_not_operation to be used with operate or operate_ordered.

    Server negates bitmap starting at bit_offset for bit_size.
    No value is returned.

    Args:
        bin_name (str): The name of the bin containing the map.
        bit_offset (int): The offset where the bits will start being scanned.
        bit_size (int): How many bits to scan.
        policy (dict, optional): The bit_policy policy dictionary. See: See :ref:`aerospike_bit_policies`. default: None

    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    return {
        OP_KEY: aerospike.OP_BIT_NOT,
        BIN_KEY: bin_name,
        BIT_OFFSET_KEY: bit_offset,
        BIT_SIZE_KEY: bit_size,
        POLICY_KEY: policy
    }


def bit_or(bin_name, bit_offset, bit_size, value_byte_size, value, policy=None):
    """Creates a bit_or_operation to be used with operate or operate_ordered.

    Creates a bit or operation. Server performs bitwise or with value and bitmap in bin
    at bit_offset for bit_size. Server returns nothing.

    Args:
        bin_name (str): The name of the bin containing the map.
        bit_offset (int): The offset where the bits will start being compared.
        bit_size (int): How many bits of value to or.
        value_byte_size (int): Length of value in bytes.
        value (bytes/byte array): Value to be used in or operation.
        policy (dict, optional): The bit_policy policy dictionary. See: See :ref:`aerospike_bit_policies`. default: None

    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    return {
        OP_KEY: aerospike.OP_BIT_OR,
        BIN_KEY: bin_name,
        POLICY_KEY: policy,
        BIT_OFFSET_KEY: bit_offset,
        BIT_SIZE_KEY: bit_size,
        VALUE_BYTE_SIZE_KEY: value_byte_size,
        VALUE_KEY: value
    }


def bit_rscan(bin_name, bit_offset, bit_size, value):
    """Creates a bit_rscan_operation to be used with operate or operate_ordered.

    Server returns an integer representing the bit offset of the last occurence
    of the specified value bit. Starts scanning at bit_offset for bit_size. Returns
    -1 if value not found.

    Args:
        bin_name (str): The name of the bin containing the map.
        bit_offset (int): The offset where the bits will start being scanned.
        bit_size (int): How many bits to scan.
        value (bool): True: Look for 1, False: look for 0.

    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    return {
        OP_KEY: aerospike.OP_BIT_RSCAN,
        BIN_KEY: bin_name,
        BIT_OFFSET_KEY: bit_offset,
        BIT_SIZE_KEY: bit_size,
        VALUE_KEY: value
    }


def bit_rshift(bin_name, bit_offset, bit_size, shift, policy=None):
    """Creates a bit_rshift_operation to be used with operate or operate_ordered.

    Server right shifts bitmap starting at bit_offset for bit_size by shift bits.
    No value is returned.

    Args:
        bin_name (str): The name of the bin containing the map.
        bit_offset (int): The offset where the bits will start being shifted.
        bit_size (int): The number of bits that will be shifted by shift places.
        shift (int): How many bits to shift by.
        policy (dict, optional): The bit_policy policy dictionary. See: See :ref:`aerospike_bit_policies`. default: None

    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    return {
        OP_KEY: aerospike.OP_BIT_RSHIFT,
        BIN_KEY: bin_name,
        BIT_OFFSET_KEY: bit_offset,
        BIT_SIZE_KEY: bit_size,
        VALUE_KEY: shift,
        POLICY_KEY: policy
    }


def bit_subtract(bin_name, bit_offset, bit_size, value, sign, action, policy=None):
    """Creates a bit_subtract_operation to be used with operate or operate_ordered.

    Creates a bit add operation. Server subtracts value from the bits at bit_offset for bit_size.
    bit_size must <= 64. If sign is true value will be treated as a signed number.
    If an underflow or overflow occurs, as_bit_overflow_action is used. Server returns nothing.

    Args:
        bin_name (str): The name of the bin containing the map.
        bit_offset (int): The offset where the bits will be subtracted.
        bit_size (int): How many bits of value to subtract.
        value (int): The value to be subtracted.
        sign (bool): True: treat value as signed, False: treat value as unsigned.
        action (aerospike.constant): Action taken if an overflow/underflow occurs.
        policy (dict, optional): The bit_policy policy dictionary. See: See :ref:`aerospike_bit_policies`. default: None

    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    return {
        OP_KEY: aerospike.OP_BIT_SUBTRACT,
        BIN_KEY: bin_name,
        POLICY_KEY: policy,
        BIT_OFFSET_KEY: bit_offset,
        BIT_SIZE_KEY: bit_size,
        VALUE_KEY: value,
        SIGN_KEY: sign,
        ACTION_KEY: action
    }


def bit_xor(bin_name, bit_offset, bit_size, value_byte_size, value, policy=None):
    """Creates a bit_xor_operation to be used with operate or operate_ordered.

    Creates a bit and operation. Server performs bitwise xor with value and bitmap in bin
    at bit_offset for bit_size. Server returns nothing.

    Args:
        bin_name (str): The name of the bin containing the map.
        bit_offset (int): The offset where the bits will start being compared.
        bit_size (int): How many bits of value to xor.
        value_byte_size (int): Length of value in bytes.
        value (bytes/byte array): Value to be used in xor operation.
        policy (dict, optional): The bit_policy policy dictionary. See: See :ref:`aerospike_bit_policies`. default: None

    Returns:
        A dictionary usable in operate or operate_ordered. The format of the dictionary
        should be considered an internal detail, and subject to change.
    """
    return {
        OP_KEY: aerospike.OP_BIT_XOR,
        BIN_KEY: bin_name,
        POLICY_KEY: policy,
        BIT_OFFSET_KEY: bit_offset,
        BIT_SIZE_KEY: bit_size,
        VALUE_BYTE_SIZE_KEY: value_byte_size,
        VALUE_KEY: value
    }