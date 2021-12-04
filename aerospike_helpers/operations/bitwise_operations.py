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
'''
Helper functions to create bit operation dictionary arguments for
the :meth:`aerospike.operate` and :meth:`aerospike.operate_ordered` methods of the aerospike client.

    .. note:: Bitwise operations require server version >= 4.6.0

Bit offsets are oriented left to right. Negative offsets are supported and start backwards from the end of the target bitmap.

Offset examples:
    * 0: leftmost bit in the map
    * 4: fifth bit in the map
    * -1: rightmost bit in the map
    * -4: 3 bits from rightmost

Example::

    import aerospike
    from aerospike import exception as ex
    from aerospike_helpers.operations import bitwise_operations
    import sys

    # Configure the client.
    config = {"hosts": [("127.0.0.1", 3000)]}

    # Create a client and connect it to the cluster.
    try:
        client = aerospike.client(config).connect()
    except ex.ClientError as e:
        print("Error: {0} [{1}]".format(e.msg, e.code))
        sys.exit(1)

    key = ("test", "demo", "foo")
    five_ones_bin_name = "bitwise1"
    five_one_blob = bytearray([1] * 5)

    # Write the record.
    try:
        client.put(key, {five_ones_bin_name: five_one_blob})
    except ex.RecordError as e:
        print("Error: {0} [{1}]".format(e.msg, e.code))

    # EXAMPLE 1: resize the five_ones bin to a bytesize of 10.
    try:
        ops = [bitwise_operations.bit_resize(five_ones_bin_name, 10)]

        _, _, bins = client.get(key)
        _, _, _ = client.operate(key, ops)
        _, _, newbins = client.get(key)
        print("EXAMPLE 1: before resize: ", bins)
        print("EXAMPLE 1: is now: ", newbins)
    except ex.ClientError as e:
        print("Error: {0} [{1}]".format(e.msg, e.code))
        sys.exit(1)

    # EXAMPLE 2: shrink the five_ones bin to a bytesize of 5 from the front.
    try:
        ops = [
            bitwise_operations.bit_resize(
                five_ones_bin_name, 5, resize_flags=aerospike.BIT_RESIZE_FROM_FRONT
            )
        ]

        _, _, bins = client.get(key)
        _, _, _ = client.operate(key, ops)
        _, _, newbins = client.get(key)
        print("EXAMPLE 2: before resize: ", bins)
        print("EXAMPLE 2: is now: ", newbins)
    except ex.ClientError as e:
        print("Error: {0} [{1}]".format(e.msg, e.code))
        sys.exit(1)

    # Cleanup and close the connection to the Aerospike cluster.
    client.remove(key)
    client.close()

    """
    EXPECTED OUTPUT:
    EXAMPLE 1: before resize:  {'bitwise1': bytearray(b'\\x01\\x01\\x01\\x01\\x01')}
    EXAMPLE 1: is now:  {'bitwise1': bytearray(b'\\x01\\x01\\x01\\x01\\x01\\x00\\x00\\x00\\x00\\x00')}
    EXAMPLE 2: before resize:  {'bitwise1': bytearray(b'\\x01\\x01\\x01\\x01\\x01\\x00\\x00\\x00\\x00\\x00')}
    EXAMPLE 2: is now:  {'bitwise1': bytearray(b'\\x00\\x00\\x00\\x00\\x00')}
    """

Example::

    import aerospike
    from aerospike import exception as e
    from aerospike_helpers.operations import bitwise_operations

    config = {'hosts': [('127.0.0.1', 3000)]}
    try:
        client = aerospike.client(config).connect()
    except e.ClientError as e:
        print("Error: {0} [{1}]".format(e.msg, e.code))
        sys.exit(2)

    key = ('test', 'demo', 'bit_example')
    five_one_blob = bytearray([1] * 5)
    five_one_bin = 'bitwise1'

    try:
        if client.exists(key):
            client.remove(key)
        bit_policy = {
            'map_write_mode': aerospike.BIT_WRITE_DEFAULT,
        }
        client.put(
            key,
            {
                five_one_bin: five_one_blob
            }
        )

        # Example 1: read bits
        ops = [
            bitwise_operations.bit_get(five_one_bin, 0, 40)
        ]
        print('=====EXAMPLE1=====')
        _, _, results = client.operate(key, ops)
        print(results)

        # Example 2: modify bits using the 'or' op, then read bits
        ops = [
            bitwise_operations.bit_or(five_one_bin, 0, 8, 1, bytearray([255]), bit_policy),
            bitwise_operations.bit_get(five_one_bin, 0, 40)
        ]
        print('=====EXAMPLE2=====')
        _, _, results = client.operate(key, ops)
        print(results)

        # Example 3: modify bits using the 'remove' op, then read bits'
        ops = [
            bitwise_operations.bit_remove(five_one_bin, 0, 2, bit_policy),
            bitwise_operations.bit_get(five_one_bin, 0, 24)
        ]
        print('=====EXAMPLE3=====')
        _, _, results = client.operate(key, ops)
        print(results)

    except e.AerospikeError as e:
        print("Error: {0} [{1}]".format(e.msg, e.code))

    client.close()

    """
    EXPECTED OUTPUT:
    =====EXAMPLE1=====
    {'bitwise1': bytearray(b'\\x01\\x01\\x01\\x01\\x01')}
    =====EXAMPLE2=====
    {'bitwise1': bytearray(b'\\xff\\x01\\x01\\x01\\x01')}
    =====EXAMPLE3=====
    {'bitwise1': bytearray(b'\\x01\\x01\\x01')}
    """

.. seealso:: `Bits (Data Types) <https://www.aerospike.com/docs/guide/bitwise.html>`_.
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


def bit_resize(bin_name: str, byte_size, policy=None, resize_flags: int=0):
    """Creates a bit_resize_operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`.

    Change the size of a bytes bin stored in a record on the Aerospike Server.

    Args:
        bin_name (str): The name of the bin containing the map.
        byte_size (int): The new size of the bytes.
        policy (dict): The :ref:`bit_policy <aerospike_bit_policies>` dictionary. default: None.
        resize_flags (int): :ref:`aerospike_bitwise_resize_flag` modifying the resize behavior (default ``aerospike.BIT_RESIZE_DEFAULT``), such as ``aerospike.BIT_RESIZE_GROW_ONLY | aerospike.BIT_RESIZE_FROM_FRONT``.

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


def bit_remove(bin_name: str, byte_offset, byte_size, policy=None):
    """Creates a bit_remove_operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`.

    Remove bytes from bitmap at byte_offset for byte_size.

    Args:
        bin_name (str): The name of the bin containing the map.
        byte_offset (int): Position of bytes to be removed.
        byte_size (int): How many bytes to remove.
        policy (dict): The :ref:`bit_policy <aerospike_bit_policies>` dictionary. default: None.

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


def bit_set(bin_name: str, bit_offset, bit_size, value_byte_size, value, policy=None):
    """Creates a bit_set_operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`.

    Set the value on a bitmap at bit_offset for bit_size in a record on the Aerospike Server.

    Args:
        bin_name (str): The name of the bin containing the map.
        bit_offset (int): The offset where the bits will be set.
        bit_size (int): How many bits of value to write.
        value_byte_size (int): Size of value in bytes.
        value (bytes, bytearray): The value to be set.
        policy (dict): The :ref:`bit_policy <aerospike_bit_policies>` dictionary. default: None.

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


def bit_count(bin_name: str, bit_offset, bit_size):
    """Creates a bit_count_operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`.

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


def bit_add(bin_name: str, bit_offset, bit_size, value, sign, action, policy=None):
    """Creates a bit_add_operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`.

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
        policy (dict): The :ref:`bit_policy <aerospike_bit_policies>` dictionary. default: None.

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


def bit_and(bin_name: str, bit_offset, bit_size, value_byte_size, value, policy=None):
    """Creates a bit_and_operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`.

    Creates a bit and operation. Server performs an and op with value and bitmap in bin
    at bit_offset for bit_size. Server returns nothing.

    Args:
        bin_name (str): The name of the bin containing the map.
        bit_offset (int): The offset where the bits will be modified.
        bit_size (int): How many bits of value to and.
        value_byte_size (int): Length of value in bytes.
        value (bytes, bytearray): Bytes to be used in and operation.
        policy (dict): The :ref:`bit_policy <aerospike_bit_policies>` dictionary. default: None.

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


def bit_get(bin_name: str, bit_offset, bit_size):
    """Creates a bit_get_operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`.

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


def bit_get_int(bin_name: str, bit_offset, bit_size, sign):
    """Creates a bit_get_int_operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`.

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


def bit_insert(bin_name: str, byte_offset, value_byte_size, value, policy=None):
    """Creates a bit_insert_operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`.

    Server inserts the bytes from value into the bitmap at byte_offset.
    No value is returned.

    Args:
        bin_name (str): The name of the bin containing the map.
        byte_offset (int): The offset where the bytes will be inserted.
        value_byte_size (int): Size of value in bytes.
        value (bytes, bytearray): The value to be inserted.
        policy (dict): The :ref:`bit_policy <aerospike_bit_policies>` dictionary. default: None.


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


def bit_lscan(bin_name: str, bit_offset, bit_size, value):
    """Creates a bit_lscan_operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`.

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


def bit_lshift(bin_name: str, bit_offset, bit_size, shift, policy=None):
    """Creates a bit_lshift_operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`.

    Server left shifts bitmap starting at bit_offset for bit_size by shift bits.
    No value is returned.

    Args:
        bin_name (str): The name of the bin containing the map.
        bit_offset (int): The offset where the bits will start being shifted.
        bit_size (int): The number of bits that will be shifted by shift places.
        shift (int): How many bits to shift by.
        policy (dict): The :ref:`bit_policy <aerospike_bit_policies>` dictionary. default: None.

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


def bit_not(bin_name: str, bit_offset, bit_size, policy=None):
    """Creates a bit_not_operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`.

    Server negates bitmap starting at bit_offset for bit_size.
    No value is returned.

    Args:
        bin_name (str): The name of the bin containing the map.
        bit_offset (int): The offset where the bits will start being scanned.
        bit_size (int): How many bits to scan.
        policy (dict): The :ref:`bit_policy <aerospike_bit_policies>` dictionary. default: None.

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


def bit_or(bin_name: str, bit_offset, bit_size, value_byte_size, value, policy=None):
    """Creates a bit_or_operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`.

    Creates a bit or operation. Server performs bitwise or with value and bitmap in bin
    at bit_offset for bit_size. Server returns nothing.

    Args:
        bin_name (str): The name of the bin containing the map.
        bit_offset (int): The offset where the bits will start being compared.
        bit_size (int): How many bits of value to or.
        value_byte_size (int): Length of value in bytes.
        value (bytes/byte array): Value to be used in or operation.
        policy (dict): The :ref:`bit_policy <aerospike_bit_policies>` dictionary. default: None.

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


def bit_rscan(bin_name: str, bit_offset, bit_size, value):
    """Creates a bit_rscan_operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`.

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


def bit_rshift(bin_name: str, bit_offset, bit_size, shift, policy=None):
    """Creates a bit_rshift_operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`.

    Server right shifts bitmap starting at bit_offset for bit_size by shift bits.
    No value is returned.

    Args:
        bin_name (str): The name of the bin containing the map.
        bit_offset (int): The offset where the bits will start being shifted.
        bit_size (int): The number of bits that will be shifted by shift places.
        shift (int): How many bits to shift by.
        policy (dict): The :ref:`bit_policy <aerospike_bit_policies>` dictionary. default: None.

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


def bit_subtract(bin_name: str, bit_offset, bit_size, value, sign, action, policy=None):
    """Creates a bit_subtract_operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`.

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
        policy (dict): The :ref:`bit_policy <aerospike_bit_policies>` dictionary. default: None.

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


def bit_xor(bin_name: str, bit_offset, bit_size, value_byte_size, value, policy=None):
    """Creates a bit_xor_operation to be used with :meth:`aerospike.operate` or :meth:`aerospike.operate_ordered`.

    Creates a bit and operation. Server performs bitwise xor with value and bitmap in bin
    at bit_offset for bit_size. Server returns nothing.

    Args:
        bin_name (str): The name of the bin containing the map.
        bit_offset (int): The offset where the bits will start being compared.
        bit_size (int): How many bits of value to xor.
        value_byte_size (int): Length of value in bytes.
        value (bytes/byte array): Value to be used in xor operation.
        policy (dict): The :ref:`bit_policy <aerospike_bit_policies>` dictionary. default: None.

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
