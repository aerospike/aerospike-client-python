# -*- coding: utf-8 -*-
import pytest
import sys
import random
from datetime import datetime
from aerospike import exception as e
from aerospike_helpers.operations import bitwise_operations

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)

random.seed(datetime.now())

class TestBitwiseOperations(object):
    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        """
        Setup Method
        """
        self.keys = []
        self.five_zero_blob = bytearray([0] * 5)
        self.five_one_blob = bytearray([1] * 5)
        self.zero_one_blob = bytearray([0] * 5 + [1] * 5)
        self.count_blob = bytearray([1] * 1 + [86] * 2 + [255] * 1 + [3] * 1)
        self.five_255_blob = bytearray([255] * 5)

        self.test_key = 'test', 'demo', 'bitwise_op'
        self.test_bin_zeroes = 'bitwise0'
        self.test_bin_ones = 'bitwise1'
        self.zero_one_bin = 'bitwise01'
        self.count_bin = 'count'
        self.five_255_bin = '255'
        self.as_connection.put(
            self.test_key,
            {
                self.test_bin_zeroes: self.five_zero_blob,
                self.test_bin_ones: self.five_one_blob,
                self.zero_one_bin: self.zero_one_blob,
                self.count_bin: self.count_blob,
                self.five_255_bin: self.five_255_blob,
            }
        )
        self.keys.append(self.test_key)

        yield

        for key in self.keys:
            try:
                self.as_connection.remove(key)
            except e.AerospikeError:
                pass

    def test_bit_resize_defaults(self):
        """
        Perform a bit_resize operation with default flags.
        """
        ops = [
            bitwise_operations.bit_resize(self.test_bin_ones, 10)
        ]
        self.as_connection.operate(self.test_key, ops)

        _, _, bins = self.as_connection.get(self.test_key)
        #  We should not have changed the zeroes bin
        assert bins[self.test_bin_zeroes] == self.five_zero_blob

        assert len(bins[self.test_bin_ones]) == 10
        # We expect the newly added zeroes to be added to the end of the bytearray
        assert bins[self.test_bin_ones] == bytearray([1] * 5 + [0] * 5)

    def test_bit_resize_from_front(self):
        """
        Perform a bit_resize operation with resize from front flags.
        """
        ops = [
            bitwise_operations.bit_resize(self.test_bin_ones, 10, resize_flags=aerospike.BIT_RESIZE_FROM_FRONT)
        ]
        self.as_connection.operate(self.test_key, ops)

        _, _, bins = self.as_connection.get(self.test_key)
        #  We should not have changed the zeroes bin
        assert bins[self.test_bin_zeroes] == self.five_zero_blob

        assert len(bins[self.test_bin_ones]) == 10
        # We expect the newly added zeroes to be added to the front of the bytearray
        assert bins[self.test_bin_ones] == bytearray([0] * 5 + [1] * 5)

    def test_bit_resize_grow_only_allows_grow(self):
        """
        Perform a bit_resize operation with grow only resize flags.
        """
        ops = [
            bitwise_operations.bit_resize(self.test_bin_ones, 10, resize_flags=aerospike.BIT_RESIZE_GROW_ONLY)
        ]
        self.as_connection.operate(self.test_key, ops)

        _, _, bins = self.as_connection.get(self.test_key)

        assert len(bins[self.test_bin_ones]) == 10

    def test_bit_resize_shrink_only_allows_shrink(self):
        """
        Perform a bit_resize operation with shrink only resize flags.
        """
        ops = [
            bitwise_operations.bit_resize(self.test_bin_ones, 1, resize_flags=aerospike.BIT_RESIZE_SHRINK_ONLY)
        ]
        self.as_connection.operate(self.test_key, ops)

        _, _, bins = self.as_connection.get(self.test_key)

        assert len(bins[self.test_bin_ones]) == 1

    def test_bit_resize_shrink_removes_from_end(self):
        '''
        Shrinking a bytearray [0, 0, 0, 0, 0, 1, 1, 1, 1, 1]
        should leave:
        [0, 0, 0, 0, 0]
        '''
        ops = [
            bitwise_operations.bit_resize(self.zero_one_bin, 5)
        ]
        self.as_connection.operate(self.test_key, ops)

        _, _, bins = self.as_connection.get(self.test_key)

        assert len(bins[self.zero_one_bin]) == 5
        assert bins[self.zero_one_bin] == bytearray([0] * 5)

    def test_bit_resize_shrink_from_front(self):
        '''
        Shrinking a bytearray [0, 0, 0, 0, 0, 1, 1, 1, 1, 1]
        from the front should leave:
        [1, 1, 1, 1, 1]
        '''
        ops = [
            bitwise_operations.bit_resize(self.zero_one_bin, 5, resize_flags=aerospike.BIT_RESIZE_FROM_FRONT)
        ]
        self.as_connection.operate(self.test_key, ops)

        _, _, bins = self.as_connection.get(self.test_key)

        assert len(bins[self.zero_one_bin]) == 5
        assert bins[self.zero_one_bin] == bytearray([1] * 5)

    def test_bit_resize_default_allows_create(self):
        '''
        By default we can create a new bin with resize.
        '''
        ops = [
            bitwise_operations.bit_resize('new_bin_name', 10)
        ]
        self.as_connection.operate(self.test_key, ops)

        _, _, bins = self.as_connection.get(self.test_key)
        assert bins['new_bin_name'] == bytearray([0] * 10)

    def test_bit_resize_create_only_allows_create(self):
        '''
        Create a bin with resize using the create only flag.
        '''
        bit_policy = {
            'bit_write_flags': aerospike.BIT_WRITE_CREATE_ONLY
        }
        ops = [
            bitwise_operations.bit_resize('new_bin_name', 10, policy=bit_policy)
        ]
        self.as_connection.operate(self.test_key, ops)

        _, _, bins = self.as_connection.get(self.test_key)
        assert bins['new_bin_name'] == bytearray([0] * 10)

    def test_bit_resize_update_only_allows_update(self):
        '''
        Update a bin with resize using the update only flag.
        '''
        bit_policy = {
            'bit_write_flags': aerospike.BIT_WRITE_UPDATE_ONLY
        }
        ops = [
            bitwise_operations.bit_resize(self.test_bin_zeroes, 10, policy=bit_policy)
        ]
        self.as_connection.operate(self.test_key, ops)

        _, _, bins = self.as_connection.get(self.test_key)
        assert bins[self.test_bin_zeroes] == bytearray([0] * 10)

    def test_bit_resize_create_only_prevents_update(self):
        '''
        Attempt to update a bin using the create only flag, should fail.
        '''
        bit_policy = {
            'bit_write_flags': aerospike.BIT_WRITE_CREATE_ONLY
        }
        ops = [
            bitwise_operations.bit_resize(self.test_bin_zeroes, 10, policy=bit_policy)
        ]
        with pytest.raises(e.BinExistsError):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_resize_update_only_prevents_create(self):
        '''
        Attempt to create a bin with the update only flag.
        '''
        bit_policy = {
            'bit_write_flags': aerospike.BIT_WRITE_UPDATE_ONLY
        }
        ops = [
            bitwise_operations.bit_resize('new_binname', 10, policy=bit_policy)
        ]
        with pytest.raises(e.BinNotFound):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_resize_partial_no_fail(self):
        '''
        By default we can create a new bin with resize.
        '''
        bit_policy = {
            'bit_write_flags': aerospike.BIT_WRITE_CREATE_ONLY | aerospike.BIT_WRITE_NO_FAIL | aerospike.BIT_WRITE_PARTIAL
        }
        ops = [
            bitwise_operations.bit_resize(self.test_bin_zeroes, 15, policy=bit_policy),
            bitwise_operations.bit_resize(self.test_bin_zeroes, 20)

        ]
        self.as_connection.operate(self.test_key, ops)
        _, _, bins = self.as_connection.get(self.test_key)
        assert bins[self.test_bin_zeroes] == bytearray([0] * 20)

    def test_bit_resize_partial_no_fail(self):
        '''
        By default we can create a new bin with resize.
        '''
        bit_policy = {
            'bit_write_flags': aerospike.BIT_WRITE_UPDATE_ONLY | aerospike.BIT_WRITE_NO_FAIL
        }
        ops = [
            bitwise_operations.bit_resize('new_binname', 10, policy=bit_policy)
        ]
        self.as_connection.operate(self.test_key, ops)
        _, _, bins = self.as_connection.get(self.test_key)
        assert 'new_binname' not in bins

    def test_bit_resize_grow_only_does_not_allow_shrink(self):
        '''
        Resize with grow only flags.
        '''
        ops = [
            bitwise_operations.bit_resize(self.test_bin_ones, 1, resize_flags=aerospike.BIT_RESIZE_GROW_ONLY)
        ]
        with pytest.raises(e.InvalidRequest):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_resize_shrink_only_does_not_allow_grow(self):
        '''
        Prevent bin growth with shrin only flag.
        '''
        ops = [
            bitwise_operations.bit_resize(self.test_bin_ones, 10, resize_flags=aerospike.BIT_RESIZE_SHRINK_ONLY)
        ]
        with pytest.raises(e.InvalidRequest):
            self.as_connection.operate(self.test_key, ops)
    
    def test_bit_remove_two_bytes(self):
        '''
        Perform a bit_remove op with offset 1 and byte_size 2.
        '''
        ops = [
            bitwise_operations.bit_remove(self.test_bin_zeroes, 1, 2, None)
        ]
        self.as_connection.operate(self.test_key, ops)

        _, _, bins = self.as_connection.get(self.test_key)
        assert bins[self.test_bin_zeroes] == bytearray([0] * 3)
        # should have removed 2 0s)

        assert len(bins[self.test_bin_zeroes]) == 3
        # should have len 3 after removing 2 0s
    
    def test_bit_remove_randnumbytes_randoffset(self):
        '''
        Perform a bit_remove op with random offset and random byte_size.
        '''
        offset = random.randint(0, 4)
        num_bytes = random.randint(1, (5 - offset))
        ops = [
            bitwise_operations.bit_remove(self.test_bin_zeroes, offset, num_bytes, None)
        ]
        self.as_connection.operate(self.test_key, ops)

        _, _, bins = self.as_connection.get(self.test_key)
        assert bins[self.test_bin_zeroes] == bytearray([0] * (5 - num_bytes))
        # should have removed num_bytes 0s
    
    def test_bit_remove_offset_out_of_range(self):
        '''
        Perform a bit_remove op with an offset outside the bit_map being
        modified.
        '''
        ops = [
            bitwise_operations.bit_remove(self.test_bin_zeroes, 6, 1, None)
        ]

        with pytest.raises(e.InvalidRequest):
            self.as_connection.operate(self.test_key, ops)
    
    def test_bit_remove_byte_size_too_large(self):
        '''
        Perform a bit_remove op with byte_size larger than bit_map being modified.
        '''
        ops = [
            bitwise_operations.bit_remove(self.test_bin_zeroes, 0, 6, None)
        ]

        with pytest.raises(e.InvalidRequest):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_remove_byte_offset_with_byte_too_large(self):
        '''
        Perform a bit_remove op with an offset and byte_size that attempt
        to modify the bitmap outside of bounds.
        '''
        ops = [
            bitwise_operations.bit_remove(self.test_bin_zeroes, 3, 3, None)
        ]

        with pytest.raises(e.InvalidRequest):
            self.as_connection.operate(self.test_key, ops)
    
    def test_bit_remove_bad_bin_name(self):
        '''
        Perform a bit_remove op with on a non existent bin.
        '''
        ops = [
            bitwise_operations.bit_remove("bad_name", 3, 3, None)
        ]

        with pytest.raises(e.BinNotFound):
            self.as_connection.operate(self.test_key, ops)
    
    def test_bit_remove_bad_arg_type(self):
        '''
        Perform a bit_remove op with an unsuported float type argument.
        '''
        ops = [
            bitwise_operations.bit_remove("bad_name", 3, 3.5, None)
        ]

        with pytest.raises(e.ParamError):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_set_bit_random_byte_random_offset(self):
        '''
        Perform a bit_set op with a random offset and random valued byte.
        '''
        value = bytearray()
        rand_byte = random.randint(0,255)
        value.append(rand_byte)
        rand_offset = random.randint(0, 4) * 8
        ops = [
            bitwise_operations.bit_set(self.test_bin_zeroes, rand_offset, 8, 1, value, None)
        ]
        self.as_connection.operate(self.test_key, ops)

        _, _, bins = self.as_connection.get(self.test_key)
        expected_result = bytearray([0] * 5)
        expected_result[rand_offset // 8] = rand_byte
        assert bins[self.test_bin_zeroes] == expected_result
        # should set the byte at rand_offset/8 to rand_byte

    def test_bit_set_bit_inbetween_bytes(self):
        '''
        Perform a bit_set op with an offset that places the value across bytes.
        '''
        value = bytearray()
        value.append(255)
        ops = [
            bitwise_operations.bit_set(self.test_bin_zeroes, 4, 8, 1, value, None)
        ]
        self.as_connection.operate(self.test_key, ops)

        _, _, bins = self.as_connection.get(self.test_key)
        expected_result = bytearray([15] * 1 + [240] * 1 + [0] * 3)
        assert bins[self.test_bin_zeroes] == expected_result

    def test_bit_set_bit_multiple_bytes(self):
        '''
        Perform a bit_set op with a random number of bytes.
        '''
        value = bytearray()
        value = bytearray()
        rand_byte = random.randint(0,255)
        num_bytes = random.randint(1,5)
        for x in range(0, num_bytes):
            value.append(rand_byte)
        ops = [
            bitwise_operations.bit_set(self.test_bin_zeroes, 0, (num_bytes * 8), num_bytes, value, None)
        ]
        self.as_connection.operate(self.test_key, ops)

        _, _, bins = self.as_connection.get(self.test_key)
        expected_result = bytearray(([rand_byte] * num_bytes) + [0] * (5 - num_bytes))
        assert bins[self.test_bin_zeroes] == expected_result
    
    def test_bit_set_bit_index_out_of_range(self):
        ''''
        Perform a bit_set op with a bit offset that is larger than the bit_map being modified.
        '''
        value = bytearray()
        value.append(255)
        ops = [
            bitwise_operations.bit_set(self.test_bin_zeroes, 41, 8, 1, value, None)
        ]
        with pytest.raises(e.OpNotApplicable):
            self.as_connection.operate(self.test_key, ops)
    
    def test_bit_set_bit_value_size_too_large(self):
        '''
        Perform a bit_set op with a value larger than the bit_map being modified.
        '''
        value = bytearray()
        for x in range(0,5):
            value.append(255)
        ops = [
            bitwise_operations.bit_set(self.test_bin_zeroes, 0, 48, 6, value, None)
        ]
        with pytest.raises(e.OpNotApplicable):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_set_bit_bit_size_too_large(self):
        '''
        Perform a bit_set op with an bit size larger than the actual value.
        '''
        value = bytearray()
        value.append(255)
        ops = [
            bitwise_operations.bit_set(self.test_bin_zeroes, 0, 16, 1, value, None)
        ]
        with pytest.raises(e.InvalidRequest):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_set_bit_invalid_arg_type(self):
        '''
        Perform a bit_set op with an unsuported float.
        '''
        value = 85323.9
        ops = [
            bitwise_operations.bit_set(self.test_bin_zeroes, 0, 8, 1, value, None)
        ]
        with pytest.raises(e.ParamError):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_set_bit_bad_bin_name(self):
        '''
        Perform a bit_set op with a non existent bin.
        '''
        value = bytearray()
        value.append(255)
        ops = [
            bitwise_operations.bit_set("bad_name", 0, 8, 1, value, None)
        ]
        with pytest.raises(e.BinNotFound):
            self.as_connection.operate(self.test_key, ops)
    
    def test_bit_count_seven(self):
        '''
        Perform a bitwise count op.
        '''
        ops = [
            bitwise_operations.bit_count(self.count_bin, 20, 9)
        ]

        _, _, result = self.as_connection.operate(self.test_key, ops)
        assert result['count'] == 7

    def test_bit_count_one(self):
        '''
        Perform a bitwise count op.
        '''
        ops = [
            bitwise_operations.bit_count(self.zero_one_bin, 47, 8)
        ]

        _, _, result = self.as_connection.operate(self.test_key, ops)
        assert result['bitwise01'] == 1

    def test_bit_count_random_bit_size(self):
        '''
        Perform a bitwise count op.
        '''
        bit_size = random.randint(1, 40)
        ops = [
            bitwise_operations.bit_count(self.five_255_bin, 0, bit_size)
        ]

        _, _, result = self.as_connection.operate(self.test_key, ops)
        assert result['255'] == bit_size
    
    def test_bit_count_bit_offset_out_of_range(self):
        '''
        Attempts to perform a bitwise count op with bit_offset outside of
        the bitmap being counted.
        '''
        ops = [
            bitwise_operations.bit_count(self.zero_one_bin, 81, 1)
        ]

        with pytest.raises(e.OpNotApplicable):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_count_bit_size_too_large(self):
        '''
        Attempts to perform a bitwise count op on more bits than exist.
        '''
        ops = [
            bitwise_operations.bit_count(self.zero_one_bin, 1, 81)
        ]

        with pytest.raises(e.OpNotApplicable):
            self.as_connection.operate(self.test_key, ops)
    
    def test_bit_count_bit_size_with_offset_too_large(self):
        '''
        Attempts to perform a bitwise count past the bounds of the bitmap being counted.
        '''
        ops = [
            bitwise_operations.bit_count(self.zero_one_bin, 40, 41)
        ]

        with pytest.raises(e.OpNotApplicable):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_add_simple(self):
        '''
        Perform a bitwise add op.
        '''
        ops = [
            bitwise_operations.bit_add(self.test_bin_ones, 0, 8, 1, False, aerospike.BIT_OVERFLOW_FAIL, None)
        ]

        self.as_connection.operate(self.test_key, ops)

        _, _, bins = self.as_connection.get(self.test_key)
        expected_result = bytearray([2] * 1 + [1] * 4)
        assert bins[self.test_bin_ones] == expected_result

    def test_bit_add_inbetween_bytes(self):
        '''
        Perform a bitwise add op with an offset that lands inbetween bytes.
        '''
        ops = [
            bitwise_operations.bit_add(self.test_bin_zeroes, 4, 8, 255, False, aerospike.BIT_OVERFLOW_FAIL, None)
        ]

        self.as_connection.operate(self.test_key, ops)

        _, _, bins = self.as_connection.get(self.test_key)
        expected_result = bytearray([15] * 1 + [240] * 1 + [0] * 3)
        assert bins[self.test_bin_zeroes] == expected_result

    def test_bit_add_multiple_bytes(self):
        '''
        Perform a bitwise add op with multiple bytes.
        '''
        ops = [
            bitwise_operations.bit_add(self.test_bin_zeroes, 8, 16, 65535, False, aerospike.BIT_OVERFLOW_FAIL, None)
        ]

        self.as_connection.operate(self.test_key, ops)

        _, _, bins = self.as_connection.get(self.test_key)
        expected_result = bytearray([0] * 1 + [255] * 2 + [0] * 2)
        assert bins[self.test_bin_zeroes] == expected_result

    def test_bit_add_bad_bin_name(self):
        '''
        Perform a bitwise add op on a nonexistent bin.
        '''
        ops = [
            bitwise_operations.bit_add("bad_name", 8, 16, 65535, False, aerospike.BIT_OVERFLOW_FAIL, None)
        ]

        with pytest.raises(e.BinNotFound):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_add_bit_offset_out_of_range(self):
        '''
        Perform a bitwise add op with a bit_offset that is out of range
        for the bitmap being modified.
        '''
        ops = [
            bitwise_operations.bit_add(self.test_bin_zeroes, 41, 1, 1, False, aerospike.BIT_OVERFLOW_FAIL, None)
        ]

        with pytest.raises(e.OpNotApplicable):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_add_bit_size_out_of_range(self):
        '''
        Perform a bitwise add op with a bit size too large for
        the bitmap being modified.
        '''
        ops = [
            bitwise_operations.bit_add(self.test_bin_zeroes, 0, 41, 1, False, aerospike.BIT_OVERFLOW_FAIL, None)
        ]

        with pytest.raises(e.OpNotApplicable):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_add_bit_size_signed(self):
        '''
        Perform a bitwise add op subtraction.
        '''
        ops = [
            bitwise_operations.bit_add(self.five_255_bin, 0, 8, 254, True, aerospike.BIT_OVERFLOW_WRAP, None)
        ]

        self.as_connection.operate(self.test_key, ops)

        _, _, bins = self.as_connection.get(self.test_key)
        expected_result = bytearray([253] * 1 + [255] * 4)
        assert bins[self.five_255_bin] == expected_result
    
    def test_bit_add_overflow_fail(self):
        '''
        Perform a bitwise add op that overflows with the BIT_OVERFLOW_FAIL action.
        '''
        ops = [
            bitwise_operations.bit_add(self.five_255_bin, 0, 8, 1, False, aerospike.BIT_OVERFLOW_FAIL, None)
        ]

        with pytest.raises(e.OpNotApplicable):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_add_overflow_saturate(self):
        '''
        Perform a bitwise add op that overflows with the BIT_OVERFLOW_SATURATE action.
        '''
        ops = [
            bitwise_operations.bit_add(self.five_255_bin, 0, 8, 1, False, aerospike.BIT_OVERFLOW_SATURATE, None)
        ]

        self.as_connection.operate(self.test_key, ops)

        _, _, bins = self.as_connection.get(self.test_key)
        expected_result = bytearray([255] * 5)
        assert bins[self.five_255_bin] == expected_result

    def test_bit_add_overflow_wrap(self):
        '''
        Perform a bitwise add op that overflows with the BIT_OVERFLOW_WRAP action.
        '''
        ops = [
            bitwise_operations.bit_add(self.five_255_bin, 0, 8, 1, False, aerospike.BIT_OVERFLOW_WRAP, None)
        ]

        self.as_connection.operate(self.test_key, ops)

        _, _, bins = self.as_connection.get(self.test_key)
        expected_result = bytearray([0] * 1 + [255] * 4)
        assert bins[self.five_255_bin] == expected_result

    def test_bit_and(self):
        '''
        Perform a bitwise and op.
        '''
        value = bytearray()
        value.append(0)
        ops = [
            bitwise_operations.bit_and(self.five_255_bin, 0, 8, 1, value, None)
        ]

        self.as_connection.operate(self.test_key, ops)

        _, _, bins = self.as_connection.get(self.test_key)
        expected_result = bytearray([0] * 1 + [255] * 4)
        assert bins[self.five_255_bin] == expected_result

    def test_bit_and_across_bytes(self):
        '''
        Perform a bitwise and op with a bit_offset that causes the op to span bytes.
        '''
        value = bytearray()
        value.append(0)
        ops = [
            bitwise_operations.bit_and(self.five_255_bin, 7, 8, 1, value, None)
        ]

        self.as_connection.operate(self.test_key, ops)

        _, _, bins = self.as_connection.get(self.test_key)
        expected_result = bytearray([254] * 1 + [1] * 1 + [255] * 3)
        assert bins[self.five_255_bin] == expected_result

    def test_bit_and_multiple_bytes(self):
        '''
        Perform a bitwise and op with value large enough to span multiple bytes.
        '''
        value = bytearray()
        value.append(1)
        value.append(1)
        value.append(1)
        ops = [
            bitwise_operations.bit_and(self.five_255_bin, 8, 17, 3, value, None)
        ]

        self.as_connection.operate(self.test_key, ops)

        _, _, bins = self.as_connection.get(self.test_key)
        expected_result = bytearray([255] * 1 + [1] * 2 + [127] * 1 + [255] * 1)
        assert bins[self.five_255_bin] == expected_result

    def test_bit_and_offset_out_of_range(self):
        '''
        Perform a bitwise and op with a bit_offset outside of the bitmap being modified.
        '''
        value = bytearray()
        value.append(0)
        ops = [
            bitwise_operations.bit_and(self.five_255_bin, 41, 8, 1, value, None)
        ]

        with pytest.raises(e.OpNotApplicable):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_and_offset_bit_size_larger_than_val(self):
        '''
        Perform a bitwise and op with a bit_size > the size of value.
        '''
        value = bytearray()
        value.append(0)
        ops = [
            bitwise_operations.bit_and(self.five_255_bin, 0, 16, 1, value, None)
        ]

        with pytest.raises(e.InvalidRequest):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_and_offset_value_byte_size_too_large(self):
        '''
        Perform a bitwise and op with a value_byte_size larger than the bitmap
        being modified.
        '''
        value = bytearray()
        for x in range(0,5):
            value.append(0)
        ops = [
            bitwise_operations.bit_and(self.five_255_bin, 0, 48, 6, value, None)
        ]

        with pytest.raises(e.OpNotApplicable):
            self.as_connection.operate(self.test_key, ops)
    
    def test_bit_and_invalid_bin_name(self):
        '''
        Perform a bitwise and op with a non existent bin name.
        '''
        value = bytearray()
        value.append(0)
        ops = [
            bitwise_operations.bit_and("bad_name", 0, 8, 1, value, None)
        ]

        with pytest.raises(e.BinNotFound):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_and_invalid_value(self):
        '''
        Perform a bitwise and op with a non existent bin name.
        '''
        ops = [
            bitwise_operations.bit_and("bad_name", 0, 8, 1, 1.5, None)
        ]

        with pytest.raises(e.ParamError):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_get(self):
        '''
        Perform a bitwise get op.
        '''
        ops = [
            bitwise_operations.bit_get(self.five_255_bin, 0, 8)
        ]

        _, _, result = self.as_connection.operate(self.test_key, ops)

        expected_result = bytearray([255] * 1)
        assert result['255'] == expected_result

    def test_bit_get_negative_offset(self):
        '''
        Perform a bitwise get op with a negative offset.
        '''
        ops = [
            bitwise_operations.bit_get(self.count_bin, -2, 2)
        ]

        _, _, result = self.as_connection.operate(self.test_key, ops)

        expected_result = bytearray([192] * 1)
        assert result[self.count_bin] == expected_result

    def test_bit_get_accross_bytes(self):
        '''
        Perform a bitwise get op across bytes.
        '''
        ops = [
            bitwise_operations.bit_get(self.test_bin_ones, 4, 8)
        ]

        _, _, result = self.as_connection.operate(self.test_key, ops)

        expected_result = bytearray([16] * 1)
        assert result['bitwise1'] == expected_result
    
    def test_bit_get_fraction_of_byte(self):
        '''
        Perform a bitwise get op on a portion of a byte.
        '''
        ops = [
            bitwise_operations.bit_get(self.five_255_bin, 4, 2)
        ]

        _, _, result = self.as_connection.operate(self.test_key, ops)

        expected_result = bytearray([192] * 1)
        assert result['255'] == expected_result

    def test_bit_get_multiple_bytes(self):
        '''
        Perform a bitwise get op across multiple bytes.
        '''
        ops = [
            bitwise_operations.bit_get(self.five_255_bin, 4, 17)
        ]

        _, _, result = self.as_connection.operate(self.test_key, ops)

        expected_result = bytearray([255] * 2 + [128] * 1)
        assert result['255'] == expected_result

    def test_bit_get_bit_offset_out_of_range(self):
        '''
        Perform a bitwise get with a bitoffset outside of the bitmap
        being read.
        '''
        ops = [
            bitwise_operations.bit_get(self.test_bin_ones, 41, 1)
        ]

        with pytest.raises(e.OpNotApplicable):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_get_bit_size_too_large(self):
        '''
        Perform a bitwise get with a bit_size larger than the bitmap
        being read.
        '''
        ops = [
            bitwise_operations.bit_get(self.test_bin_ones, 0, 41)
        ]

        with pytest.raises(e.OpNotApplicable):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_get_bit_size_too_large(self):
        '''
        Perform a bitwise get with a bit_size larger than the bitmap
        being read.
        '''
        ops = [
            bitwise_operations.bit_get(self.test_bin_ones, 0, 41)
        ]

        with pytest.raises(e.OpNotApplicable):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_get_bad_bin_name(self):
        '''
        Perform a bitwise get with a non existent bin.
        '''
        ops = [
            bitwise_operations.bit_get("bad_name", 0, 1)
        ]
        _, _, result = self.as_connection.operate(self.test_key, ops)

        expected_result = None
        assert result['bad_name'] == expected_result

    def test_bit_get_bad_argument_type(self):
        '''
        Perform a bitwise get with a float for bit_size.
        '''
        ops = [
            bitwise_operations.bit_get(self.test_bin_ones, 0, 1.5)
        ]

        with pytest.raises(e.ParamError):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_get_int(self):
        '''
        Perform a bitwise get int op.
        '''
        ops = [
            bitwise_operations.bit_get_int(self.five_255_bin, 0, 8, False)
        ]

        _, _, result = self.as_connection.operate(self.test_key, ops)

        expected_result = 255
        assert result['255'] == expected_result

    def test_bit_get_int_accross_bytes(self):
        '''
        Perform a bitwise get int op across bytes.
        '''
        ops = [
            bitwise_operations.bit_get_int(self.test_bin_ones, 4, 8, False)
        ]

        _, _, result = self.as_connection.operate(self.test_key, ops)

        expected_result = 16
        assert result['bitwise1'] == expected_result
    
    def test_bit_get_int_fraction_of_byte(self):
        '''
        Perform a bitwise get op on a portion of a byte.
        '''
        ops = [
            bitwise_operations.bit_get_int(self.five_255_bin, 4, 2, False)
        ]

        _, _, result = self.as_connection.operate(self.test_key, ops)

        expected_result = 3
        assert result['255'] == expected_result

    def test_bit_get_int_multiple_bytes(self):
        '''
        Perform a bitwise get int op across multiple bytes.
        '''
        ops = [
            bitwise_operations.bit_get_int(self.five_255_bin, 4, 17, False)
        ]

        _, _, result = self.as_connection.operate(self.test_key, ops)

        expected_result = 131071
        assert result['255'] == expected_result

    def test_bit_get_int_bit_offset_out_of_range(self):
        '''
        Perform a bitwise get int with a bitoffset outside of the bitmap
        being read.
        '''
        ops = [
            bitwise_operations.bit_get_int(self.test_bin_ones, 41, 1, False)
        ]

        with pytest.raises(e.OpNotApplicable):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_get_int_bit_size_too_large(self):
        '''
        Perform a bitwise get int with a bit_size larger than the bitmap
        being read.
        '''
        ops = [
            bitwise_operations.bit_get_int(self.test_bin_ones, 0, 41, False)
        ]

        with pytest.raises(e.OpNotApplicable):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_get_int_bit_size_too_large(self):
        '''
        Perform a bitwise get int with a bit_size larger than the bitmap
        being read.
        '''
        ops = [
            bitwise_operations.bit_get_int(self.test_bin_ones, 0, 41, False)
        ]

        with pytest.raises(e.OpNotApplicable):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_get_int_bad_bin_name(self):
        '''
        Perform a bitwise get int with a non existent bin.
        '''
        ops = [
            bitwise_operations.bit_get_int("bad_name", 0, 1, False)
        ]
        _, _, result = self.as_connection.operate(self.test_key, ops)

        expected_result = None
        assert result['bad_name'] == expected_result

    def test_bit_get_int_bad_argument_type(self):
        '''
        Perform a bitwise get int with a float for bit_size.
        '''
        ops = [
            bitwise_operations.bit_get_int(self.test_bin_ones, 0, 1.5, False)
        ]

        with pytest.raises(e.ParamError):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_get_int_signed(self):
        '''
        Perform a bitwise get int op with sign true.
        '''
        ops = [
            bitwise_operations.bit_get_int(self.five_255_bin, 0, 8, True)
        ]

        _, _, result = self.as_connection.operate(self.test_key, ops)

        expected_result = -1
        assert result['255'] == expected_result

    def test_bit_insert(self):
        '''
        Perform a bitwise insert op.
        '''
        value = bytearray([3])
        ops = [
            bitwise_operations.bit_insert(self.test_bin_zeroes, 0, 1, value, None)
        ]
        self.as_connection.operate(self.test_key, ops)

        _, _, bins = self.as_connection.get(self.test_key)
        expected_result = bytearray([3] * 1 + [0] * 5)
        assert bins[self.test_bin_zeroes] == expected_result
    
    def test_bit_insert_multiple_bytes(self):
        '''
        Perform a bitwise insert op.
        '''
        value = bytearray([3] * 3)
        ops = [
            bitwise_operations.bit_insert(self.test_bin_zeroes, 0, 3, value, None)
        ]
        self.as_connection.operate(self.test_key, ops)

        _, _, bins = self.as_connection.get(self.test_key)
        expected_result = bytearray([3] * 3 + [0] * 5)
        assert bins[self.test_bin_zeroes] == expected_result

    def test_bit_insert_multiple_bytes_with_offset(self):
        '''
        Perform a bitwise insert op with multiple bytes and a non 0 offset.
        '''
        value = bytearray([3] * 3)
        ops = [
            bitwise_operations.bit_insert(self.test_bin_zeroes, 2, 3, value, None)
        ]
        self.as_connection.operate(self.test_key, ops)

        _, _, bins = self.as_connection.get(self.test_key)
        expected_result = bytearray([0] * 2 + [3] * 3 + [0] * 3)
        assert bins[self.test_bin_zeroes] == expected_result

    def test_bit_insert_offset_out_of_range(self):
        '''
        Perform a bitwise insert op where byte_offset is out of range for
        the bitmap being modified. Places 0 untill proper offset is reached.
        '''
        value = bytearray([3])
        ops = [
            bitwise_operations.bit_insert(self.five_255_bin, 9, 1, value, None)
        ]

        self.as_connection.operate(self.test_key, ops)

        _, _, bins = self.as_connection.get(self.test_key)
        expected_result = bytearray([255] * 5 + [0] * 4 + [3] * 1)
        assert bins[self.five_255_bin] == expected_result

    def test_bit_insert_value_byte_size_too_large(self):
        '''
        Perform a bitwise insert op where value_byte_size is larger than the bitmap
        being modified.
        '''
        value = bytearray([3] * 6)
        ops = [
            bitwise_operations.bit_insert(self.five_255_bin, 0, 6, value, None)
        ]

        self.as_connection.operate(self.test_key, ops)

        _, _, bins = self.as_connection.get(self.test_key)
        expected_result = bytearray([3] * 6 + [255] * 5)
        assert bins[self.five_255_bin] == expected_result
    
    def test_bit_insert_value_byte_size_smaller_than_value(self):
        '''
        Perform a bitwise insert op where value_byte_size is smaller than the bitmap
        being modified.
        '''
        value = bytearray([3] * 6)
        ops = [
            bitwise_operations.bit_insert(self.five_255_bin, 0, 2, value, None)
        ]

        self.as_connection.operate(self.test_key, ops)

        _, _, bins = self.as_connection.get(self.test_key)
        expected_result = bytearray([3] * 2 + [255] * 5)
        assert bins[self.five_255_bin] == expected_result

    def test_bit_insert_nonexistent_bin_name(self):
        '''
        Perform a bitwise insert op with a non existent bin.
        '''
        value = bytearray([3])
        ops = [
            bitwise_operations.bit_insert('bad_name', 0, 1, value, None)
        ]

        self.as_connection.operate(self.test_key, ops)

        _, _, bins = self.as_connection.get(self.test_key)
        expected_result = bytearray([3])
        assert bins['bad_name'] == expected_result

    def test_bit_insert_bad_arg_type(self):
        '''
        Perform a bitwise insert op with a float byte_size.
        '''
        value = bytearray([3])
        ops = [
            bitwise_operations.bit_insert(self.five_255_bin, 0, 1.5, value, None)
        ]

        with pytest.raises(e.ParamError):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_lscan(self):
        '''
        Perform a bitwise lscan op.
        '''
        value = True
        ops = [
            bitwise_operations.bit_lscan(self.count_bin, 32, 8, value)
        ]

        expected_value = 6
        _, _, result = self.as_connection.operate(self.test_key, ops)
        assert result[self.count_bin] == expected_value

    def test_bit_lscan_across_bytes(self):
        '''
        Perform a bitwise lscan op with an offset that causes the scan to
        search multiple bytes.
        '''
        value = False
        ops = [
            bitwise_operations.bit_lscan(self.test_bin_ones, 7, 8, value)
        ]

        expected_value = 1
        _, _, result = self.as_connection.operate(self.test_key, ops)
        assert result[self.test_bin_ones] == expected_value

    def test_bit_lscan_value_not_found(self):
        '''
        Perform a bitwise lscan op with an offset that causes the scan to
        search multiple bytes.
        '''
        value = False
        ops = [
            bitwise_operations.bit_lscan(self.five_255_bin, 0, 40, value)
        ]

        expected_value = -1
        _, _, result = self.as_connection.operate(self.test_key, ops)
        assert result[self.five_255_bin] == expected_value

    def test_bit_lscan_offset_out_of_range(self):
        '''
        Perform a bitwise lscan op with bit_offset outside bitmap.S
        '''
        value = True
        ops = [
            bitwise_operations.bit_lscan(self.count_bin, 41, 8, value)
        ]

        with pytest.raises(e.OpNotApplicable):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_lscan_bit_size_too_large(self):
        '''
        Perform a bitwise lscan op with bit_size larger than bitmap.S
        '''
        value = True
        ops = [
            bitwise_operations.bit_lscan(self.test_bin_ones, 0, 41, value)
        ]

        with pytest.raises(e.OpNotApplicable):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_lscan_bad_bin_name(self):
        '''
        Perform a bitwise lscan op with a non existent bin.
        '''
        value = True
        ops = [
            bitwise_operations.bit_lscan('bad_name', 0, 8, value)
        ]

        expected_value = None
        _, _, result = self.as_connection.operate(self.test_key, ops)
        assert result['bad_name'] == expected_value

    def test_bit_lshift(self):
        '''
        Perform a bitwise lshift op.
        '''
        ops = [
            bitwise_operations.bit_lshift(self.test_bin_ones, 0, 8, 3, None)
        ]

        self.as_connection.operate(self.test_key, ops)

        _, _, bins = self.as_connection.get(self.test_key)
        expected_result = bytearray([8] * 1 + [1] * 4)
        assert bins[self.test_bin_ones] == expected_result


    def test_bit_lshift_across_bytes(self):
        '''
        Perform a bitwise lshift op with bit_offset causing the op
        to cross bytes.
        '''
        ops = [
            bitwise_operations.bit_lshift(self.test_bin_ones, 4, 12, 3, None)
        ]

        self.as_connection.operate(self.test_key, ops)

        _, _, bins = self.as_connection.get(self.test_key)
        expected_result = bytearray([8] * 2 + [1] * 3)
        assert bins[self.test_bin_ones] == expected_result

    def test_bit_lshift_wrap(self):
        '''
        Perform a bitwise lshift op with shift > 1 byte.
        '''
        ops = [
            bitwise_operations.bit_lshift(self.test_bin_ones, 0, 40, 8, None)
        ]

        self.as_connection.operate(self.test_key, ops)

        _, _, bins = self.as_connection.get(self.test_key)
        expected_result = bytearray([1] * 4 + [0])
        assert bins[self.test_bin_ones] == expected_result

    def test_bit_lshift_offset_out_of_range(self):
        '''
        Perform a bitwise lshift op with offset > bitmap.
        '''
        ops = [
            bitwise_operations.bit_lshift(self.test_bin_ones, 41, 8, 1, None)
        ]

        with pytest.raises(e.OpNotApplicable):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_lshift_bit_size_too_large(self):
        '''
        Perform a bitwise lshift op with bit_size > bitmap.
        '''
        ops = [
            bitwise_operations.bit_lshift(self.test_bin_ones, 0, 41, 1, None)
        ]

        with pytest.raises(e.OpNotApplicable):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_lshift_bad_bin_name(self):
        '''
        Perform a bitwise lshift op with non existent bin.
        '''
        ops = [
            bitwise_operations.bit_lshift("bad_name", 0, 8, 1, None)
        ]

        with pytest.raises(e.BinNotFound):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_lshift_bad_arg(self):
        '''
        Perform a bitwise lshift op with a float for bit_offset.
        '''
        ops = [
            bitwise_operations.bit_lshift(self.test_bin_ones, 1.5, 8, 1, None)
        ]

        with pytest.raises(e.ParamError):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_not(self):
        '''
        Perform a bitwise not op.
        '''
        ops = [
            bitwise_operations.bit_not(self.five_255_bin, 0, 40, None)
        ]

        self.as_connection.operate(self.test_key, ops)

        _, _, bins = self.as_connection.get(self.test_key)
        expected_result = bytearray([0] * 5)
        assert bins[self.five_255_bin] == expected_result

    def test_bit_not_small_bit_size(self):
        '''
        Perform a bitwise not op with a bit size < 1 byte.
        '''
        ops = [
            bitwise_operations.bit_not(self.five_255_bin, 5, 3, None)
        ]

        self.as_connection.operate(self.test_key, ops)

        _, _, bins = self.as_connection.get(self.test_key)
        expected_result = bytearray([248]  + [255] * 4)
        assert bins[self.five_255_bin] == expected_result

    def test_bit_not_offset_out_of_range(self):
        '''
        Perform a bitwise not op with a bit_offset outside the bitmap
        being modified.
        '''
        ops = [
            bitwise_operations.bit_not(self.five_255_bin, 41, 8, None)
        ]

        with pytest.raises(e.OpNotApplicable):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_not_bit_size_too_large(self):
        '''
        Perform a bitwise not op with a bit_size > bitmap.
        '''
        ops = [
            bitwise_operations.bit_not(self.five_255_bin, 0, 41, None)
        ]

        with pytest.raises(e.OpNotApplicable):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_not_bad_bin_name(self):
        '''
        Perform a bitwise not op with a bit_size > bitmap.
        '''
        ops = [
            bitwise_operations.bit_not("bad_name", 0, 8, None)
        ]

        with pytest.raises(e.BinNotFound):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_not_bad_arg(self):
        '''
        Perform a bitwise not op with a float for bit_offset.
        '''
        ops = [
            bitwise_operations.bit_not(self.five_255_bin, 2.7, 8, None)
        ]

        with pytest.raises(e.ParamError):
            self.as_connection.operate(self.test_key, ops)


    def test_bit_or(self):
        '''
        Perform a bitwise or op.
        '''
        value = bytearray()
        value.append(8)
        ops = [
            bitwise_operations.bit_or(self.test_bin_ones, 0, 8, 1, value, None)
        ]

        self.as_connection.operate(self.test_key, ops)

        _, _, bins = self.as_connection.get(self.test_key)
        expected_result = bytearray([9] * 1 + [1] * 4)
        assert bins[self.test_bin_ones] == expected_result

    def test_bit_or_multiple_bytes(self):
        '''
        Perform a bitwise or op on multiple bytes.
        '''
        value = bytearray([8] * 5)
        ops = [
            bitwise_operations.bit_or(self.test_bin_ones, 0, 40, 5, value, None)
        ]

        self.as_connection.operate(self.test_key, ops)

        _, _, bins = self.as_connection.get(self.test_key)
        expected_result = bytearray([9] * 5)
        assert bins[self.test_bin_ones] == expected_result

    def test_bit_or_multiple_bytes_value_unchanged(self):
        '''
        Perform a bitwise or op.
        '''
        value = bytearray([255])
        ops = [
            bitwise_operations.bit_or(self.five_255_bin, 7, 8, 1, value, None)
        ]

        self.as_connection.operate(self.test_key, ops)

        _, _, bins = self.as_connection.get(self.test_key)
        expected_result = bytearray([255] * 5)
        assert bins[self.five_255_bin] == expected_result

    def test_bit_or_bit_offset_out_of_range(self):
        '''
        Perform a bitwise or op with bit_offset > bitmap.
        '''
        value = bytearray()
        value.append(8)
        ops = [
            bitwise_operations.bit_or(self.test_bin_ones, 41, 8, 1, value, None)
        ]

        with pytest.raises(e.OpNotApplicable):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_or_bit_size_larger_than_value(self):
        '''
        Perform a bitwise or op with bit_size > value.
        '''
        value = bytearray()
        value.append(8)
        ops = [
            bitwise_operations.bit_or(self.test_bin_ones, 0, 9, 1, value, None)
        ]

        with pytest.raises(e.InvalidRequest):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_or_bit_size_too_large(self):
        '''
        Perform a bitwise or op with bit_size > bitmap.
        '''
        value = bytearray([8] * 6)
        ops = [
            bitwise_operations.bit_or(self.test_bin_ones, 0, 41, 6, value, None)
        ]

        with pytest.raises(e.OpNotApplicable):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_or_bad_bin_name(self):
        '''
        Perform a bitwise or op with a non existent bin.
        '''
        value = bytearray([8])
        ops = [
            bitwise_operations.bit_or("bad_name", 0, 8, 1, value, None)
        ]

        with pytest.raises(e.BinNotFound):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_or_bad_arg(self):
        '''
        Perform a bitwise or op with an integer for value.
        '''
        value = 1
        ops = [
            bitwise_operations.bit_or("bad_name", 0, 8, 1, value, None)
        ]

        with pytest.raises(e.ParamError):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_rscan(self):
        '''
        Perform a bitwise rscan op.
        '''
        value = True
        ops = [
            bitwise_operations.bit_rscan(self.count_bin, 32, 8, value)
        ]

        expected_value = 7
        _, _, result = self.as_connection.operate(self.test_key, ops)
        assert result[self.count_bin] == expected_value

    def test_bit_rscan_across_bytes(self):
        '''
        Perform a bitwise rscan op with an offset that causes the scan to
        search multiple bytes.
        '''
        value = False
        ops = [
            bitwise_operations.bit_rscan(self.count_bin, 4, 8, value)
        ]

        expected_value = 6
        _, _, result = self.as_connection.operate(self.test_key, ops)
        assert result[self.count_bin] == expected_value

    def test_bit_rscan_value_not_found(self):
        '''
        Perform a bitwise rscan op with an offset that causes the scan to
        search multiple bytes.
        '''
        value = False
        ops = [
            bitwise_operations.bit_rscan(self.five_255_bin, 0, 40, value)
        ]

        expected_value = -1
        _, _, result = self.as_connection.operate(self.test_key, ops)
        assert result[self.five_255_bin] == expected_value

    def test_bit_rscan_offset_out_of_range(self):
        '''
        Perform a bitwise rscan op with bit_offset outside bitmap.
        '''
        value = True
        ops = [
            bitwise_operations.bit_rscan(self.count_bin, 41, 8, value)
        ]

        with pytest.raises(e.OpNotApplicable):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_rscan_bit_size_too_large(self):
        '''
        Perform a bitwise rscan op with bit_size larger than bitmap.
        '''
        value = True
        ops = [
            bitwise_operations.bit_rscan(self.test_bin_ones, 0, 41, value)
        ]

        with pytest.raises(e.OpNotApplicable):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_rscan_bad_bin_name(self):
        '''
        Perform a bitwise rscan op with a non existent bin.
        '''
        value = True
        ops = [
            bitwise_operations.bit_rscan('bad_name', 0, 8, value)
        ]

        expected_value = None
        _, _, result = self.as_connection.operate(self.test_key, ops)
        assert result['bad_name'] == expected_value

    def test_bit_rshift(self):
        '''
        Perform a bitwise rshift op.
        '''
        ops = [
            bitwise_operations.bit_rshift(self.count_bin, 8, 8, 3, None)
        ]

        self.as_connection.operate(self.test_key, ops)

        _, _, bins = self.as_connection.get(self.test_key)
        expected_result = bytearray([1] * 1 + [10] * 1 + [86] * 1 + [255] * 1 + [3] * 1)
        assert bins[self.count_bin] == expected_result

    def test_bit_rshift_across_bytes(self):
        '''
        Perform a bitwise rshift op with bit_offset causing the op
        to cross bytes.
        '''
        ops = [
            bitwise_operations.bit_rshift(self.test_bin_ones, 4, 16, 3, None)
        ]

        self.as_connection.operate(self.test_key, ops)

        _, _, bins = self.as_connection.get(self.test_key)
        expected_result = bytearray([0] + [32] + [33] + [1] * 2)
        assert bins[self.test_bin_ones] == expected_result

    def test_bit_rshift_wrap(self):
        '''
        Perform a bitwise rshift op with shift > 1 byte.
        '''
        ops = [
            bitwise_operations.bit_rshift(self.test_bin_ones, 0, 40, 8, None)
        ]

        self.as_connection.operate(self.test_key, ops)

        _, _, bins = self.as_connection.get(self.test_key)
        expected_result = bytearray([0] + [1] * 4)
        assert bins[self.test_bin_ones] == expected_result

    def test_bit_rshift_offset_out_of_range(self):
        '''
        Perform a bitwise rshift op with offset > bitmap.
        '''
        ops = [
            bitwise_operations.bit_rshift(self.test_bin_ones, 41, 8, 1, None)
        ]

        with pytest.raises(e.OpNotApplicable):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_rshift_bit_size_too_large(self):
        '''
        Perform a bitwise rshift op with bit_size > bitmap.
        '''
        ops = [
            bitwise_operations.bit_rshift(self.test_bin_ones, 0, 41, 1, None)
        ]

        with pytest.raises(e.OpNotApplicable):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_rshift_bad_bin_name(self):
        '''
        Perform a bitwise rshift op with non existent bin.
        '''
        ops = [
            bitwise_operations.bit_rshift("bad_name", 0, 8, 1, None)
        ]

        with pytest.raises(e.BinNotFound):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_rshift_bad_arg(self):
        '''
        Perform a bitwise rshift op with a float for bit_offset.
        '''
        ops = [
            bitwise_operations.bit_rshift(self.test_bin_ones, 1.5, 8, 1, None)
        ]

        with pytest.raises(e.ParamError):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_subtract_simple(self):
        '''
        Perform a bitwise subtract op.
        '''
        ops = [
            bitwise_operations.bit_subtract(self.test_bin_ones, 0, 8, 1, False, aerospike.BIT_OVERFLOW_FAIL, None)
        ]

        self.as_connection.operate(self.test_key, ops)

        _, _, bins = self.as_connection.get(self.test_key)
        expected_result = bytearray([0] * 1 + [1] * 4)
        assert bins[self.test_bin_ones] == expected_result

    def test_bit_subtract_inbetween_bytes(self):
        '''
        Perform a bitwise subtract op with an offset that lands inbetween bytes.
        '''
        ops = [
            bitwise_operations.bit_subtract(self.five_255_bin, 4, 8, 255, False, aerospike.BIT_OVERFLOW_FAIL, None)
        ]

        self.as_connection.operate(self.test_key, ops)

        _, _, bins = self.as_connection.get(self.test_key)
        expected_result = bytearray([240] * 1 + [15] * 1 + [255] * 3)
        assert bins[self.five_255_bin] == expected_result

    def test_bit_subtract_multiple_bytes(self):
        '''
        Perform a bitwise subtract op with multiple bytes.
        '''
        ops = [
            bitwise_operations.bit_subtract(self.test_bin_ones, 8, 16, 257, False, aerospike.BIT_OVERFLOW_FAIL, None)
        ]

        self.as_connection.operate(self.test_key, ops)

        _, _, bins = self.as_connection.get(self.test_key)
        expected_result = bytearray([1] * 1 + [0] * 2 + [1] * 2)
        assert bins[self.test_bin_ones] == expected_result

    def test_bit_subtract_bad_bin_name(self):
        '''
        Perform a bitwise subtract op on a nonexistent bin.
        '''
        ops = [
            bitwise_operations.bit_subtract("bad_name", 8, 16, 257, False, aerospike.BIT_OVERFLOW_FAIL, None)
        ]

        with pytest.raises(e.BinNotFound):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_subtract_bit_offset_out_of_range(self):
        '''
        Perform a bitwise subtract op with a bit_offset that is out of range
        for the bitmap being modified.
        '''
        ops = [
            bitwise_operations.bit_subtract(self.test_bin_zeroes, 41, 1, 1, False, aerospike.BIT_OVERFLOW_FAIL, None)
        ]

        with pytest.raises(e.OpNotApplicable):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_subtract_bit_size_out_of_range(self):
        '''
        Perform a bitwise subtract op with a bit size too large for
        the bitmap being modified.
        '''
        ops = [
            bitwise_operations.bit_subtract(self.test_bin_zeroes, 0, 41, 1, False, aerospike.BIT_OVERFLOW_FAIL, None)
        ]

        with pytest.raises(e.OpNotApplicable):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_subtract_bit_size_signed(self):
        '''
        Perform a bitwise subtract op subtraction.
        '''
        ops = [
            bitwise_operations.bit_subtract(self.five_255_bin, 0, 8, 156, True, aerospike.BIT_OVERFLOW_FAIL, None)
        ]

        self.as_connection.operate(self.test_key, ops)

        _, _, bins = self.as_connection.get(self.test_key)
        expected_result = bytearray([99] * 1 + [255] * 4)
        assert bins[self.five_255_bin] == expected_result
    
    def test_bit_subtract_overflow_fail(self):
        '''
        Perform a bitwise subtract op that overflows with the BIT_OVERFLOW_FAIL action.
        '''
        ops = [
            bitwise_operations.bit_subtract(self.test_bin_ones, 0, 8, 255, False, aerospike.BIT_OVERFLOW_FAIL, None)
        ]

        with pytest.raises(e.OpNotApplicable):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_subtract_overflow_saturate(self):
        '''
        Perform a bitwise subtract op that overflows with the BIT_OVERFLOW_SATURATE action.
        '''
        ops = [
            bitwise_operations.bit_subtract(self.test_bin_ones, 0, 8, 255, False, aerospike.BIT_OVERFLOW_SATURATE, None)
        ]

        self.as_connection.operate(self.test_key, ops)

        _, _, bins = self.as_connection.get(self.test_key)
        expected_result = bytearray([255] * 5)
        assert bins[self.five_255_bin] == expected_result

    def test_bit_subtract_overflow_wrap(self):
        '''
        Perform a bitwise subtract op that overflows with the BIT_OVERFLOW_WRAP action.
        '''
        ops = [
            bitwise_operations.bit_subtract(self.five_255_bin, 0, 8, 1, False, aerospike.BIT_OVERFLOW_WRAP, None)
        ]

        self.as_connection.operate(self.test_key, ops)

        _, _, bins = self.as_connection.get(self.test_key)
        expected_result = bytearray([254] * 1 + [255] * 4)
        assert bins[self.five_255_bin] == expected_result

    def test_bit_xor(self):
        '''
        Perform a bitwise xor op.
        '''
        value = bytearray([1])
        ops = [
            bitwise_operations.bit_xor(self.test_bin_ones, 0, 8, 1, value, None)
        ]

        self.as_connection.operate(self.test_key, ops)

        _, _, bins = self.as_connection.get(self.test_key)
        expected_result = bytearray([0] * 1 + [1] * 4)
        assert bins[self.test_bin_ones] == expected_result

    def test_bit_xor_multiple_bytes(self):
        '''
        Perform a bitwise xor op on multiple bytes.
        '''
        value = bytearray([8] * 5)
        ops = [
            bitwise_operations.bit_xor(self.five_255_bin, 0, 40, 5, value, None)
        ]

        self.as_connection.operate(self.test_key, ops)

        _, _, bins = self.as_connection.get(self.test_key)
        expected_result = bytearray([247] * 5)
        assert bins[self.five_255_bin] == expected_result

    def test_bit_xor_multiple_bytes_value_unchanged(self):
        '''
        Perform a bitwise xor op.
        '''
        value = bytearray([0])
        ops = [
            bitwise_operations.bit_xor(self.test_bin_zeroes, 7, 8, 1, value, None)
        ]

        self.as_connection.operate(self.test_key, ops)

        _, _, bins = self.as_connection.get(self.test_key)
        expected_result = bytearray([0] * 5)
        assert bins[self.test_bin_zeroes] == expected_result

    def test_bit_xor_bit_offset_out_of_range(self):
        '''
        Perform a bitwise xor op with bit_offset > bitmap.
        '''
        value = bytearray()
        value.append(8)
        ops = [
            bitwise_operations.bit_xor(self.test_bin_ones, 41, 8, 1, value, None)
        ]

        with pytest.raises(e.OpNotApplicable):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_xor_bit_size_larger_than_value(self):
        '''
        Perform a bitwise xor op with bit_size > value.
        '''
        value = bytearray()
        value.append(8)
        ops = [
            bitwise_operations.bit_xor(self.test_bin_ones, 0, 9, 1, value, None)
        ]

        with pytest.raises(e.InvalidRequest):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_xor_bit_size_too_large(self):
        '''
        Perform a bitwise xor op with bit_size > bitmap.
        '''
        value = bytearray([8] * 6)
        ops = [
            bitwise_operations.bit_xor(self.test_bin_ones, 0, 41, 6, value, None)
        ]

        with pytest.raises(e.OpNotApplicable):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_xor_bad_bin_name(self):
        '''
        Perform a bitwise xor op with a non existent bin.
        '''
        value = bytearray([8])
        ops = [
            bitwise_operations.bit_xor("bad_name", 0, 8, 1, value, None)
        ]

        with pytest.raises(e.BinNotFound):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_xor_bad_arg(self):
        '''
        Perform a bitwise xor op with an integer for value.
        '''
        value = 1
        ops = [
            bitwise_operations.bit_xor("bad_name", 0, 8, 1, value, None)
        ]

        with pytest.raises(e.ParamError):
            self.as_connection.operate(self.test_key, ops)

    def test_bit_xor_with_policy(self):
        '''
        Perform a bitwise xor op with a policy.
        '''
        value = bytearray([0])
        bit_policy = {
            'bit_write_flags': aerospike.BIT_WRITE_UPDATE_ONLY,
        }
        ops = [
            bitwise_operations.bit_xor(self.test_bin_zeroes, 7, 8, 1, value, bit_policy)
        ]

        self.as_connection.operate(self.test_key, ops)

        _, _, bins = self.as_connection.get(self.test_key)
        expected_result = bytearray([0] * 5)
        assert bins[self.test_bin_zeroes] == expected_result