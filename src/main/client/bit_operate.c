/*******************************************************************************
 * Copyright 2013-2019 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

#include <Python.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <aerospike/as_operations.h>

#include "bit_operations.h"
#include "client.h"
#include "cdt_operation_utils.h"
#include "conversions.h"
#include "exceptions.h"
#include "policy.h"
#include "serializer.h"

#define BIN_KEY "bin"
#define BYTE_SIZE_KEY "byte_size"
#define BYTE_OFFSET_KEY "byte_offset"
#define BIT_OFFSET_KEY "bit_offset"
#define BIT_SIZE_KEY "bit_size"
#define VALUE_BYTE_SIZE_KEY "value_byte_size"
#define VALUE_KEY "value"
#define COUNT_KEY "count"
#define OFFSET_KEY "offset"
#define OP_KEY "op"
#define POLICY_KEY "policy"
#define SIGN_KEY "sign"
#define ACTION_KEY "action"
#define RESIZE_FLAGS_KEY "resize_flags"

//Dictionary field extraction functions

static as_status get_bit_policy(as_error *err, PyObject *op_dict,
                                as_bit_policy *policy, bool validate_keys);

static as_status get_bit_resize_flags(as_error *err, PyObject *op_dict,
                                      as_bit_resize_flags *resize_flags);

static as_status get_uint8t_from_pyargs(as_error *err, char *key,
                                        PyObject *op_dict, uint8_t **value);

static as_status get_uint32t_from_pyargs(as_error *err, char *key,
                                         PyObject *op_dict, uint32_t *value);

extern const char *op_code_to_names[];

// End forwards
as_status add_new_bit_op(AerospikeClient *self, as_error *err,
                         PyObject *op_dict, as_vector *unicodeStrVector,
                         as_static_pool *static_pool, as_operations *ops,
                         long operation_code, long *ret_type,
                         int serializer_type)

{
    char *bin = NULL;

    if (get_bin(err, op_dict, unicodeStrVector, &bin) != AEROSPIKE_OK) {
        goto exit;
    }

    bool bool_value = false;
    switch (operation_code) {
    case OP_BIT_ADD:
    case OP_BIT_SUBTRACT:
    case OP_BIT_GET_INT:
    case OP_BIT_LSCAN:
    case OP_BIT_RSCAN: {

        char *bool_key = VALUE_KEY;
        switch (operation_code) {
        case OP_BIT_ADD:
        case OP_BIT_SUBTRACT:
        case OP_BIT_GET_INT:
            bool_key = SIGN_KEY;
            break;
        }

        if (get_bool_from_pyargs(err, bool_key, op_dict, &bool_value) !=
            AEROSPIKE_OK) {
            goto exit;
        }
        break;
    }
    }

    as_bit_policy bit_policy;
    switch (operation_code) {
    case OP_BIT_RESIZE:
    case OP_BIT_SET:
    case OP_BIT_SET_INT:
    case OP_BIT_REMOVE:
    case OP_BIT_ADD:
    case OP_BIT_AND:
    case OP_BIT_GET:
    case OP_BIT_GET_INT:
    case OP_BIT_INSERT:
    case OP_BIT_LSHIFT:
    case OP_BIT_NOT:
    case OP_BIT_OR:
    case OP_BIT_RSHIFT:
    case OP_BIT_SUBTRACT:
    case OP_BIT_XOR:
        if (get_bit_policy(err, op_dict, &bit_policy, self->validate_keys) !=
            AEROSPIKE_OK) {
            goto exit;
        }
        break;
    }

    as_bit_resize_flags flags = AS_BIT_RESIZE_DEFAULT;
    if (operation_code == OP_BIT_RESIZE &&
        get_bit_resize_flags(err, op_dict, &flags) != AEROSPIKE_OK) {
        goto exit;
    }

    uint32_t byte_size = 0;
    switch (operation_code) {
    case OP_BIT_RESIZE:
    case OP_BIT_REMOVE:
        if (get_uint32t_from_pyargs(err, BYTE_SIZE_KEY, op_dict, &byte_size) !=
            AEROSPIKE_OK) {
            goto exit;
        }
        break;
    }

    int64_t bit_offset = 0;
    uint32_t bit_size = 0;
    switch (operation_code) {
    case OP_BIT_SET:
    case OP_BIT_SET_INT:
    case OP_BIT_COUNT:
    case OP_BIT_ADD:
    case OP_BIT_AND:
    case OP_BIT_GET:
    case OP_BIT_GET_INT:
    case OP_BIT_LSCAN:
    case OP_BIT_LSHIFT:
    case OP_BIT_NOT:
    case OP_BIT_OR:
    case OP_BIT_RSCAN:
    case OP_BIT_RSHIFT:
    case OP_BIT_SUBTRACT:
    case OP_BIT_XOR:
        if (get_int64_t(err, BIT_OFFSET_KEY, op_dict, &bit_offset) !=
            AEROSPIKE_OK) {
            goto exit;
        }

        if (get_uint32t_from_pyargs(err, BIT_SIZE_KEY, op_dict, &bit_size) !=
            AEROSPIKE_OK) {
            goto exit;
        }
        break;
    }

    uint32_t value_byte_size = 0;
    uint8_t *uint8_array_value = NULL;
    switch (operation_code) {
    case OP_BIT_SET:
    case OP_BIT_AND:
    case OP_BIT_INSERT:
    case OP_BIT_OR:
    case OP_BIT_XOR:
        if (get_uint32t_from_pyargs(err, VALUE_BYTE_SIZE_KEY, op_dict,
                                    &value_byte_size) != AEROSPIKE_OK) {
            goto exit;
        }

        if (get_uint8t_from_pyargs(err, VALUE_KEY, op_dict,
                                   &uint8_array_value) != AEROSPIKE_OK) {
            goto exit;
        }
        break;
    }

    as_bit_overflow_action action = AS_BIT_OVERFLOW_FAIL;
    switch (operation_code) {
    case OP_BIT_ADD:
    case OP_BIT_SUBTRACT: {
        int64_t action_int64;
        if (get_int64_t(err, ACTION_KEY, op_dict, &action_int64) !=
            AEROSPIKE_OK) {
            goto exit;
        }
        action = action_int64;
        break;
    }
    }

    int64_t int64_value = 0;
    switch (operation_code) {
    case OP_BIT_SET_INT:
        if (get_int64_t(err, VALUE_KEY, op_dict, &int64_value) !=
            AEROSPIKE_OK) {
            goto exit;
        }
    }

    uint64_t uint64_value = 0;
    switch (operation_code) {
    case OP_BIT_ADD:
    case OP_BIT_SUBTRACT:
        if (get_uint64_t(err, VALUE_KEY, op_dict, &uint64_value) !=
            AEROSPIKE_OK) {
            goto exit;
        }
    }

    uint32_t shift = 0;
    switch (operation_code) {
    case OP_BIT_LSHIFT:
    case OP_BIT_RSHIFT:
        if (get_uint32t_from_pyargs(err, VALUE_KEY, op_dict, &shift) !=
            AEROSPIKE_OK) {
            goto exit;
        }
    }

    int64_t byte_offset = 0;
    switch (operation_code) {
    case OP_BIT_REMOVE:
    case OP_BIT_INSERT:
        if (get_int64_t(err, BYTE_OFFSET_KEY, op_dict, &byte_offset) !=
            AEROSPIKE_OK) {
            goto exit;
        }
        break;
    }

    bool success = false;
    switch (operation_code) {
    case OP_BIT_RESIZE:
        success = as_operations_bit_resize(ops, bin, NULL, &bit_policy,
                                           byte_size, flags);
        break;
    case OP_BIT_SET:
        success =
            as_operations_bit_set(ops, bin, NULL, &bit_policy, bit_offset,
                                  bit_size, value_byte_size, uint8_array_value);
        break;
    case OP_BIT_SET_INT:
        success = as_operations_bit_set_int(ops, bin, NULL, &bit_policy,
                                            bit_offset, bit_size, int64_value);
        break;
    case OP_BIT_REMOVE:
        success = as_operations_bit_remove(ops, bin, NULL, &bit_policy,
                                           byte_offset, byte_size);
        break;
    case OP_BIT_COUNT:
        success = as_operations_bit_count(ops, bin, NULL, bit_offset, bit_size);
        break;
    case OP_BIT_ADD:
        success =
            as_operations_bit_add(ops, bin, NULL, &bit_policy, bit_offset,
                                  bit_size, uint64_value, bool_value, action);
        break;
    case OP_BIT_AND:
        success =
            as_operations_bit_and(ops, bin, NULL, &bit_policy, bit_offset,
                                  bit_size, value_byte_size, uint8_array_value);
        break;
    case OP_BIT_GET:
        success = as_operations_bit_get(ops, bin, NULL, bit_offset, bit_size);
        break;
    case OP_BIT_GET_INT:
        success = as_operations_bit_get_int(ops, bin, NULL, bit_offset,
                                            bit_size, bool_value);
        break;
    case OP_BIT_INSERT:
        success =
            as_operations_bit_insert(ops, bin, NULL, &bit_policy, byte_offset,
                                     value_byte_size, uint8_array_value);
        break;
    case OP_BIT_LSCAN:
        success = as_operations_bit_lscan(ops, bin, NULL, bit_offset, bit_size,
                                          bool_value);
        break;
    case OP_BIT_LSHIFT:
        success = as_operations_bit_lshift(ops, bin, NULL, &bit_policy,
                                           bit_offset, bit_size, shift);
        break;
    case OP_BIT_NOT:
        success = as_operations_bit_not(ops, bin, NULL, &bit_policy, bit_offset,
                                        bit_size);
        break;
    case OP_BIT_OR:
        success =
            as_operations_bit_or(ops, bin, NULL, &bit_policy, bit_offset,
                                 bit_size, value_byte_size, uint8_array_value);
        break;
    case OP_BIT_RSCAN:
        success = as_operations_bit_rscan(ops, bin, NULL, bit_offset, bit_size,
                                          bool_value);
        break;
    case OP_BIT_RSHIFT:
        success = as_operations_bit_rshift(ops, bin, NULL, &bit_policy,
                                           bit_offset, bit_size, shift);
        break;
    case OP_BIT_SUBTRACT:
        success = as_operations_bit_subtract(ops, bin, NULL, &bit_policy,
                                             bit_offset, bit_size, uint64_value,
                                             bool_value, action);
        break;
    case OP_BIT_XOR:
        success =
            as_operations_bit_xor(ops, bin, NULL, &bit_policy, bit_offset,
                                  bit_size, value_byte_size, uint8_array_value);
        break;
    default:
        // This should never be possible since we only get here if we know that the operation is valid.
        as_error_update(err, AEROSPIKE_ERR_PARAM, "Unknown operation");
        goto exit;
    }

    if (!success) {
        as_error_update(err, AEROSPIKE_ERR_CLIENT, "Failed to add %s operation",
                        op_code_to_names[operation_code]);
    }

exit:
    return err->code;
}
static as_status get_bit_resize_flags(as_error *err, PyObject *op_dict,
                                      as_bit_resize_flags *resize_flags)
{
    int64_t flags64;
    bool found = false;
    *resize_flags = AS_BIT_RESIZE_DEFAULT;

    if (get_optional_int64_t(err, RESIZE_FLAGS_KEY, op_dict, &flags64,
                             &found) != AEROSPIKE_OK) {
        return err->code;
    }
    if (found) {
        *resize_flags = (as_bit_resize_flags)flags64;
    }

    return AEROSPIKE_OK;
}

static as_status get_bit_policy(as_error *err, PyObject *op_dict,
                                as_bit_policy *policy, bool validate_keys)
{
    PyObject *py_bit_policy = PyDict_GetItemString(op_dict, POLICY_KEY);

    // This handles a null policy
    if (pyobject_to_bit_policy(err, py_bit_policy, policy, validate_keys) !=
        AEROSPIKE_OK) {
        return err->code;
    }

    return AEROSPIKE_OK;
}

static as_status get_uint8t_from_pyargs(as_error *err, char *key,
                                        PyObject *op_dict, uint8_t **value)
{
    PyObject *py_val = PyDict_GetItemString(op_dict, key);
    if (!py_val) {
        return as_error_update(err, AEROSPIKE_ERR_PARAM, "Failed to convert %s",
                               key)
    }

    if (PyBytes_Check(py_val)) {
        *value = (uint8_t *)PyBytes_AsString(py_val);
        if (PyErr_Occurred()) {
            return as_error_update(err, AEROSPIKE_ERR_PARAM,
                                   "Failed to convert %s", key);
        }
    }
    else if (PyByteArray_Check(py_val)) {
        *value = (uint8_t *)PyByteArray_AsString(py_val);
        if (PyErr_Occurred()) {
            return as_error_update(err, AEROSPIKE_ERR_PARAM,
                                   "Failed to convert %s", key);
        }
    }
    else {
        return as_error_update(err, AEROSPIKE_ERR_PARAM,
                               "%s must be bytes or byte array", key);
    }

    return AEROSPIKE_OK;
}

static as_status get_uint32t_from_pyargs(as_error *err, char *key,
                                         PyObject *op_dict, uint32_t *value)
{
    int64_t value64 = 0;

    if (get_int64_t(err, key, op_dict, &value64) != AEROSPIKE_OK) {
        return err->code;
    }

    if (value64 < 0 || value64 > UINT32_MAX) {
        return as_error_update(err, AEROSPIKE_ERR_PARAM,
                               "%s is not a valid uint32", key);
    }

    *value = (uint32_t)value64;
    return AEROSPIKE_OK;
}
