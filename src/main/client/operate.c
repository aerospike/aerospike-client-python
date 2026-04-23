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
#include <aerospike/aerospike_key.h>
#include <aerospike/as_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_record.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_map_operations.h>
#include <aerospike/aerospike_info.h>
#include "client.h"
#include "conversions.h"
#include "exceptions.h"
#include "policy.h"
#include "serializer.h"
#include "geo.h"
#include "cdt_list_operations.h"
#include "cdt_map_operations.h"
#include "bit_operations.h"
#include "hll_operations.h"
#include "pythoncapi_compat.h"
#include "expression_operations.h"

#include <aerospike/as_double.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_geojson.h>
#include <aerospike/as_nil.h>

static void get_op_code_from_py_op_dict(as_error *err, PyObject *op_dict,
                                        long *op_code_ref);

static inline bool isNewMapOp(int op);
static inline bool isBitOp(int op);
static inline bool isHllOp(int op);
static inline bool isExprOp(int op);

#define PY_OPERATION_KEY "op"

#define BASE_VARIABLES                                                         \
    as_error err;                                                              \
    as_error_init(&err);                                                       \
    PyObject *py_key = NULL;                                                   \
    PyObject *py_policy = NULL;                                                \
    PyObject *py_result = NULL;                                                \
    PyObject *py_meta = NULL;                                                  \
    as_key key;

#define CHECK_CONNECTED(__err)                                                 \
    if (!self || !self->as) {                                                  \
        as_error_update(__err, AEROSPIKE_ERR_PARAM,                            \
                        "Invalid aerospike object");                           \
        goto CLEANUP;                                                          \
    }                                                                          \
    if (!self->is_conn_16) {                                                   \
        as_error_update(__err, AEROSPIKE_ERR_CLUSTER,                          \
                        "No connection to aerospike cluster");                 \
        goto CLEANUP;                                                          \
    }

#define EXCEPTION_ON_ERROR()                                                   \
    if (err.code != AEROSPIKE_OK) {                                            \
        raise_exception_base(&err, py_key, py_bin, Py_None, Py_None, Py_None); \
        return NULL;                                                           \
    }

#define DECREF_LIST_AND_RESULT()                                               \
    if (py_list) {                                                             \
        Py_DECREF(py_list);                                                    \
    }                                                                          \
    if (err.code == AEROSPIKE_OK) {                                            \
        if (!py_result) {                                                      \
            return NULL;                                                       \
        }                                                                      \
        else {                                                                 \
            Py_DECREF(py_result);                                              \
        }                                                                      \
    }

#define CONVERT_KEY_TO_AS_VAL()                                                \
    if (as_val_new_from_pyobject(self, err, py_key, &put_key, static_pool,     \
                                 SERIALIZER_PYTHON) != AEROSPIKE_OK) {         \
        return err->code;                                                      \
    }

#define CONVERT_PY_CTX_TO_AS_CTX()                                             \
    if (get_cdt_ctx(self, err, &ctx, py_operation_dict, &ctx_in_use,           \
                    static_pool, SERIALIZER_PYTHON) != AEROSPIKE_OK) {         \
        return err->code;                                                      \
    }

#define CONVERT_RANGE_TO_AS_VAL()                                              \
    if (as_val_new_from_pyobject(self, err, py_range, &put_range, static_pool, \
                                 SERIALIZER_PYTHON) != AEROSPIKE_OK) {         \
        return err->code;                                                      \
    }

static as_status invertIfSpecified(as_error *err, PyObject *op_dict,
                                   uint64_t *return_value);
/**
 *******************************************************************************************************
 * This function will check whether operation can be performed
 * based on operation and value type.
 *
 * @param py_list               The List.
 * @param operation             The operation to perform.
 * @param py_bin                The bin_name name to perform operation.
 * @param py_value              The value to perform operation.
 * @param py_initial_val        The initial value for increment operation.
 *
 * Returns 0 if operation can be performed.
 *******************************************************************************************************
 */
PyObject *create_pylist(PyObject *py_list, long operation, PyObject *py_bin,
                        PyObject *py_value)
{
    PyObject *dict = PyDict_New();
    py_list = PyList_New(0);
    PyDict_SetItemString(dict, "op", PyLong_FromLong(operation));
    if (operation != AS_OPERATOR_TOUCH) {
        PyDict_SetItemString(dict, "bin_name", py_bin);
    }
    PyDict_SetItemString(dict, "val", py_value);

    PyList_Append(py_list, dict);
    Py_DECREF(dict);

    return py_list;
}

/**
 *******************************************************************************************************
 * This function will check whether operation can be performed
 * based on operation and value type.
 *
 * @param py_value              The value to perform operations.
 * @param op                    The operation to perform.
 *
 * Returns 0 if operation can be performed.
 *******************************************************************************************************
 */
int check_type(AerospikeClient *self, PyObject *py_value, int op, as_error *err)
{
    if ((!PyLong_Check(py_value) &&
         strcmp(py_value->ob_type->tp_name, "aerospike.null")) &&
        (op == AS_OPERATOR_TOUCH)) {
        as_error_update(
            err, AEROSPIKE_ERR_PARAM,
            "Unsupported operand type(s) for touch : only int or long allowed");
        return 1;
    }
    else if ((!PyLong_Check(py_value) && (!PyFloat_Check(py_value)) &&
              strcmp(py_value->ob_type->tp_name, "aerospike.null")) &&
             op == AS_OPERATOR_INCR) {
        as_error_update(
            err, AEROSPIKE_ERR_PARAM,
            "Unsupported operand type(s) for +: only 'int' allowed");
        return 1;
    }
    else if ((!PyUnicode_Check(py_value) && !PyByteArray_Check(py_value) &&
              !PyBytes_Check(py_value) &&
              strcmp(py_value->ob_type->tp_name, "aerospike.null")) &&
             (op == AS_OPERATOR_APPEND || op == AS_OPERATOR_PREPEND)) {
        as_error_update(err, AEROSPIKE_ERR_PARAM,
                        "Cannot concatenate 'str' and 'non-str' objects");
        return 1;
    }
    else if (!PyList_Check(py_value) && op == OP_LIST_APPEND_ITEMS) {
        as_error_update(err, AEROSPIKE_ERR_PARAM,
                        "Value of list_append_items should be of type list");
        return 1;
    }
    else if (!PyList_Check(py_value) && op == OP_LIST_INSERT_ITEMS) {
        as_error_update(err, AEROSPIKE_ERR_PARAM,
                        "Value of list_insert_items should be of type list");
        return 1;
    }
    return 0;
}

static inline bool isNewMapOp(int op)
{
    return (op == OP_MAP_REMOVE_BY_KEY_INDEX_RANGE_REL ||
            op == OP_MAP_REMOVE_BY_VALUE_RANK_RANGE_REL ||
            op == OP_MAP_GET_BY_VALUE_RANK_RANGE_REL ||
            op == OP_MAP_GET_BY_KEY_INDEX_RANGE_REL);
}

static inline bool isBitOp(int op)
{
    int bit_start = OP_BIT_RESIZE;
    int bit_end = OP_BIT_RSCAN;
    return (op >= bit_start && op <= bit_end);
}

static inline bool isHllOp(int op)
{
    int hll_start = OP_HLL_ADD;
    int hll_end = OP_HLL_SET_UNION;
    return (op >= hll_start && op <= hll_end);
}

static inline bool isExprOp(int op)
{
    int expr_start = OP_EXPR_READ;
    int expr_end = OP_EXPR_WRITE;
    return (op >= expr_start && op <= expr_end);
}

bool opRequiresIndex(int op)
{
    return (op == OP_LIST_INSERT || op == OP_LIST_INSERT_ITEMS ||
            op == OP_LIST_POP || op == OP_LIST_POP_RANGE ||
            op == OP_LIST_REMOVE || op == OP_LIST_REMOVE_RANGE ||
            op == OP_LIST_SET || op == OP_LIST_GET || op == OP_LIST_GET_RANGE ||
            op == OP_LIST_TRIM || op == OP_MAP_REMOVE_BY_INDEX ||
            op == OP_MAP_REMOVE_BY_RANK || op == OP_MAP_REMOVE_BY_RANK_RANGE ||
            op == OP_MAP_GET_BY_INDEX || op == OP_MAP_GET_BY_INDEX_RANGE ||
            op == OP_MAP_GET_BY_RANK || op == OP_MAP_GET_BY_RANK_RANGE ||
            op == OP_MAP_REMOVE_BY_INDEX_RANGE || op == OP_LIST_INCREMENT);
}

// TODO: Can be optimized with bitwise operations?
bool opRequiresValue(int op)
{
    return (op != AS_OPERATOR_READ && op != AS_OPERATOR_TOUCH &&
            op != OP_LIST_POP && op != OP_LIST_REMOVE && op != OP_LIST_CLEAR &&
            op != OP_LIST_GET && op != OP_LIST_SIZE &&
            op != OP_MAP_GET_BY_KEY && op != OP_MAP_SET_POLICY &&
            op != OP_MAP_SIZE && op != OP_MAP_CLEAR &&
            op != OP_MAP_REMOVE_BY_KEY && op != OP_MAP_REMOVE_BY_INDEX &&
            op != OP_MAP_REMOVE_BY_RANK && op != OP_MAP_GET_BY_KEY &&
            op != OP_MAP_GET_BY_INDEX && op != OP_MAP_GET_BY_KEY_RANGE &&
            op != OP_MAP_GET_BY_RANK && op != AS_OPERATOR_DELETE &&
            op != OP_MAP_CREATE && op != OP_LIST_CREATE &&
            op != AS_OPERATOR_CDT_READ && op != AS_OPERATOR_CDT_MODIFY);
}

bool opRequiresRange(int op)
{
    return (op == OP_MAP_REMOVE_BY_VALUE_RANGE ||
            op == OP_MAP_GET_BY_VALUE_RANGE || op == OP_MAP_GET_BY_KEY_RANGE);
}

bool opReturnsResult(int op)
{
    return (op == AS_OPERATOR_READ || op == OP_LIST_APPEND ||
            op == OP_LIST_SIZE || op == OP_LIST_APPEND_ITEMS ||
            op == OP_LIST_REMOVE || op == OP_LIST_REMOVE_RANGE ||
            op == OP_LIST_TRIM || op == OP_LIST_CLEAR || op == OP_LIST_GET ||
            op == OP_LIST_GET_RANGE || op == OP_LIST_INSERT ||
            op == OP_LIST_INSERT_ITEMS || op == OP_LIST_POP ||
            op == OP_LIST_POP_RANGE || op == OP_LIST_SET ||
            op == OP_MAP_GET_BY_KEY || op == OP_MAP_GET_BY_KEY_RANGE ||
            op == OP_LIST_INCREMENT);
}

bool opRequiresMapPolicy(int op) { return (op == OP_MAP_SET_POLICY); }

bool opRequiresKey(int op)
{
    return (op == OP_MAP_PUT || op == OP_MAP_INCREMENT ||
            op == OP_MAP_DECREMENT || op == OP_MAP_REMOVE_BY_KEY ||
            op == OP_MAP_REMOVE_BY_KEY_RANGE || op == OP_MAP_GET_BY_KEY ||
            op == OP_MAP_GET_BY_KEY_RANGE);
}

static bool op_requires_bin_name[] = {[AS_OPERATOR_READ] = true,
                                      [AS_OPERATOR_WRITE] = true,
                                      [AS_OPERATOR_INCR] = true,
                                      [AS_OPERATOR_APPEND] = true,
                                      [AS_OPERATOR_PREPEND] = true,
                                      // Last element of enum
                                      [AS_OPERATOR_HLL_MODIFY] = false};

static bool op_requires_value[] = {[AS_OPERATOR_WRITE] = true,
                                   // TODO
                                   [AS_OPERATOR_HLL_MODIFY] = false};

as_status add_op(AerospikeClient *self, as_error *err, PyObject *py_op_dict,
                 as_vector *unicodeStrVector, as_static_pool *static_pool,
                 as_operations *ops, long *op, long *ret_type)
{
    as_val *put_key = NULL;
    as_val *put_range = NULL;
    as_cdt_ctx ctx;
    as_cdt_ctx *ctx_ref = NULL;
    bool ctx_in_use = false;
    char *val = NULL;
    long offset = 0;
    long ttl = 0;
    double double_offset = 0.0;
    int index = 0;
    long op_code = 0;
    uint64_t return_type = AS_MAP_RETURN_NONE;
    PyObject *py_ustr = NULL;
    PyObject *py_ustr1 = NULL;
    PyObject *py_bin = NULL;
    as_exp *mod_exp = NULL;

    PyObject *py_dict_key = NULL, *py_dict_value = NULL;
    PyObject *py_value = NULL;
    PyObject *py_key = NULL;
    PyObject *py_index = NULL;
    PyObject *py_range = NULL;
    PyObject *py_map_policy = NULL;
    PyObject *py_return_type = NULL;
    // For map_create operation
    PyObject *py_map_order = NULL;
    PyObject *py_persist_index = NULL;

    Py_ssize_t pos = 0;

    if (!PyDict_Check(py_op_dict)) {
        as_error_update(err, AEROSPIKE_ERR_PARAM,
                        "Op is not a Python dictionary");
        return err->code;
    }

    // Get required op dictionary entries
    if (get_op_code_from_py_op_dict(err, py_op_dict, &op_code)) {
        return err->code;
    }

    const char *bin_name = NULL;
    if (op_requires_bin_name[op_code] == true) {
        PyObject *py_key_for_bin_name = PyUnicode_FromString("bin_name");
        if (py_key_for_bin_name == NULL) {
            return NULL;
        }

        PyObject *py_bin_name =
            PyDict_GetItemWithError(py_op_dict, py_key_for_bin_name);
        Py_DECREF(py_key_for_bin_name);
        if (py_bin_name == NULL) {
            if (!PyErr_Occurred()) {
                // Key not found
                return as_error_update(err, AEROSPIKE_ERR_PARAM,
                                       "This operation requires a bin_name");
            }
            else {
                return NULL;
            }
        }

        // Should be valid as long as dictionary and the bin_name entry exists
        bin_name = PyUnicode_AsUTF8(py_bin_name);
        if (bin_name == NULL) {
            return NULL;
        }
    }

    as_val *put_val = NULL;
    if (op_requires_value[op_code]) {
        if (pyobject_to_val(self, err, py_value, &put_val, static_pool,
                            SERIALIZER_PYTHON) != AEROSPIKE_OK) {
            return err->code;
        }
    }

    // TODO: use python hashset?
    const char *valid_keys[] = {"op",  "bin_name", "index",
                                "key", "val",      "return_type"};
    while (PyDict_Next(py_op_dict, &pos, &py_dict_key, &py_dict_value)) {
        if (!PyUnicode_Check(py_dict_key)) {
            return as_error_update(err, AEROSPIKE_ERR_CLIENT,
                                   "An operation key must be a string.");
        }

        const char *dict_key = PyUnicode_AsUTF8(py_dict_key);
        for (unsigned long i = 0;
             i < sizeof(valid_keys) / sizeof(valid_keys[0]); i++) {
            if (strcmp(dict_key, valid_keys[i])) {
                as_error_update(
                    err, AEROSPIKE_ERR_PARAM,
                    "Operation can contain only op, bin_name, index, key, val, "
                    "return_type and map_policy keys");
                goto CLEANUP;
            }
        }
    }

    // No way to define an array of function pointers with differing arguments
    switch (op_code) {
    // TODO
    case AS_OPERATOR_TOUCH:
        if (py_value) {
            if (pyobject_to_index(self, err, py_value, &ttl) != AEROSPIKE_OK) {
                goto CLEANUP;
            }
            ops->ttl = ttl;
        }
        as_operations_add_touch(ops);
        break;
    case AS_OPERATOR_READ:
        as_operations_add_read(ops, bin_name);
        break;
    case AS_OPERATOR_DELETE:
        as_operations_add_delete(ops);
        break;
    case AS_OPERATOR_WRITE:
        CONVERT_VAL_TO_AS_VAL();
        as_operations_add_write(ops, bin_name, (as_bin_value *)put_val);
        break;
    }

    /* Handle the list operations with a helper in the cdt_list_operate.c file */
    if (op_code >= OP_LIST_APPEND && op_code <= OP_LIST_CREATE) {
        return add_new_list_op(
            self, err, py_op_dict, unicodeStrVector, static_pool, ops, op_code,
            ret_type,
            SERIALIZER_PYTHON); //This hardcoding matches current behavior
    }

    if (op_code >= OP_MAP_SET_POLICY && op <= OP_MAP_CREATE) {
        return add_new_map_op(self, err, py_op_dict, unicodeStrVector,
                              static_pool, ops, op_code, ret_type,
                              SERIALIZER_PYTHON);
    }

    if (isBitOp(op_code)) {
        return add_new_bit_op(self, err, py_op_dict, unicodeStrVector,
                              static_pool, ops, op_code, ret_type,
                              SERIALIZER_PYTHON);
    }

    if (isHllOp(op_code)) {
        return add_new_hll_op(self, err, py_op_dict, unicodeStrVector,
                              static_pool, ops, op_code, ret_type,
                              SERIALIZER_PYTHON);
    }

    if (isExprOp(op_code)) {
        return add_new_expr_op(self, err, py_op_dict, unicodeStrVector, ops,
                               op_code, SERIALIZER_PYTHON);
    }

    *op = op_code;

    if (py_bin) {
        if (PyUnicode_Check(py_bin)) {
            py_ustr = PyUnicode_AsUTF8String(py_bin);
            bin_name = strdup(PyBytes_AsString(py_ustr));
            as_vector_append(unicodeStrVector, &bin_name);
            Py_DECREF(py_ustr);
        }
        else if (PyUnicode_Check(py_bin)) {
            bin_name = (char *)PyUnicode_AsUTF8(py_bin);
        }
        else if (PyByteArray_Check(py_bin)) {
            bin_name = PyByteArray_AsString(py_bin);
        }
        else {
            as_error_update(err, AEROSPIKE_ERR_PARAM,
                            "Bin name should be of type string");
            goto CLEANUP;
        }

        if (self->strict_types) {
            if (strlen(bin_name) > AS_BIN_NAME_MAX_LEN) {
                as_error_update(
                    err, AEROSPIKE_ERR_BIN_NAME,
                    "A bin_name name should not exceed 15 characters limit");
                goto CLEANUP;
            }
        }
    }
    else if (op_code != AS_OPERATOR_TOUCH && op_code != AS_OPERATOR_DELETE) {
        as_error_update(err, AEROSPIKE_ERR_PARAM, "Bin is not given");
        goto CLEANUP;
    }

    if (py_value) {
        if (self->strict_types) {
            if (check_type(self, py_value, op_code, err)) {
                goto CLEANUP;
            }
        }
    }
    else if (opRequiresValue(op_code)) {
        as_error_update(err, AEROSPIKE_ERR_PARAM, "Value should be given");
        goto CLEANUP;
    }

    if (!py_key && opRequiresKey(op_code)) {
        return as_error_update(err, AEROSPIKE_ERR_PARAM,
                               "Operation requires key parameter");
        goto CLEANUP;
    }

    as_map_policy map_policy;
    as_map_policy_init(&map_policy);
    if (py_map_policy) {
        if (pyobject_to_map_policy(err, py_map_policy, &map_policy,
                                   self->validate_keys) != AEROSPIKE_OK) {
            goto CLEANUP;
        }
    }
    else if (opRequiresMapPolicy(op_code)) {
        as_error_update(err, AEROSPIKE_ERR_PARAM,
                        "Operation requires map_policy parameter");
        goto CLEANUP;
    }

    if (!py_range && opRequiresRange(op_code)) {
        as_error_update(err, AEROSPIKE_ERR_PARAM, "Range should be given");
        goto CLEANUP;
    }

    if (py_return_type) {
        if (!PyLong_Check(py_return_type)) {
            as_error_update(err, AEROSPIKE_ERR_PARAM,
                            "Return type should be an integer");
            goto CLEANUP;
        }
        return_type = PyLong_AsLong(py_return_type);
    }

    /* Add the inverted flag to the return type if it's present */
    if (invertIfSpecified(err, py_op_dict, &return_type) != AEROSPIKE_OK) {
        goto CLEANUP;
    }

    if (err->code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

    *ret_type = return_type;

    if (py_index) {
        if (self->strict_types && !opRequiresIndex(op_code)) {
            as_error_update(err, AEROSPIKE_ERR_PARAM,
                            "Operation does not need an index value");
            goto CLEANUP;
        }
        if (PyLong_Check(py_index)) {
            index = PyLong_AsLong(py_index);
        }
        else {
            as_error_update(err, AEROSPIKE_ERR_PARAM,
                            "Index should be an integer");
            goto CLEANUP;
        }
    }
    else if (opRequiresIndex(op_code)) {
        as_error_update(err, AEROSPIKE_ERR_PARAM,
                        "Operation needs an index value");
        goto CLEANUP;
    }

    switch (op_code) {
    case AS_OPERATOR_CDT_READ:
    case AS_OPERATOR_CDT_MODIFY: {
        PyObject *py_flags = NULL;
        int retval =
            PyDict_GetItemStringRef(py_op_dict, _CDT_FLAGS_KEY, &py_flags);
        if (retval == 0) {
            as_error_update(err, AEROSPIKE_ERR_PARAM,
                            "CDT operation is missing a flags argument");
            goto CLEANUP;
        }
        else if (retval == -1) {
            as_error_update(err, AEROSPIKE_ERR_CLIENT, "Internal error");
            goto CLEANUP;
        }

        uint32_t flags = convert_pyobject_to_uint32_t(py_flags);
        Py_DECREF(py_flags);
        if (PyErr_Occurred()) {
            as_error_update(err, AEROSPIKE_ERR_PARAM,
                            "CDT operation's flags argument is invalid");
            goto CLEANUP;
        }

        if (op_code == AS_OPERATOR_CDT_READ) {
            as_operations_select_by_path(err, ops, bin_name, ctx_ref, flags);
        }
        else if (op_code == AS_OPERATOR_CDT_MODIFY) {
            PyObject *py_expr = NULL;
            int retval = PyDict_GetItemStringRef(
                py_op_dict, _CDT_APPLY_MOD_EXP_KEY, &py_expr);
            if (retval == 0) {
                as_error_update(err, AEROSPIKE_ERR_PARAM,
                                "CDT operation is missing a flags argument");
                goto CLEANUP;
            }
            else if (retval == -1) {
                as_error_update(err, AEROSPIKE_ERR_CLIENT, "Internal error");
                goto CLEANUP;
            }

            as_status status =
                as_exp_new_from_pyobject(self, py_expr, &mod_exp, err, false);
            Py_DECREF(py_expr);
            if (status != AEROSPIKE_OK) {
                goto CLEANUP;
            }

            as_operations_modify_by_path(err, ops, bin_name, ctx_ref, mod_exp,
                                         flags);
        }

        break;
    }
    case AS_OPERATOR_APPEND:
        if (PyUnicode_Check(py_value)) {
            py_ustr1 = PyUnicode_AsUTF8String(py_value);
            val = strdup(PyBytes_AsString(py_ustr1));
            as_operations_add_append_str(ops, bin_name, val);
            as_vector_append(unicodeStrVector, &val);
            Py_DECREF(py_ustr1);
        }
        else if (PyByteArray_Check(py_value) || PyBytes_Check(py_value)) {
            as_bytes *bytes;
            GET_BYTES_POOL(bytes, static_pool, err);
            if (err->code == AEROSPIKE_OK) {
                if (serialize_based_on_serializer_policy(
                        self, SERIALIZER_PYTHON, &bytes, py_value, err) !=
                    AEROSPIKE_OK) {
                    goto CLEANUP;
                }
                as_operations_add_append_rawp(ops, bin_name, bytes->value,
                                              bytes->size, true);
            }
        }
        else {
            if (!self->strict_types ||
                !strcmp(py_value->ob_type->tp_name, "aerospike.null")) {
                as_operations *pointer_ops = ops;
                as_binop *binop =
                    &pointer_ops->binops.entries[pointer_ops->binops.size++];
                binop->op = AS_OPERATOR_APPEND;
                initialize_bin_for_strictypes(self, err, py_value, binop,
                                              bin_name, static_pool);
            }
        }
        break;
    case AS_OPERATOR_PREPEND:
        if (PyUnicode_Check(py_value)) {
            py_ustr1 = PyUnicode_AsUTF8String(py_value);
            val = strdup(PyBytes_AsString(py_ustr1));
            as_operations_add_prepend_str(ops, bin_name, val);
            as_vector_append(unicodeStrVector, &val);
            Py_DECREF(py_ustr1);
        }
        else if (PyByteArray_Check(py_value) || PyBytes_Check(py_value)) {
            as_bytes *bytes;
            GET_BYTES_POOL(bytes, static_pool, err);
            if (err->code == AEROSPIKE_OK) {
                if (serialize_based_on_serializer_policy(
                        self, SERIALIZER_PYTHON, &bytes, py_value, err) !=
                    AEROSPIKE_OK) {
                    goto CLEANUP;
                }
                as_operations_add_prepend_rawp(ops, bin_name, bytes->value,
                                               bytes->size, true);
            }
        }
        else {
            if (!self->strict_types ||
                !strcmp(py_value->ob_type->tp_name, "aerospike.null")) {
                as_operations *pointer_ops = ops;
                as_binop *binop =
                    &pointer_ops->binops.entries[pointer_ops->binops.size++];
                binop->op = AS_OPERATOR_PREPEND;
                initialize_bin_for_strictypes(self, err, py_value, binop,
                                              bin_name, static_pool);
            }
        }
        break;
    case AS_OPERATOR_INCR:
        if (PyLong_Check(py_value)) {
            offset = PyLong_AsLong(py_value);
            if (offset == -1 && PyErr_Occurred() && self->strict_types) {
                if (PyErr_ExceptionMatches(PyExc_OverflowError)) {
                    as_error_update(err, AEROSPIKE_ERR_PARAM,
                                    "integer value exceeds sys.maxsize");
                    goto CLEANUP;
                }
            }
            as_operations_add_incr(ops, bin_name, offset);
        }
        else if (PyFloat_Check(py_value)) {
            double_offset = PyFloat_AsDouble(py_value);
            as_operations_add_incr_double(ops, bin_name, double_offset);
        }
        else {
            if (!self->strict_types ||
                !strcmp(py_value->ob_type->tp_name, "aerospike.null")) {
                as_operations *pointer_ops = ops;
                as_binop *binop =
                    &pointer_ops->binops.entries[pointer_ops->binops.size++];
                binop->op = AS_OPERATOR_INCR;
                initialize_bin_for_strictypes(self, err, py_value, binop,
                                              bin_name, static_pool);
            }
        }
        break;
    //------- MAP OPERATIONS ---------
    case OP_MAP_SET_POLICY:
        as_operations_map_set_policy(ops, bin_name, ctx_ref, &map_policy);
        break;
    case OP_MAP_CREATE:;
        as_map_order order = (as_map_order)PyLong_AsLong(py_map_order);
        bool persist_index = PyObject_IsTrue(py_persist_index);
        as_operations_map_create_all(ops, bin_name, ctx_ref, order,
                                     persist_index);
        break;
    case OP_MAP_PUT:
        CONVERT_VAL_TO_AS_VAL();
        CONVERT_KEY_TO_AS_VAL();
        as_operations_map_put(ops, bin_name, ctx_ref, &map_policy, put_key,
                              put_val);
        break;
    case OP_MAP_PUT_ITEMS:
        CONVERT_VAL_TO_AS_VAL();
        as_operations_map_put_items(ops, bin_name, ctx_ref, &map_policy,
                                    (as_map *)put_val);
        break;
    case OP_MAP_INCREMENT:
        CONVERT_VAL_TO_AS_VAL();
        CONVERT_KEY_TO_AS_VAL();
        as_operations_map_increment(ops, bin_name, ctx_ref, &map_policy,
                                    put_key, put_val);
        break;
    case OP_MAP_DECREMENT:
        CONVERT_VAL_TO_AS_VAL();
        CONVERT_KEY_TO_AS_VAL();
        as_operations_map_decrement(ops, bin_name, ctx_ref, &map_policy,
                                    put_key, put_val);
        break;
    case OP_MAP_SIZE:
        as_operations_map_size(ops, bin_name, ctx_ref);
        break;
    case OP_MAP_CLEAR:
        as_operations_map_clear(ops, bin_name, ctx_ref);
        break;
    case OP_MAP_REMOVE_BY_KEY:
        CONVERT_KEY_TO_AS_VAL();
        as_operations_map_remove_by_key(ops, bin_name, ctx_ref, put_key,
                                        return_type);
        break;
    case OP_MAP_REMOVE_BY_KEY_LIST:
        CONVERT_VAL_TO_AS_VAL();
        as_operations_map_remove_by_key_list(ops, bin_name, ctx_ref,
                                             (as_list *)put_val, return_type);
        break;
    case OP_MAP_REMOVE_BY_KEY_RANGE:
        CONVERT_VAL_TO_AS_VAL();
        CONVERT_KEY_TO_AS_VAL();
        as_operations_map_remove_by_key_range(ops, bin_name, ctx_ref, put_key,
                                              put_val, return_type);
        break;
    case OP_MAP_REMOVE_BY_VALUE:
        CONVERT_VAL_TO_AS_VAL();
        as_operations_map_remove_by_value(ops, bin_name, ctx_ref, put_val,
                                          return_type);
        break;
    case OP_MAP_REMOVE_BY_VALUE_LIST:
        CONVERT_VAL_TO_AS_VAL();
        as_operations_map_remove_by_value_list(ops, bin_name, ctx_ref,
                                               (as_list *)put_val, return_type);
        break;
    case OP_MAP_REMOVE_BY_VALUE_RANGE:
        CONVERT_VAL_TO_AS_VAL();
        CONVERT_RANGE_TO_AS_VAL();
        as_operations_map_remove_by_value_range(ops, bin_name, ctx_ref, put_val,
                                                put_range, return_type);
        break;
    case OP_MAP_REMOVE_BY_INDEX:
        as_operations_map_remove_by_index(ops, bin_name, ctx_ref, index,
                                          return_type);
        break;
    case OP_MAP_REMOVE_BY_INDEX_RANGE:
        if (pyobject_to_index(self, err, py_value, &offset) != AEROSPIKE_OK) {
            goto CLEANUP;
        }
        as_operations_map_remove_by_index_range(ops, bin_name, ctx_ref, index,
                                                offset, return_type);
        break;
    case OP_MAP_REMOVE_BY_RANK:
        as_operations_map_remove_by_rank(ops, bin_name, ctx_ref, index,
                                         return_type);
        break;
    case OP_MAP_REMOVE_BY_RANK_RANGE:
        if (pyobject_to_index(self, err, py_value, &offset) != AEROSPIKE_OK) {
            goto CLEANUP;
        }
        as_operations_map_remove_by_rank_range(ops, bin_name, ctx_ref, index,
                                               offset, return_type);
        break;
    case OP_MAP_GET_BY_KEY:
        CONVERT_KEY_TO_AS_VAL();
        as_operations_map_get_by_key(ops, bin_name, ctx_ref, put_key,
                                     return_type);
        break;
    case OP_MAP_GET_BY_KEY_RANGE:
        CONVERT_RANGE_TO_AS_VAL();
        CONVERT_KEY_TO_AS_VAL();
        as_operations_map_get_by_key_range(ops, bin_name, ctx_ref, put_key,
                                           put_range, return_type);
        break;
    case OP_MAP_GET_BY_KEY_LIST:
        CONVERT_VAL_TO_AS_VAL();
        as_operations_map_get_by_key_list(ops, bin_name, ctx_ref,
                                          (as_list *)put_val, return_type);
        break;
    case OP_MAP_GET_BY_VALUE:
        CONVERT_VAL_TO_AS_VAL();
        as_operations_map_get_by_value(ops, bin_name, ctx_ref, put_val,
                                       return_type);
        break;
    case OP_MAP_GET_BY_VALUE_RANGE:
        CONVERT_VAL_TO_AS_VAL();
        CONVERT_RANGE_TO_AS_VAL();
        as_operations_map_get_by_value_range(ops, bin_name, ctx_ref, put_val,
                                             put_range, return_type);
        break;
    case OP_MAP_GET_BY_VALUE_LIST:
        CONVERT_VAL_TO_AS_VAL();
        as_operations_map_get_by_value_list(ops, bin_name, ctx_ref,
                                            (as_list *)put_val, return_type);
        break;
    case OP_MAP_GET_BY_INDEX:
        as_operations_map_get_by_index(ops, bin_name, ctx_ref, index,
                                       return_type);
        break;
    case OP_MAP_GET_BY_INDEX_RANGE:
        if (pyobject_to_index(self, err, py_value, &offset) != AEROSPIKE_OK) {
            goto CLEANUP;
        }
        as_operations_map_get_by_index_range(ops, bin_name, ctx_ref, index,
                                             offset, return_type);
        break;
    case OP_MAP_GET_BY_RANK:
        as_operations_map_get_by_rank(ops, bin_name, ctx_ref, index,
                                      return_type);
        break;
    case OP_MAP_GET_BY_RANK_RANGE:
        if (pyobject_to_index(self, err, py_value, &offset) != AEROSPIKE_OK) {
            goto CLEANUP;
        }
        as_operations_map_get_by_rank_range(ops, bin_name, ctx_ref, index,
                                            offset, return_type);
        break;

    default:
        if (self->strict_types) {
            as_error_update(err, AEROSPIKE_ERR_PARAM,
                            "Invalid operation given");
            goto CLEANUP;
        }
    }

CLEANUP:
    if (mod_exp) {
        as_exp_destroy(mod_exp);
    }
    if (ctx_in_use) {
        as_cdt_ctx_destroy(&ctx);
    }

    return err->code;
}

/**
 *******************************************************************************************************
 * This function invokes csdk's API's.
 *
 * @param self                  AerospikeClient object
 * @param err                   The as_error to be populated by the function
 *                              with the encountered error if any.
 * @param key                   The C client's as_key that identifies the record.
 * @param py_list               The list containing op, bin_name and value.
 * @param py_meta               The metadata for the operation.
 * @param py_policy      		Python dict used to populate the operate_policy or map_policy.
 *******************************************************************************************************
 */
static PyObject *AerospikeClient_Operate_Invoke(AerospikeClient *self,
                                                as_error *err, as_key *key,
                                                PyObject *py_list,
                                                PyObject *py_meta,
                                                PyObject *py_policy)
{
    long operation;
    long return_type = -1;
    bool operation_succeeded = false;
    PyObject *py_rec = NULL;
    as_record *rec = NULL;
    as_policy_operate operate_policy;
    as_policy_operate *operate_policy_p = NULL;

    // For expressions conversion.
    as_exp *exp_list_p = NULL;

    as_vector *unicodeStrVector = as_vector_create(sizeof(char *), 128);

    as_operations ops;
    Py_ssize_t size = PyList_Size(py_list);
    if (PyErr_Occurred()) {
        return NULL;
    }

    as_operations_inita(&ops, size);

    if (py_policy) {
        if (pyobject_to_policy_operate(self, err, py_policy, &operate_policy,
                                       &operate_policy_p,
                                       &self->as->config.policies.operate,
                                       &exp_list_p) != AEROSPIKE_OK) {
            goto CLEANUP;
        }
    }

    as_static_pool static_pool;
    memset(&static_pool, 0, sizeof(static_pool));
    CHECK_CONNECTED(err);

    if (check_and_set_meta(py_meta, &ops.ttl, &ops.gen, err,
                           self->validate_keys) != AEROSPIKE_OK) {
        goto CLEANUP;
    }

    for (Py_ssize_t i = 0; i < size; i++) {
        PyObject *py_op_dict = PyList_GetItem(py_list, i);
        if (py_op_dict == NULL) {
            goto CLEANUP;
        }

        if (add_op(self, err, py_op_dict, unicodeStrVector, &static_pool, &ops,
                   &operation, &return_type) != AEROSPIKE_OK) {
            goto CLEANUP;
        }
    }

    Py_BEGIN_ALLOW_THREADS
    aerospike_key_operate(self->as, err, operate_policy_p, key, &ops, &rec);
    Py_END_ALLOW_THREADS

    if (err->code != AEROSPIKE_OK) {
        goto CLEANUP;
    }
    /* The op succeeded; it's now safe to free the record */
    operation_succeeded = true;

    if (rec) {
        as_status retval = record_to_pyobject(self, err, rec, key, &py_rec);
        if (!retval) {
            goto CLEANUP;
        }
    }

CLEANUP:
    for (unsigned int i = 0; i < unicodeStrVector->size; i++) {
        free(as_vector_get_ptr(unicodeStrVector, i));
    }

    if (exp_list_p) {
        as_exp_destroy(exp_list_p);
    }

    as_vector_destroy(unicodeStrVector);

    if (rec && operation_succeeded) {
        as_record_destroy(rec);
    }
    if (key->valuep) {
        as_key_destroy(key);
    }

    as_operations_destroy(&ops);

    if (err->code != AEROSPIKE_OK) {
        raise_exception(err);
        return NULL;
    }

    if (py_rec) {
        return py_rec;
    }
    else {
        return PyLong_FromLong(0);
    }
}

/**
 *******************************************************************************************************
 * Multiple operations on a single record
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns tuple of bins on success if read operation is given.
 * Otherwise returns 0 on success for other operations.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_Operate(AerospikeClient *self, PyObject *args,
                                  PyObject *kwds)
{
    BASE_VARIABLES
    PyObject *py_list = NULL;
    PyObject *py_bin = NULL;

    // Python Function Keyword Arguments
    static char *kwlist[] = {"key", "list", "meta", "policy", NULL};
    if (PyArg_ParseTupleAndKeywords(args, kwds, "OO|OO:operate", kwlist,
                                    &py_key, &py_list, &py_meta,
                                    &py_policy) == false) {
        return NULL;
    }

    CHECK_CONNECTED(&err);

    if (pyobject_to_key(&err, py_key, &key) != AEROSPIKE_OK) {
        goto CLEANUP;
    }

    if (py_list && PyList_Check(py_list)) {
        py_result = AerospikeClient_Operate_Invoke(self, &err, &key, py_list,
                                                   py_meta, py_policy);
    }
    else {
        as_error_update(&err, AEROSPIKE_ERR_PARAM,
                        "Operations should be of type list");
    }

CLEANUP:
    EXCEPTION_ON_ERROR();

    return py_result;
}

/**
 *******************************************************************************************************
 * This function invokes csdk's API's.
 *
 * @param self                  AerospikeClient object
 * @param err                   The as_error to be populated by the function
 *                              with the encountered error if any.
 * @param key                   The C client's as_key that identifies the record.
 * @param py_list               The list containing op, bin_name and value.
 * @param py_meta               The metadata for the operation.
 * @param operate_policy_p      The value for operate policy.
 *******************************************************************************************************
 */
static PyObject *
AerospikeClient_OperateOrdered_Invoke(AerospikeClient *self, as_error *err,
                                      as_key *key, PyObject *py_list,
                                      PyObject *py_meta, PyObject *py_policy)
{
    long operation;
    long return_type = -1;
    bool operation_succeeded = false;

    PyObject *py_rec = NULL;
    as_record *rec = NULL;
    as_policy_operate operate_policy;
    as_policy_operate *operate_policy_p = NULL;

    as_vector *unicodeStrVector = as_vector_create(sizeof(char *), 128);

    as_static_pool static_pool;
    memset(&static_pool, 0, sizeof(static_pool));

    as_operations ops;
    Py_ssize_t ops_list_size = PyList_Size(py_list);
    as_operations_inita(&ops, ops_list_size);

    // For expressions conversion.
    as_exp *exp_list_p = NULL;

    /* These are the values which will be returned in a 3 element list */
    PyObject *py_return_key = NULL;
    PyObject *py_return_meta = NULL;
    PyObject *py_return_bins = NULL;

    CHECK_CONNECTED(err);

    if (py_policy) {
        if (pyobject_to_policy_operate(self, err, py_policy, &operate_policy,
                                       &operate_policy_p,
                                       &self->as->config.policies.operate,
                                       &exp_list_p) != AEROSPIKE_OK) {
            goto CLEANUP;
        }
    }

    if (check_and_set_meta(py_meta, &ops.ttl, &ops.gen, err,
                           self->validate_keys) != AEROSPIKE_OK) {
        goto CLEANUP;
    }

    for (Py_ssize_t i = 0; i < ops_list_size; i++) {

        PyObject *py_current_op = NULL;
        py_current_op = PyList_GetItem(py_list, i);

        if (PyDict_Check(py_current_op)) {
            if (add_op(self, err, py_current_op, unicodeStrVector, &static_pool,
                       &ops, &operation, &return_type) != AEROSPIKE_OK) {
                goto CLEANUP;
            }
        }
        else {
            as_error_update(err, AEROSPIKE_ERR_PARAM,
                            "Operation must be a dict");
            goto CLEANUP;
        }
    }

    if (err->code != AEROSPIKE_OK) {
        as_error_update(err, err->code, NULL);
        goto CLEANUP;
    }

    Py_BEGIN_ALLOW_THREADS
    aerospike_key_operate(self->as, err, operate_policy_p, key, &ops, &rec);
    Py_END_ALLOW_THREADS

    if (err->code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

    operation_succeeded = true;
    if (rec) {
        /* Build the return tuple: (key, meta, bins) */
        key_to_pyobject(err, key, &py_return_key);
        if (err->code != AEROSPIKE_OK || !py_return_key) {
            goto CLEANUP;
        }

        metadata_to_pyobject(err, rec, &py_return_meta);
        if (err->code != AEROSPIKE_OK || !py_return_meta) {
            Py_XDECREF(py_return_key);
            goto CLEANUP;
        }

        operate_bins_to_pyobject(self, err, rec, &py_return_bins);
        if (err->code != AEROSPIKE_OK || !py_return_bins) {
            Py_XDECREF(py_return_key);
            Py_XDECREF(py_return_meta);
            goto CLEANUP;
        }

        py_rec =
            Py_BuildValue("OOO", py_return_key, py_return_meta, py_return_bins);
        if (!py_rec) {
            as_error_update(err, AEROSPIKE_ERR_CLIENT,
                            "Unable to build return tuple");
        }

        /* If Py_BuildValue succeeded it increased the reference count of all 3 of these to 2,
		 * so we decref them.*
		 * If Py_BuildValue failed, we aren't returning anything, so they need to be
		 * decref'd in that case as well.
		 */
        Py_XDECREF(py_return_key);
        Py_XDECREF(py_return_bins);
        Py_XDECREF(py_return_meta);
    }

CLEANUP:
    for (unsigned int i = 0; i < unicodeStrVector->size; i++) {
        free(as_vector_get_ptr(unicodeStrVector, i));
    }

    as_vector_destroy(unicodeStrVector);

    if (exp_list_p) {
        as_exp_destroy(exp_list_p);
    }

    if (rec && operation_succeeded) {
        as_record_destroy(rec);
    }
    if (key->valuep) {
        as_key_destroy(key);
    }

    as_operations_destroy(&ops);

    if (err->code != AEROSPIKE_OK) {
        raise_exception(err);
        return NULL;
    }

    if (py_rec) {
        return py_rec;
    }
    else {
        return PyLong_FromLong(0);
    }
}

/**
 *******************************************************************************************************
 * Multiple operations on a single record. Results are returned in an ordered
 * manner
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns tuple of bins on success if read operation is given.
 * Otherwise returns 0 on success for other operations.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_OperateOrdered(AerospikeClient *self, PyObject *args,
                                         PyObject *kwds)
{
    // Initialize error
    as_error err;
    as_error_init(&err);

    // Python Function Arguments
    PyObject *py_key = NULL;
    PyObject *py_list = NULL;
    PyObject *py_policy = NULL;
    PyObject *py_result = NULL;
    PyObject *py_meta = NULL;

    as_key key;

    // Python Function Keyword Arguments
    static char *kwlist[] = {"key", "list", "meta", "policy", NULL};

    // Python Function Argument Parsing
    if (PyArg_ParseTupleAndKeywords(args, kwds, "OO|OO:operate_ordered", kwlist,
                                    &py_key, &py_list, &py_meta,
                                    &py_policy) == false) {
        return NULL;
    }

    CHECK_CONNECTED(&err);

    if (pyobject_to_key(&err, py_key, &key) != AEROSPIKE_OK) {
        goto CLEANUP;
    }

    if (py_list && PyList_Check(py_list)) {
        py_result = AerospikeClient_OperateOrdered_Invoke(
            self, &err, &key, py_list, py_meta, py_policy);
    }
    else {
        as_error_update(&err, AEROSPIKE_ERR_PARAM,
                        "Operations should be of type list");
        goto CLEANUP;
    }

CLEANUP:
    if (err.code != AEROSPIKE_OK) {
        raise_exception_base(&err, py_key, Py_None, Py_None, Py_None, Py_None);
        return NULL;
    }
    return py_result;
}

/**
 *******************************************************************************************************
 * Appends a string to the string value in a bin_name.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns an integer status. 0(Zero) is success value.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_Append(AerospikeClient *self, PyObject *args,
                                 PyObject *kwds)
{
    BASE_VARIABLES
    PyObject *py_bin = NULL;
    PyObject *py_append_str = NULL;

    // Python Function Keyword Arguments
    static char *kwlist[] = {"key", "bin_name", "val", "meta", "policy", NULL};
    if (PyArg_ParseTupleAndKeywords(args, kwds, "OOO|OO:append", kwlist,
                                    &py_key, &py_bin, &py_append_str, &py_meta,
                                    &py_policy) == false) {
        return NULL;
    }

    CHECK_CONNECTED(&err);

    if (pyobject_to_key(&err, py_key, &key) != AEROSPIKE_OK) {
        goto CLEANUP;
    }

    PyObject *py_list = NULL;
    py_list = create_pylist(py_list, AS_OPERATOR_APPEND, py_bin, py_append_str);
    py_result = AerospikeClient_Operate_Invoke(self, &err, &key, py_list,
                                               py_meta, py_policy);

    DECREF_LIST_AND_RESULT();

CLEANUP:
    EXCEPTION_ON_ERROR();

    return PyLong_FromLong(0);
}

/**
 *******************************************************************************************************
 * Prepends a string to the string value in a bin_name
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns an integer status. 0(Zero) is success value.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_Prepend(AerospikeClient *self, PyObject *args,
                                  PyObject *kwds)
{
    BASE_VARIABLES
    PyObject *py_bin = NULL;
    PyObject *py_prepend_str = NULL;

    // Python Function Keyword Arguments
    static char *kwlist[] = {"key", "bin_name", "val", "meta", "policy", NULL};
    if (PyArg_ParseTupleAndKeywords(args, kwds, "OOO|OO:prepend", kwlist,
                                    &py_key, &py_bin, &py_prepend_str, &py_meta,
                                    &py_policy) == false) {
        return NULL;
    }

    CHECK_CONNECTED(&err);

    if (pyobject_to_key(&err, py_key, &key) != AEROSPIKE_OK) {
        goto CLEANUP;
    }

    PyObject *py_list = NULL;
    py_list =
        create_pylist(py_list, AS_OPERATOR_PREPEND, py_bin, py_prepend_str);
    py_result = AerospikeClient_Operate_Invoke(self, &err, &key, py_list,
                                               py_meta, py_policy);

    DECREF_LIST_AND_RESULT();

CLEANUP:
    EXCEPTION_ON_ERROR();

    return PyLong_FromLong(0);
}

/**
 *******************************************************************************************************
 * Increments a numeric value in a bin_name.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns an integer status. 0(Zero) is success value.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_Increment(AerospikeClient *self, PyObject *args,
                                    PyObject *kwds)
{
    BASE_VARIABLES
    PyObject *py_bin = NULL;
    PyObject *py_offset_value = 0;

    // Python Function Keyword Arguments
    static char *kwlist[] = {"key",  "bin_name", "offset",
                             "meta", "policy",   NULL};
    if (PyArg_ParseTupleAndKeywords(args, kwds, "OOO|OO:increment", kwlist,
                                    &py_key, &py_bin, &py_offset_value,
                                    &py_meta, &py_policy) == false) {
        return NULL;
    }

    CHECK_CONNECTED(&err);

    if (pyobject_to_key(&err, py_key, &key) != AEROSPIKE_OK) {
        goto CLEANUP;
    }

    PyObject *py_list = NULL;
    py_list = create_pylist(py_list, AS_OPERATOR_INCR, py_bin, py_offset_value);
    py_result = AerospikeClient_Operate_Invoke(self, &err, &key, py_list,
                                               py_meta, py_policy);

    DECREF_LIST_AND_RESULT();

CLEANUP:
    EXCEPTION_ON_ERROR();

    return PyLong_FromLong(0);
}

/**
 *******************************************************************************************************
 * Touch a record in the Aerospike DB
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns an integer status. 0(Zero) is success value.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_Touch(AerospikeClient *self, PyObject *args,
                                PyObject *kwds)
{
    BASE_VARIABLES
    PyObject *py_touchvalue = NULL;
    PyObject *py_bin = NULL;

    // Python Function Keyword Arguments
    static char *kwlist[] = {"key", "val", "meta", "policy", NULL};
    if (PyArg_ParseTupleAndKeywords(args, kwds, "O|OOO:touch", kwlist, &py_key,
                                    &py_touchvalue, &py_meta,
                                    &py_policy) == false) {
        return NULL;
    }

    CHECK_CONNECTED(&err);

    if (pyobject_to_key(&err, py_key, &key) != AEROSPIKE_OK) {
        goto CLEANUP;
    }

    if (py_touchvalue == NULL) {
        py_touchvalue = PyLong_FromLong(0);
    }

    PyObject *py_list = NULL;
    py_list = create_pylist(py_list, AS_OPERATOR_TOUCH, NULL, py_touchvalue);
    py_result = AerospikeClient_Operate_Invoke(self, &err, &key, py_list,
                                               py_meta, py_policy);

    DECREF_LIST_AND_RESULT();

CLEANUP:
    EXCEPTION_ON_ERROR();

    return PyLong_FromLong(0);
}

static as_status get_op_code_from_py_op_dict(as_error *err, PyObject *op_dict,
                                             long *op_code_ref)
{
    PyObject *py_operation = PyDict_GetItemString(op_dict, PY_OPERATION_KEY);
    if (!py_operation) {
        return as_error_update(err, AEROSPIKE_ERR_PARAM,
                               "Operation must contain an \"op\" entry");
    }
    if (!PyLong_Check(py_operation)) {
        return as_error_update(err, AEROSPIKE_ERR_PARAM,
                               "Operation must be an integer");
    }

    long op_code = PyLong_AsLong(py_operation);
    if (op_code == -1 && PyErr_Occurred()) {
        if (PyErr_ExceptionMatches(PyExc_OverflowError)) {
            as_error_update(err, AEROSPIKE_ERR_PARAM,
                            "Operation code too large");
        }
        else {
            as_error_update(err, AEROSPIKE_ERR_PARAM,
                            "Unable to convert Python operation to C long");
        }
        PyErr_Clear();
        return err->code;
    }

    *op_code_ref = op_code;
    return AEROSPIKE_OK;
}

static as_status invertIfSpecified(as_error *err, PyObject *op_dict,
                                   uint64_t *return_value)
{
    PyObject *pyInverted = PyDict_GetItemString(op_dict, "inverted");
    int truthValue;
    if (!pyInverted) {
        return AEROSPIKE_OK;
    }
    truthValue = PyObject_IsTrue(pyInverted);

    /* An error ocurred, update the flag */
    if (truthValue == -1) {
        return as_error_update(err, AEROSPIKE_ERR_PARAM,
                               "Invalid inverted value");
    }

    if (truthValue) {
        *return_value |= AS_MAP_RETURN_INVERTED;
    }

    return AEROSPIKE_OK;
}
