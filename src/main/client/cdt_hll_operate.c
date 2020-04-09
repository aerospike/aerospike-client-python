/*******************************************************************************
* Copyright 2013-2020 Aerospike, Inc.
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
#include <aerospike/as_hll_operations.h>
#include <aerospike/as_cdt_ctx.h>

#include "client.h"
#include "conversions.h"
#include "exceptions.h"
#include "policy.h"
#include "serializer.h"
#include "cdt_hll_operations.h"
#include "cdt_operation_utils.h"

#define AS_PY_LIST_RETURN_KEY "return_type"
#define AS_PY_LIST_ORDER "list_order"
#define AS_PY_LIST_SORT_FLAGS "sort_flags"
#define AS_PY_HLL_POLICY "list_policy"
#define AS_PY_HLL_INDEX_BIT_COUNT "index_bit_count"


static as_status
get_hll_policy(as_error* err, PyObject* op_dict, as_hll_policy* policy, bool* found);

static as_status
add_op_hll_add(AerospikeClient* self, as_error* err, char* bin,
        PyObject* op_dict, as_operations* ops,
        as_static_pool* static_pool, int serializer_type);

as_status
add_new_hll_op(AerospikeClient* self, as_error* err, PyObject* op_dict, as_vector* unicodeStrVector,
	    as_static_pool* static_pool, as_operations* ops, long operation_code, long* ret_type, int serializer_type)

{
    char* bin = NULL;

    if (get_bin(err, op_dict, unicodeStrVector, &bin) != AEROSPIKE_OK) {
        return err->code;
    }

    switch(operation_code) {
    	case OP_HLL_ADD:
    		return add_op_hll_add(self, err, bin, op_dict, ops, static_pool, serializer_type);

        default:
            // This should never be possible since we only get here if we know that the operation is valid.
            return as_error_update(err, AEROSPIKE_ERR_PARAM, "Unknown operation");
    }

	return err->code;
}

static as_status
add_op_hll_add(AerospikeClient* self, as_error* err, char* bin,
        PyObject* op_dict, as_operations* ops,
        as_static_pool* static_pool, int serializer_type)
{

    as_list* value_list = NULL;
    as_hll_policy hll_policy;
    int index_bit_count;
    as_cdt_ctx ctx;
    bool ctx_in_use = false;
    bool policy_in_use = false;

    if (get_int(err, AS_PY_HLL_INDEX_BIT_COUNT, op_dict, &index_bit_count) != AEROSPIKE_OK) {
        return err->code;
    }

    if (get_hll_policy(err, op_dict, &hll_policy, &policy_in_use) != AEROSPIKE_OK) {
        return err->code;
    }

    if (get_cdt_ctx(self, err, &ctx, op_dict, &ctx_in_use, static_pool, serializer_type) != AEROSPIKE_OK) {
        return err->code;
    }

    if (get_val_list(self, err, AS_PY_VALUES_KEY, op_dict, &value_list, static_pool, serializer_type) != AEROSPIKE_OK) {
        return err->code;
    }

    if (as_operations_hll_add(ops, bin, NULL, &hll_policy, value_list, index_bit_count)){
        return err->code;
    }

    if (ctx_in_use) {
        as_cdt_ctx_destroy(&ctx);
    }

    return err->code;
}

static as_status
get_hll_policy(as_error* err, PyObject* op_dict, as_hll_policy* policy, bool* found) {
    *found = false;

    PyObject* hll_policy = PyDict_GetItemString(op_dict, AS_PY_HLL_POLICY);

    if (hll_policy) {
        if (pyobject_to_hll_policy(err, hll_policy, policy) != AEROSPIKE_OK) {
            return err -> code;
        }
        *found = true;
    }

    return AEROSPIKE_OK;
}