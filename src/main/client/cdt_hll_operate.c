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
#include <aerospike/as_list_operations.h>
#include <aerospike/as_cdt_ctx.h>

#include "client.h"
#include "conversions.h"
#include "exceptions.h"
#include "policy.h"
#include "serializer.h"
#include "cdt_list_operations.h"
#include "cdt_operation_utils.h"

#define AS_PY_LIST_RETURN_KEY "return_type"
#define AS_PY_LIST_ORDER "list_order"
#define AS_PY_LIST_SORT_FLAGS "sort_flags"
#define AS_PY_LIST_POLICY "list_policy"

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
    int64_t index;
    int return_type = AS_LIST_RETURN_VALUE;
    bool ctx_in_use = false;
    as_cdt_ctx ctx;

    /* Get the index*/
    if (get_int64_t(err, AS_PY_INDEX_KEY, op_dict, &index) != AEROSPIKE_OK) {
        return err->code;
    }

    if (get_list_return_type(err, op_dict, &return_type) != AEROSPIKE_OK) {
        return err->code;
    }

    if (get_cdt_ctx(self, err, &ctx, op_dict, &ctx_in_use, static_pool, serializer_type) != AEROSPIKE_OK) {
        return err->code;
    }

    if (! as_operations_hll_add(ops, bin, (ctx_in_use ? &ctx : NULL), index, return_type)) {
        as_error_update(err, AEROSPIKE_ERR_CLIENT, "Failed to add get_by_list_index operation");
    }

    if (ctx_in_use) {
        as_cdt_ctx_destroy(&ctx);
    }

    return err->code;
}