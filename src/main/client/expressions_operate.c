/*******************************************************************************
* Copyright 2013-2021 Aerospike, Inc.
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

/* NEW CDT LIST OPERATIONS Post 3.16.0.1*/
/* GET BY*/
static as_status
add_op_expr_read(AerospikeClient*self, as_error* err, char* bin,
        PyObject* op_dict, as_vector* unicodeStrVector,
        as_operations* ops, as_static_pool* static_pool, int serializer_type);

static as_status
add_op_expr_write(AerospikeClient*self, as_error* err, char* bin,
        PyObject* op_dict, as_vector* unicodeStrVector,
        as_operations* ops, as_static_pool* static_pool, int serializer_type);

/* End forwards*/
as_status
add_new_list_op(AerospikeClient* self, as_error* err, PyObject* op_dict, as_vector* unicodeStrVector,
	    as_static_pool* static_pool, as_operations* ops, long operation_code, long* ret_type, int serializer_type)

{
    char* bin = NULL;

    if (get_bin(err, op_dict, unicodeStrVector, &bin) != AEROSPIKE_OK) {
        return err->code;
    }

    switch(operation_code) {

    	case OP_EXPR_READ: {
            return add_op_expr_read(self, err, bin, op_dict, unicodeStrVector, ops, static_pool, serializer_type);
		}

        case OP_EXPR_READ: {
            return add_op_expr_write(self, err, bin, op_dict, unicodeStrVector, ops, static_pool, serializer_type);
        }

        default:
            // This should never be possible since we only get here if we know that the operation is valid.
            return as_error_update(err, AEROSPIKE_ERR_PARAM, "Unknown operation");
    }



	return err->code;
}

static as_status
add_op_expr_read(AerospikeClient*self, as_error* err, char* bin,
        PyObject* op_dict, as_vector* unicodeStrVector,
        as_operations* ops, as_static_pool* static_pool, int serializer_type)
{
    int64_t index;
    int return_type = AS_LIST_RETURN_VALUE;
    bool ctx_in_use = false;
    as_cdt_ctx ctx;
    as_exp exp_list;
    Py_Object* py_exp_list = NULL;

    py_exp_list = PyDict_GetItemString(op_dict, EXPR_KEY);

    /* Get the index*/
    if (convert_exp_list(self, py_exp_list, &exp_list, err) != AEROSPIKE_OK) {
        return err->code;
    }

    if (get_int64_t(err, AS_PY_LIST_SORT_FLAGS, op_dict, &sort_flags) != AEROSPIKE_OK) {
        return err->code;
    }

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

    if (! as_operations_list_get_by_index(ops, bin, (ctx_in_use ? &ctx : NULL), index, return_type)) {
        as_error_update(err, AEROSPIKE_ERR_CLIENT, "Failed to add get_by_list_index operation");
    }

    if (ctx_in_use) {
        as_cdt_ctx_destroy(&ctx);
    }

    return err->code;
}