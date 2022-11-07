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
#include <aerospike/as_exp_operations.h>

#include "client.h"
#include "conversions.h"
#include "exceptions.h"
#include "policy.h"
#include "serializer.h"
#include "expression_operations.h"
#include "cdt_operation_utils.h"

static as_status add_op_expr_read(AerospikeClient *self, as_error *err,
								  PyObject *op_dict,
								  as_vector *unicodeStrVector,
								  as_operations *ops, int serializer_type);

static as_status add_op_expr_write(AerospikeClient *self, as_error *err,
								   PyObject *op_dict,
								   as_vector *unicodeStrVector,
								   as_operations *ops, int serializer_type);

/* End forwards*/
as_status add_new_expr_op(AerospikeClient *self, as_error *err,
						  PyObject *op_dict, as_vector *unicodeStrVector,
						  as_operations *ops, long operation_code,
						  int serializer_type)

{
	switch (operation_code) {

	case OP_EXPR_READ: {
		return add_op_expr_read(self, err, op_dict, unicodeStrVector, ops,
								serializer_type);
	}

	case OP_EXPR_WRITE: {
		return add_op_expr_write(self, err, op_dict, unicodeStrVector, ops,
								 serializer_type);
	}

	default:
		// This should never be possible since we only get here if we know that the operation is valid.
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
							   "Unknown expression operation");
	}

	return err->code;
}

static as_status add_op_expr_write(AerospikeClient *self, as_error *err,
								   PyObject *op_dict,
								   as_vector *unicodeStrVector,
								   as_operations *ops, int serializer_type)
{
	as_exp *exp_list_p = NULL;
	PyObject *py_exp_list = NULL;
	int64_t exp_write_flags = AS_EXP_WRITE_DEFAULT;
	char *bin = NULL;

	if (get_bin(err, op_dict, unicodeStrVector, &bin) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_int64_t(err, AS_EXPR_FLAGS_KEY, op_dict, &exp_write_flags) !=
		AEROSPIKE_OK) {
		return err->code;
	}

	py_exp_list = PyDict_GetItemString(op_dict, AS_EXPR_KEY);

	if (convert_exp_list(self, py_exp_list, &exp_list_p, err) != AEROSPIKE_OK) {
		return err->code;
	}

	if (!as_operations_exp_write(ops, bin, exp_list_p, exp_write_flags)) {
		as_error_update(err, AEROSPIKE_ERR_CLIENT,
						"Failed to pack write expression op.");
	}

	if (exp_list_p) {
		as_exp_destroy(exp_list_p);
	}

	return err->code;
}

static as_status add_op_expr_read(AerospikeClient *self, as_error *err,
								  PyObject *op_dict,
								  as_vector *unicodeStrVector,
								  as_operations *ops, int serializer_type)
{
	as_exp *exp_list_p = NULL;
	PyObject *py_exp_list = NULL;
	int64_t exp_read_flags = AS_EXP_READ_DEFAULT;
	char *bin = NULL;

	if (get_bin(err, op_dict, unicodeStrVector, &bin) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_int64_t(err, AS_EXPR_FLAGS_KEY, op_dict, &exp_read_flags) !=
		AEROSPIKE_OK) {
		return err->code;
	}

	py_exp_list = PyDict_GetItemString(op_dict, AS_EXPR_KEY);

	if (convert_exp_list(self, py_exp_list, &exp_list_p, err) != AEROSPIKE_OK) {
		return err->code;
	}

	if (!as_operations_exp_read(ops, bin, exp_list_p, exp_read_flags)) {
		as_error_update(err, AEROSPIKE_ERR_CLIENT,
						"Failed to pack read expression op.");
	}

	if (exp_list_p) {
		as_exp_destroy(exp_list_p);
	}

	return err->code;
}