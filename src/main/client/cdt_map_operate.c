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
#include <aerospike/as_map_operations.h>

#include "client.h"
#include "conversions.h"
#include "exceptions.h"
#include "policy.h"
#include "serializer.h"
#include "cdt_map_operations.h"
#include "cdt_operation_utils.h"

#define AS_PY_MAP_RETURN_KEY "return_type"
#define AS_PY_MAP_KEY_KEY "key"
#define AS_PY_RETURN_INVERTED_KEY "inverted"
#define AS_PY_MAP_INDEX_KEY "index"

static as_status get_map_return_type(as_error *err, PyObject *op_dict,
									 int *return_type);

static as_status add_op_map_remove_by_value_rel_rank_range(
	AerospikeClient *self, as_error *err, char *bin, PyObject *op_dict,
	as_operations *ops, as_static_pool *static_pool, int serializer_type);

static as_status add_op_map_get_by_value_rel_rank_range(
	AerospikeClient *self, as_error *err, char *bin, PyObject *op_dict,
	as_operations *ops, as_static_pool *static_pool, int serializer_type);

static as_status add_op_map_remove_by_key_rel_index_range(
	AerospikeClient *self, as_error *err, char *bin, PyObject *op_dict,
	as_operations *ops, as_static_pool *static_pool, int serializer_type);

static as_status add_op_map_get_by_key_rel_index_range(
	AerospikeClient *self, as_error *err, char *bin, PyObject *op_dict,
	as_operations *ops, as_static_pool *static_pool, int serializer_type);

as_status add_new_map_op(AerospikeClient *self, as_error *err,
						 PyObject *op_dict, as_vector *unicodeStrVector,
						 as_static_pool *static_pool, as_operations *ops,
						 long operation_code, long *ret_type,
						 int serializer_type)

{
	char *bin = NULL;

	if (get_bin(err, op_dict, unicodeStrVector, &bin) != AEROSPIKE_OK) {
		return err->code;
	}

	switch (operation_code) {

	case OP_MAP_REMOVE_BY_VALUE_RANK_RANGE_REL: {
		return add_op_map_remove_by_value_rel_rank_range(
			self, err, bin, op_dict, ops, static_pool, serializer_type);
	}

	case OP_MAP_GET_BY_VALUE_RANK_RANGE_REL: {
		return add_op_map_get_by_value_rel_rank_range(
			self, err, bin, op_dict, ops, static_pool, serializer_type);
	}

	case OP_MAP_REMOVE_BY_KEY_INDEX_RANGE_REL: {
		return add_op_map_remove_by_key_rel_index_range(
			self, err, bin, op_dict, ops, static_pool, serializer_type);
	}

	case OP_MAP_GET_BY_KEY_INDEX_RANGE_REL: {
		return add_op_map_get_by_key_rel_index_range(
			self, err, bin, op_dict, ops, static_pool, serializer_type);
	}
	default:
		// This should never be possible since we only get here if we know that the operation is valid.
		return as_error_update(err, AEROSPIKE_ERR_PARAM, "Unknown operation");
	}

	return err->code;
}

static as_status add_op_map_remove_by_value_rel_rank_range(
	AerospikeClient *self, as_error *err, char *bin, PyObject *op_dict,
	as_operations *ops, as_static_pool *static_pool, int serializer_type)
{
	bool count_present = false;
	int64_t count;
	int return_type = AS_MAP_RETURN_VALUE;
	int64_t rank;
	as_val *value = NULL;
	bool ctx_in_use = false;
	as_cdt_ctx ctx;

	if (get_map_return_type(err, op_dict, &return_type) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_int64_t(err, AS_PY_RANK_KEY, op_dict, &rank) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_optional_int64_t(err, AS_PY_COUNT_KEY, op_dict, &count,
							 &count_present) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_asval(self, err, AS_PY_VAL_KEY, op_dict, &value, static_pool,
				  serializer_type, true) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_cdt_ctx(self, err, &ctx, op_dict, &ctx_in_use, static_pool,
					serializer_type) != AEROSPIKE_OK) {
		return err->code;
	}

	if (count_present) {
		if (!as_operations_map_remove_by_value_rel_rank_range(
				ops, bin, (ctx_in_use ? &ctx : NULL), value, rank,
				(uint64_t)count, return_type)) {
			as_cdt_ctx_destroy(&ctx);
			return as_error_update(
				err, AEROSPIKE_ERR_CLIENT,
				"Failed to add map remove by value rank relative operation");
		}
	}
	else {
		if (!as_operations_map_remove_by_value_rel_rank_range_to_end(
				ops, bin, (ctx_in_use ? &ctx : NULL), value, rank,
				return_type)) {
			as_cdt_ctx_destroy(&ctx);
			return as_error_update(
				err, AEROSPIKE_ERR_CLIENT,
				"Failed to add map remove by value rank relative operation");
		}
	}

	if (ctx_in_use) {
		as_cdt_ctx_destroy(&ctx);
	}

	return AEROSPIKE_OK;
}

static as_status add_op_map_get_by_value_rel_rank_range(
	AerospikeClient *self, as_error *err, char *bin, PyObject *op_dict,
	as_operations *ops, as_static_pool *static_pool, int serializer_type)
{
	bool count_present = false;
	int64_t count;
	int return_type = AS_MAP_RETURN_VALUE;
	int64_t rank;
	as_val *value = NULL;
	bool ctx_in_use = false;
	as_cdt_ctx ctx;

	if (get_map_return_type(err, op_dict, &return_type) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_int64_t(err, AS_PY_RANK_KEY, op_dict, &rank) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_optional_int64_t(err, AS_PY_COUNT_KEY, op_dict, &count,
							 &count_present) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_asval(self, err, AS_PY_VAL_KEY, op_dict, &value, static_pool,
				  serializer_type, true) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_cdt_ctx(self, err, &ctx, op_dict, &ctx_in_use, static_pool,
					serializer_type) != AEROSPIKE_OK) {
		return err->code;
	}

	if (count_present) {
		if (!as_operations_map_get_by_value_rel_rank_range(
				ops, bin, (ctx_in_use ? &ctx : NULL), value, rank,
				(uint64_t)count, return_type)) {
			as_cdt_ctx_destroy(&ctx);
			return as_error_update(
				err, AEROSPIKE_ERR_CLIENT,
				"Failed to add map get by value rank relative operation");
		}
	}
	else {
		if (!as_operations_map_get_by_value_rel_rank_range_to_end(
				ops, bin, (ctx_in_use ? &ctx : NULL), value, rank,
				return_type)) {
			as_cdt_ctx_destroy(&ctx);
			return as_error_update(
				err, AEROSPIKE_ERR_CLIENT,
				"Failed to add map get by value rank relative operation");
		}
	}

	if (ctx_in_use) {
		as_cdt_ctx_destroy(&ctx);
	}

	return AEROSPIKE_OK;
}

static as_status add_op_map_remove_by_key_rel_index_range(
	AerospikeClient *self, as_error *err, char *bin, PyObject *op_dict,
	as_operations *ops, as_static_pool *static_pool, int serializer_type)
{
	bool count_present = false;
	int64_t count;
	int return_type = AS_MAP_RETURN_VALUE;
	int64_t rank;
	as_val *key = NULL;
	bool ctx_in_use = false;
	as_cdt_ctx ctx;

	if (get_map_return_type(err, op_dict, &return_type) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_int64_t(err, AS_PY_INDEX_KEY, op_dict, &rank) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_optional_int64_t(err, AS_PY_COUNT_KEY, op_dict, &count,
							 &count_present) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_asval(self, err, AS_PY_MAP_KEY_KEY, op_dict, &key, static_pool,
				  serializer_type, true) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_cdt_ctx(self, err, &ctx, op_dict, &ctx_in_use, static_pool,
					serializer_type) != AEROSPIKE_OK) {
		return err->code;
	}

	if (count_present) {
		if (!as_operations_map_remove_by_key_rel_index_range(
				ops, bin, (ctx_in_use ? &ctx : NULL), key, rank,
				(uint64_t)count, return_type)) {
			as_cdt_ctx_destroy(&ctx);
			return as_error_update(
				err, AEROSPIKE_ERR_CLIENT,
				"Failed to add map remove by key rank relative operation");
		}
	}
	else {
		if (!as_operations_map_remove_by_key_rel_index_range_to_end(
				ops, bin, (ctx_in_use ? &ctx : NULL), key, rank, return_type)) {
			as_cdt_ctx_destroy(&ctx);
			return as_error_update(
				err, AEROSPIKE_ERR_CLIENT,
				"Failed to add map remove by key rank relative operation");
		}
	}

	if (ctx_in_use) {
		as_cdt_ctx_destroy(&ctx);
	}

	return AEROSPIKE_OK;
}

static as_status add_op_map_get_by_key_rel_index_range(
	AerospikeClient *self, as_error *err, char *bin, PyObject *op_dict,
	as_operations *ops, as_static_pool *static_pool, int serializer_type)
{
	bool count_present = false;
	int64_t count;
	int return_type = AS_MAP_RETURN_VALUE;
	int64_t rank;
	as_val *key = NULL;
	bool ctx_in_use = false;
	as_cdt_ctx ctx;

	if (get_map_return_type(err, op_dict, &return_type) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_int64_t(err, AS_PY_INDEX_KEY, op_dict, &rank) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_optional_int64_t(err, AS_PY_COUNT_KEY, op_dict, &count,
							 &count_present) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_asval(self, err, AS_PY_MAP_KEY_KEY, op_dict, &key, static_pool,
				  serializer_type, true) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_cdt_ctx(self, err, &ctx, op_dict, &ctx_in_use, static_pool,
					serializer_type) != AEROSPIKE_OK) {
		return err->code;
	}

	if (count_present) {
		if (!as_operations_map_get_by_key_rel_index_range(
				ops, bin, (ctx_in_use ? &ctx : NULL), key, rank,
				(uint64_t)count, return_type)) {
			as_cdt_ctx_destroy(&ctx);
			return as_error_update(
				err, AEROSPIKE_ERR_CLIENT,
				"Failed to add map get by key rank relative operation");
		}
	}
	else {
		if (!as_operations_map_get_by_key_rel_index_range_to_end(
				ops, bin, (ctx_in_use ? &ctx : NULL), key, rank, return_type)) {
			as_cdt_ctx_destroy(&ctx);
			return as_error_update(
				err, AEROSPIKE_ERR_CLIENT,
				"Failed to add map get by key rank relative operation");
		}
	}

	if (ctx_in_use) { //add these spaces
		as_cdt_ctx_destroy(&ctx);
	}

	return AEROSPIKE_OK;
}

static as_status get_map_return_type(as_error *err, PyObject *op_dict,
									 int *return_type)
{
	int64_t int64_return_type;
	int py_bool_val = -1;

	if (get_int64_t(err, AS_PY_MAP_RETURN_KEY, op_dict, &int64_return_type) !=
		AEROSPIKE_OK) {
		return err->code;
	}
	*return_type = int64_return_type;
	PyObject *py_inverted = PyDict_GetItemString(
		op_dict, AS_PY_RETURN_INVERTED_KEY); //NOT A MAGIC STRING

	if (py_inverted) {
		py_bool_val = PyObject_IsTrue(py_inverted);
		/* Essentially bool(py_bool_val) failed, so we raise an exception */
		if (py_bool_val == -1) {
			return as_error_update(err, AEROSPIKE_ERR_PARAM,
								   "Invalid inverted option");
		}
		if (py_bool_val == 1) {
			*return_type |= AS_MAP_RETURN_INVERTED;
		}
	}

	return AEROSPIKE_OK;
}