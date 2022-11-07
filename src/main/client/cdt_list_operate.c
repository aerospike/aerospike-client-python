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
#include <aerospike/as_list_operations.h>
#include <aerospike/as_cdt_ctx.h>

#include "client.h"
#include "conversions.h"
#include "exceptions.h"
#include "policy.h"
#include "serializer.h"
#include "cdt_list_operations.h"
#include "cdt_operation_utils.h"

static as_status add_op_list_append(AerospikeClient *self, as_error *err,
									char *bin, PyObject *op_dict,
									as_operations *ops,
									as_static_pool *static_pool,
									int serializer_type);

static as_status add_op_list_append_items(AerospikeClient *self, as_error *err,
										  char *bin, PyObject *op_dict,
										  as_operations *ops,
										  as_static_pool *static_pool,
										  int serializer_type);

static as_status add_op_list_insert(AerospikeClient *self, as_error *err,
									char *bin, PyObject *op_dict,
									as_operations *ops,
									as_static_pool *static_pool,
									int serializer_type);

static as_status add_op_list_insert_items(AerospikeClient *self, as_error *err,
										  char *bin, PyObject *op_dict,
										  as_operations *ops,
										  as_static_pool *static_pool,
										  int serializer_type);

static as_status add_op_list_increment(AerospikeClient *self, as_error *err,
									   char *bin, PyObject *op_dict,
									   as_operations *ops,
									   as_static_pool *static_pool,
									   int serializer_type);

static as_status add_op_list_size(AerospikeClient *self, as_error *err,
								  char *bin, PyObject *op_dict,
								  as_operations *ops,
								  as_static_pool *static_pool,
								  int serializer_type);

static as_status add_op_list_pop(AerospikeClient *self, as_error *err,
								 char *bin, PyObject *op_dict,
								 as_operations *ops,
								 as_static_pool *static_pool,
								 int serializer_type);

static as_status add_op_list_pop_range(AerospikeClient *self, as_error *err,
									   char *bin, PyObject *op_dict,
									   as_operations *ops,
									   as_static_pool *static_pool,
									   int serializer_type);

static as_status add_op_list_remove(AerospikeClient *self, as_error *err,
									char *bin, PyObject *op_dict,
									as_operations *ops,
									as_static_pool *static_pool,
									int serializer_type);

static as_status add_op_list_remove_range(AerospikeClient *self, as_error *err,
										  char *bin, PyObject *op_dict,
										  as_operations *ops,
										  as_static_pool *static_pool,
										  int serializer_type);

static as_status add_op_list_clear(AerospikeClient *self, as_error *err,
								   char *bin, PyObject *op_dict,
								   as_operations *ops,
								   as_static_pool *static_pool,
								   int serializer_type);

static as_status add_op_list_get(AerospikeClient *self, as_error *err,
								 char *bin, PyObject *op_dict,
								 as_operations *ops,
								 as_static_pool *static_pool,
								 int serializer_type);

static as_status add_op_list_get_range(AerospikeClient *self, as_error *err,
									   char *bin, PyObject *op_dict,
									   as_operations *ops,
									   as_static_pool *static_pool,
									   int serializer_type);

static as_status add_op_list_trim(AerospikeClient *self, as_error *err,
								  char *bin, PyObject *op_dict,
								  as_operations *ops,
								  as_static_pool *static_pool,
								  int serializer_type);

static as_status add_op_list_set(AerospikeClient *self, as_error *err,
								 char *bin, PyObject *op_dict,
								 as_operations *ops,
								 as_static_pool *static_pool,
								 int serializer_type);

/* NEW CDT LIST OPERATIONS Post 3.16.0.1*/
/* GET BY*/
static as_status add_op_list_get_by_index(AerospikeClient *self, as_error *err,
										  char *bin, PyObject *op_dict,
										  as_vector *unicodeStrVector,
										  as_operations *ops,
										  as_static_pool *static_pool,
										  int serializer_type);

static as_status
add_op_list_get_by_index_range(AerospikeClient *self, as_error *err, char *bin,
							   PyObject *op_dict, as_vector *unicodeStrVector,
							   as_operations *ops, as_static_pool *static_pool,
							   int serializer_type);

static as_status add_op_list_get_by_rank(AerospikeClient *self, as_error *err,
										 char *bin, PyObject *op_dict,
										 as_vector *unicodeStrVector,
										 as_operations *ops,
										 as_static_pool *static_pool,
										 int serializer_type);

static as_status
add_op_list_get_by_rank_range(AerospikeClient *self, as_error *err, char *bin,
							  PyObject *op_dict, as_vector *unicodeStrVector,
							  as_operations *ops, as_static_pool *static_pool,
							  int serializer_type);

static as_status add_op_list_get_by_value(AerospikeClient *self, as_error *err,
										  char *bin, PyObject *op_dict,
										  as_operations *ops,
										  as_static_pool *static_pool,
										  int serializer_type);

static as_status
add_op_list_get_by_value_list(AerospikeClient *self, as_error *err, char *bin,
							  PyObject *op_dict, as_vector *unicodeStrVector,
							  as_operations *ops, as_static_pool *static_pool,
							  int serializer_type);

static as_status
add_op_list_get_by_value_range(AerospikeClient *self, as_error *err, char *bin,
							   PyObject *op_dict, as_vector *unicodeStrVector,
							   as_operations *ops, as_static_pool *static_pool,
							   int serializer_type);

/* remove by*/

static as_status
add_op_list_remove_by_index(AerospikeClient *self, as_error *err, char *bin,
							PyObject *op_dict, as_vector *unicodeStrVector,
							as_operations *ops, as_static_pool *static_pool,
							int serializer_type);

static as_status add_op_list_remove_by_index_range(
	AerospikeClient *self, as_error *err, char *bin, PyObject *op_dict,
	as_vector *unicodeStrVector, as_operations *ops,
	as_static_pool *static_pool, int serializer_type);

static as_status
add_op_list_remove_by_rank(AerospikeClient *self, as_error *err, char *bin,
						   PyObject *op_dict, as_vector *unicodeStrVector,
						   as_operations *ops, as_static_pool *static_pool,
						   int serializer_type);

static as_status add_op_list_remove_by_rank_range(
	AerospikeClient *self, as_error *err, char *bin, PyObject *op_dict,
	as_vector *unicodeStrVector, as_operations *ops,
	as_static_pool *static_pool, int serializer_type);

static as_status
add_op_list_remove_by_value(AerospikeClient *self, as_error *err, char *bin,
							PyObject *op_dict, as_vector *unicodeStrVector,
							as_operations *ops, as_static_pool *static_pool,
							int serializer_type);

static as_status add_op_list_remove_by_value_list(
	AerospikeClient *self, as_error *err, char *bin, PyObject *op_dict,
	as_vector *unicodeStrVector, as_operations *ops,
	as_static_pool *static_pool, int serializer_type);

static as_status add_op_list_remove_by_value_range(
	AerospikeClient *self, as_error *err, char *bin, PyObject *op_dict,
	as_operations *ops, as_static_pool *static_pool, int serializer_type);

/* Set Order*/
static as_status add_op_list_set_order(AerospikeClient *self, as_error *err,
									   char *bin, PyObject *op_dict,
									   as_operations *ops,
									   as_static_pool *static_pool,
									   int serializer_type);

/* List sort*/
static as_status add_op_list_sort(AerospikeClient *self, as_error *err,
								  char *bin, PyObject *op_dict,
								  as_operations *ops,
								  as_static_pool *static_pool,
								  int serializer_type);

/* Server 4.3.0 relative operations*/

static as_status add_add_op_list_remove_by_value_rel_rank_range(
	AerospikeClient *self, as_error *err, char *bin, PyObject *op_dict,
	as_operations *ops, as_static_pool *static_pool, int serializer_type);

static as_status add_add_op_list_get_by_value_rel_rank_range(
	AerospikeClient *self, as_error *err, char *bin, PyObject *op_dict,
	as_operations *ops, as_static_pool *static_pool, int serializer_type);
/* End forwards*/
as_status add_new_list_op(AerospikeClient *self, as_error *err,
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
	case OP_LIST_APPEND:
		return add_op_list_append(self, err, bin, op_dict, ops, static_pool,
								  serializer_type);

	case OP_LIST_APPEND_ITEMS:
		return add_op_list_append_items(self, err, bin, op_dict, ops,
										static_pool, serializer_type);

	case OP_LIST_SIZE:
		return add_op_list_size(self, err, bin, op_dict, ops, static_pool,
								serializer_type);

	case OP_LIST_INSERT:
		return add_op_list_insert(self, err, bin, op_dict, ops, static_pool,
								  serializer_type);

	case OP_LIST_INSERT_ITEMS:
		return add_op_list_insert_items(self, err, bin, op_dict, ops,
										static_pool, serializer_type);

	case OP_LIST_INCREMENT:
		return add_op_list_increment(self, err, bin, op_dict, ops, static_pool,
									 serializer_type);

	case OP_LIST_POP:
		return add_op_list_pop(self, err, bin, op_dict, ops, static_pool,
							   serializer_type);

	case OP_LIST_POP_RANGE:
		return add_op_list_pop_range(self, err, bin, op_dict, ops, static_pool,
									 serializer_type);

	case OP_LIST_REMOVE:
		return add_op_list_remove(self, err, bin, op_dict, ops, static_pool,
								  serializer_type);

	case OP_LIST_REMOVE_RANGE:
		return add_op_list_remove_range(self, err, bin, op_dict, ops,
										static_pool, serializer_type);

	case OP_LIST_CLEAR:
		return add_op_list_clear(self, err, bin, op_dict, ops, static_pool,
								 serializer_type);

	case OP_LIST_SET:
		return add_op_list_set(self, err, bin, op_dict, ops, static_pool,
							   serializer_type);

	case OP_LIST_GET:
		return add_op_list_get(self, err, bin, op_dict, ops, static_pool,
							   serializer_type);

	case OP_LIST_GET_RANGE:
		return add_op_list_get_range(self, err, bin, op_dict, ops, static_pool,
									 serializer_type);

	case OP_LIST_TRIM:
		return add_op_list_trim(self, err, bin, op_dict, ops, static_pool,
								serializer_type);
		/***** New List ops****/

	case OP_LIST_GET_BY_INDEX: {
		return add_op_list_get_by_index(self, err, bin, op_dict,
										unicodeStrVector, ops, static_pool,
										serializer_type);
	}

	case OP_LIST_GET_BY_INDEX_RANGE: {
		return add_op_list_get_by_index_range(self, err, bin, op_dict,
											  unicodeStrVector, ops,
											  static_pool, serializer_type);
	}

	case OP_LIST_GET_BY_RANK: {
		return add_op_list_get_by_rank(self, err, bin, op_dict,
									   unicodeStrVector, ops, static_pool,
									   serializer_type);
	}

	case OP_LIST_GET_BY_RANK_RANGE: {
		return add_op_list_get_by_rank_range(self, err, bin, op_dict,
											 unicodeStrVector, ops, static_pool,
											 serializer_type);
	}

	case OP_LIST_GET_BY_VALUE: {
		return add_op_list_get_by_value(self, err, bin, op_dict, ops,
										static_pool, serializer_type);
	}

	case OP_LIST_GET_BY_VALUE_LIST: {
		return add_op_list_get_by_value_list(self, err, bin, op_dict,
											 unicodeStrVector, ops, static_pool,
											 serializer_type);
	}

	case OP_LIST_GET_BY_VALUE_RANGE: {
		return add_op_list_get_by_value_range(self, err, bin, op_dict,
											  unicodeStrVector, ops,
											  static_pool, serializer_type);
	}

	case OP_LIST_REMOVE_BY_INDEX: {
		return add_op_list_remove_by_index(self, err, bin, op_dict,
										   unicodeStrVector, ops, static_pool,
										   serializer_type);
	}

	case OP_LIST_REMOVE_BY_INDEX_RANGE: {
		return add_op_list_remove_by_index_range(self, err, bin, op_dict,
												 unicodeStrVector, ops,
												 static_pool, serializer_type);
	}

	case OP_LIST_REMOVE_BY_RANK: {
		return add_op_list_remove_by_rank(self, err, bin, op_dict,
										  unicodeStrVector, ops, static_pool,
										  serializer_type);
	}

	case OP_LIST_REMOVE_BY_RANK_RANGE: {
		return add_op_list_remove_by_rank_range(self, err, bin, op_dict,
												unicodeStrVector, ops,
												static_pool, serializer_type);
	}

	case OP_LIST_REMOVE_BY_VALUE: {
		return add_op_list_remove_by_value(self, err, bin, op_dict,
										   unicodeStrVector, ops, static_pool,
										   serializer_type);
	}

	case OP_LIST_REMOVE_BY_VALUE_LIST: {
		return add_op_list_remove_by_value_list(self, err, bin, op_dict,
												unicodeStrVector, ops,
												static_pool, serializer_type);
	}

	case OP_LIST_REMOVE_BY_VALUE_RANGE: {
		return add_op_list_remove_by_value_range(self, err, bin, op_dict, ops,
												 static_pool, serializer_type);
	}

	case OP_LIST_SET_ORDER: {
		return add_op_list_set_order(self, err, bin, op_dict, ops, static_pool,
									 serializer_type);
	}

	case OP_LIST_SORT: {
		return add_op_list_sort(self, err, bin, op_dict, ops, static_pool,
								serializer_type);
	}

	case OP_LIST_REMOVE_BY_VALUE_RANK_RANGE_REL: {
		return add_add_op_list_remove_by_value_rel_rank_range(
			self, err, bin, op_dict, ops, static_pool, serializer_type);
	}

	case OP_LIST_GET_BY_VALUE_RANK_RANGE_REL: {
		return add_add_op_list_get_by_value_rel_rank_range(
			self, err, bin, op_dict, ops, static_pool, serializer_type);
	}

	default:
		// This should never be possible since we only get here if we know that the operation is valid.
		return as_error_update(err, AEROSPIKE_ERR_PARAM, "Unknown operation");
	}

	return err->code;
}

static as_status add_op_list_get_by_index(AerospikeClient *self, as_error *err,
										  char *bin, PyObject *op_dict,
										  as_vector *unicodeStrVector,
										  as_operations *ops,
										  as_static_pool *static_pool,
										  int serializer_type)
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

	if (get_cdt_ctx(self, err, &ctx, op_dict, &ctx_in_use, static_pool,
					serializer_type) != AEROSPIKE_OK) {
		return err->code;
	}

	if (!as_operations_list_get_by_index(ops, bin, (ctx_in_use ? &ctx : NULL),
										 index, return_type)) {
		as_error_update(err, AEROSPIKE_ERR_CLIENT,
						"Failed to add get_by_list_index operation");
	}

	if (ctx_in_use) {
		as_cdt_ctx_destroy(&ctx);
	}

	return err->code;
}

static as_status
add_op_list_get_by_index_range(AerospikeClient *self, as_error *err, char *bin,
							   PyObject *op_dict, as_vector *unicodeStrVector,
							   as_operations *ops, as_static_pool *static_pool,
							   int serializer_type)
{
	int64_t index;
	int64_t count;
	bool range_specified = false;
	bool success = false;
	int return_type = AS_LIST_RETURN_VALUE;
	bool ctx_in_use = false;
	as_cdt_ctx ctx;

	/* Get the index*/
	if (get_int64_t(err, AS_PY_INDEX_KEY, op_dict, &index) != AEROSPIKE_OK) {
		return err->code;
	}

	/* Get the count of items, and store whether it was found in range_specified*/
	if (get_optional_int64_t(err, AS_PY_COUNT_KEY, op_dict, &count,
							 &range_specified) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_list_return_type(err, op_dict, &return_type) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_cdt_ctx(self, err, &ctx, op_dict, &ctx_in_use, static_pool,
					serializer_type) != AEROSPIKE_OK) {
		return err->code;
	}

	if (range_specified) {
		success = as_operations_list_get_by_index_range(
			ops, bin, (ctx_in_use ? &ctx : NULL), index, (uint64_t)count,
			return_type);
	}
	else {
		success = as_operations_list_get_by_index_range_to_end(
			ops, bin, (ctx_in_use ? &ctx : NULL), index, return_type);
	}

	if (!success) {
		as_error_update(err, AEROSPIKE_ERR_CLIENT,
						"Failed to add get_by_list_index_range operation");
	}

	if (ctx_in_use) {
		as_cdt_ctx_destroy(&ctx);
	}

	return err->code;
}

static as_status add_op_list_get_by_rank(AerospikeClient *self, as_error *err,
										 char *bin, PyObject *op_dict,
										 as_vector *unicodeStrVector,
										 as_operations *ops,
										 as_static_pool *static_pool,
										 int serializer_type)
{
	int64_t rank;
	int return_type = AS_LIST_RETURN_VALUE;
	bool ctx_in_use = false;
	as_cdt_ctx ctx;

	/* Get the index*/
	if (get_int64_t(err, AS_PY_RANK_KEY, op_dict, &rank) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_list_return_type(err, op_dict, &return_type) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_cdt_ctx(self, err, &ctx, op_dict, &ctx_in_use, static_pool,
					serializer_type) != AEROSPIKE_OK) {
		return err->code;
	}

	if (!as_operations_list_get_by_rank(ops, bin, (ctx_in_use ? &ctx : NULL),
										rank, return_type)) {
		as_error_update(err, AEROSPIKE_ERR_CLIENT,
						"Failed to add get_by_list_index operation");
	}

	if (ctx_in_use) {
		as_cdt_ctx_destroy(&ctx);
	}

	return err->code;
}

static as_status
add_op_list_get_by_rank_range(AerospikeClient *self, as_error *err, char *bin,
							  PyObject *op_dict, as_vector *unicodeStrVector,
							  as_operations *ops, as_static_pool *static_pool,
							  int serializer_type)
{
	int64_t rank;
	int64_t count;
	bool range_specified = false;
	bool success = false;
	int return_type = AS_LIST_RETURN_VALUE;
	bool ctx_in_use = false;
	as_cdt_ctx ctx;

	/* Get the index*/
	if (get_int64_t(err, AS_PY_RANK_KEY, op_dict, &rank) != AEROSPIKE_OK) {
		return err->code;
	}

	/* Get the count of items, and store whether it was found in range_specified*/
	if (get_optional_int64_t(err, AS_PY_COUNT_KEY, op_dict, &count,
							 &range_specified) != AEROSPIKE_OK) {
		return err->code;
	}
	if (get_list_return_type(err, op_dict, &return_type) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_cdt_ctx(self, err, &ctx, op_dict, &ctx_in_use, static_pool,
					serializer_type) != AEROSPIKE_OK) {
		return err->code;
	}

	if (range_specified) {
		success = as_operations_list_get_by_rank_range(
			ops, bin, (ctx_in_use ? &ctx : NULL), rank, (uint64_t)count,
			return_type);
	}
	else {
		success = as_operations_list_get_by_rank_range_to_end(
			ops, bin, (ctx_in_use ? &ctx : NULL), rank, return_type);
	}

	if (!success) {
		as_error_update(err, AEROSPIKE_ERR_CLIENT,
						"Failed to add list_get_by_rank_range operation");
	}

	if (ctx_in_use) {
		as_cdt_ctx_destroy(&ctx);
	}

	return err->code;
}

static as_status add_op_list_get_by_value(AerospikeClient *self, as_error *err,
										  char *bin, PyObject *op_dict,
										  as_operations *ops,
										  as_static_pool *static_pool,
										  int serializer_type)
{
	as_val *val = NULL;
	int return_type = AS_LIST_RETURN_VALUE;
	bool ctx_in_use = false;
	as_cdt_ctx ctx;

	if (get_list_return_type(err, op_dict, &return_type) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_asval(self, err, AS_PY_VAL_KEY, op_dict, &val, static_pool,
				  serializer_type, true) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_cdt_ctx(self, err, &ctx, op_dict, &ctx_in_use, static_pool,
					serializer_type) != AEROSPIKE_OK) {
		as_error_update(err, AEROSPIKE_ERR_CLIENT,
						"Failed to convert ctx list");
	}

	if (!as_operations_list_get_by_value(ops, bin, (ctx_in_use ? &ctx : NULL),
										 val, return_type)) {
		as_error_update(err, AEROSPIKE_ERR_CLIENT,
						"Failed to add list_get_by_value operation");
	}

	if (ctx_in_use) {
		as_cdt_ctx_destroy(&ctx);
	}

	return err->code;
}

static as_status
add_op_list_get_by_value_list(AerospikeClient *self, as_error *err, char *bin,
							  PyObject *op_dict, as_vector *unicodeStrVector,
							  as_operations *ops, as_static_pool *static_pool,
							  int serializer_type)
{
	as_list *value_list = NULL;
	int return_type = AS_LIST_RETURN_VALUE;
	bool ctx_in_use = false;
	as_cdt_ctx ctx;

	if (get_list_return_type(err, op_dict, &return_type) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_val_list(self, err, AS_PY_VALUES_KEY, op_dict, &value_list,
					 static_pool, serializer_type) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_cdt_ctx(self, err, &ctx, op_dict, &ctx_in_use, static_pool,
					serializer_type) != AEROSPIKE_OK) {
		/* Failed to add the operation, we need to destroy the list of values*/
		as_val_destroy(value_list);
		return err->code;
	}

	if (!as_operations_list_get_by_value_list(
			ops, bin, (ctx_in_use ? &ctx : NULL), value_list, return_type)) {
		/* Failed to add the operation, we need to destroy the list of values*/
		as_error_update(err, AEROSPIKE_ERR_CLIENT,
						"Failed to add list_get_by_value_list operation");
		as_val_destroy(value_list);
	}

	if (ctx_in_use) {
		as_cdt_ctx_destroy(&ctx);
	}

	return err->code;
}

static as_status
add_op_list_get_by_value_range(AerospikeClient *self, as_error *err, char *bin,
							   PyObject *op_dict, as_vector *unicodeStrVector,
							   as_operations *ops, as_static_pool *static_pool,
							   int serializer_type)
{
	as_val *val_begin = NULL;
	as_val *val_end = NULL;
	bool ctx_in_use = false;
	as_cdt_ctx ctx;

	int return_type = AS_LIST_RETURN_VALUE;

	if (get_list_return_type(err, op_dict, &return_type) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_asval(self, err, AS_PY_VAL_BEGIN_KEY, op_dict, &val_begin,
				  static_pool, serializer_type, false) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_asval(self, err, AS_PY_VAL_END_KEY, op_dict, &val_end, static_pool,
				  serializer_type, false) != AEROSPIKE_OK) {
		goto ERROR;
	}

	if (get_cdt_ctx(self, err, &ctx, op_dict, &ctx_in_use, static_pool,
					serializer_type) != AEROSPIKE_OK) {
		goto ERROR;
	}

	if (!as_operations_list_get_by_value_range(
			ops, bin, (ctx_in_use ? &ctx : NULL), val_begin, val_end,
			return_type)) {
		as_error_update(err, AEROSPIKE_ERR_CLIENT,
						"Failed to add list_get_by_value_range operation");
		goto ERROR;
	}

	if (ctx_in_use) {
		as_cdt_ctx_destroy(&ctx);
	}

	return err->code;

ERROR:
	/* Free the as_vals if they exists*/
	if (val_begin) {
		as_val_destroy(val_begin);
	}

	if (val_end) {
		as_val_destroy(val_end);
	}

	if (ctx_in_use) {
		as_cdt_ctx_destroy(&ctx);
	}

	return err->code;
}

static as_status
add_op_list_remove_by_index(AerospikeClient *self, as_error *err, char *bin,
							PyObject *op_dict, as_vector *unicodeStrVector,
							as_operations *ops, as_static_pool *static_pool,
							int serializer_type)
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

	if (get_cdt_ctx(self, err, &ctx, op_dict, &ctx_in_use, static_pool,
					serializer_type) != AEROSPIKE_OK) {
		return err->code;
	}

	if (!as_operations_list_remove_by_index(
			ops, bin, (ctx_in_use ? &ctx : NULL), index, return_type)) {
		as_error_update(err, AEROSPIKE_ERR_CLIENT,
						"Failed to add remove_by_list_index operation");
	}

	if (ctx_in_use) {
		as_cdt_ctx_destroy(&ctx);
	}

	return err->code;
}

static as_status add_op_list_remove_by_index_range(
	AerospikeClient *self, as_error *err, char *bin, PyObject *op_dict,
	as_vector *unicodeStrVector, as_operations *ops,
	as_static_pool *static_pool, int serializer_type)
{
	int64_t index;
	int64_t count;
	bool range_specified = false;
	bool success = false;
	int return_type = AS_LIST_RETURN_VALUE;
	bool ctx_in_use = false;
	as_cdt_ctx ctx;

	/* Get the index*/
	if (get_int64_t(err, AS_PY_INDEX_KEY, op_dict, &index) != AEROSPIKE_OK) {
		return err->code;
	}

	/* Get the count of items, and store whether it was found in range_specified*/
	if (get_optional_int64_t(err, AS_PY_COUNT_KEY, op_dict, &count,
							 &range_specified) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_list_return_type(err, op_dict, &return_type) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_cdt_ctx(self, err, &ctx, op_dict, &ctx_in_use, static_pool,
					serializer_type) != AEROSPIKE_OK) {
		return err->code;
	}

	if (range_specified) {
		success = as_operations_list_remove_by_index_range(
			ops, bin, (ctx_in_use ? &ctx : NULL), index, (uint64_t)count,
			return_type);
	}
	else {
		success = as_operations_list_remove_by_index_range_to_end(
			ops, bin, (ctx_in_use ? &ctx : NULL), index, return_type);
	}

	if (!success) {
		as_error_update(err, AEROSPIKE_ERR_CLIENT,
						"Failed to add remove_by_list_index_range operation");
	}

	if (ctx_in_use) {
		as_cdt_ctx_destroy(&ctx);
	}

	return err->code;
}

static as_status
add_op_list_remove_by_rank(AerospikeClient *self, as_error *err, char *bin,
						   PyObject *op_dict, as_vector *unicodeStrVector,
						   as_operations *ops, as_static_pool *static_pool,
						   int serializer_type)
{
	int64_t rank;
	int return_type = AS_LIST_RETURN_VALUE;
	bool ctx_in_use = false;
	as_cdt_ctx ctx;

	/* Get the index*/
	if (get_int64_t(err, AS_PY_RANK_KEY, op_dict, &rank) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_list_return_type(err, op_dict, &return_type) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_cdt_ctx(self, err, &ctx, op_dict, &ctx_in_use, static_pool,
					serializer_type) != AEROSPIKE_OK) {
		return err->code;
	}

	if (!as_operations_list_remove_by_rank(ops, bin, (ctx_in_use ? &ctx : NULL),
										   rank, return_type)) {
		as_error_update(err, AEROSPIKE_ERR_CLIENT,
						"Failed to add list_remove_by_rank operation");
	}

	if (ctx_in_use) {
		as_cdt_ctx_destroy(&ctx);
	}

	return err->code;
}

static as_status add_op_list_remove_by_rank_range(
	AerospikeClient *self, as_error *err, char *bin, PyObject *op_dict,
	as_vector *unicodeStrVector, as_operations *ops,
	as_static_pool *static_pool, int serializer_type)
{
	int64_t rank;
	int64_t count;
	bool range_specified = false;
	bool success = false;
	int return_type = AS_LIST_RETURN_VALUE;
	bool ctx_in_use = false;
	as_cdt_ctx ctx;

	/* Get the index*/
	if (get_int64_t(err, AS_PY_RANK_KEY, op_dict, &rank) != AEROSPIKE_OK) {
		return err->code;
	}

	/* Get the count of items, and store whether it was found in range_specified*/
	if (get_optional_int64_t(err, AS_PY_COUNT_KEY, op_dict, &count,
							 &range_specified) != AEROSPIKE_OK) {
		return err->code;
	}
	if (get_list_return_type(err, op_dict, &return_type) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_cdt_ctx(self, err, &ctx, op_dict, &ctx_in_use, static_pool,
					serializer_type) != AEROSPIKE_OK) {
		return err->code;
	}

	if (range_specified) {
		success = as_operations_list_remove_by_rank_range(
			ops, bin, (ctx_in_use ? &ctx : NULL), rank, (uint64_t)count,
			return_type);
	}
	else {
		success = as_operations_list_remove_by_rank_range_to_end(
			ops, bin, (ctx_in_use ? &ctx : NULL), rank, return_type);
	}

	if (!success) {
		as_error_update(err, AEROSPIKE_ERR_CLIENT,
						"Failed to add list_remove_by_rank_range operation");
	}

	if (ctx_in_use) {
		as_cdt_ctx_destroy(&ctx);
	}

	return err->code;
}

static as_status
add_op_list_remove_by_value(AerospikeClient *self, as_error *err, char *bin,
							PyObject *op_dict, as_vector *unicodeStrVector,
							as_operations *ops, as_static_pool *static_pool,
							int serializer_type)
{
	as_val *val = NULL;
	int return_type = AS_LIST_RETURN_VALUE;
	bool ctx_in_use = false;
	as_cdt_ctx ctx;

	if (get_list_return_type(err, op_dict, &return_type) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_asval(self, err, AS_PY_VAL_KEY, op_dict, &val, static_pool,
				  serializer_type, true) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_cdt_ctx(self, err, &ctx, op_dict, &ctx_in_use, static_pool,
					serializer_type) != AEROSPIKE_OK) {
		as_val_destroy(val);
		return err->code;
	}

	if (!as_operations_list_remove_by_value(
			ops, bin, (ctx_in_use ? &ctx : NULL), val, return_type)) {
		as_error_update(err, AEROSPIKE_ERR_CLIENT,
						"Failed to add list_remove_by_value operation");
	}

	if (ctx_in_use) {
		as_cdt_ctx_destroy(&ctx);
	}

	return err->code;
}

static as_status add_op_list_remove_by_value_list(
	AerospikeClient *self, as_error *err, char *bin, PyObject *op_dict,
	as_vector *unicodeStrVector, as_operations *ops,
	as_static_pool *static_pool, int serializer_type)
{
	as_list *value_list = NULL;
	int return_type = AS_LIST_RETURN_VALUE;
	bool ctx_in_use = false;
	as_cdt_ctx ctx;

	if (get_list_return_type(err, op_dict, &return_type) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_val_list(self, err, AS_PY_VALUES_KEY, op_dict, &value_list,
					 static_pool, serializer_type) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_cdt_ctx(self, err, &ctx, op_dict, &ctx_in_use, static_pool,
					serializer_type) != AEROSPIKE_OK) {
		/* Failed to convert ctx, we need to destroy the list of values*/
		as_val_destroy(value_list);
		return err->code;
	}

	if (!as_operations_list_remove_by_value_list(
			ops, bin, (ctx_in_use ? &ctx : NULL), value_list, return_type)) {
		/* Failed to add the operation, we need to destroy the list of values*/
		as_error_update(err, AEROSPIKE_ERR_CLIENT,
						"Failed to add list_get_by_value_list operation");
		as_val_destroy(value_list);
	}

	if (ctx_in_use) {
		as_cdt_ctx_destroy(&ctx);
	}

	return err->code;
}

static as_status add_op_list_remove_by_value_range(
	AerospikeClient *self, as_error *err, char *bin, PyObject *op_dict,
	as_operations *ops, as_static_pool *static_pool, int serializer_type)
{
	as_val *val_begin = NULL;
	as_val *val_end = NULL;
	bool ctx_in_use = false;
	as_cdt_ctx ctx;
	int return_type = AS_LIST_RETURN_VALUE;

	if (get_list_return_type(err, op_dict, &return_type) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_asval(self, err, AS_PY_VAL_BEGIN_KEY, op_dict, &val_begin,
				  static_pool, serializer_type, false) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_asval(self, err, AS_PY_VAL_END_KEY, op_dict, &val_end, static_pool,
				  serializer_type, false) != AEROSPIKE_OK) {
		goto ERROR;
	}

	if (get_cdt_ctx(self, err, &ctx, op_dict, &ctx_in_use, static_pool,
					serializer_type) != AEROSPIKE_OK) {
		goto ERROR;
	}

	if (!as_operations_list_remove_by_value_range(
			ops, bin, (ctx_in_use ? &ctx : NULL), val_begin, val_end,
			return_type)) {
		as_error_update(err, AEROSPIKE_ERR_CLIENT,
						"Failed to add list_remove_by_value_range operation");
		goto ERROR;
	}

	if (ctx_in_use) {
		as_cdt_ctx_destroy(&ctx);
	}

	return err->code;

ERROR:
	/* Free the as_vals if they exist */
	if (val_begin) {
		as_val_destroy(val_begin);
	}

	if (val_end) {
		as_val_destroy(val_end);
	}

	if (ctx_in_use) {
		as_cdt_ctx_destroy(&ctx);
	}

	return err->code;
}

static as_status add_op_list_set_order(AerospikeClient *self, as_error *err,
									   char *bin, PyObject *op_dict,
									   as_operations *ops,
									   as_static_pool *static_pool,
									   int serializer_type)
{
	int64_t order_type_int;
	bool ctx_in_use = false;
	as_cdt_ctx ctx;

	if (get_int64_t(err, AS_PY_LIST_ORDER, op_dict, &order_type_int) !=
		AEROSPIKE_OK) {
		return err->code;
	}

	if (get_cdt_ctx(self, err, &ctx, op_dict, &ctx_in_use, static_pool,
					serializer_type) != AEROSPIKE_OK) {
		return err->code;
	}

	if (!as_operations_list_set_order(ops, bin, (ctx_in_use ? &ctx : NULL),
									  (as_list_order)order_type_int)) {
		as_cdt_ctx_destroy(&ctx);
		return as_error_update(err, AEROSPIKE_ERR_CLIENT,
							   "Failed to add list_set_order operation");
	}

	if (ctx_in_use) {
		as_cdt_ctx_destroy(&ctx);
	}

	return AEROSPIKE_OK;
}

static as_status add_op_list_sort(AerospikeClient *self, as_error *err,
								  char *bin, PyObject *op_dict,
								  as_operations *ops,
								  as_static_pool *static_pool,
								  int serializer_type)
{
	int64_t sort_flags;
	bool ctx_in_use = false;
	as_cdt_ctx ctx;

	if (get_int64_t(err, AS_PY_LIST_SORT_FLAGS, op_dict, &sort_flags) !=
		AEROSPIKE_OK) {
		return err->code;
	}

	if (get_cdt_ctx(self, err, &ctx, op_dict, &ctx_in_use, static_pool,
					serializer_type) != AEROSPIKE_OK) {
		return err->code;
	}

	if (!as_operations_list_sort(ops, bin, (ctx_in_use ? &ctx : NULL),
								 (as_list_sort_flags)sort_flags)) {
		as_cdt_ctx_destroy(&ctx);
		return as_error_update(err, AEROSPIKE_ERR_CLIENT,
							   "Failed to add list_sort operation");
	}

	if (ctx_in_use) {
		as_cdt_ctx_destroy(&ctx);
	}

	return AEROSPIKE_OK;
}

static as_status add_op_list_append(AerospikeClient *self, as_error *err,
									char *bin, PyObject *op_dict,
									as_operations *ops,
									as_static_pool *static_pool,
									int serializer_type)
{
	as_val *val = NULL;
	as_list_policy list_policy;
	bool policy_in_use = false;
	bool ctx_in_use = false;
	as_cdt_ctx ctx;

	if (get_list_policy(err, op_dict, &list_policy, &policy_in_use) !=
		AEROSPIKE_OK) {
		return err->code;
	}

	if (get_asval(self, err, AS_PY_VAL_KEY, op_dict, &val, static_pool,
				  serializer_type, true) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_cdt_ctx(self, err, &ctx, op_dict, &ctx_in_use, static_pool,
					serializer_type) != AEROSPIKE_OK) {
		as_val_destroy(val);
		return err->code;
	}

	if (!as_operations_list_append(ops, bin, (ctx_in_use ? &ctx : NULL),
								   (policy_in_use ? &list_policy : NULL),
								   val)) {
		as_val_destroy(val);
		as_cdt_ctx_destroy(&ctx);
		return as_error_update(err, AEROSPIKE_ERR_CLIENT,
							   "Failed to add list_append operation");
	}

	if (ctx_in_use) {
		as_cdt_ctx_destroy(&ctx);
	}

	return AEROSPIKE_OK;
}

static as_status add_op_list_append_items(AerospikeClient *self, as_error *err,
										  char *bin, PyObject *op_dict,
										  as_operations *ops,
										  as_static_pool *static_pool,
										  int serializer_type)
{
	as_list *items_list = NULL;
	as_list_policy list_policy;
	bool policy_in_use = false;
	bool ctx_in_use = false;
	as_cdt_ctx ctx;

	if (get_list_policy(err, op_dict, &list_policy, &policy_in_use) !=
		AEROSPIKE_OK) {
		return err->code;
	}

	if (get_val_list(self, err, AS_PY_VAL_KEY, op_dict, &items_list,
					 static_pool, serializer_type) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_cdt_ctx(self, err, &ctx, op_dict, &ctx_in_use, static_pool,
					serializer_type) != AEROSPIKE_OK) {
		as_val_destroy(items_list);
		return err->code;
	}

	if (!as_operations_list_append_items(ops, bin, (ctx_in_use ? &ctx : NULL),
										 (policy_in_use ? &list_policy : NULL),
										 items_list)) {
		as_val_destroy(items_list);
		as_cdt_ctx_destroy(&ctx);
		return as_error_update(err, AEROSPIKE_ERR_CLIENT,
							   "Failed to add list_append_items operation");
	}

	if (ctx_in_use) {
		as_cdt_ctx_destroy(&ctx);
	}

	return AEROSPIKE_OK;
}

static as_status add_op_list_insert(AerospikeClient *self, as_error *err,
									char *bin, PyObject *op_dict,
									as_operations *ops,
									as_static_pool *static_pool,
									int serializer_type)
{
	as_val *val = NULL;
	int64_t index;
	as_list_policy list_policy;
	bool policy_in_use = false;
	bool ctx_in_use = false;
	as_cdt_ctx ctx;

	if (get_int64_t(err, AS_PY_INDEX_KEY, op_dict, &index) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_list_policy(err, op_dict, &list_policy, &policy_in_use) !=
		AEROSPIKE_OK) {
		return err->code;
	}

	if (get_asval(self, err, AS_PY_VAL_KEY, op_dict, &val, static_pool,
				  serializer_type, true) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_cdt_ctx(self, err, &ctx, op_dict, &ctx_in_use, static_pool,
					serializer_type) != AEROSPIKE_OK) {
		as_val_destroy(val);
		return err->code;
	}

	if (!as_operations_list_insert(ops, bin, (ctx_in_use ? &ctx : NULL),
								   (policy_in_use ? &list_policy : NULL), index,
								   val)) {
		as_val_destroy(val);
		as_cdt_ctx_destroy(&ctx);
		return as_error_update(err, AEROSPIKE_ERR_CLIENT,
							   "Failed to add list_insert operation");
	}

	if (ctx_in_use) {
		as_cdt_ctx_destroy(&ctx);
	}

	return AEROSPIKE_OK;
}

static as_status add_op_list_insert_items(AerospikeClient *self, as_error *err,
										  char *bin, PyObject *op_dict,
										  as_operations *ops,
										  as_static_pool *static_pool,
										  int serializer_type)
{
	as_list *items_list = NULL;
	int64_t index;
	as_list_policy list_policy;
	bool policy_in_use = false;
	bool ctx_in_use = false;
	as_cdt_ctx ctx;

	if (get_int64_t(err, AS_PY_INDEX_KEY, op_dict, &index) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_list_policy(err, op_dict, &list_policy, &policy_in_use) !=
		AEROSPIKE_OK) {
		return err->code;
	}

	if (get_val_list(self, err, AS_PY_VAL_KEY, op_dict, &items_list,
					 static_pool, serializer_type) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_cdt_ctx(self, err, &ctx, op_dict, &ctx_in_use, static_pool,
					serializer_type) != AEROSPIKE_OK) {
		as_val_destroy(items_list);
		return err->code;
	}

	if (!as_operations_list_insert_items(ops, bin, (ctx_in_use ? &ctx : NULL),
										 (policy_in_use ? &list_policy : NULL),
										 index, items_list)) {
		as_val_destroy(items_list);
		as_cdt_ctx_destroy(&ctx);
		return as_error_update(err, AEROSPIKE_ERR_CLIENT,
							   "Failed to add list_insert_items operation");
	}

	if (ctx_in_use) {
		as_cdt_ctx_destroy(&ctx);
	}

	return AEROSPIKE_OK;
}

static as_status add_op_list_increment(AerospikeClient *self, as_error *err,
									   char *bin, PyObject *op_dict,
									   as_operations *ops,
									   as_static_pool *static_pool,
									   int serializer_type)
{
	as_val *incr = NULL;
	int64_t index;
	as_list_policy list_policy;
	bool policy_in_use = false;
	bool ctx_in_use = false;
	as_cdt_ctx ctx;

	if (get_list_policy(err, op_dict, &list_policy, &policy_in_use) !=
		AEROSPIKE_OK) {
		return err->code;
	}

	if (get_int64_t(err, AS_PY_INDEX_KEY, op_dict, &index) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_asval(self, err, AS_PY_VAL_KEY, op_dict, &incr, static_pool,
				  serializer_type, true) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_cdt_ctx(self, err, &ctx, op_dict, &ctx_in_use, static_pool,
					serializer_type) != AEROSPIKE_OK) {
		as_val_destroy(incr);
		return err->code;
	}

	if (!as_operations_list_increment(ops, bin, (ctx_in_use ? &ctx : NULL),
									  (policy_in_use ? &list_policy : NULL),
									  index, incr)) {
		as_val_destroy(incr);
		as_cdt_ctx_destroy(&ctx);
		return as_error_update(err, AEROSPIKE_ERR_CLIENT,
							   "Failed to add list_increment operation");
	}

	if (ctx_in_use) {
		as_cdt_ctx_destroy(&ctx);
	}

	return AEROSPIKE_OK;
}

static as_status add_op_list_pop(AerospikeClient *self, as_error *err,
								 char *bin, PyObject *op_dict,
								 as_operations *ops,
								 as_static_pool *static_pool,
								 int serializer_type)
{
	int64_t index;
	bool ctx_in_use = false;
	as_cdt_ctx ctx;

	/* Get the index*/
	if (get_int64_t(err, AS_PY_INDEX_KEY, op_dict, &index) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_cdt_ctx(self, err, &ctx, op_dict, &ctx_in_use, static_pool,
					serializer_type) != AEROSPIKE_OK) {
		return err->code;
	}

	if (!as_operations_list_pop(ops, bin, (ctx_in_use ? &ctx : NULL), index)) {
		as_error_update(err, AEROSPIKE_ERR_CLIENT,
						"Failed to add list_pop operation");
	}

	if (ctx_in_use) {
		as_cdt_ctx_destroy(&ctx);
	}

	return err->code;
}

static as_status add_op_list_pop_range(AerospikeClient *self, as_error *err,
									   char *bin, PyObject *op_dict,
									   as_operations *ops,
									   as_static_pool *static_pool,
									   int serializer_type)
{
	int64_t index;
	int64_t count;
	bool ctx_in_use = false;
	as_cdt_ctx ctx;

	/* Get the index*/
	if (get_int64_t(err, AS_PY_INDEX_KEY, op_dict, &index) != AEROSPIKE_OK) {
		return err->code;
	}

	/* Get the count*/
	if (get_int64_t(err, AS_PY_VAL_KEY, op_dict, &count) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_cdt_ctx(self, err, &ctx, op_dict, &ctx_in_use, static_pool,
					serializer_type) != AEROSPIKE_OK) {
		return err->code;
	}

	if (!as_operations_list_pop_range(ops, bin, (ctx_in_use ? &ctx : NULL),
									  index, (uint64_t)count)) {
		as_error_update(err, AEROSPIKE_ERR_CLIENT,
						"Failed to list_pop_range operation");
	}

	if (ctx_in_use) {
		as_cdt_ctx_destroy(&ctx);
	}

	return err->code;
}

static as_status add_op_list_remove(AerospikeClient *self, as_error *err,
									char *bin, PyObject *op_dict,
									as_operations *ops,
									as_static_pool *static_pool,
									int serializer_type)
{
	int64_t index;
	bool ctx_in_use = false;
	as_cdt_ctx ctx;

	if (get_int64_t(err, AS_PY_INDEX_KEY, op_dict, &index) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_cdt_ctx(self, err, &ctx, op_dict, &ctx_in_use, static_pool,
					serializer_type) != AEROSPIKE_OK) {
		;
		return err->code;
	}

	if (!as_operations_list_remove(ops, bin, (ctx_in_use ? &ctx : NULL),
								   index)) {
		as_cdt_ctx_destroy(&ctx);
		return as_error_update(err, AEROSPIKE_ERR_CLIENT,
							   "Failed to add list_remove operation");
	}

	if (ctx_in_use) {
		as_cdt_ctx_destroy(&ctx);
	}

	return AEROSPIKE_OK;
}

static as_status add_op_list_remove_range(AerospikeClient *self, as_error *err,
										  char *bin, PyObject *op_dict,
										  as_operations *ops,
										  as_static_pool *static_pool,
										  int serializer_type)
{
	int64_t index;
	int64_t count;
	bool ctx_in_use = false;
	as_cdt_ctx ctx;

	/* Get the index*/
	if (get_int64_t(err, AS_PY_INDEX_KEY, op_dict, &index) != AEROSPIKE_OK) {
		return err->code;
	}

	/* Get the count*/
	if (get_int64_t(err, AS_PY_VAL_KEY, op_dict, &count) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_cdt_ctx(self, err, &ctx, op_dict, &ctx_in_use, static_pool,
					serializer_type) != AEROSPIKE_OK) {
		return err->code;
	}

	if (!as_operations_list_remove_range(ops, bin, (ctx_in_use ? &ctx : NULL),
										 index, (uint64_t)count)) {
		as_error_update(err, AEROSPIKE_ERR_CLIENT,
						"Failed to list_remove_range operation");
	}

	if (ctx_in_use) {
		as_cdt_ctx_destroy(&ctx);
	}

	return err->code;
}

static as_status add_op_list_clear(AerospikeClient *self, as_error *err,
								   char *bin, PyObject *op_dict,
								   as_operations *ops,
								   as_static_pool *static_pool,
								   int serializer_type)
{
	bool ctx_in_use = false;
	as_cdt_ctx ctx;

	if (get_cdt_ctx(self, err, &ctx, op_dict, &ctx_in_use, static_pool,
					serializer_type) != AEROSPIKE_OK) {
		return err->code;
	}

	if (!as_operations_list_clear(ops, bin, (ctx_in_use ? &ctx : NULL))) {
		as_cdt_ctx_destroy(&ctx);
		return as_error_update(err, AEROSPIKE_ERR_CLIENT,
							   "Failed to add list_clear operation");
	}

	if (ctx_in_use) {
		as_cdt_ctx_destroy(&ctx);
	}

	return AEROSPIKE_OK;
}

static as_status add_op_list_set(AerospikeClient *self, as_error *err,
								 char *bin, PyObject *op_dict,
								 as_operations *ops,
								 as_static_pool *static_pool,
								 int serializer_type)
{
	as_val *val = NULL;
	int64_t index;
	as_list_policy list_policy;
	bool policy_in_use = false;
	bool ctx_in_use = false;
	as_cdt_ctx ctx;

	if (get_list_policy(err, op_dict, &list_policy, &policy_in_use) !=
		AEROSPIKE_OK) {
		return err->code;
	}

	if (get_int64_t(err, AS_PY_INDEX_KEY, op_dict, &index) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_asval(self, err, AS_PY_VAL_KEY, op_dict, &val, static_pool,
				  serializer_type, true) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_cdt_ctx(self, err, &ctx, op_dict, &ctx_in_use, static_pool,
					serializer_type) != AEROSPIKE_OK) {
		as_val_destroy(val);
		return err->code;
	}

	if (!as_operations_list_set(ops, bin, (ctx_in_use ? &ctx : NULL),
								(policy_in_use ? &list_policy : NULL), index,
								val)) {
		as_val_destroy(val);
		as_cdt_ctx_destroy(&ctx);
		return as_error_update(err, AEROSPIKE_ERR_CLIENT,
							   "Failed to add list_set operation");
	}

	if (ctx_in_use) {
		as_cdt_ctx_destroy(&ctx);
	}

	return AEROSPIKE_OK;
}

static as_status add_op_list_get(AerospikeClient *self, as_error *err,
								 char *bin, PyObject *op_dict,
								 as_operations *ops,
								 as_static_pool *static_pool,
								 int serializer_type)
{
	int64_t index;
	bool ctx_in_use = false;
	as_cdt_ctx ctx;

	if (get_int64_t(err, AS_PY_INDEX_KEY, op_dict, &index) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_cdt_ctx(self, err, &ctx, op_dict, &ctx_in_use, static_pool,
					serializer_type) != AEROSPIKE_OK) {
		return err->code;
	}

	if (!as_operations_list_get(ops, bin, (ctx_in_use ? &ctx : NULL), index)) {
		as_cdt_ctx_destroy(&ctx);
		return as_error_update(err, AEROSPIKE_ERR_CLIENT,
							   "Failed to add list_get operation");
	}

	return AEROSPIKE_OK;
}

static as_status add_op_list_get_range(AerospikeClient *self, as_error *err,
									   char *bin, PyObject *op_dict,
									   as_operations *ops,
									   as_static_pool *static_pool,
									   int serializer_type)
{
	int64_t index;
	int64_t count;
	bool ctx_in_use = false;
	as_cdt_ctx ctx;

	/* Get the index*/
	if (get_int64_t(err, AS_PY_INDEX_KEY, op_dict, &index) != AEROSPIKE_OK) {
		return err->code;
	}

	/* Get the count*/
	if (get_int64_t(err, AS_PY_VAL_KEY, op_dict, &count) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_cdt_ctx(self, err, &ctx, op_dict, &ctx_in_use, static_pool,
					serializer_type) != AEROSPIKE_OK) {
		return err->code;
	}

	if (!as_operations_list_get_range(ops, bin, (ctx_in_use ? &ctx : NULL),
									  index, (uint64_t)count)) {
		as_error_update(err, AEROSPIKE_ERR_CLIENT,
						"Failed to list_get_range operation");
	}

	if (ctx_in_use) {
		as_cdt_ctx_destroy(&ctx);
	}

	return err->code;
}

static as_status add_op_list_trim(AerospikeClient *self, as_error *err,
								  char *bin, PyObject *op_dict,
								  as_operations *ops,
								  as_static_pool *static_pool,
								  int serializer_type)
{
	int64_t index;
	int64_t count;
	bool ctx_in_use = false;
	as_cdt_ctx ctx;

	/* Get the index*/
	if (get_int64_t(err, AS_PY_INDEX_KEY, op_dict, &index) != AEROSPIKE_OK) {
		return err->code;
	}

	/* Get the count*/
	if (get_int64_t(err, AS_PY_VAL_KEY, op_dict, &count) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_cdt_ctx(self, err, &ctx, op_dict, &ctx_in_use, static_pool,
					serializer_type) != AEROSPIKE_OK) {
		return err->code;
	}

	if (!as_operations_list_trim(ops, bin, (ctx_in_use ? &ctx : NULL), index,
								 (uint64_t)count)) {
		as_error_update(err, AEROSPIKE_ERR_CLIENT,
						"Failed to list_trim operation");
	}

	if (ctx_in_use) {
		as_cdt_ctx_destroy(&ctx);
	}

	return err->code;
}

static as_status add_op_list_size(AerospikeClient *self, as_error *err,
								  char *bin, PyObject *op_dict,
								  as_operations *ops,
								  as_static_pool *static_pool,
								  int serializer_type)
{
	bool ctx_in_use = false;
	as_cdt_ctx ctx;

	if (get_cdt_ctx(self, err, &ctx, op_dict, &ctx_in_use, static_pool,
					serializer_type) != AEROSPIKE_OK) {
		return err->code;
	}

	if (!as_operations_list_size(ops, bin, (ctx_in_use ? &ctx : NULL))) {
		as_cdt_ctx_destroy(&ctx);
		return as_error_update(err, AEROSPIKE_ERR_CLIENT,
							   "Failed to add list_size operation");
	}

	if (ctx_in_use) {
		as_cdt_ctx_destroy(&ctx);
	}

	return AEROSPIKE_OK;
}

static as_status add_add_op_list_remove_by_value_rel_rank_range(
	AerospikeClient *self, as_error *err, char *bin, PyObject *op_dict,
	as_operations *ops, as_static_pool *static_pool, int serializer_type)
{
	bool count_present = false;
	int64_t count;
	int return_type;
	int64_t rank;
	as_val *value = NULL;
	bool ctx_in_use = false;
	as_cdt_ctx ctx;

	if (get_list_return_type(err, op_dict, &return_type) != AEROSPIKE_OK) {
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
		if (!as_operations_list_remove_by_value_rel_rank_range(
				ops, bin, (ctx_in_use ? &ctx : NULL), value, rank,
				(uint64_t)count, return_type)) {
			as_cdt_ctx_destroy(&ctx);
			return as_error_update(
				err, AEROSPIKE_ERR_CLIENT,
				"Failed to add list remove by value rank relative operation");
		}
	}
	else {
		if (!as_operations_list_remove_by_value_rel_rank_range_to_end(
				ops, bin, (ctx_in_use ? &ctx : NULL), value, rank,
				return_type)) {
			as_cdt_ctx_destroy(&ctx);
			return as_error_update(
				err, AEROSPIKE_ERR_CLIENT,
				"Failed to add list remove by value rank relative operation");
		}
	}

	if (ctx_in_use) {
		as_cdt_ctx_destroy(&ctx);
	}

	return AEROSPIKE_OK;
}

static as_status add_add_op_list_get_by_value_rel_rank_range(
	AerospikeClient *self, as_error *err, char *bin, PyObject *op_dict,
	as_operations *ops, as_static_pool *static_pool, int serializer_type)
{

	bool count_present = false;
	int64_t count;
	int return_type;
	int64_t rank;
	as_val *value = NULL;
	bool ctx_in_use = false;
	as_cdt_ctx ctx;

	if (get_list_return_type(err, op_dict, &return_type) != AEROSPIKE_OK) {
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
		if (!as_operations_list_get_by_value_rel_rank_range(
				ops, bin, (ctx_in_use ? &ctx : NULL), value, rank,
				(uint64_t)count, return_type)) {
			as_cdt_ctx_destroy(&ctx);
			return as_error_update(
				err, AEROSPIKE_ERR_CLIENT,
				"Failed to add list remove by value rank relative operation");
		}
	}
	else {
		if (!as_operations_list_get_by_value_rel_rank_range_to_end(
				ops, bin, (ctx_in_use ? &ctx : NULL), value, rank,
				return_type)) {
			as_cdt_ctx_destroy(&ctx);
			return as_error_update(
				err, AEROSPIKE_ERR_CLIENT,
				"Failed to add list remove by value rank relative operation");
		}
	}

	if (ctx_in_use) {
		as_cdt_ctx_destroy(&ctx);
	}

	return AEROSPIKE_OK;
}
