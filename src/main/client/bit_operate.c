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
								as_bit_policy *policy);

static as_status get_bit_resize_flags(as_error *err, PyObject *op_dict,
									  as_bit_resize_flags *resize_flags);

static as_status get_uint8t_from_pyargs(as_error *err, char *key,
										PyObject *op_dict, uint8_t **value);

static as_status get_bool_from_pyargs(as_error *err, char *key,
									  PyObject *op_dict, bool *boolean);

static as_status get_uint32t_from_pyargs(as_error *err, char *key,
										 PyObject *op_dict, uint32_t *value);

static as_status add_op_bit_resize(AerospikeClient *self, as_error *err,
								   char *bin, PyObject *op_dict,
								   as_operations *ops,
								   as_static_pool *static_pool,
								   int serializer_type);

static as_status add_op_bit_set(AerospikeClient *self, as_error *err, char *bin,
								PyObject *op_dict, as_operations *ops,
								as_static_pool *static_pool,
								int serializer_type);

static as_status add_op_bit_remove(AerospikeClient *self, as_error *err,
								   char *bin, PyObject *op_dict,
								   as_operations *ops,
								   as_static_pool *static_pool,
								   int serializer_type);

static as_status add_op_bit_count(AerospikeClient *self, as_error *err,
								  char *bin, PyObject *op_dict,
								  as_operations *ops,
								  as_static_pool *static_pool,
								  int serializer_type);

static as_status add_op_bit_add(AerospikeClient *self, as_error *err, char *bin,
								PyObject *op_dict, as_operations *ops,
								as_static_pool *static_pool,
								int serializer_type);

static as_status add_op_bit_and(AerospikeClient *self, as_error *err, char *bin,
								PyObject *op_dict, as_operations *ops,
								as_static_pool *static_pool,
								int serializer_type);

static as_status add_op_bit_get(AerospikeClient *self, as_error *err, char *bin,
								PyObject *op_dict, as_operations *ops,
								as_static_pool *static_pool,
								int serializer_type);

static as_status add_op_bit_get_int(AerospikeClient *self, as_error *err,
									char *bin, PyObject *op_dict,
									as_operations *ops,
									as_static_pool *static_pool,
									int serializer_type);

static as_status add_op_bit_insert(AerospikeClient *self, as_error *err,
								   char *bin, PyObject *op_dict,
								   as_operations *ops,
								   as_static_pool *static_pool,
								   int serializer_type);

static as_status add_op_bit_lscan(AerospikeClient *self, as_error *err,
								  char *bin, PyObject *op_dict,
								  as_operations *ops,
								  as_static_pool *static_pool,
								  int serializer_type);

static as_status add_op_bit_lshift(AerospikeClient *self, as_error *err,
								   char *bin, PyObject *op_dict,
								   as_operations *ops,
								   as_static_pool *static_pool,
								   int serializer_type);

static as_status add_op_bit_not(AerospikeClient *self, as_error *err, char *bin,
								PyObject *op_dict, as_operations *ops,
								as_static_pool *static_pool,
								int serializer_type);

static as_status add_op_bit_or(AerospikeClient *self, as_error *err, char *bin,
							   PyObject *op_dict, as_operations *ops,
							   as_static_pool *static_pool,
							   int serializer_type);

static as_status add_op_bit_rscan(AerospikeClient *self, as_error *err,
								  char *bin, PyObject *op_dict,
								  as_operations *ops,
								  as_static_pool *static_pool,
								  int serializer_type);

static as_status add_op_bit_rshift(AerospikeClient *self, as_error *err,
								   char *bin, PyObject *op_dict,
								   as_operations *ops,
								   as_static_pool *static_pool,
								   int serializer_type);

static as_status add_op_bit_subtract(AerospikeClient *self, as_error *err,
									 char *bin, PyObject *op_dict,
									 as_operations *ops,
									 as_static_pool *static_pool,
									 int serializer_type);

static as_status add_op_bit_xor(AerospikeClient *self, as_error *err, char *bin,
								PyObject *op_dict, as_operations *ops,
								as_static_pool *static_pool,
								int serializer_type);

// End forwards
as_status add_new_bit_op(AerospikeClient *self, as_error *err,
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
	case OP_BIT_RESIZE:
		return add_op_bit_resize(self, err, bin, op_dict, ops, static_pool,
								 serializer_type);
	case OP_BIT_SET:
		return add_op_bit_set(self, err, bin, op_dict, ops, static_pool,
							  serializer_type);
	case OP_BIT_REMOVE:
		return add_op_bit_remove(self, err, bin, op_dict, ops, static_pool,
								 serializer_type);
	case OP_BIT_COUNT:
		return add_op_bit_count(self, err, bin, op_dict, ops, static_pool,
								serializer_type);
	case OP_BIT_ADD:
		return add_op_bit_add(self, err, bin, op_dict, ops, static_pool,
							  serializer_type);
	case OP_BIT_AND:
		return add_op_bit_and(self, err, bin, op_dict, ops, static_pool,
							  serializer_type);
	case OP_BIT_GET:
		return add_op_bit_get(self, err, bin, op_dict, ops, static_pool,
							  serializer_type);
	case OP_BIT_GET_INT:
		return add_op_bit_get_int(self, err, bin, op_dict, ops, static_pool,
								  serializer_type);
	case OP_BIT_INSERT:
		return add_op_bit_insert(self, err, bin, op_dict, ops, static_pool,
								 serializer_type);
	case OP_BIT_LSCAN:
		return add_op_bit_lscan(self, err, bin, op_dict, ops, static_pool,
								serializer_type);
	case OP_BIT_LSHIFT:
		return add_op_bit_lshift(self, err, bin, op_dict, ops, static_pool,
								 serializer_type);
	case OP_BIT_NOT:
		return add_op_bit_not(self, err, bin, op_dict, ops, static_pool,
							  serializer_type);
	case OP_BIT_OR:
		return add_op_bit_or(self, err, bin, op_dict, ops, static_pool,
							 serializer_type);
	case OP_BIT_RSCAN:
		return add_op_bit_rscan(self, err, bin, op_dict, ops, static_pool,
								serializer_type);
	case OP_BIT_RSHIFT:
		return add_op_bit_rshift(self, err, bin, op_dict, ops, static_pool,
								 serializer_type);
	case OP_BIT_SUBTRACT:
		return add_op_bit_subtract(self, err, bin, op_dict, ops, static_pool,
								   serializer_type);
	case OP_BIT_XOR:
		return add_op_bit_xor(self, err, bin, op_dict, ops, static_pool,
							  serializer_type);

	default:
		// This should never be possible since we only get here if we know that the operation is valid.
		return as_error_update(err, AEROSPIKE_ERR_PARAM, "Unknown operation");
	}

	return err->code;
}

static as_status add_op_bit_resize(AerospikeClient *self, as_error *err,
								   char *bin, PyObject *op_dict,
								   as_operations *ops,
								   as_static_pool *static_pool,
								   int serializer_type)
{
	as_bit_policy bit_policy;
	as_bit_resize_flags flags = AS_BIT_RESIZE_DEFAULT;
	uint32_t new_size = 0;

	if (get_bit_policy(err, op_dict, &bit_policy) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_bit_resize_flags(err, op_dict, &flags) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_uint32t_from_pyargs(err, BYTE_SIZE_KEY, op_dict, &new_size) !=
		AEROSPIKE_OK) {
		return err->code;
	}

	if (!as_operations_bit_resize(ops, bin, NULL, &bit_policy, new_size,
								  flags)) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
							   "Failed to add bit resize operation");
	}

	return AEROSPIKE_OK;
}

static as_status add_op_bit_set(AerospikeClient *self, as_error *err, char *bin,
								PyObject *op_dict, as_operations *ops,
								as_static_pool *static_pool,
								int serializer_type)
{
	as_bit_policy bit_policy;
	int64_t bit_offset = 0;
	uint32_t bit_size = 0;
	uint32_t value_byte_size = 0;
	uint8_t *value = NULL;

	if (get_bit_policy(err, op_dict, &bit_policy) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_int64_t(err, BIT_OFFSET_KEY, op_dict, &bit_offset) !=
		AEROSPIKE_OK) {
		return err->code;
	}

	if (get_uint32t_from_pyargs(err, BIT_SIZE_KEY, op_dict, &bit_size) !=
		AEROSPIKE_OK) {
		return err->code;
	}

	if (get_uint32t_from_pyargs(err, VALUE_BYTE_SIZE_KEY, op_dict,
								&value_byte_size) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_uint8t_from_pyargs(err, VALUE_KEY, op_dict, &value) !=
		AEROSPIKE_OK) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
							   "unable to parse value from add_op_bit_set");
	}

	if (!as_operations_bit_set(ops, bin, NULL, &bit_policy, bit_offset,
							   bit_size, value_byte_size, value)) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
							   "Failed to add bit set operation")
	}

	return AEROSPIKE_OK;
}

static as_status add_op_bit_remove(AerospikeClient *self, as_error *err,
								   char *bin, PyObject *op_dict,
								   as_operations *ops,
								   as_static_pool *static_pool,
								   int serializer_type)
{
	as_bit_policy bit_policy;
	int64_t byte_offset = 0;
	uint32_t byte_size = 0;

	if (get_bit_policy(err, op_dict, &bit_policy) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_int64_t(err, BYTE_OFFSET_KEY, op_dict, &byte_offset) !=
		AEROSPIKE_OK) {
		return err->code;
	}

	if (get_uint32t_from_pyargs(err, BYTE_SIZE_KEY, op_dict, &byte_size) !=
		AEROSPIKE_OK) {
		return err->code;
	}

	if (!as_operations_bit_remove(ops, bin, NULL, &bit_policy, byte_offset,
								  byte_size)) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
							   "Failed to add bit remove operation")
	}

	return AEROSPIKE_OK;
}

static as_status add_op_bit_count(AerospikeClient *self, as_error *err,
								  char *bin, PyObject *op_dict,
								  as_operations *ops,
								  as_static_pool *static_pool,
								  int serializer_type)
{
	int64_t bit_offset = 0;
	uint32_t bit_size = 0;

	if (get_int64_t(err, BIT_OFFSET_KEY, op_dict, &bit_offset) !=
		AEROSPIKE_OK) {
		return err->code;
	}

	if (get_uint32t_from_pyargs(err, BIT_SIZE_KEY, op_dict, &bit_size) !=
		AEROSPIKE_OK) {
		return err->code;
	}

	if (!as_operations_bit_count(ops, bin, NULL, bit_offset, bit_size)) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
							   "Failed to add bit count operation")
	}

	return AEROSPIKE_OK;
}

static as_status add_op_bit_add(AerospikeClient *self, as_error *err, char *bin,
								PyObject *op_dict, as_operations *ops,
								as_static_pool *static_pool,
								int serializer_type)
{
	as_bit_policy bit_policy;
	int64_t bit_offset = 0;
	uint32_t bit_size = 0;
	int64_t value = 0;
	bool sign = false;
	as_bit_overflow_action action;

	if (get_bit_policy(err, op_dict, &bit_policy) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_int64_t(err, BIT_OFFSET_KEY, op_dict, &bit_offset) !=
		AEROSPIKE_OK) {
		return err->code;
	}

	if (get_int64_t(err, VALUE_KEY, op_dict, &value) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_bool_from_pyargs(err, SIGN_KEY, op_dict, &sign) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_int64_t(err, ACTION_KEY, op_dict, ((int64_t *)(&action))) !=
		AEROSPIKE_OK) {
		return err->code;
	}

	if (get_uint32t_from_pyargs(err, BIT_SIZE_KEY, op_dict, &bit_size) !=
		AEROSPIKE_OK) {
		return err->code;
	}

	if (!as_operations_bit_add(ops, bin, NULL, &bit_policy, bit_offset,
							   bit_size, value, sign, action)) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
							   "Failed to add bit add operation")
	}

	return AEROSPIKE_OK;
}

static as_status add_op_bit_and(AerospikeClient *self, as_error *err, char *bin,
								PyObject *op_dict, as_operations *ops,
								as_static_pool *static_pool,
								int serializer_type)
{
	as_bit_policy bit_policy;
	int64_t bit_offset = 0;
	uint32_t bit_size = 0;
	uint32_t value_byte_size = 0;
	uint8_t *value = NULL;

	if (get_bit_policy(err, op_dict, &bit_policy) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_int64_t(err, BIT_OFFSET_KEY, op_dict, &bit_offset) !=
		AEROSPIKE_OK) {
		return err->code;
	}

	if (get_uint32t_from_pyargs(err, BIT_SIZE_KEY, op_dict, &bit_size) !=
		AEROSPIKE_OK) {
		return err->code;
	}

	if (get_uint32t_from_pyargs(err, VALUE_BYTE_SIZE_KEY, op_dict,
								&value_byte_size) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_uint8t_from_pyargs(err, VALUE_KEY, op_dict, &value) !=
		AEROSPIKE_OK) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
							   "unable to parse value from add_op_bit_and");
	}

	if (!as_operations_bit_and(ops, bin, NULL, &bit_policy, bit_offset,
							   bit_size, value_byte_size, value)) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
							   "Failed to add bit and operation")
	}

	return AEROSPIKE_OK;
}

static as_status add_op_bit_get(AerospikeClient *self, as_error *err, char *bin,
								PyObject *op_dict, as_operations *ops,
								as_static_pool *static_pool,
								int serializer_type)
{
	int64_t bit_offset = 0;
	uint32_t bit_size = 0;

	if (get_int64_t(err, BIT_OFFSET_KEY, op_dict, &bit_offset) !=
		AEROSPIKE_OK) {
		return err->code;
	}

	if (get_uint32t_from_pyargs(err, BIT_SIZE_KEY, op_dict, &bit_size) !=
		AEROSPIKE_OK) {
		return err->code;
	}

	if (!as_operations_bit_get(ops, bin, NULL, bit_offset, bit_size)) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
							   "Failed to add bit get int operation")
	}

	return AEROSPIKE_OK;
}

static as_status add_op_bit_get_int(AerospikeClient *self, as_error *err,
									char *bin, PyObject *op_dict,
									as_operations *ops,
									as_static_pool *static_pool,
									int serializer_type)
{
	int64_t bit_offset = 0;
	uint32_t bit_size = 0;
	bool sign = false;

	if (get_bool_from_pyargs(err, SIGN_KEY, op_dict, &sign) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_int64_t(err, BIT_OFFSET_KEY, op_dict, &bit_offset) !=
		AEROSPIKE_OK) {
		return err->code;
	}

	if (get_uint32t_from_pyargs(err, BIT_SIZE_KEY, op_dict, &bit_size) !=
		AEROSPIKE_OK) {
		return err->code;
	}

	if (!as_operations_bit_get_int(ops, bin, NULL, bit_offset, bit_size,
								   sign)) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
							   "Failed to add bit get operation")
	}

	return AEROSPIKE_OK;
}

static as_status add_op_bit_insert(AerospikeClient *self, as_error *err,
								   char *bin, PyObject *op_dict,
								   as_operations *ops,
								   as_static_pool *static_pool,
								   int serializer_type)
{
	as_bit_policy bit_policy;
	int64_t byte_offset = 0;
	uint32_t value_byte_size = 0;
	uint8_t *value = NULL;

	if (get_bit_policy(err, op_dict, &bit_policy) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_int64_t(err, BYTE_OFFSET_KEY, op_dict, &byte_offset) !=
		AEROSPIKE_OK) {
		return err->code;
	}

	if (get_uint32t_from_pyargs(err, VALUE_BYTE_SIZE_KEY, op_dict,
								&value_byte_size) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_uint8t_from_pyargs(err, VALUE_KEY, op_dict, &value) !=
		AEROSPIKE_OK) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
							   "unable to parse value from add_op_bit_insert");
	}

	if (!as_operations_bit_insert(ops, bin, NULL, &bit_policy, byte_offset,
								  value_byte_size, value)) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
							   "Failed to add bit insert operation")
	}

	return AEROSPIKE_OK;
}

static as_status add_op_bit_lscan(AerospikeClient *self, as_error *err,
								  char *bin, PyObject *op_dict,
								  as_operations *ops,
								  as_static_pool *static_pool,
								  int serializer_type)
{
	int64_t bit_offset = 0;
	uint32_t bit_size = 0;
	bool value = false;

	if (get_bool_from_pyargs(err, VALUE_KEY, op_dict, &value) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_int64_t(err, BIT_OFFSET_KEY, op_dict, &bit_offset) !=
		AEROSPIKE_OK) {
		return err->code;
	}

	if (get_uint32t_from_pyargs(err, BIT_SIZE_KEY, op_dict, &bit_size) !=
		AEROSPIKE_OK) {
		return err->code;
	}

	if (!as_operations_bit_lscan(ops, bin, NULL, bit_offset, bit_size, value)) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
							   "Failed to add bit lscan operation")
	}

	return AEROSPIKE_OK;
}

static as_status add_op_bit_lshift(AerospikeClient *self, as_error *err,
								   char *bin, PyObject *op_dict,
								   as_operations *ops,
								   as_static_pool *static_pool,
								   int serializer_type)
{
	as_bit_policy bit_policy;
	int64_t bit_offset = 0;
	uint32_t bit_size = 0;
	uint32_t shift = 0;

	if (get_bit_policy(err, op_dict, &bit_policy) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_int64_t(err, BIT_OFFSET_KEY, op_dict, &bit_offset) !=
		AEROSPIKE_OK) {
		return err->code;
	}

	if (get_uint32t_from_pyargs(err, BIT_SIZE_KEY, op_dict, &bit_size) !=
		AEROSPIKE_OK) {
		return err->code;
	}

	if (get_uint32t_from_pyargs(err, VALUE_KEY, op_dict, &shift) !=
		AEROSPIKE_OK) {
		return err->code;
	}

	if (!as_operations_bit_lshift(ops, bin, NULL, &bit_policy, bit_offset,
								  bit_size, shift)) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
							   "Failed to add bit lshift operation")
	}

	return AEROSPIKE_OK;
}

static as_status add_op_bit_not(AerospikeClient *self, as_error *err, char *bin,
								PyObject *op_dict, as_operations *ops,
								as_static_pool *static_pool,
								int serializer_type)
{
	as_bit_policy bit_policy;
	int64_t bit_offset = 0;
	uint32_t bit_size = 0;

	if (get_bit_policy(err, op_dict, &bit_policy) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_int64_t(err, BIT_OFFSET_KEY, op_dict, &bit_offset) !=
		AEROSPIKE_OK) {
		return err->code;
	}

	if (get_uint32t_from_pyargs(err, BIT_SIZE_KEY, op_dict, &bit_size) !=
		AEROSPIKE_OK) {
		return err->code;
	}

	if (!as_operations_bit_not(ops, bin, NULL, &bit_policy, bit_offset,
							   bit_size)) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
							   "Failed to add bit not operation")
	}

	return AEROSPIKE_OK;
}

static as_status add_op_bit_or(AerospikeClient *self, as_error *err, char *bin,
							   PyObject *op_dict, as_operations *ops,
							   as_static_pool *static_pool, int serializer_type)
{
	as_bit_policy bit_policy;
	int64_t bit_offset = 0;
	uint32_t bit_size = 0;
	uint32_t value_byte_size = 0;
	uint8_t *value = NULL;

	if (get_bit_policy(err, op_dict, &bit_policy) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_int64_t(err, BIT_OFFSET_KEY, op_dict, &bit_offset) !=
		AEROSPIKE_OK) {
		return err->code;
	}

	if (get_uint32t_from_pyargs(err, BIT_SIZE_KEY, op_dict, &bit_size) !=
		AEROSPIKE_OK) {
		return err->code;
	}

	if (get_uint32t_from_pyargs(err, VALUE_BYTE_SIZE_KEY, op_dict,
								&value_byte_size) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_uint8t_from_pyargs(err, VALUE_KEY, op_dict, &value) !=
		AEROSPIKE_OK) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
							   "unable to parse value from add_op_bit_or");
	}

	if (!as_operations_bit_or(ops, bin, NULL, &bit_policy, bit_offset, bit_size,
							  value_byte_size, value)) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
							   "Failed to add bit or operation")
	}

	return AEROSPIKE_OK;
}

static as_status add_op_bit_rscan(AerospikeClient *self, as_error *err,
								  char *bin, PyObject *op_dict,
								  as_operations *ops,
								  as_static_pool *static_pool,
								  int serializer_type)
{
	int64_t bit_offset = 0;
	uint32_t bit_size = 0;
	bool value = false;

	if (get_bool_from_pyargs(err, VALUE_KEY, op_dict, &value) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_int64_t(err, BIT_OFFSET_KEY, op_dict, &bit_offset) !=
		AEROSPIKE_OK) {
		return err->code;
	}

	if (get_uint32t_from_pyargs(err, BIT_SIZE_KEY, op_dict, &bit_size) !=
		AEROSPIKE_OK) {
		return err->code;
	}

	if (!as_operations_bit_rscan(ops, bin, NULL, bit_offset, bit_size, value)) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
							   "Failed to add bit rscan operation")
	}

	return AEROSPIKE_OK;
}

static as_status add_op_bit_rshift(AerospikeClient *self, as_error *err,
								   char *bin, PyObject *op_dict,
								   as_operations *ops,
								   as_static_pool *static_pool,
								   int serializer_type)
{
	as_bit_policy bit_policy;
	int64_t bit_offset = 0;
	uint32_t bit_size = 0;
	uint32_t shift = 0;

	if (get_bit_policy(err, op_dict, &bit_policy) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_int64_t(err, BIT_OFFSET_KEY, op_dict, &bit_offset) !=
		AEROSPIKE_OK) {
		return err->code;
	}

	if (get_uint32t_from_pyargs(err, BIT_SIZE_KEY, op_dict, &bit_size) !=
		AEROSPIKE_OK) {
		return err->code;
	}

	if (get_uint32t_from_pyargs(err, VALUE_KEY, op_dict, &shift) !=
		AEROSPIKE_OK) {
		return err->code;
	}

	if (!as_operations_bit_rshift(ops, bin, NULL, &bit_policy, bit_offset,
								  bit_size, shift)) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
							   "Failed to add bit rshift operation")
	}

	return AEROSPIKE_OK;
}

static as_status add_op_bit_subtract(AerospikeClient *self, as_error *err,
									 char *bin, PyObject *op_dict,
									 as_operations *ops,
									 as_static_pool *static_pool,
									 int serializer_type)
{
	as_bit_policy bit_policy;
	int64_t bit_offset = 0;
	uint32_t bit_size = 0;
	int64_t value = 0;
	bool sign = 0;
	as_bit_overflow_action action;

	if (get_bit_policy(err, op_dict, &bit_policy) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_int64_t(err, BIT_OFFSET_KEY, op_dict, &bit_offset) !=
		AEROSPIKE_OK) {
		return err->code;
	}

	if (get_int64_t(err, VALUE_KEY, op_dict, &value) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_bool_from_pyargs(err, SIGN_KEY, op_dict, &sign) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_int64_t(err, ACTION_KEY, op_dict, ((int64_t *)(&action))) !=
		AEROSPIKE_OK) {
		return err->code;
	}

	if (get_uint32t_from_pyargs(err, BIT_SIZE_KEY, op_dict, &bit_size) !=
		AEROSPIKE_OK) {
		return err->code;
	}

	if (!as_operations_bit_subtract(ops, bin, NULL, &bit_policy, bit_offset,
									bit_size, value, sign, action)) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
							   "Failed to add bit subtract operation")
	}

	return AEROSPIKE_OK;
}

static as_status add_op_bit_xor(AerospikeClient *self, as_error *err, char *bin,
								PyObject *op_dict, as_operations *ops,
								as_static_pool *static_pool,
								int serializer_type)
{
	as_bit_policy bit_policy;
	int64_t bit_offset = 0;
	uint32_t bit_size = 0;
	uint32_t value_byte_size = 0;
	uint8_t *value = NULL;

	if (get_bit_policy(err, op_dict, &bit_policy) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_int64_t(err, BIT_OFFSET_KEY, op_dict, &bit_offset) !=
		AEROSPIKE_OK) {
		return err->code;
	}

	if (get_uint32t_from_pyargs(err, BIT_SIZE_KEY, op_dict, &bit_size) !=
		AEROSPIKE_OK) {
		return err->code;
	}

	if (get_uint32t_from_pyargs(err, VALUE_BYTE_SIZE_KEY, op_dict,
								&value_byte_size) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_uint8t_from_pyargs(err, VALUE_KEY, op_dict, &value) !=
		AEROSPIKE_OK) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
							   "unable to parse value from add_op_bit_xor");
	}

	if (!as_operations_bit_xor(ops, bin, NULL, &bit_policy, bit_offset,
							   bit_size, value_byte_size, value)) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
							   "Failed to add bit xor operation")
	}

	return AEROSPIKE_OK;
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
								as_bit_policy *policy)
{
	PyObject *py_bit_policy = PyDict_GetItemString(op_dict, POLICY_KEY);

	// This handles a null policy
	if (pyobject_to_bit_policy(err, py_bit_policy, policy) != AEROSPIKE_OK) {
		return err->code;
	}

	return AEROSPIKE_OK;
}

static as_status get_bool_from_pyargs(as_error *err, char *key,
									  PyObject *op_dict, bool *boolean)
{
	PyObject *py_val = PyDict_GetItemString(op_dict, key);

	if (!py_val) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM, "Failed to convert %s",
							   key);
	}

	if (PyBool_Check(py_val)) {
		if (get_int64_t(err, key, op_dict, ((int64_t *)(boolean))) !=
			AEROSPIKE_OK) {
			return err->code;
		}
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