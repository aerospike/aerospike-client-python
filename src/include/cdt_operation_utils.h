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

#pragma once
#include <Python.h>
#include <stdbool.h>
#include <aerospike/as_error.h>
#include <aerospike/as_vector.h>
#include <aerospike/as_list.h>
#include <aerospike/as_list_operations.h>
#include "types.h"

#define AS_PY_BIN_KEY "bin"
#define AS_PY_VAL_KEY "val"
#define AS_PY_VALUES_KEY "value_list"
#define AS_PY_VAL_BEGIN_KEY "value_begin"
#define AS_PY_VAL_END_KEY "value_end"
#define AS_PY_INDEX_KEY "index"
#define AS_PY_COUNT_KEY "count"
#define AS_PY_RANK_KEY "rank"
#define AS_PY_VALUE_TYPE_KEY "value_type"
#define AS_PY_LIST_RETURN_KEY "return_type"
#define AS_PY_MAP_RETURN_KEY "return_type"
#define AS_PY_LIST_ORDER "list_order"
#define AS_PY_LIST_SORT_FLAGS "sort_flags"
#define AS_PY_LIST_POLICY "list_policy"
#define AS_PY_MAP_POLICY "map_policy"
#define AS_EXPR_KEY "expr"
#define AS_EXPR_FLAGS_KEY "expr_flags"

as_status get_bin(as_error *err, PyObject *op_dict, as_vector *unicodeStrVector,
				  char **binName);

as_status get_asval(AerospikeClient *self, as_error *err, char *key,
					PyObject *op_dict, as_val **val,
					as_static_pool *static_pool, int serializer_type,
					bool required);

as_status get_val_list(AerospikeClient *self, as_error *err,
					   const char *list_key, PyObject *op_dict, as_list **list,
					   as_static_pool *static_pool, int serializer_type);

as_status get_int64_t(as_error *err, const char *key, PyObject *op_dict,
					  int64_t *i64_valptr);

as_status get_optional_int64_t(as_error *err, const char *key,
							   PyObject *op_dict, int64_t *i64_valptr,
							   bool *found);

as_status get_int_from_py_dict(as_error *err, const char *key,
							   PyObject *op_dict, int *int_pointer);

as_status get_list_return_type(as_error *err, PyObject *op_dict,
							   int *return_type);

as_status get_list_policy(as_error *err, PyObject *op_dict,
						  as_list_policy *policy, bool *found);