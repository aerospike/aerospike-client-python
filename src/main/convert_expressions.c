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

#include <aerospike/aerospike_index.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_exp.h>
#include <aerospike/as_vector.h>
#include <aerospike/as_geojson.h>
#include <aerospike/as_msgpack_ext.h>

#include "client.h"
#include "conversions.h"
#include "serializer.h"
#include "exceptions.h"
#include "policy.h"
#include "cdt_operation_utils.h"
#include "geo.h"
#include "cdt_types.h"

// EXPR OPS
#define VAL 0
#define EQ 1
#define NE 2
#define GT 3
#define GE 4
#define LT 5
#define LE 6
#define CMP_REGEX 7
#define CMP_GEO 8

#define AND 16
#define OR 17
#define NOT 18

#define META_DIGEST_MOD 64
#define META_DEVICE_SIZE 65
#define META_LAST_UPDATE_TIME 66
#define META_VOID_TIME 67
#define META_TTL 68
#define META_SET_NAME 69
#define META_KEY_EXISTS 70
#define META_SINCE_UPDATE_TIME 71
#define META_IS_TOMBSTONE 72

#define REC_KEY 80
#define BIN 81
#define BIN_TYPE 82
#define BIN_EXISTS 83

#define CALL 127
#define BOOL 133
#define LIST_MOD 139

// RESULT TYPES
#define BOOLEAN 1
#define INTEGER 2
#define STRING 3
#define LIST 4
#define MAP 5
#define BLOB 6
#define FLOAT 7
#define GEOJSON 8
#define HLL 9

// VIRTUAL OPS
#define END_VA_ARGS 128
#define _AS_EXP_BIT_FLAGS 150
#define _TRUE 151
#define _FALSE 152

// UTILITY CONSTANTS
#define NO_BIT_FLAGS 0

// FIXED DICTIONARY KEYS
#define OP_TYPE_KEY "ot_key"
#define LIST_ORDER_KEY "list_order"
#define REGEX_OPTIONS_KEY "regex_options"

// UTILITY MACROS
#define EXP_SZ(_expr) sizeof((as_exp_entry[]){ _expr })

#define APPEND_ARRAY(_sub_values, ...) {\
	{\
		as_exp_entry tmp_expr_array[] = {__VA_ARGS__};\
		int expr_array_size = sizeof(tmp_expr_array) / sizeof(as_exp_entry) - _sub_values;\
		memcpy(&((*expressions)[(*bottom)]), &tmp_expr_array, (expr_array_size * sizeof(as_exp_entry)));\
		(*bottom) += expr_array_size;\
	}\
}

#define BIN_EXPR()\
	{.op=_AS_EXP_CODE_BIN, .count=3}, as_exp_int(pred->result_type), _AS_EXP_VAL_RAWSTR(bin_name)

#define KEY_EXPR()\
	{.op=_AS_EXP_CODE_KEY, .count=2}, as_exp_int(pred->result_type)

#define LIST_MOD_EXP()\
	{.op=pred->op, .v.list_pol = pred->list_policy}

// STRUCT DEFINITIONS
typedef struct {
	long op;
	long result_type;
	union {
		as_list * val_list_p;
		as_map * val_map_p;
		char * val_string_p;
	} val;
	uint8_t val_flag;
	PyObject * pydict;
	PyObject * pytuple;
	as_cdt_ctx * ctx;

	as_list_policy * list_policy;
	as_map_policy * map_policy;

	long num_children;
} pred_op;

// FUNCTION DEFINITIONS
static
as_status get_expr_size(int * size_to_alloc, int * preds_size, as_vector * preds, as_error * err);

static
as_status get_exp_val_from_pyval(AerospikeClient * self, as_static_pool * static_pool, int serializer_type, as_exp_entry * new_entry, PyObject * py_obj, pred_op * pred, as_error * err);

static
as_status add_pred_macros(AerospikeClient * self, as_static_pool * static_pool, int serializer_type, as_vector * unicodeStrVector, as_vector * pred_op_vector, as_exp_entry ** expressions, int * bottom, int * size, as_error * err);

/*
* get_expr_size 
* Sets `size_to_alloc` to the byte count required to fit the array of as_exp_entry that will be allocated
* when `preds` is converted.
* Note that because preds has an entry for every child of every expression, the child values do not need to be counted
* because their parents' size accounts for them.
*/
static
as_status get_expr_size(int * size_to_alloc, int * preds_size, as_vector * preds, as_error * err) {

	static const int EXPR_SIZES[] = {
		[BIN]                                            = EXP_SZ(as_exp_bin_int(0)),
		[VAL]                                            = 0, //EXP_SZ(as_exp_val(NULL)), // NOTE if I don't count vals I don't need to subtract from other ops
		[EQ]                                             = EXP_SZ(as_exp_cmp_eq({},{})),
		[NE]                                             = EXP_SZ(as_exp_cmp_ne({},{})),
		[GT]                                             = EXP_SZ(as_exp_cmp_gt({},{})),
		[GE]                                             = EXP_SZ(as_exp_cmp_ge({},{})),
		[LT]                                             = EXP_SZ(as_exp_cmp_lt({},{})),
		[LE]                                             = EXP_SZ(as_exp_cmp_le({},{})),
		[CMP_REGEX]                                      = EXP_SZ(as_exp_cmp_regex(0, "", {})),
		[CMP_GEO]                                        = EXP_SZ(as_exp_cmp_geo({},{})),
		[AND]                                            = EXP_SZ(as_exp_and({})),
		[OR]                                             = EXP_SZ(as_exp_or({})),
		[NOT]                                            = EXP_SZ(as_exp_not({})),
		[END_VA_ARGS]                                    = EXP_SZ({.op = _AS_EXP_CODE_END_OF_VA_ARGS}),
		[META_DIGEST_MOD]                                = EXP_SZ(as_exp_digest_modulo(0)),
		[META_DEVICE_SIZE]                               = EXP_SZ(as_exp_device_size()),
		[META_LAST_UPDATE_TIME]                          = EXP_SZ(as_exp_last_update()),
		[META_VOID_TIME]                                 = EXP_SZ(as_exp_void_time()),
		[META_TTL]                                       = EXP_SZ(as_exp_ttl()),
		[META_SET_NAME]                                  = EXP_SZ(as_exp_set_name()),
		[META_KEY_EXISTS]                                = EXP_SZ(as_exp_key_exist()),
		[REC_KEY]                                        = EXP_SZ(as_exp_key_int()), // this covers as_exp_key_int() -> as_exp_key_blob
		[BIN_TYPE]                                       = EXP_SZ(as_exp_bin_type("")),
		[BIN_EXISTS]                                     = EXP_SZ(as_exp_bin_exists("")),
		[OP_LIST_GET_BY_INDEX]                           = EXP_SZ(as_exp_list_get_by_index(NULL, 0, 0, {}, {})),
		[OP_LIST_SIZE]                                   = EXP_SZ(as_exp_list_size(NULL, {})),
		[OP_LIST_GET_BY_VALUE]                           = EXP_SZ(as_exp_list_get_by_value(NULL, 0, {}, {})),
		[OP_LIST_GET_BY_VALUE_RANGE]                     = EXP_SZ(as_exp_list_get_by_value_range(NULL, 0, {}, {}, {})),
		[OP_LIST_GET_BY_VALUE_LIST]                      = EXP_SZ(as_exp_list_get_by_value_list(NULL, 0, {}, {})),
		[OP_LIST_GET_BY_VALUE_RANK_RANGE_REL_TO_END]     = EXP_SZ(as_exp_list_get_by_rel_rank_range_to_end(NULL, 0, {}, {}, {})),
		[OP_LIST_GET_BY_VALUE_RANK_RANGE_REL]            = EXP_SZ(as_exp_list_get_by_rel_rank_range(NULL, 0, {}, {}, {}, {})),
		[OP_LIST_GET_BY_INDEX_RANGE_TO_END]              = EXP_SZ(as_exp_list_get_by_index_range_to_end(NULL, 0, {}, {})),
		[OP_LIST_GET_BY_INDEX_RANGE]                     = EXP_SZ(as_exp_list_get_by_index_range(NULL, 0, {}, {}, {})),
		[OP_LIST_GET_BY_RANK]                            = EXP_SZ(as_exp_list_get_by_rank(NULL, 0, 0, {}, {})),
		[OP_LIST_GET_BY_RANK_RANGE_TO_END]               = EXP_SZ(as_exp_list_get_by_rank_range_to_end(NULL, 0, {}, {})),
		[OP_LIST_GET_BY_RANK_RANGE]                      = EXP_SZ(as_exp_list_get_by_rank_range(NULL, 0, {}, {}, {})),
		[OP_LIST_APPEND]                                 = EXP_SZ(as_exp_list_append(NULL, NULL, {}, {})),
		[OP_LIST_APPEND_ITEMS]                           = EXP_SZ(as_exp_list_append_items(NULL, NULL, {}, {})),
		[OP_LIST_INSERT]                                 = EXP_SZ(as_exp_list_insert(NULL, NULL, {}, {}, {})),
		[OP_LIST_INSERT_ITEMS]                           = EXP_SZ(as_exp_list_insert_items(NULL, NULL, {}, {}, {})),
		[OP_LIST_INCREMENT]                              = EXP_SZ(as_exp_list_increment(NULL, NULL, {}, {}, {})),
		[OP_LIST_SET]                                    = EXP_SZ(as_exp_list_set(NULL, NULL, {}, {}, {})),
		[OP_LIST_CLEAR]                                  = EXP_SZ(as_exp_list_clear(NULL, {})),
		[OP_LIST_SORT]                                   = EXP_SZ(as_exp_list_sort(NULL, 0, {})),
		[OP_LIST_REMOVE_BY_VALUE]                        = EXP_SZ(as_exp_list_remove_by_value(NULL, {}, {})),
		[OP_LIST_REMOVE_BY_VALUE_LIST]                   = EXP_SZ(as_exp_list_remove_by_value_list(NULL, {}, {})),
		[OP_LIST_REMOVE_BY_VALUE_RANGE]                  = EXP_SZ(as_exp_list_remove_by_value_range(NULL, {}, {}, {})),
		[OP_LIST_REMOVE_BY_REL_RANK_RANGE_TO_END]        = EXP_SZ(as_exp_list_remove_by_rel_rank_range_to_end(NULL, {}, {}, {})),
		[OP_LIST_REMOVE_BY_REL_RANK_RANGE]               = EXP_SZ(as_exp_list_remove_by_rel_rank_range(NULL, {}, {}, {}, {})),
		[OP_LIST_REMOVE_BY_INDEX]                        = EXP_SZ(as_exp_list_remove_by_index(NULL, {}, {})),
		[OP_LIST_REMOVE_BY_INDEX_RANGE_TO_END]           = EXP_SZ(as_exp_list_remove_by_index_range_to_end(NULL, {}, {})),
		[OP_LIST_REMOVE_BY_INDEX_RANGE]                  = EXP_SZ(as_exp_list_remove_by_index_range(NULL, {}, {}, {})),
		[OP_LIST_REMOVE_BY_RANK]                         = EXP_SZ(as_exp_list_remove_by_rank(NULL, {}, {})),
		[OP_LIST_REMOVE_BY_RANK_RANGE_TO_END]            = EXP_SZ(as_exp_list_remove_by_rank_range_to_end(NULL, {}, {})),
		[OP_LIST_REMOVE_BY_RANK_RANGE]                   = EXP_SZ(as_exp_list_remove_by_rank_range(NULL, {}, {}, {})),
		[OP_MAP_PUT]                                     = EXP_SZ(as_exp_map_put(NULL, NULL, {}, {}, {})),
		[OP_MAP_PUT_ITEMS]                               = EXP_SZ(as_exp_map_put_items(NULL, NULL, {}, {})),
		[OP_MAP_INCREMENT]                               = EXP_SZ(as_exp_map_increment(NULL, NULL, {}, {}, {})),
		[OP_MAP_CLEAR]                                   = EXP_SZ(as_exp_map_clear(NULL, {})),
		[OP_MAP_REMOVE_BY_KEY]                           = EXP_SZ(as_exp_map_remove_by_key(NULL, {}, {})),
		[OP_MAP_REMOVE_BY_KEY_LIST]                      = EXP_SZ(as_exp_map_remove_by_key_list(NULL, {}, {})),
		[OP_MAP_REMOVE_BY_KEY_RANGE]                     = EXP_SZ(as_exp_map_remove_by_key_range(NULL, {}, {}, {})),
		[OP_MAP_REMOVE_BY_KEY_REL_INDEX_RANGE_TO_END]    = EXP_SZ(as_exp_map_remove_by_key_rel_index_range_to_end(NULL, {}, {}, {})),
		[OP_MAP_REMOVE_BY_KEY_REL_INDEX_RANGE]           = EXP_SZ(as_exp_map_remove_by_key_rel_index_range(NULL, {}, {}, {}, {})),
		[OP_MAP_REMOVE_BY_VALUE]                         = EXP_SZ(as_exp_map_remove_by_value(NULL, {}, {})),
		[OP_MAP_REMOVE_BY_VALUE_LIST]                    = EXP_SZ(as_exp_map_remove_by_value_list(NULL, {}, {})),
		[OP_MAP_REMOVE_BY_VALUE_RANGE]                   = EXP_SZ(as_exp_map_remove_by_value_range(NULL, {}, {}, {})),
		[OP_MAP_REMOVE_BY_VALUE_REL_RANK_RANGE_TO_END]   = EXP_SZ(as_exp_map_remove_by_value_rel_rank_range_to_end(NULL, {}, {}, {})),
		[OP_MAP_REMOVE_BY_VALUE_REL_RANK_RANGE]          = EXP_SZ(as_exp_map_remove_by_value_rel_rank_range(NULL, {}, {}, {}, {})),
		[OP_MAP_REMOVE_BY_INDEX]                         = EXP_SZ(as_exp_map_remove_by_index(NULL, {}, {})),
		[OP_MAP_REMOVE_BY_INDEX_RANGE_TO_END]            = EXP_SZ(as_exp_map_remove_by_index_range_to_end(NULL, {}, {})),
		[OP_MAP_REMOVE_BY_INDEX_RANGE]                   = EXP_SZ(as_exp_map_remove_by_index_range(NULL, {}, {}, {})),
		[OP_MAP_REMOVE_BY_RANK]                          = EXP_SZ(as_exp_map_remove_by_rank(NULL, {}, {})),
		[OP_MAP_REMOVE_BY_RANK_RANGE_TO_END]             = EXP_SZ(as_exp_map_remove_by_rank_range_to_end(NULL, {}, {})),
		[OP_MAP_REMOVE_BY_RANK_RANGE]                    = EXP_SZ(as_exp_map_remove_by_rank_range(NULL, {}, {}, {})),
		[OP_MAP_SIZE]                                    = EXP_SZ(as_exp_map_size(NULL, {})),
		[OP_MAP_GET_BY_KEY]                              = EXP_SZ(as_exp_map_get_by_key(NULL, 0, 0, {}, {})),
		[OP_MAP_GET_BY_KEY_RANGE]                        = EXP_SZ(as_exp_map_get_by_key_range(NULL, 0, {}, {}, {})),
		[OP_MAP_GET_BY_KEY_LIST]                         = EXP_SZ(as_exp_map_get_by_key_list(NULL, 0, {}, {})),
		[OP_MAP_GET_BY_KEY_REL_INDEX_RANGE_TO_END]       = EXP_SZ(as_exp_map_get_by_key_rel_index_range_to_end(NULL, 0, {}, {}, {})),
		[OP_MAP_GET_BY_KEY_REL_INDEX_RANGE]              = EXP_SZ(as_exp_map_get_by_key_rel_index_range(NULL, 0, {}, {}, {}, {})),
		[OP_MAP_GET_BY_VALUE]                            = EXP_SZ(as_exp_map_get_by_value(NULL, 0, {}, {})),
		[OP_MAP_GET_BY_VALUE_RANGE]                      = EXP_SZ(as_exp_map_get_by_value_range(NULL, 0, {}, {}, {})),
		[OP_MAP_GET_BY_VALUE_LIST]                       = EXP_SZ(as_exp_map_get_by_value_list(NULL, 0, {}, {})),
		[OP_MAP_GET_BY_VALUE_RANK_RANGE_REL_TO_END]      = EXP_SZ(as_exp_map_get_by_value_rel_rank_range_to_end(NULL, 0, {}, {}, {})),
		[OP_MAP_GET_BY_VALUE_RANK_RANGE_REL]             = EXP_SZ(as_exp_map_get_by_value_rel_rank_range(NULL, 0, {}, {}, {}, {})),
		[OP_MAP_GET_BY_INDEX]                            = EXP_SZ(as_exp_map_get_by_index(NULL, 0, 0, {}, {})),
		[OP_MAP_GET_BY_INDEX_RANGE_TO_END]               = EXP_SZ(as_exp_map_get_by_index_range_to_end(NULL, 0, {}, {})),
		[OP_MAP_GET_BY_INDEX_RANGE]                      = EXP_SZ(as_exp_map_get_by_index_range(NULL, 0, {}, {}, {})),
		[OP_MAP_GET_BY_RANK]                             = EXP_SZ(as_exp_map_get_by_rank(NULL, 0, 0, {}, {})),
		[OP_MAP_GET_BY_RANK_RANGE_TO_END]                = EXP_SZ(as_exp_map_get_by_rank_range_to_end(NULL, 0, {}, {})),
		[OP_MAP_GET_BY_RANK_RANGE]                       = EXP_SZ(as_exp_map_get_by_rank_range(NULL, 0, {}, {}, {})),
		[_AS_EXP_BIT_FLAGS]                              = 0, //EXP_SZ(as_exp_uint(0)),
		[OP_BIT_RESIZE]                                  = EXP_SZ(as_exp_bit_resize(NULL, {}, 0, {})),
		[OP_BIT_INSERT]                                  = EXP_SZ(as_exp_bit_insert(NULL, {}, {}, {})),
		[OP_BIT_REMOVE]                                  = EXP_SZ(as_exp_bit_remove(NULL, {}, {}, {})),
		[OP_BIT_SET]                                     = EXP_SZ(as_exp_bit_set(NULL, {}, {}, {}, {})),
		[OP_BIT_OR]                                      = EXP_SZ(as_exp_bit_or(NULL, {}, {}, {}, {})),
		[OP_BIT_XOR]                                     = EXP_SZ(as_exp_bit_xor(NULL, {}, {}, {}, {})),
		[OP_BIT_AND]                                     = EXP_SZ(as_exp_bit_and(NULL, {}, {}, {}, {})),
		[OP_BIT_NOT]                                     = EXP_SZ(as_exp_bit_not(NULL, {}, {}, {})),
		[OP_BIT_LSHIFT]                                  = EXP_SZ(as_exp_bit_lshift(NULL, {}, {}, {}, {})),
		[OP_BIT_RSHIFT]                                  = EXP_SZ(as_exp_bit_rshift(NULL, {}, {}, {}, {})),
		[OP_BIT_ADD]                                     = EXP_SZ(as_exp_bit_add(NULL, {}, {}, {}, 0, {})),
		[OP_BIT_SUBTRACT]                                = EXP_SZ(as_exp_bit_subtract(NULL, {}, {}, {}, 0, {})),
		[OP_BIT_SET_INT]                                 = EXP_SZ(as_exp_bit_set_int(NULL, {}, {}, {}, {})),
		[OP_BIT_GET]                                     = EXP_SZ(as_exp_bit_get({}, {}, {})),
		[OP_BIT_COUNT]                                   = EXP_SZ(as_exp_bit_count({}, {}, {})),
		[OP_BIT_LSCAN]                                   = EXP_SZ(as_exp_bit_lscan({}, {}, {}, {})),
		[OP_BIT_RSCAN]                                   = EXP_SZ(as_exp_bit_rscan({}, {}, {}, {})),
		[OP_BIT_GET_INT]                                 = EXP_SZ(as_exp_bit_get_int({}, {}, 0, {})),
		[OP_HLL_ADD]                                     = EXP_SZ(as_exp_hll_add_mh(NULL, {}, 0, 0, {})),
		[OP_HLL_GET_COUNT]                               = EXP_SZ(as_exp_hll_update(NULL, {}, {})),
		[OP_HLL_GET_UNION]                               = EXP_SZ(as_exp_hll_get_union({}, {})),
		[OP_HLL_GET_UNION_COUNT]                         = EXP_SZ(as_exp_hll_get_union_count({}, {})),
		[OP_HLL_GET_INTERSECT_COUNT]                     = EXP_SZ(as_exp_hll_get_intersect_count({}, {})),
		[OP_HLL_GET_SIMILARITY]                          = EXP_SZ(as_exp_hll_get_similarity({}, {})),
		[OP_HLL_DESCRIBE]                                = EXP_SZ(as_exp_hll_describe({})),
		[OP_HLL_MAY_CONTAIN]                             = EXP_SZ(as_exp_hll_may_contain({}, {})),
		[_AS_EXP_CODE_CDT_LIST_CRMOD]                    = 0, //EXP_SZ(as_exp_val(NULL)),
		[_AS_EXP_CODE_CDT_LIST_MOD]                      = 0, //EXP_SZ(as_exp_val(NULL)),
		[_AS_EXP_CODE_CDT_MAP_CRMOD]                     = 0, //EXP_SZ(as_exp_val(NULL)),
		[_AS_EXP_CODE_CDT_MAP_CR]                        = 0, //EXP_SZ(as_exp_val(NULL)),
		[_AS_EXP_CODE_CDT_MAP_MOD]                       = 0, //EXP_SZ(as_exp_val(NULL))
		[BOOL]                                           = 0,
		[_TRUE]                                          = 0,
		[_FALSE]                                         = 0
	};


	for (int i = 0; i < *preds_size; ++i) {
		pred_op * pred = (pred_op *) as_vector_get(preds, (uint32_t)i);
		(*size_to_alloc) += EXPR_SIZES[pred->op];
	}

	if(size_to_alloc <= 0) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid expression");
	}

	return AEROSPIKE_OK;
}

/*
* get_exp_val_from_pyval
* Converts a python value into an expression value.
*/
static
as_status get_exp_val_from_pyval(AerospikeClient * self, as_static_pool * static_pool, int serializer_type, as_exp_entry * new_entry, PyObject * py_obj, pred_op * pred, as_error * err) {
	as_error_reset(err);

	if (!py_obj) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "value is null");
	} else if (PyBool_Check(py_obj)) {
		as_bytes *bytes;
		GET_BYTES_POOL(bytes, static_pool, err);
		if (err->code == AEROSPIKE_OK) {
			if (serialize_based_on_serializer_policy(self, serializer_type,
				&bytes, py_obj, err) != AEROSPIKE_OK) {
				return err->code;
			}

			as_exp_entry tmp_entry = as_exp_val((as_val *) bytes);
			*new_entry = tmp_entry;
		}

	} else if (PyLong_Check(py_obj)) {
		int64_t l = (int64_t) PyLong_AsLongLong(py_obj);
		if (l == -1 && PyErr_Occurred()) {
			if (PyErr_ExceptionMatches(PyExc_OverflowError)) {
				return as_error_update(err, AEROSPIKE_ERR_PARAM, "integer value exceeds sys.maxsize");
			}
		}

		as_exp_entry tmp_entry = as_exp_int(l);
		*new_entry = tmp_entry;
	} else if (PyUnicode_Check(py_obj)) {
		PyObject * py_ustr = PyUnicode_AsUTF8String(py_obj);
		char * str = PyBytes_AsString(py_ustr);
		{
			pred->val.val_string_p = strdup(str);
			pred->val_flag = 1;
			as_exp_entry tmp_entry = as_exp_str(pred->val.val_string_p);
			*new_entry = tmp_entry;
		}
		Py_DECREF(py_ustr);
	 } else if (PyBytes_Check(py_obj)) {
	 	uint8_t * b = (uint8_t *) PyBytes_AsString(py_obj);
	 	uint32_t b_len  = (uint32_t)  PyBytes_Size(py_obj);
		{
			as_exp_entry tmp_entry = as_exp_bytes(b, b_len);
			*new_entry = tmp_entry;
		}
	} else if (!strcmp(py_obj->ob_type->tp_name, "aerospike.Geospatial")) {
		PyObject *py_parameter = PyString_FromString("geo_data");
		PyObject* py_data = PyObject_GenericGetAttr(py_obj, py_parameter);
		Py_DECREF(py_parameter);
		char *geo_value = PyString_AsString(AerospikeGeospatial_DoDumps(py_data, err));
		as_exp_entry tmp_entry = as_exp_geo(geo_value);
		*new_entry = tmp_entry;
	} else if (PyByteArray_Check(py_obj)) {
		as_bytes *bytes;
		GET_BYTES_POOL(bytes, static_pool, err);
		if (err->code == AEROSPIKE_OK) {
			if (serialize_based_on_serializer_policy(self, serializer_type,
					&bytes, py_obj, err) != AEROSPIKE_OK) {
				return err->code;
			}
			{
				as_exp_entry tmp_entry = as_exp_val((as_val *) bytes);
				*new_entry = tmp_entry;
			}
		}
	} else if (PyList_Check(py_obj)) {
		as_list * list = NULL;
		pyobject_to_list(self, err, py_obj, &list, static_pool, serializer_type);
		if (err->code == AEROSPIKE_OK) {
			pred->val.val_list_p = list;
			pred->val_flag = 3;
			{
				as_exp_entry tmp_entry = as_exp_val(list);
				*new_entry = tmp_entry;
			}
		}
	} else if (PyDict_Check(py_obj)) {
		as_map * map = NULL;
		pyobject_to_map(self, err, py_obj, &map, static_pool, serializer_type);
		if (err->code == AEROSPIKE_OK) {
			pred->val.val_map_p = map;
			{
				as_exp_entry tmp_entry = as_exp_val(map);
				*new_entry = tmp_entry;
			}
		}
	} else if (Py_None == py_obj) {
		{
			as_exp_entry tmp_entry = as_exp_nil();
			*new_entry = tmp_entry;
		}
	} else if (!strcmp(py_obj->ob_type->tp_name, "aerospike.null")) {
		{
			as_exp_entry tmp_entry = as_exp_nil();
			*new_entry = tmp_entry;
		}
	} else if (AS_Matches_Classname(py_obj, AS_CDT_WILDCARD_NAME)) {
		{
			as_exp_entry tmp_entry = as_exp_val((as_val *) as_val_reserve(&as_cmp_wildcard));
			*new_entry = tmp_entry;
		}
	} else if (AS_Matches_Classname(py_obj, AS_CDT_INFINITE_NAME)) {
		{
			as_exp_entry tmp_entry = as_exp_val((as_val *) as_val_reserve(&as_cmp_inf));
			*new_entry = tmp_entry;
		}
	} else {
		if (PyFloat_Check(py_obj)) {
			double d = PyFloat_AsDouble(py_obj);
			{
				as_exp_entry tmp_entry = as_exp_float(d);
				*new_entry = tmp_entry;
			}
		} else {
			as_bytes *bytes;
			GET_BYTES_POOL(bytes, static_pool, err);
			if (err->code == AEROSPIKE_OK) {
				if (serialize_based_on_serializer_policy(self, serializer_type,
					&bytes, py_obj, err) != AEROSPIKE_OK) {
					return err->code;
				}

				{
					as_exp_entry tmp_entry = as_exp_val((as_val *) bytes);
					*new_entry = tmp_entry;
				}
			}
		}
	}

	return err->code;
}

/*
* add_pred_macros
* Converts each pred struct in pred_op_vector to as_exp_entry/ies and copies them to expressions.
* Note that a count of as_exp_entries to leave out of the copy is passed to the `APPEND_ARRAY` macro.
* Since this function uses the C expressions macros directly, we don't want to copy the useless junk generated by the 
* empty arguments used. Each expression child/value has a pred struct in pred_op_vector so the missing values will be copied later.
* These counts need to be updated if the C client macro changes.
*/
static
as_status add_pred_macros(AerospikeClient * self, as_static_pool * static_pool, int serializer_type, as_vector * unicodeStrVector, as_vector * pred_op_vector, as_exp_entry ** expressions, int * bottom, int * size, as_error * err) {

	for ( int i = 0; i < *size; ++i ) {
		pred_op * pred = (pred_op *) as_vector_get(pred_op_vector, (uint32_t)i);

		int64_t lval1 = 0;
		int64_t lval2 = 0;
		char * bin_name = NULL;
		
		if (pred->op >= _AS_EXP_CODE_CDT_LIST_CRMOD && pred->op <= _AS_EXP_CODE_CDT_MAP_MOD) {

			if (pred->op == _AS_EXP_CODE_CDT_LIST_CRMOD || pred->op == _AS_EXP_CODE_CDT_LIST_MOD) {
				APPEND_ARRAY(0, LIST_MOD_EXP());
			} else if (pred->op >= _AS_EXP_CODE_CDT_MAP_CRMOD && pred->op <= _AS_EXP_CODE_CDT_MAP_MOD) {
				APPEND_ARRAY(0, {.op=pred->op, .v.map_pol = pred->map_policy});
			}

			continue;
		}

		switch (pred->op) {
			case BIN:
				if (get_bin(err, pred->pydict, unicodeStrVector, &bin_name) != AEROSPIKE_OK) {
					return err->code;
				}

				APPEND_ARRAY(0, BIN_EXPR());
				break;
			case VAL:;
				as_exp_entry tmp_expr;
				if (get_exp_val_from_pyval(self, static_pool, serializer_type, &tmp_expr, PyDict_GetItemString(pred->pydict, AS_PY_VAL_KEY), pred, err) != AEROSPIKE_OK) {
					return err->code;
				}

				APPEND_ARRAY(0, tmp_expr);
				break;
			case EQ:
				APPEND_ARRAY(2, as_exp_cmp_eq({},{}));
				break;
			case NE:
				APPEND_ARRAY(2, as_exp_cmp_ne({},{}));
				break;
			case GT:
				APPEND_ARRAY(2, as_exp_cmp_gt({},{}));
				break;
			case GE:
				APPEND_ARRAY(2, as_exp_cmp_ge({},{}));
				break;
			case LT:
				APPEND_ARRAY(2, as_exp_cmp_lt({},{}));
				break;
			case LE:
				APPEND_ARRAY(2, as_exp_cmp_le({},{}));
				break;
			case CMP_REGEX:
				if (get_int64_t(err, REGEX_OPTIONS_KEY, pred->pydict, &lval1) != AEROSPIKE_OK) {
					return err->code;
				}

				PyObject * py_obj = PyDict_GetItemString(pred->pydict, AS_PY_VAL_KEY);
				char * regex_str = NULL;
				if (PyUnicode_Check(py_obj)) {
					PyObject * py_ustr = PyUnicode_AsUTF8String(py_obj);
					regex_str = strdup(PyBytes_AsString(py_ustr));
					pred->val.val_string_p = regex_str;
					Py_DECREF(py_ustr);
				} else {
					return as_error_update(err, AEROSPIKE_ERR_PARAM, "regex_str must be a string.");
				}
				
				APPEND_ARRAY(1, as_exp_cmp_regex(lval1, regex_str, {}));
				break;
			case CMP_GEO:
				APPEND_ARRAY(2, as_exp_cmp_geo({},{}));
				break;
			case AND:
				APPEND_ARRAY(2, as_exp_and({}));
				break;
			case OR:
				APPEND_ARRAY(2, as_exp_or({}));
				break;
			case NOT:
				APPEND_ARRAY(1, as_exp_not({}));
				break;
			case END_VA_ARGS:
				APPEND_ARRAY(0, {.op=_AS_EXP_CODE_END_OF_VA_ARGS}); //NOTE: this case handles the end of arguments to an AND/OR expression.
				break;
			case META_DIGEST_MOD:
				if (get_int64_t(err, AS_PY_VAL_KEY, pred->pydict, &lval1) != AEROSPIKE_OK) {
					return err->code;
				}

				APPEND_ARRAY(0, as_exp_digest_modulo(lval1));
				break;
			case META_DEVICE_SIZE:
					APPEND_ARRAY(0, as_exp_device_size());
				break;
			case META_LAST_UPDATE_TIME:
				APPEND_ARRAY(0, as_exp_last_update());
				break;
			case META_SINCE_UPDATE_TIME:
				APPEND_ARRAY(0, as_exp_since_update());
				break;
			case META_IS_TOMBSTONE:
				APPEND_ARRAY(0, as_exp_is_tombstone());
				break;
			case META_VOID_TIME:
				APPEND_ARRAY(0, as_exp_void_time());
				break;
			case META_TTL:
				APPEND_ARRAY(0, as_exp_ttl());
				break;
			case META_SET_NAME:
				APPEND_ARRAY(0, as_exp_set_name());
				break;
			case META_KEY_EXISTS:
				APPEND_ARRAY(0, as_exp_key_exist());
				break;
			case REC_KEY:
				APPEND_ARRAY(0, KEY_EXPR());
				break;
			case BIN_TYPE:
				if (get_bin(err, pred->pydict, unicodeStrVector, &bin_name) != AEROSPIKE_OK) {
					return err->code;
				}

				APPEND_ARRAY(0, as_exp_bin_type(bin_name));
				break;
			case BIN_EXISTS:
				if (get_bin(err, pred->pydict, unicodeStrVector, &bin_name) != AEROSPIKE_OK) {
					return err->code;
				}

				APPEND_ARRAY(0, as_exp_bin_exists(bin_name));
				break;
			case OP_LIST_GET_BY_INDEX:
				if (get_int64_t(err, AS_PY_BIN_TYPE_KEY, pred->pydict, &lval2) != AEROSPIKE_OK) {
					return err->code;
				}

				if (get_int64_t(err, AS_PY_LIST_RETURN_KEY, pred->pydict, &lval1) != AEROSPIKE_OK) {
					return err->code;
				}

				APPEND_ARRAY(2, as_exp_list_get_by_index(pred->ctx, lval1, lval2, {}, {})); // - 2 for index, bin
				break;
			case OP_LIST_SIZE:
				APPEND_ARRAY(1, as_exp_list_size(pred->ctx, {}));
				break;
			case OP_LIST_GET_BY_VALUE:
				if (get_int64_t(err, AS_PY_LIST_RETURN_KEY, pred->pydict, &lval1) != AEROSPIKE_OK) {
					return err->code;
				}

				APPEND_ARRAY(2, as_exp_list_get_by_value(pred->ctx, lval1, {}, {})); // - 2 for value, bin
				break;
			case OP_LIST_GET_BY_VALUE_RANGE:
				if (get_int64_t(err, AS_PY_LIST_RETURN_KEY, pred->pydict, &lval1) != AEROSPIKE_OK) {
					return err->code;
				}

				APPEND_ARRAY(3, as_exp_list_get_by_value_range(pred->ctx, lval1, {}, {}, {})); // - 3 for begin, end, bin
				break;
			case OP_LIST_GET_BY_VALUE_LIST:
				if (get_int64_t(err, AS_PY_LIST_RETURN_KEY, pred->pydict, &lval1) != AEROSPIKE_OK) {
					return err->code;
				}

				APPEND_ARRAY(2, as_exp_list_get_by_value_list(pred->ctx, lval1, {}, {})); // - 2 for value, bin
				break;
			case OP_LIST_GET_BY_VALUE_RANK_RANGE_REL_TO_END:
				if (get_int64_t(err, AS_PY_LIST_RETURN_KEY, pred->pydict, &lval1) != AEROSPIKE_OK) {
					return err->code;
				}

				APPEND_ARRAY(3, as_exp_list_get_by_rel_rank_range_to_end(pred->ctx, lval1, {}, {}, {})); // - 3 for value, rank, bin
				break;
			case OP_LIST_GET_BY_VALUE_RANK_RANGE_REL:
				if (get_int64_t(err, AS_PY_LIST_RETURN_KEY, pred->pydict, &lval1) != AEROSPIKE_OK) {
					return err->code;
				}

				APPEND_ARRAY(4, as_exp_list_get_by_rel_rank_range(pred->ctx, lval1, {}, {}, {}, {})); // - 4 for value, rank, count, bin
				break;
			case OP_LIST_GET_BY_INDEX_RANGE_TO_END:
				if (get_int64_t(err, AS_PY_LIST_RETURN_KEY, pred->pydict, &lval1) != AEROSPIKE_OK) {
					return err->code;
				}
				
				APPEND_ARRAY(2, as_exp_list_get_by_index_range_to_end(pred->ctx, lval1, {}, {})); // - 2 for index, bin
				break;
			case OP_LIST_GET_BY_INDEX_RANGE:
				if (get_int64_t(err, AS_PY_LIST_RETURN_KEY, pred->pydict, &lval1) != AEROSPIKE_OK) {
					return err->code;
				}

				APPEND_ARRAY(3, as_exp_list_get_by_index_range(pred->ctx, lval1, {}, {}, {})); // - 3 for index, count, bin
				break;
			case OP_LIST_GET_BY_RANK:
				if (get_int64_t(err, AS_PY_LIST_RETURN_KEY, pred->pydict, &lval1) != AEROSPIKE_OK) {
					return err->code;
				}

				if (get_int64_t(err, AS_PY_BIN_TYPE_KEY, pred->pydict, &lval2) != AEROSPIKE_OK) {
					return err->code;
				}

				APPEND_ARRAY(2, as_exp_list_get_by_rank(pred->ctx, lval1, lval2, {}, {})); // - 2 for rank, bin
				break;
			case OP_LIST_GET_BY_RANK_RANGE_TO_END:
				if (get_int64_t(err, AS_PY_LIST_RETURN_KEY, pred->pydict, &lval1) != AEROSPIKE_OK) {
					return err->code;
				}

				APPEND_ARRAY(2, as_exp_list_get_by_rank_range_to_end(pred->ctx, lval1, {}, {})); // - 2 for rank, bin
				break;
			case OP_LIST_GET_BY_RANK_RANGE:
				if (get_int64_t(err, AS_PY_LIST_RETURN_KEY, pred->pydict, &lval1) != AEROSPIKE_OK) {
					return err->code;
				}

				APPEND_ARRAY(3, as_exp_list_get_by_rank_range(pred->ctx, lval1, {}, {}, {})); // - 3 for rank, count, bin
				break;
			case OP_LIST_APPEND:
				APPEND_ARRAY(3, as_exp_list_append(pred->ctx, pred->list_policy, {}, {}));
				break;
			case OP_LIST_APPEND_ITEMS:
				APPEND_ARRAY(3, as_exp_list_append_items(pred->ctx, pred->list_policy, {}, {}));
				break;
			case OP_LIST_INSERT:
				APPEND_ARRAY(4, as_exp_list_insert(pred->ctx, pred->list_policy, {}, {}, {}));
				break;
			case OP_LIST_INSERT_ITEMS:
				APPEND_ARRAY(4, as_exp_list_insert_items(pred->ctx, pred->list_policy, {}, {}, {}));
				break;
			case OP_LIST_INCREMENT:
				APPEND_ARRAY(4, as_exp_list_increment(pred->ctx, pred->list_policy, {}, {}, {}));
				break;
			case OP_LIST_SET:
				APPEND_ARRAY(4, as_exp_list_set(pred->ctx, pred->list_policy, {}, {}, {}));
				break;
			case OP_LIST_CLEAR:
				APPEND_ARRAY(1, as_exp_list_clear(pred->ctx, {})); // -1 for bin
				break;
			case OP_LIST_SORT:
				if (get_int64_t(err, LIST_ORDER_KEY, pred->pydict, &lval1) != AEROSPIKE_OK) {
					return err->code;
				}

				APPEND_ARRAY(1, as_exp_list_sort(pred->ctx, lval1, {})); // -1 for bin
				break;
			case OP_LIST_REMOVE_BY_VALUE:
				APPEND_ARRAY(2, as_exp_list_remove_by_value(pred->ctx, {}, {})); // -2 for bin and val
				break;
			case OP_LIST_REMOVE_BY_VALUE_LIST:
				APPEND_ARRAY(2, as_exp_list_remove_by_value_list(pred->ctx, {}, {})); // -2 for bin and val
				break;
			case OP_LIST_REMOVE_BY_VALUE_RANGE:
				APPEND_ARRAY(3, as_exp_list_remove_by_value_range(pred->ctx, {}, {}, {})); // - 3 for begin, end, val
				break;
			case OP_LIST_REMOVE_BY_REL_RANK_RANGE_TO_END:
				APPEND_ARRAY(3, as_exp_list_remove_by_rel_rank_range_to_end(pred->ctx, {}, {}, {})); // -3 for value, rank, bin
				break;
			case OP_LIST_REMOVE_BY_REL_RANK_RANGE:
				APPEND_ARRAY(4, as_exp_list_remove_by_rel_rank_range(pred->ctx, {}, {}, {}, {})); // -4 for value, rank, count, bin
				break;
			case OP_LIST_REMOVE_BY_INDEX:
				APPEND_ARRAY(2, as_exp_list_remove_by_index(pred->ctx, {}, {})); // -2 for index, bin
				break;
			case OP_LIST_REMOVE_BY_INDEX_RANGE_TO_END:
				APPEND_ARRAY(2, as_exp_list_remove_by_index_range_to_end(pred->ctx, {}, {})); // -2 for index, bin
				break;
			case OP_LIST_REMOVE_BY_INDEX_RANGE:
				APPEND_ARRAY(3, as_exp_list_remove_by_index_range(pred->ctx, {}, {}, {})); // - 3 for index, count, bin
				break;
			case OP_LIST_REMOVE_BY_RANK:
				APPEND_ARRAY(2, as_exp_list_remove_by_rank(pred->ctx, {}, {})); // -2 for rank, bin
				break;
			case OP_LIST_REMOVE_BY_RANK_RANGE_TO_END:
				APPEND_ARRAY(2, as_exp_list_remove_by_rank_range_to_end(pred->ctx, {}, {})); // - 2 for rank, bin
				break;
			case OP_LIST_REMOVE_BY_RANK_RANGE:
				APPEND_ARRAY(3, as_exp_list_remove_by_rank_range(pred->ctx, {}, {}, {})); // - 3 for rank, count, bin
				break;
			case OP_MAP_PUT:
				APPEND_ARRAY(4, as_exp_map_put(pred->ctx, pred->map_policy, {}, {}, {}));
				break;
			case OP_MAP_PUT_ITEMS:
				APPEND_ARRAY(3, as_exp_map_put_items(pred->ctx, pred->map_policy, {}, {}));
				break;
			case OP_MAP_INCREMENT:
				APPEND_ARRAY(4, as_exp_map_increment(pred->ctx, pred->map_policy, {}, {}, {}));
				break;
			case OP_MAP_CLEAR:
				APPEND_ARRAY(1, as_exp_map_clear(pred->ctx, {})); // - 1 for bin
				break;
			case OP_MAP_REMOVE_BY_KEY:
				APPEND_ARRAY(2, as_exp_map_remove_by_key(pred->ctx, {}, {})); // - 2 for key, bin
				break;
			case OP_MAP_REMOVE_BY_KEY_LIST:
				APPEND_ARRAY(2, as_exp_map_remove_by_key_list(pred->ctx, {}, {})); // - 2 for key, bin
				break;
			case OP_MAP_REMOVE_BY_KEY_RANGE:
				APPEND_ARRAY(3, as_exp_map_remove_by_key_range(pred->ctx, {}, {}, {})); // - 3 for begin, end, bin
				break;
			case OP_MAP_REMOVE_BY_KEY_REL_INDEX_RANGE_TO_END:
				APPEND_ARRAY(3, as_exp_map_remove_by_key_rel_index_range_to_end(pred->ctx, {}, {}, {})); // - 3 for key, index, bin
				break;
			case OP_MAP_REMOVE_BY_KEY_REL_INDEX_RANGE:
				APPEND_ARRAY(4, as_exp_map_remove_by_key_rel_index_range(pred->ctx, {}, {}, {}, {})); // - 4 for key, index, count, bin
				break;
			case OP_MAP_REMOVE_BY_VALUE:
				APPEND_ARRAY(2, as_exp_map_remove_by_value(pred->ctx, {}, {})); // - 2 for val, bin
				break;
			case OP_MAP_REMOVE_BY_VALUE_LIST:
				APPEND_ARRAY(2, as_exp_map_remove_by_value_list(pred->ctx, {}, {})); // - 2 for values, bin
				break;
			case OP_MAP_REMOVE_BY_VALUE_RANGE:
				APPEND_ARRAY(3, as_exp_map_remove_by_value_range(pred->ctx, {}, {}, {})); // - 3 for begin, end, bin
				break;
			case OP_MAP_REMOVE_BY_VALUE_REL_RANK_RANGE_TO_END:
				APPEND_ARRAY(3, as_exp_map_remove_by_value_rel_rank_range_to_end(pred->ctx, {}, {}, {})); // - 3 for val, rank, bin
				break;
			case OP_MAP_REMOVE_BY_VALUE_REL_RANK_RANGE:
				APPEND_ARRAY(4, as_exp_map_remove_by_value_rel_rank_range(pred->ctx, {}, {}, {}, {})); // - 4 for val, rank, count, bin
				break;
			case OP_MAP_REMOVE_BY_INDEX:
				APPEND_ARRAY(2, as_exp_map_remove_by_index(pred->ctx, {}, {})); // - 2 for index, bin
				break;
			case OP_MAP_REMOVE_BY_INDEX_RANGE_TO_END:
				APPEND_ARRAY(2, as_exp_map_remove_by_index_range_to_end(pred->ctx, {}, {})); // - 2 for index, bin
				break;
			case OP_MAP_REMOVE_BY_INDEX_RANGE:
				APPEND_ARRAY(3, as_exp_map_remove_by_index_range(pred->ctx, {}, {}, {})); // - 3 for index, count, bin
				break;
			case OP_MAP_REMOVE_BY_RANK:
				APPEND_ARRAY(2, as_exp_map_remove_by_rank(pred->ctx, {}, {})); // - 2 for rank, bin
				break;
			case OP_MAP_REMOVE_BY_RANK_RANGE_TO_END:
				APPEND_ARRAY(2, as_exp_map_remove_by_rank_range_to_end(pred->ctx, {}, {})); // - 2 for rank, bin
				break;
			case OP_MAP_REMOVE_BY_RANK_RANGE:
				APPEND_ARRAY(3, as_exp_map_remove_by_rank_range(pred->ctx, {}, {}, {})); // - 3 for rank, count, bin
				break;
			case OP_MAP_SIZE:
				APPEND_ARRAY(1, as_exp_map_size(pred->ctx, {})); // - 1 for bin
				break;
			case OP_MAP_GET_BY_KEY:
				if (get_int64_t(err, AS_PY_MAP_RETURN_KEY, pred->pydict, &lval1) != AEROSPIKE_OK) {
					return err->code;
				}

				if (get_int64_t(err, AS_PY_BIN_TYPE_KEY, pred->pydict, &lval2) != AEROSPIKE_OK) {
					return err->code;
				}

				APPEND_ARRAY(2, as_exp_map_get_by_key(pred->ctx, lval1, lval2, {}, {})); // - 2 for key, bin
				break;
			case OP_MAP_GET_BY_KEY_RANGE:
				if (get_int64_t(err, AS_PY_MAP_RETURN_KEY, pred->pydict, &lval1) != AEROSPIKE_OK) {
					return err->code;
				}

				APPEND_ARRAY(3, as_exp_map_get_by_key_range(pred->ctx, lval1, {}, {}, {})); // - 3 for begin, end, bin
				break;
			case OP_MAP_GET_BY_KEY_LIST:
				if (get_int64_t(err, AS_PY_MAP_RETURN_KEY, pred->pydict, &lval1) != AEROSPIKE_OK) {
					return err->code;
				}

				APPEND_ARRAY(2, as_exp_map_get_by_key_list(pred->ctx, lval1, {}, {})); // - 2 for keys, bin
				break;
			case OP_MAP_GET_BY_KEY_REL_INDEX_RANGE_TO_END:
				if (get_int64_t(err, AS_PY_MAP_RETURN_KEY, pred->pydict, &lval1) != AEROSPIKE_OK) {
					return err->code;
				}

				APPEND_ARRAY(3, as_exp_map_get_by_key_rel_index_range_to_end(pred->ctx, lval1, {}, {}, {})); // - 3 for key, index, bin
				break;
			case OP_MAP_GET_BY_KEY_REL_INDEX_RANGE:
				if (get_int64_t(err, AS_PY_MAP_RETURN_KEY, pred->pydict, &lval1) != AEROSPIKE_OK) {
					return err->code;
				}

				APPEND_ARRAY(4, as_exp_map_get_by_key_rel_index_range(pred->ctx, lval1, {}, {}, {}, {})); // - 4 for key, index, count, bin
				break;
			case OP_MAP_GET_BY_VALUE:
				if (get_int64_t(err, AS_PY_MAP_RETURN_KEY, pred->pydict, &lval1) != AEROSPIKE_OK) {
					return err->code;
				}

				APPEND_ARRAY(2, as_exp_map_get_by_value(pred->ctx, lval1, {}, {})); // - 2 for value, bin
				break;
			case OP_MAP_GET_BY_VALUE_RANGE:
				if (get_int64_t(err, AS_PY_MAP_RETURN_KEY, pred->pydict, &lval1) != AEROSPIKE_OK) {
					return err->code;
				}

				APPEND_ARRAY(3, as_exp_map_get_by_value_range(pred->ctx, lval1, {}, {}, {})); // - 3 for begin, end, bin
				break;
			case OP_MAP_GET_BY_VALUE_LIST:
				if (get_int64_t(err, AS_PY_MAP_RETURN_KEY, pred->pydict, &lval1) != AEROSPIKE_OK) {
					return err->code;
				}

				APPEND_ARRAY(2, as_exp_map_get_by_value_list(pred->ctx, lval1, {}, {})); // - 2 for value, bin
				break;
			case OP_MAP_GET_BY_VALUE_RANK_RANGE_REL_TO_END:
				if (get_int64_t(err, AS_PY_MAP_RETURN_KEY, pred->pydict, &lval1) != AEROSPIKE_OK) {
					return err->code;
				}

				APPEND_ARRAY(3, as_exp_map_get_by_value_rel_rank_range_to_end(pred->ctx, lval1, {}, {}, {})); // - 3 for value, rank, bin
				break;
			case OP_MAP_GET_BY_VALUE_RANK_RANGE_REL:
				if (get_int64_t(err, AS_PY_MAP_RETURN_KEY, pred->pydict, &lval1) != AEROSPIKE_OK) {
					return err->code;
				}

				APPEND_ARRAY(4, as_exp_map_get_by_value_rel_rank_range(pred->ctx, lval1, {}, {}, {}, {})); // - 4 for value, rank, count, bin
				break;
			case OP_MAP_GET_BY_INDEX:
				if (get_int64_t(err, AS_PY_MAP_RETURN_KEY, pred->pydict, &lval1) != AEROSPIKE_OK) {
					return err->code;
				}

				if (get_int64_t(err, AS_PY_BIN_TYPE_KEY, pred->pydict, &lval2) != AEROSPIKE_OK) {
					return err->code;
				}

				APPEND_ARRAY(2, as_exp_map_get_by_index(pred->ctx, lval1, lval2, {}, {})); // - 2 for index, bin
				break;
			case OP_MAP_GET_BY_INDEX_RANGE_TO_END:
				if (get_int64_t(err, AS_PY_MAP_RETURN_KEY, pred->pydict, &lval1) != AEROSPIKE_OK) {
					return err->code;
				}

				APPEND_ARRAY(2, as_exp_map_get_by_index_range_to_end(pred->ctx, lval1, {}, {})); // - 2 for index, bin
				break;
			case OP_MAP_GET_BY_INDEX_RANGE:
				if (get_int64_t(err, AS_PY_MAP_RETURN_KEY, pred->pydict, &lval1) != AEROSPIKE_OK) {
					return err->code;
				}

				APPEND_ARRAY(3, as_exp_map_get_by_index_range(pred->ctx, lval1, {}, {}, {})); // - 3 for index, count, bin
				break;
			case OP_MAP_GET_BY_RANK:
				if (get_int64_t(err, AS_PY_MAP_RETURN_KEY, pred->pydict, &lval1) != AEROSPIKE_OK) {
					return err->code;
				}

				if (get_int64_t(err, AS_PY_BIN_TYPE_KEY, pred->pydict, &lval2) != AEROSPIKE_OK) {
					return err->code;
				}

				APPEND_ARRAY(2, as_exp_map_get_by_rank(pred->ctx, lval1, lval2, {}, {})); // - 2 for rank, bin
				break;
			case OP_MAP_GET_BY_RANK_RANGE_TO_END:
				if (get_int64_t(err, AS_PY_MAP_RETURN_KEY, pred->pydict, &lval1) != AEROSPIKE_OK) {
					return err->code;
				}

				APPEND_ARRAY(2, as_exp_map_get_by_rank_range_to_end(pred->ctx, lval1, {}, {})); // - 2 for rank, bin
				break;
			case OP_MAP_GET_BY_RANK_RANGE:
				if (get_int64_t(err, AS_PY_MAP_RETURN_KEY, pred->pydict, &lval1) != AEROSPIKE_OK) {
					return err->code;
				}

				APPEND_ARRAY(3, as_exp_map_get_by_rank_range(pred->ctx, lval1, {}, {}, {})); // - 3 for rank, count, bin
				break;
			case _AS_EXP_BIT_FLAGS:
				if (get_int64_t(err, AS_PY_VAL_KEY, pred->pydict, &lval1) != AEROSPIKE_OK) {
					return err->code;
				}

				APPEND_ARRAY(0, as_exp_uint((uint64_t)lval1));
				break;
			case OP_BIT_RESIZE:
				APPEND_ARRAY(4, as_exp_bit_resize(NULL, {}, NO_BIT_FLAGS, {})); // - 4 for byte_size, policy, flags, bin
				break;
			case OP_BIT_INSERT:
				APPEND_ARRAY(4, as_exp_bit_insert(NULL, {}, {}, {}));
				break;
			case OP_BIT_REMOVE:
				APPEND_ARRAY(4, as_exp_bit_remove(NULL, {}, {}, {}));
				break;
			case OP_BIT_SET:
				APPEND_ARRAY(5, as_exp_bit_set(NULL, {}, {}, {}, {}));
				break;
			case OP_BIT_OR:
				APPEND_ARRAY(5, as_exp_bit_or(NULL, {}, {}, {}, {}));
				break;
			case OP_BIT_XOR:
				APPEND_ARRAY(5, as_exp_bit_xor(NULL, {}, {}, {}, {}));
				break;
			case OP_BIT_AND:
				APPEND_ARRAY(5, as_exp_bit_and(NULL, {}, {}, {}, {}));
				break;
			case OP_BIT_NOT:
				APPEND_ARRAY(4, as_exp_bit_not(NULL, {}, {}, {}));
				break;
			case OP_BIT_LSHIFT:
				APPEND_ARRAY(5, as_exp_bit_lshift(NULL, {}, {}, {}, {}));
				break;
			case OP_BIT_RSHIFT:
				APPEND_ARRAY(5, as_exp_bit_rshift(NULL, {}, {}, {}, {}));
				break;
			case OP_BIT_ADD:
				APPEND_ARRAY(6, as_exp_bit_add(NULL, {}, {}, {}, NO_BIT_FLAGS, {}));
				break;
			case OP_BIT_SUBTRACT:
				APPEND_ARRAY(6, as_exp_bit_subtract(NULL, {}, {}, {}, NO_BIT_FLAGS, {}));
				break;
			case OP_BIT_SET_INT:
				APPEND_ARRAY(5, as_exp_bit_set_int(NULL, {}, {}, {}, {}));
				break;
			case OP_BIT_GET:
				APPEND_ARRAY(3, as_exp_bit_get({}, {}, {})); // - 3 for bit_offset, bit_size, bin
				break;
			case OP_BIT_COUNT:
				APPEND_ARRAY(3, as_exp_bit_count({}, {}, {})); // - 3 for bit_offset, bit_size, bin
				break;
			case OP_BIT_LSCAN:
				APPEND_ARRAY(4, as_exp_bit_lscan({}, {}, {}, {}));
				break;
			case OP_BIT_RSCAN:
				APPEND_ARRAY(4, as_exp_bit_rscan({}, {}, {}, {}));
				break;
			case OP_BIT_GET_INT:
				APPEND_ARRAY(4, as_exp_bit_get_int({}, {}, 0, {}));
				break;
			case OP_HLL_ADD: // NOTE: this case covers HLLAddMH, HLLAdd, and HLLUpdate
				APPEND_ARRAY(5, as_exp_hll_add_mh(NULL, {}, 0, 0, {})); // - 5 for list, index_bit_count, -1, policy, bin
				break;
			case OP_HLL_GET_COUNT:
				APPEND_ARRAY(1, as_exp_hll_get_count({})); // - 1 for bin
				break;
			case OP_HLL_GET_UNION:
				APPEND_ARRAY(2, as_exp_hll_get_union({}, {})); // - 2 for list, bin
				break;
			case OP_HLL_GET_UNION_COUNT:
				APPEND_ARRAY(2, as_exp_hll_get_union_count({}, {})); // - 2 for list, bin
				break;
			case OP_HLL_GET_INTERSECT_COUNT:
				APPEND_ARRAY(2, as_exp_hll_get_intersect_count({}, {})); // - 2 for list, bin
				break;
			case OP_HLL_GET_SIMILARITY:
				APPEND_ARRAY(2, as_exp_hll_get_similarity({}, {})); // - 2 for list, bin
				break;
			case OP_HLL_DESCRIBE:
				APPEND_ARRAY(1, as_exp_hll_describe({})); // - 1 for bin
				break;
			case OP_HLL_MAY_CONTAIN:
				APPEND_ARRAY(2, as_exp_hll_may_contain({}, {})); // - 2 for list, bin
				break;
			case _TRUE: // This handles Python to as_exp_bool translation for expressions like OP_BIT_LSCAN
				APPEND_ARRAY(0, as_exp_bool(1));
				break;
			case _FALSE:
				APPEND_ARRAY(0, as_exp_bool(0));
				break;
			default:
				return as_error_update(err, AEROSPIKE_ERR_PARAM, "Unrecognised expression op type.");
				break;
		}
	}

	return err->code;
}

/*
* convert_exp_list
* Converts expressions from python into pred structs.
* Initiates the conversion from pred structs to expressions.
* builds the expressions.
*/
as_status convert_exp_list(AerospikeClient * self, PyObject* py_exp_list, as_exp** exp_list, as_error* err) {
	int bottom = 0;
	Py_ssize_t size = PyList_Size(py_exp_list);
	if (size <= 0) {
		as_error_update(err, AEROSPIKE_ERR_PARAM, "Expressions must be a non empty list of 4 element tuples, generated by a compiled aerospike expression");
		return err->code;
	}

	long op = 0;
	long result_type = 0;
	long num_children = 0;
	int child_count = 1;
	int processed_exp_count = 0;
	int size_to_alloc = 0;
	uint8_t va_flag = 0;
	bool ctx_in_use = false;
	PyObject * py_pred_tuple = NULL;
	PyObject * py_list_policy_p = NULL;
	PyObject * py_map_policy_p = NULL;
	PyObject * py_ctx_list_p = NULL;
	as_vector pred_queue;
	pred_op pred;
	as_exp_entry * c_pred_entries = NULL;

	as_vector * unicodeStrVector = as_vector_create(sizeof(char *), 128);
	as_vector_inita(&pred_queue, sizeof(pred_op), size);

	as_static_pool static_pool;
	memset(&static_pool, 0, sizeof(static_pool));

    for ( int i = 0; i < size; ++i ) {
		pred.ctx = NULL;
		pred.pydict = NULL;
		pred.list_policy = NULL;
		pred.map_policy = NULL;
		pred.val_flag = 0;
		ctx_in_use = false;

		if (child_count == 0 && va_flag >= 1) { //this handles AND, OR
			pred.op=END_VA_ARGS;
			as_vector_append(&pred_queue, (void*) &pred);
			processed_exp_count++;
			--va_flag;
			continue;
		}

        py_pred_tuple = PyList_GetItem(py_exp_list, (Py_ssize_t)i);
		if ( !PyTuple_Check(py_pred_tuple) || PyTuple_Size(py_pred_tuple) != 4) {
			as_error_update(err, AEROSPIKE_ERR_PARAM, "Expressions must be a non empty list of 4 element tuples, generated by a compiled aerospike expression");
			goto CLEANUP;
		}

		pred.pytuple = py_pred_tuple;
        op = PyInt_AsLong(PyTuple_GetItem(py_pred_tuple, 0));
		if (result_type == -1 && PyErr_Occurred()) {
			as_error_update(err, AEROSPIKE_ERR_PARAM, "Failed to get op from expression tuple, op must be an int.");
			goto CLEANUP;
		}

		PyObject * rt_tmp = PyTuple_GetItem(py_pred_tuple, 1);
		if (rt_tmp != Py_None) {
			result_type = PyInt_AsLong(rt_tmp);
			if (result_type == -1 && PyErr_Occurred()) {
				as_error_update(err, AEROSPIKE_ERR_PARAM, "Failed to get result_type from expression tuple, rt must be an int.");
				goto CLEANUP;
			}
		}


        pred.pydict = PyTuple_GetItem(py_pred_tuple, 2);
		if (pred.pydict != Py_None) {
			if ( !PyDict_Check(pred.pydict)) {
				as_error_update(err, AEROSPIKE_ERR_PARAM, "Failed to get fixed dictionary from expression tuple, fixed must be a dict.");
				goto CLEANUP;
			}
		}

		py_ctx_list_p = PyDict_GetItemString(pred.pydict, CTX_KEY);
		if (py_ctx_list_p != NULL) {
			pred.ctx = malloc(sizeof(as_cdt_ctx));
			if (pred.ctx == NULL) {
				as_error_update(err, AEROSPIKE_ERR, "Could not malloc mem for pred.ctx.");
				goto CLEANUP;
			}

			if (get_cdt_ctx(self, err, pred.ctx, pred.pydict, &ctx_in_use, &static_pool, SERIALIZER_PYTHON) != AEROSPIKE_OK) {
				pred.ctx = NULL;
				goto CLEANUP;
			}
		}

		py_list_policy_p = PyDict_GetItemString(pred.pydict, AS_PY_LIST_POLICY);
		if (py_list_policy_p != NULL) {
			if (PyDict_Check(py_list_policy_p) && PyDict_Size(py_list_policy_p) > 0) {
				pred.list_policy = malloc(sizeof(as_list_policy));
				if (pred.list_policy == NULL) {
					as_error_update(err, AEROSPIKE_ERR, "Could not malloc mem for pred.list_policy.");
					goto CLEANUP;
				}

				bool policy_in_use = false;
				if (get_list_policy(err, pred.pydict, pred.list_policy, &policy_in_use) != AEROSPIKE_OK) {
					pred.list_policy = NULL;
					goto CLEANUP;
				}
			}
		}

		py_map_policy_p = PyDict_GetItemString(pred.pydict, AS_PY_MAP_POLICY);
		if (py_map_policy_p != NULL) {
			if (PyDict_Check(py_map_policy_p) && PyDict_Size(py_map_policy_p) > 0) {
				pred.map_policy = malloc(sizeof(as_map_policy));
				if (pred.map_policy == NULL) {
					as_error_update(err, AEROSPIKE_ERR, "Could not malloc mem for pred.map_policy.");
					goto CLEANUP;
				}

				if (pyobject_to_map_policy(err, py_map_policy_p, pred.map_policy) != AEROSPIKE_OK) {
					pred.map_policy = NULL;
					goto CLEANUP;
				}
			}
		}


		if (op == AND || op == OR) {
			++va_flag;
			++size;
		}

        num_children = PyInt_AsLong(PyTuple_GetItem(py_pred_tuple, 3));
		if (num_children == -1 && PyErr_Occurred()) {
			as_error_update(err, AEROSPIKE_ERR_PARAM, "Failed to get num_children from expression tuple, num_children must be an int.");
			goto CLEANUP;
		}

		pred.op = op;
		pred.result_type = result_type;
		pred.num_children = num_children;
		as_vector_append(&pred_queue, (void*) &pred);
		processed_exp_count++;
		if (va_flag) {
			child_count += num_children - 1;
		}
    }

	if (get_expr_size(&size_to_alloc, (int*) &size, &pred_queue, err) != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	c_pred_entries = (as_exp_entry*) malloc(size_to_alloc);
	if (c_pred_entries == NULL) {
		as_error_update(err, AEROSPIKE_ERR, "Could not malloc mem for c_pred_entries.");
		goto CLEANUP;
	}

	if (add_pred_macros(self, &static_pool, SERIALIZER_PYTHON, unicodeStrVector, &pred_queue, &c_pred_entries, &bottom, (int*)&size, err) != AEROSPIKE_OK) { //TODO add user defined serializer support
		goto CLEANUP;
	}

	*exp_list = as_exp_compile(c_pred_entries, bottom);
CLEANUP:
	for (int i = 0; i < processed_exp_count; ++i) {
		pred_op * pred = (pred_op *) as_vector_get(&pred_queue, (uint32_t)i);

		if (pred == NULL) {
			continue;
		}

		if (pred->list_policy != NULL) {
			free(pred->list_policy);
		}

		if (pred->map_policy != NULL) {
			free(pred->map_policy);
		}

		if (pred->ctx != NULL) {
			as_cdt_ctx_destroy(pred->ctx);
		}

		switch (pred->val_flag) {
			case 1:
				free(pred->val.val_string_p);
				break;
			case 2:
				as_list_destroy(pred->val.val_list_p);
				break;
			case 3:
				as_map_destroy(pred->val.val_map_p);
				break;
		}

		pred->pydict = NULL;
		pred->pytuple = NULL;
		pred->ctx = NULL;
	}

	as_vector_destroy(&pred_queue);
	if (c_pred_entries != NULL) {
		free(c_pred_entries);
	}

	POOL_DESTROY(&static_pool);
	as_vector_destroy(unicodeStrVector);
	return err->code;
}