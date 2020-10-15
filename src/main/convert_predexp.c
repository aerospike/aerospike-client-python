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
#include <aerospike/as_msgpack.h>
#include <aerospike/as_msgpack_ext.h>

#include "client.h"
#include "conversions.h"
#include "serializer.h"
#include "exceptions.h"
#include "policy.h"
#include "cdt_operation_utils.h"
#include "geo.h"
#include "cdt_types.h"

/**********
* TODO
* Implement list and map ops.
* Improve Error checking.
* Improve memory handling.
* Add getfromtuple macros for int,str,list,policy
***********/

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

#define REC_KEY 80
#define BIN 81
#define BIN_TYPE 82
#define BIN_EXISTS 83

#define CALL 127
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

// UTILITY CONSTANTS
#define MAX_ELEMENTS 11 //TODO find largest macro and adjust this val
#define FIXED_ACTIVE 1
#define fixed_num_ACTIVE 2

// Fixed dictionary keys
#define OP_TYPE_KEY "ot_key"
#define LIST_ORDER_KEY "list_order"

typedef struct {
	long op;
	long result_type;
	// union {
	// 	char * fixed;
	// 	int64_t fixed_num;
	// };
	//PyObject * fixed;
	PyObject * pydict;
	PyObject * pytuple;
	as_cdt_ctx * ctx;
	// PyObject * pyval2;
	// PyObject * pyval3;
	// PyObject * pyval4;

	long num_children;
	// uint8_t fixed_active;
	//as_policy_write * policy;// todo add a member for an as_val
} pred_op;

int bottom = 0;


// IMP do error checking for memcpy below
#define append_array(ar_size) {\
	memcpy(&((*expressions)[bottom]), &new_entries, sizeof(as_exp_entry) * ar_size);\
	bottom += ar_size;\
}

static
as_status get_exp_val_from_pyval(AerospikeClient * self, as_static_pool * static_pool, int serializer_type, as_exp_entry * new_entry, PyObject * py_obj, as_error * err) {
	//as_exp_entry new_entries[] = {AS_EXP_VAL_INT(pred->fixed_num)};
	//TODO why not pass in the array and do the copy in here
	as_error_reset(err);

	if (!py_obj) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "value is null");
	} else if (PyBool_Check(py_obj)) { //TODO
		//return as_error_update(err, AEROSPIKE_ERR, "NOT YET IMPLEMENTED1\n");
		as_bytes *bytes;
		GET_BYTES_POOL(bytes, static_pool, err);
		if (err->code == AEROSPIKE_OK) {
			if (serialize_based_on_serializer_policy(self, serializer_type,
				&bytes, py_obj, err) != AEROSPIKE_OK) {
				return err->code;
			}

			as_exp_entry tmp_entry = AS_EXP_VAL((as_val *) bytes);
			*new_entry = tmp_entry;
		}
		// {
		// 	as_exp_entry tmp_entry = AS_EXP_BOOL(PyObject_IsTrue(py_obj));
		// 	*new_entry = tmp_entry;
		// }
	} else if (PyInt_Check(py_obj)) {
		int64_t i = (int64_t) PyInt_AsLong(py_obj);
		if (i == -1 && PyErr_Occurred()) {
			if (PyErr_ExceptionMatches(PyExc_OverflowError)) {
				return as_error_update(err, AEROSPIKE_ERR_PARAM, "integer value exceeds sys.maxsize");
			}
		}

		{
			as_exp_entry tmp_entry = AS_EXP_INT(i);
			*new_entry = tmp_entry;
		}
	} else if (PyLong_Check(py_obj)) {
		int64_t l = (int64_t) PyLong_AsLongLong(py_obj);
		if (l == -1 && PyErr_Occurred()) {
			if (PyErr_ExceptionMatches(PyExc_OverflowError)) {
				return as_error_update(err, AEROSPIKE_ERR_PARAM, "integer value exceeds sys.maxsize");
			}
		}

		{
			as_exp_entry tmp_entry = AS_EXP_INT(l);
			*new_entry = tmp_entry;
		}
	} else if (PyUnicode_Check(py_obj)) {
		PyObject * py_ustr = PyUnicode_AsUTF8String(py_obj);
		char * str = PyBytes_AsString(py_ustr);
		{
			as_exp_entry tmp_entry = AS_EXP_STR(strdup(str));
			*new_entry = tmp_entry;
		}
		Py_DECREF(py_ustr);
	} else if (PyString_Check(py_obj)) {
		char * s = PyString_AsString(py_obj);
		{
			as_exp_entry tmp_entry = AS_EXP_STR(s);
			*new_entry = tmp_entry;
		}
	 } else if (PyBytes_Check(py_obj)) { //TODO
	 	//return as_error_update(err, AEROSPIKE_ERR, "NOT YET IMPLEMENTED2\n");
	 	uint8_t * b = (uint8_t *) PyBytes_AsString(py_obj);
	 	uint32_t b_len  = (uint32_t)  PyBytes_Size(py_obj);
		{
			as_exp_entry tmp_entry = AS_EXP_BYTES(b, b_len);
			*new_entry = tmp_entry;
		}
	 	//*val = (as_val *) as_bytes_new_wrap(b, b_len, false);
	} else if (!strcmp(py_obj->ob_type->tp_name, "aerospike.Geospatial")) {
		PyObject *py_parameter = PyString_FromString("geo_data");
		PyObject* py_data = PyObject_GenericGetAttr(py_obj, py_parameter);
		Py_DECREF(py_parameter);
		char *geo_value = PyString_AsString(AerospikeGeospatial_DoDumps(py_data, err));
		if (aerospike_has_geo(self->as)) {
			{
				as_exp_entry tmp_entry = AS_EXP_GEO(geo_value);
				*new_entry = tmp_entry;
			}
		} else { // TODO
			return as_error_update(err, AEROSPIKE_ERR, "NOT YET IMPLEMENTED3\n");
			// as_bytes *bytes;
			// GET_BYTES_POOL(bytes, static_pool, err);
			// if (err->code == AEROSPIKE_OK) {
			// 	if (serialize_based_on_serializer_policy(self, serializer_type,
			// 		&bytes, py_data, err) != AEROSPIKE_OK) {
			// 		return err->code;
			// 	}
			// 	*val = (as_val *) bytes;
			// }
		}
	} else if (PyByteArray_Check(py_obj)) { // TODO
		//return as_error_update(err, AEROSPIKE_ERR, "NOT YET IMPLEMENTED4\n");
		as_bytes *bytes;
		GET_BYTES_POOL(bytes, static_pool, err);
		if (err->code == AEROSPIKE_OK) {
			if (serialize_based_on_serializer_policy(self, serializer_type,
					&bytes, py_obj, err) != AEROSPIKE_OK) {
				return err->code;
			}
			{
				as_exp_entry tmp_entry = AS_EXP_VAL((as_val *) bytes);
				*new_entry = tmp_entry;
			}
		}
	} else if (PyList_Check(py_obj)) {
		as_list * list = NULL;
		pyobject_to_list(self, err, py_obj, &list, static_pool, serializer_type);
		if (err->code == AEROSPIKE_OK) {
			{
				as_exp_entry tmp_entry = AS_EXP_VAL(list);
				*new_entry = tmp_entry;
			}
		}
	} else if (PyDict_Check(py_obj)) {
		as_map * map = NULL;
		pyobject_to_map(self, err, py_obj, &map, static_pool, serializer_type);
		if (err->code == AEROSPIKE_OK) {
			{
				as_exp_entry tmp_entry = AS_EXP_VAL(map);
				*new_entry = tmp_entry;
			}
		}
	} else if (Py_None == py_obj) {
		{
			as_exp_entry tmp_entry = AS_EXP_NIL();
			*new_entry = tmp_entry;
		}
	} else if (!strcmp(py_obj->ob_type->tp_name, "aerospike.null")) {
		{
			as_exp_entry tmp_entry = AS_EXP_NIL();
			*new_entry = tmp_entry;
		}
	} else if (AS_Matches_Classname(py_obj, AS_CDT_WILDCARD_NAME)) {
		{
			as_exp_entry tmp_entry = AS_EXP_VAL((as_val *) as_val_reserve(&as_cmp_wildcard));
			*new_entry = tmp_entry;
		}
	} else if (AS_Matches_Classname(py_obj, AS_CDT_INFINITE_NAME)) {
		{
			as_exp_entry tmp_entry = AS_EXP_VAL((as_val *) as_val_reserve(&as_cmp_inf));
			*new_entry = tmp_entry;
		}
	} else {
		if (PyFloat_Check(py_obj)) {
			double d = PyFloat_AsDouble(py_obj);
			{
				as_exp_entry tmp_entry = AS_EXP_FLOAT(d);
				*new_entry = tmp_entry;
			}
		} else { //TODO
			//return as_error_update(err, AEROSPIKE_ERR, "NOT YET IMPLEMENTED5\n");
			as_bytes *bytes;
			GET_BYTES_POOL(bytes, static_pool, err);
			if (err->code == AEROSPIKE_OK) {
				if (serialize_based_on_serializer_policy(self, serializer_type,
					&bytes, py_obj, err) != AEROSPIKE_OK) {
					return err->code;
				}

				{
					as_exp_entry tmp_entry = AS_EXP_VAL((as_val *) bytes);
					*new_entry = tmp_entry;
				}
			}
		}
	} //note

	return err->code;
}


as_status add_pred_macros(AerospikeClient * self, as_static_pool * static_pool, int serializer_type, as_vector * unicodeStrVector, as_exp_entry ** expressions, pred_op * pred, as_error * err) {
	// PyObject * tuple_py_val = NULL;
	// PyObject * utf8_temp = NULL;
	// char* bin_name = NULL;
	int64_t lval1 = 0;
	int64_t lval2 = 0;
	//int64_t lval3 = 0;
	//int64_t lval4 = 0;
	char * bin_name = NULL;


	//pred->ctx = ctx_in_use ? &ctx : NULL;
	
	switch (pred->op) {
		case BIN:
			printf("in bin case######\n");
			{
				if (get_bin(err, pred->pydict, unicodeStrVector, &bin_name) != AEROSPIKE_OK) {
					return err->code;
				}

				as_exp_entry new_entries[] = {
					{.op=_AS_EXP_CODE_BIN, .count=3},
					AS_EXP_INT(pred->result_type),
					_AS_EXP_VAL_RAWSTR(bin_name)
				};
				append_array(3);
			}
			break;
		case VAL:;
			{
				as_exp_entry tmp_expr;
				if (get_exp_val_from_pyval(self, static_pool, serializer_type, &tmp_expr, PyDict_GetItemString(pred->pydict, AS_PY_VAL_KEY), err) != AEROSPIKE_OK) {
					return err->code;
				}

				{
					as_exp_entry new_entries[] = {tmp_expr};
					append_array(sizeof(new_entries) / sizeof(as_exp_entry));
				}
			}
			break;
		case EQ:;
			{
				as_exp_entry new_entries[] = {AS_EXP_CMP_EQ({},{})};
				append_array(1);
			}
			break;
		case NE:;
			{
				as_exp_entry new_entries[] = {AS_EXP_CMP_NE({},{})};
				append_array(1);
			}
			break;
		case GT:;
			{
				as_exp_entry new_entries[] = {AS_EXP_CMP_GT({},{})};
				append_array(1);
			}
			break;
		case GE:;
			{
				as_exp_entry new_entries[] = {AS_EXP_CMP_GE({},{})};
				append_array(1);
			}
			break;
		case LT:;
			{
				as_exp_entry new_entries[] = {AS_EXP_CMP_LT({},{})};
				append_array(1);
			}
			break;
		case LE:;
			{
				as_exp_entry new_entries[] = {AS_EXP_CMP_LE({},{})};
				append_array(1);
			}
			break;
		case AND:;
			{
				as_exp_entry new_entries[] = {AS_EXP_AND({})};
				append_array(1);
			}
			break;
		case OR:;
			{
				as_exp_entry new_entries[] = {AS_EXP_OR({})};
				append_array(1);
			}
			break;
		case NOT:;
			{
				as_exp_entry new_entries[] = {AS_EXP_NOT({})};
				append_array(1);
			}
			break;
		case END_VA_ARGS:;
			{
				as_exp_entry new_entries[] = {{.op=_AS_EXP_CODE_END_OF_VA_ARGS}};
				append_array(1);
			}
			break;
		case META_DIGEST_MOD:;
			{
				if (get_int64_t(err, AS_PY_VAL_KEY, pred->pydict, &lval1) != AEROSPIKE_OK) {
					return err->code;
				}

				as_exp_entry new_entries[] = {AS_EXP_DIGEST_MODULO(lval1)};
				append_array(2);
			}
			break;
		case META_DEVICE_SIZE:;
			{
				as_exp_entry new_entries[] = {AS_EXP_DEVICE_SIZE()};
				append_array(1);
			}
			break;
		case META_LAST_UPDATE_TIME:;
			{
				as_exp_entry new_entries[] = {AS_EXP_LAST_UPDATE()};
				append_array(1);
			}
			break;
		case META_VOID_TIME:;
			{
				as_exp_entry new_entries[] = {AS_EXP_VOID_TIME()};
				append_array(1);
			}
			break;
		case META_TTL:;
			{
				as_exp_entry new_entries[] = {AS_EXP_TTL()};
				append_array(1);
			}
			break;
		case META_SET_NAME:;
			{
				as_exp_entry new_entries[] = {AS_EXP_SET_NAME()};
				append_array(sizeof(new_entries) / sizeof(as_exp_entry));
			}
			break;
		case META_KEY_EXISTS:;
			{
				as_exp_entry new_entries[] = {AS_EXP_KEY_EXIST()};
				append_array(sizeof(new_entries) / sizeof(as_exp_entry)); // TODO add size to append_array
			}
			break;
		case REC_KEY:;
			{
				as_exp_entry new_entries[] = {
					{.op=_AS_EXP_CODE_KEY, .count=2},
					AS_EXP_INT(pred->result_type)
				};
				append_array(sizeof(new_entries) / sizeof(as_exp_entry));
			}
			break;
		case BIN_TYPE:;
			if (get_bin(err, pred->pydict, unicodeStrVector, &bin_name) != AEROSPIKE_OK) {
				return err->code;
			}


			{
				as_exp_entry new_entries[] = {AS_EXP_BIN_TYPE(bin_name)};
				append_array(2);
			}
			break;
		case OP_LIST_EXP_GET_BY_INDEX:;
			printf("in get_by_index\n");
			{
				if (get_int64_t(err, AS_PY_BIN_TYPE_KEY, pred->pydict, &lval1) != AEROSPIKE_OK) {
					return err->code;
				}

				if (get_int64_t(err, AS_PY_LIST_RETURN_KEY, pred->pydict, &lval2) != AEROSPIKE_OK) {
					return err->code;
				}

				as_exp_entry new_entries[] = {
					_AS_EXP_CDT_LIST_READ(lval1, lval2, false),
					_AS_EXP_LIST_START(pred->ctx, AS_CDT_OP_LIST_GET_BY_INDEX, 2),
					AS_EXP_INT(lval2)
				};

				printf("size is: %lud\n", sizeof(new_entries) / sizeof(as_exp_entry));
				append_array(sizeof(new_entries) / sizeof(as_exp_entry));
			}
			break;
		case OP_LIST_EXP_SIZE:;
			printf("in list_size\n");
			{
				as_exp_entry new_entries[] = {
					_AS_EXP_CDT_LIST_READ(AS_EXP_TYPE_AUTO, AS_LIST_RETURN_COUNT, false),
					_AS_EXP_LIST_START(pred->ctx, AS_CDT_OP_LIST_SIZE, 0),
				};

				printf("size is: %lud\n", sizeof(new_entries) / sizeof(as_exp_entry));
				append_array(sizeof(new_entries) / sizeof(as_exp_entry));
			}
			break;
		case OP_LIST_EXP_GET_BY_VALUE:;
			printf("in get_by_val\n");
			{	
				if (get_int64_t(err, AS_PY_LIST_RETURN_KEY, pred->pydict, &lval1) != AEROSPIKE_OK) {
					return err->code;
				}

				as_exp_entry new_entries[] = {
					_AS_EXP_CDT_LIST_READ(AS_EXP_TYPE_AUTO, lval1, true),
					_AS_EXP_LIST_START(pred->ctx, AS_CDT_OP_LIST_GET_ALL_BY_VALUE, 2),
					AS_EXP_INT(lval1),
				};

				printf("size is: %lud\n", sizeof(new_entries) / sizeof(as_exp_entry));
				append_array(sizeof(new_entries) / sizeof(as_exp_entry));
			}
			break;
		case OP_LIST_EXP_GET_BY_VALUE_RANGE:;
			printf("in get_by_val_range\n");
			{
				if (get_int64_t(err, AS_PY_LIST_RETURN_KEY, pred->pydict, &lval1) != AEROSPIKE_OK) {
					return err->code;
				}

				as_exp_entry new_entries[] = {
					_AS_EXP_CDT_LIST_READ(AS_EXP_TYPE_AUTO, lval1, true),
					_AS_EXP_LIST_START(pred->ctx, AS_CDT_OP_LIST_GET_BY_VALUE_INTERVAL, 3),
					AS_EXP_INT(lval1)
				};

				printf("size is: %lud\n", sizeof(new_entries) / sizeof(as_exp_entry));
				append_array(sizeof(new_entries) / sizeof(as_exp_entry));
			}
			break;
		case OP_LIST_EXP_GET_BY_VALUE_LIST:;
			printf("in get_by_val_list\n");
			{
				if (get_int64_t(err, AS_PY_LIST_RETURN_KEY, pred->pydict, &lval1) != AEROSPIKE_OK) {
					return err->code;
				}

				as_exp_entry new_entries[] = {
					_AS_EXP_CDT_LIST_READ(AS_EXP_TYPE_AUTO, lval1, true),
					_AS_EXP_LIST_START(pred->ctx, AS_CDT_OP_LIST_GET_BY_VALUE_LIST, 2),
					AS_EXP_INT(lval1)
				};
				printf("size is: %lud\n", sizeof(new_entries) / sizeof(as_exp_entry));
				append_array(sizeof(new_entries) / sizeof(as_exp_entry));
			}
			break;
		case OP_LIST_EXP_GET_BY_VALUE_RANK_RANGE_REL_TO_END:;
			printf("in get_by_val_rank_range_rel_to_end\n");
			{
				if (get_int64_t(err, AS_PY_LIST_RETURN_KEY, pred->pydict, &lval1) != AEROSPIKE_OK) {
					return err->code;
				}

				as_exp_entry new_entries[] = {
					_AS_EXP_CDT_LIST_READ(AS_EXP_TYPE_AUTO, lval1, true),
					_AS_EXP_LIST_START(pred->ctx, AS_CDT_OP_LIST_GET_BY_VALUE_REL_RANK_RANGE, 3),
					AS_EXP_INT(lval1)
				};
				printf("size is: %lud\n", sizeof(new_entries) / sizeof(as_exp_entry));
				append_array(sizeof(new_entries) / sizeof(as_exp_entry));
			}
			break;
		case OP_LIST_EXP_GET_BY_VALUE_RANK_RANGE_REL:;
			printf("in get_by_val_rank_range_rel\n");
			{
				if (get_int64_t(err, AS_PY_LIST_RETURN_KEY, pred->pydict, &lval1) != AEROSPIKE_OK) {
					return err->code;
				}

				as_exp_entry new_entries[] = {
					_AS_EXP_CDT_LIST_READ(AS_EXP_TYPE_AUTO, lval1, true),
					_AS_EXP_LIST_START(pred->ctx, AS_CDT_OP_LIST_GET_BY_VALUE_REL_RANK_RANGE, 4),
					AS_EXP_INT(lval1)
				};
				printf("size is: %lud\n", sizeof(new_entries) / sizeof(as_exp_entry));
				append_array(sizeof(new_entries) / sizeof(as_exp_entry));
			}
			break;
		case OP_LIST_EXP_GET_BY_INDEX_RANGE_TO_END:;
			printf("in get_by_index_range_to_end\n");
			{
				if (get_int64_t(err, AS_PY_LIST_RETURN_KEY, pred->pydict, &lval1) != AEROSPIKE_OK) {
					return err->code;
				}

				as_exp_entry new_entries[] = {
					_AS_EXP_CDT_LIST_READ(AS_EXP_TYPE_AUTO, lval1, true),
					_AS_EXP_LIST_START(pred->ctx, AS_CDT_OP_LIST_GET_BY_INDEX_RANGE, 2),
					AS_EXP_INT(lval1),
				};
				printf("size is: %lud\n", sizeof(new_entries) / sizeof(as_exp_entry));
				append_array(sizeof(new_entries) / sizeof(as_exp_entry));
			}
			break;
		case OP_LIST_EXP_GET_BY_INDEX_RANGE:;
			printf("in get_by_index_range\n");
			{
				if (get_int64_t(err, AS_PY_LIST_RETURN_KEY, pred->pydict, &lval1) != AEROSPIKE_OK) {
					return err->code;
				}

				as_exp_entry new_entries[] = {
					_AS_EXP_CDT_LIST_READ(AS_EXP_TYPE_AUTO, lval1, true),
					_AS_EXP_LIST_START(pred->ctx, AS_CDT_OP_LIST_GET_BY_INDEX_RANGE, 3),
					AS_EXP_INT(lval1),
				};
				printf("size is: %lud\n", sizeof(new_entries) / sizeof(as_exp_entry));
				append_array(sizeof(new_entries) / sizeof(as_exp_entry));
			}
			break;
		case OP_LIST_EXP_GET_BY_RANK:;
			printf("in get_by_rank\n");
			{
				if (get_int64_t(err, AS_PY_LIST_RETURN_KEY, pred->pydict, &lval1) != AEROSPIKE_OK) {
					return err->code;
				}

				if (get_int64_t(err, AS_PY_BIN_TYPE_KEY, pred->pydict, &lval2) != AEROSPIKE_OK) {
					return err->code;
				}

				as_exp_entry new_entries[] = {
					_AS_EXP_CDT_LIST_READ(lval2, lval1, true),
					_AS_EXP_LIST_START(pred->ctx, AS_CDT_OP_LIST_GET_BY_RANK, 2),
					AS_EXP_INT(lval1),
				};
				printf("size is: %lud\n", sizeof(new_entries) / sizeof(as_exp_entry));
				append_array(sizeof(new_entries) / sizeof(as_exp_entry));
			}
			break;
		case OP_LIST_EXP_GET_BY_RANK_RANGE_TO_END:;
			printf("in get_by_rank_range_to_end\n");
			{
				if (get_int64_t(err, AS_PY_LIST_RETURN_KEY, pred->pydict, &lval1) != AEROSPIKE_OK) {
					return err->code;
				}

				as_exp_entry new_entries[] = {
					_AS_EXP_CDT_LIST_READ(AS_EXP_TYPE_AUTO, lval1, true),
					_AS_EXP_LIST_START(pred->ctx, AS_CDT_OP_LIST_GET_BY_RANK_RANGE, 2),
					AS_EXP_INT(lval1),
				};
				printf("size is: %lud\n", sizeof(new_entries) / sizeof(as_exp_entry));
				append_array(sizeof(new_entries) / sizeof(as_exp_entry));
			}
			break;
		case OP_LIST_EXP_GET_BY_RANK_RANGE:;
			printf("in get_by_rank_range\n");
			{
				if (get_int64_t(err, AS_PY_LIST_RETURN_KEY, pred->pydict, &lval1) != AEROSPIKE_OK) {
					return err->code;
				}

				as_exp_entry new_entries[] = {
					_AS_EXP_CDT_LIST_READ(AS_EXP_TYPE_AUTO, lval1, true),
					_AS_EXP_LIST_START(pred->ctx, AS_CDT_OP_LIST_GET_BY_RANK_RANGE, 3),
					AS_EXP_INT(lval1),
				};
				printf("size is: %lud\n", sizeof(new_entries) / sizeof(as_exp_entry));
				append_array(sizeof(new_entries) / sizeof(as_exp_entry));
			}
			break;
		case OP_LIST_EXP_APPEND:;
			printf("in OP_LIST_EXP_APPEND\n");
			{
				as_list_policy list_policy; //this might have scope issues
				as_list_policy * list_policy_p = NULL;
				bool policy_in_use = false;
				if (get_list_policy(err, pred->pydict, &list_policy, &policy_in_use) != AEROSPIKE_OK) {
					return err->code;
				}

				list_policy_p = policy_in_use ? &list_policy : NULL;

				as_exp_entry new_entries[] = {
					_AS_EXP_LIST_MOD(pred->ctx, list_policy_p, AS_CDT_OP_LIST_APPEND_ITEMS, 1, 2),
				};

				printf("size is: %lud\n", sizeof(new_entries) / sizeof(as_exp_entry));
				append_array(sizeof(new_entries) / sizeof(as_exp_entry));
			}
			break;
		case OP_LIST_EXP_APPEND_ITEMS:;
			printf("in OP_LIST_EXP_APPEND_ITEMS\n");
			{
				as_list_policy list_policy; //this might have scope issues
				as_list_policy * list_policy_p = NULL;
				bool policy_in_use = false;
				if (get_list_policy(err, pred->pydict, &list_policy, &policy_in_use) != AEROSPIKE_OK) {
					return err->code;
				}

				list_policy_p = policy_in_use ? &list_policy : NULL;

				as_exp_entry new_entries[] = {
					_AS_EXP_LIST_MOD(pred->ctx, list_policy_p, AS_CDT_OP_LIST_APPEND_ITEMS, 1, 2)
				};

				printf("size is: %lud\n", sizeof(new_entries) / sizeof(as_exp_entry));
				append_array(sizeof(new_entries) / sizeof(as_exp_entry));
			}
			break;
		case OP_LIST_EXP_INSERT:;
			printf("in OP_LIST_EXP_INSERT\n");
			{
				as_list_policy list_policy; //this might have scope issues
				as_list_policy * list_policy_p = NULL;
				bool policy_in_use = false;
				if (get_list_policy(err, pred->pydict, &list_policy, &policy_in_use) != AEROSPIKE_OK) {
					return err->code;
				}

				list_policy_p = policy_in_use ? &list_policy : NULL;

				as_exp_entry new_entries[] = {
					_AS_EXP_LIST_MOD(pred->ctx, list_policy_p, AS_CDT_OP_LIST_INSERT, 2, 1)
				};

				printf("size is: %lud\n", sizeof(new_entries) / sizeof(as_exp_entry));
				append_array(sizeof(new_entries) / sizeof(as_exp_entry));
			}
			break;
		case OP_LIST_EXP_INSERT_ITEMS:;
			printf("in OP_LIST_EXP_INSERT_ITEMS\n");
			{
				as_list_policy list_policy; //this might have scope issues
				as_list_policy * list_policy_p = NULL;
				bool policy_in_use = false;
				if (get_list_policy(err, pred->pydict, &list_policy, &policy_in_use) != AEROSPIKE_OK) {
					return err->code;
				}

				list_policy_p = policy_in_use ? &list_policy : NULL;

				as_exp_entry new_entries[] = {
					_AS_EXP_LIST_MOD(pred->ctx, list_policy_p, AS_CDT_OP_LIST_INSERT_ITEMS, 2, 1)
				};

				printf("size is: %lud\n", sizeof(new_entries) / sizeof(as_exp_entry));
				append_array(sizeof(new_entries) / sizeof(as_exp_entry));
			}
			break;
		case OP_LIST_EXP_INCREMENT:;
			printf("in OP_LIST_EXP_LIST_INCREMENT\n");
			{
				as_list_policy list_policy; //this might have scope issues
				as_list_policy * list_policy_p = NULL;
				bool policy_in_use = false;
				if (get_list_policy(err, pred->pydict, &list_policy, &policy_in_use) != AEROSPIKE_OK) {
					return err->code;
				}

				list_policy_p = policy_in_use ? &list_policy : NULL;

				as_exp_entry new_entries[] = {
					_AS_EXP_LIST_MOD(pred->ctx, list_policy_p, AS_CDT_OP_LIST_INCREMENT, 2, 2)
				};

				printf("size is: %lud\n", sizeof(new_entries) / sizeof(as_exp_entry));
				append_array(sizeof(new_entries) / sizeof(as_exp_entry));
			}
			break;
		case OP_LIST_EXP_CLEAR:;
			printf("in OP_LIST_EXP_LIST_CLEAR\n");
			{
				as_list_policy list_policy; //this might have scope issues
				as_list_policy * list_policy_p = NULL;
				bool policy_in_use = false;
				if (get_list_policy(err, pred->pydict, &list_policy, &policy_in_use) != AEROSPIKE_OK) {
					return err->code;
				}

				list_policy_p = policy_in_use ? &list_policy : NULL;

				as_exp_entry new_entries[] = {AS_EXP_LIST_CLEAR(pred->ctx, {})};

				printf("size is: %lud\n", sizeof(new_entries) / sizeof(as_exp_entry) - 1);
				append_array(sizeof(new_entries) / sizeof(as_exp_entry) -1); // -1 for bin
			}
			break;
		case OP_LIST_EXP_SORT:;
			printf("in OP_LIST_EXP_LIST_SORT\n");
			{
				if (get_int64_t(err, LIST_ORDER_KEY, pred->pydict, &lval1) != AEROSPIKE_OK) {
					return err->code;
				}

				as_exp_entry new_entries[] = {AS_EXP_LIST_SORT(pred->ctx, lval1, {})};

				printf("size is: %lud\n", sizeof(new_entries) / sizeof(as_exp_entry) - 1);
				append_array(sizeof(new_entries) / sizeof(as_exp_entry) -1); // -1 for bin
			}
			break;

	}		

	return AEROSPIKE_OK;
}

as_status convert_exp_list(AerospikeClient * self, as_static_pool * static_pool, PyObject* py_exp_list, as_exp** exp_list, as_error* err) {
	bottom = 0;
	Py_ssize_t size = PyList_Size(py_exp_list);
	if (size <= 0) {
		return AEROSPIKE_OK;
	}
	printf("OP_LIST_EXP_GET_BY_INDEX is: %d\n", OP_LIST_EXP_GET_BY_INDEX);
	long op = 0;
	long result_type = 0;
	long num_children = 0;
	int child_count = 1;
	uint8_t va_flag = 0;
	as_cdt_ctx ctx;
	bool ctx_in_use = false;
	PyObject * py_pred_tuple = NULL;
	as_vector pred_queue;
	pred_op pred;
	as_exp_entry * c_pred_entries = NULL;

	as_vector * unicodeStrVector = as_vector_create(sizeof(char *), 128);

	as_vector_inita(&pred_queue, sizeof(pred_op), size);

	c_pred_entries = (as_exp_entry*) calloc((size * MAX_ELEMENTS), sizeof(as_exp_entry)); // iter and count elem?
	if (c_pred_entries == NULL) {
		printf("in no ctx\n");
		as_error_update(err, AEROSPIKE_ERR, "could not calloc mem for c_pred_entries");
	}

    for ( int i = 0; i < size; ++i ) {
		pred.ctx = NULL;
		pred.pydict = NULL;
		ctx_in_use = false;

		if (child_count == 0 && va_flag >= 1) { //this handles AND, OR
			pred.op=END_VA_ARGS;
			as_vector_append(&pred_queue, (void*) &pred);
			--va_flag;
			continue;
		}

        py_pred_tuple = PyList_GetItem(py_exp_list, (Py_ssize_t)i);
		pred.pytuple = py_pred_tuple;
		//Py_INCREF(pred.pytuple);
        op = PyInt_AsLong(PyTuple_GetItem(py_pred_tuple, 0));
		printf("processed pred op: %ld\n", op);

        result_type = PyInt_AsLong(PyTuple_GetItem(py_pred_tuple, 1));
		if (result_type == -1 && PyErr_Occurred()) {
			PyErr_Clear();
		}


        pred.pydict = PyTuple_GetItem(py_pred_tuple, 2);
		// if (pred.pydict != NULL && pred.pydict != Py_None) {
		// 	Py_INCREF(pred.pydict);
		// }

		if (get_cdt_ctx(self, err, &ctx, pred.pydict, &ctx_in_use, static_pool, SERIALIZER_PYTHON) != AEROSPIKE_OK) {
			return err->code;
			//char * tmp_warn = err->message;
			//return as_error_update(err, AEROSPIKE_ERR_PARAM, "Failed to convert cdt_ctx: %s", tmp_warn);
		}
		pred.ctx = ctx_in_use ? &ctx : NULL;

		if (op == AND || op == OR) {
			++va_flag;
			++size;
		}

        num_children = PyInt_AsLong(PyTuple_GetItem(py_pred_tuple, 3));
		pred.op = op;
		pred.result_type = result_type;
		pred.num_children = num_children;
		as_vector_append(&pred_queue, (void*) &pred);
		if (va_flag) {
			child_count += num_children - 1;
		}
    }// note

	for ( int i = 0; i < size; ++i ) {
		pred_op * pred = (pred_op *) as_vector_get(&pred_queue, (uint32_t)i);
		if (add_pred_macros(self, static_pool, SERIALIZER_PYTHON, unicodeStrVector, &c_pred_entries, pred, err) != AEROSPIKE_OK) {
			return err->code;
		}
	}

	*exp_list = as_exp_build(c_pred_entries, bottom);


CLEANUP:

	for (int i = 0; i < size; ++i) {
		printf("here\n");
		pred_op * pred = (pred_op *) as_vector_get(&pred_queue, (uint32_t)i);
		// Py_XDECREF(pred->pydict);
		// Py_XDECREF(pred->pytuple);
		// if(pred->ctx != NULL) {
		// 	as_cdt_ctx_destroy(pred->ctx);
		// }
		pred->pydict = NULL;
		pred->pytuple = NULL;
		pred->ctx = NULL;
	}

	//POOL_DESTROY(&static_pool);
	as_vector_clear(&pred_queue);
	free(c_pred_entries);

	// Py_DECREF(fixed); //this needs more decrefs for each fixed
	// fixed = NULL;
	// Py_DECREF(py_pred_tuple);
	// py_pred_tuple = NULL;
	//TODO free ctx

	return AEROSPIKE_OK; //TODO change this to err->code and redirect other error returns to CLEANUP
}