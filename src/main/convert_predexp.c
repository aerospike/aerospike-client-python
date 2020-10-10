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

#define get_from_py_tuple_at(var, pos) {\
	var = PyTuple_GetItem(pred->fixed, pos);\
	if ( ! tuple_py_val && PyErr_ExceptionMatches(PyExc_IndexError)) {\
		as_error_update(err, AEROSPIKE_ERR, "Tuple index out of bounds.")\
		return err->code;\
	}\
}

as_status get_string_from_py_tuple_at(char* var, int pos, PyObject * py_tuple, as_error * err) {
	PyObject * tuple_py_val = PyTuple_GetItem(py_tuple, pos);
	if (tuple_py_val == NULL && PyErr_ExceptionMatches(PyExc_IndexError)) {
		as_error_update(err, AEROSPIKE_ERR, "Tuple index out of bounds.")
		return err->code;
	}
	PyObject * utf8_temp = PyUnicode_AsUTF8String(tuple_py_val); //TODO decref ref
	if (utf8_temp == NULL) {
		as_error_update(err, AEROSPIKE_ERR, "Failed encoding as UTF8.")
		return err->code;
	}
	var = PyBytes_AsString(utf8_temp);
	if (var == NULL && PyErr_ExceptionMatches(PyExc_TypeError)) {
		as_error_update(err, AEROSPIKE_ERR, "utf8_temp is not a bytes object.")
		return err->code;
	}
	return AEROSPIKE_OK;
}

as_status get_string_from_py_object(char* var, PyObject * tuple_py_val, as_error * err) {
	PyObject * utf8_temp = PyUnicode_AsUTF8String(tuple_py_val); //TODO decref ref
	if (utf8_temp == NULL) { // maybe do the unicode encode before paasing to pred
		as_error_update(err, AEROSPIKE_ERR, "Failed encoding as UTF8.")
		return err->code;
	}
	var = PyBytes_AsString(utf8_temp);
	if (var == NULL && PyErr_ExceptionMatches(PyExc_TypeError)) {
		as_error_update(err, AEROSPIKE_ERR, "utf8_temp is not a bytes object.")
		return err->code;
	}
	return AEROSPIKE_OK;
}

as_status get_long_from_py_tuple_at(long * var, int pos, PyObject * py_tuple, as_error * err) {
	PyObject * tuple_py_val = PyTuple_GetItem(py_tuple, pos);
	if (tuple_py_val == NULL && PyErr_ExceptionMatches(PyExc_IndexError)) {
		as_error_update(err, AEROSPIKE_ERR, "Tuple index out of bounds.")
		return err->code;
	}
	if (PyInt_Check(tuple_py_val)) {
		*var = (int64_t) PyInt_AsLong(tuple_py_val);
		if (*var == -1 && PyErr_Occurred()) {
			if (PyErr_ExceptionMatches(PyExc_OverflowError)) {
				return as_error_update(err, AEROSPIKE_ERR_PARAM, "integer value exceeds sys.maxsize");
			}
		}
	}
	else if (PyLong_Check(tuple_py_val)) {
		*var = (int64_t) PyLong_AsLong(tuple_py_val);
		if (*var == -1 && PyErr_Occurred()) {
			if (PyErr_ExceptionMatches(PyExc_OverflowError)) {
				return as_error_update(err, AEROSPIKE_ERR_PARAM, "integer value exceeds sys.maxsize");
			}
		}
	}
	return AEROSPIKE_OK;
}

as_status get_int64_t_from_pydict(int64_t * var, PyObject * op_dict, char * key, as_error * err) {
	PyObject* py_item = PyDict_GetItemString(op_dict, key);
	if (py_item == NULL) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM, "Failed to convert %s", key);
	}

	if (PyInt_Check(py_item)) {
		*var = (int64_t) PyInt_AsLong(py_item);
		if (*var == -1 && PyErr_Occurred()) {
			if (PyErr_ExceptionMatches(PyExc_OverflowError)) {
				return as_error_update(err, AEROSPIKE_ERR_PARAM, "integer value exceeds sys.maxsize");
			}
		}
	}
	else if (PyLong_Check(py_item)) {
		*var = (int64_t) PyLong_AsLong(py_item);
		if (*var == -1 && PyErr_Occurred()) {
			if (PyErr_ExceptionMatches(PyExc_OverflowError)) {
				return as_error_update(err, AEROSPIKE_ERR_PARAM, "integer value exceeds sys.maxsize");
			}
		}
	}

	return AEROSPIKE_OK;
}

// static TODO
// as_status get_exp_bin_from_pyval

static
as_status get_exp_val_from_pyval(AerospikeClient * self, as_static_pool * static_pool, int serializer_type, as_exp_entry * new_entry, PyObject * py_obj, as_val ** tmp_val, as_error * err) {
	//as_exp_entry new_entries[] = {AS_EXP_VAL_INT(pred->fixed_num)};
	//TODO why not pass in the array and do the copy in here
	as_error_reset(err);

	if (pyobject_to_val(self, err, py_obj, tmp_val, static_pool, serializer_type) != AEROSPIKE_OK) {
		return err->code;
	}

	if (!py_obj) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "value is null");

	} else if (PyBool_Check(py_obj) || PyBytes_Check(py_obj) || PyByteArray_Check(py_obj)) { //TODO
		as_bytes * bytes = as_bytes_fromval(*tmp_val);
		as_exp_entry tmp_entry = AS_EXP_BYTES( bytes->value, bytes->size);
		*new_entry = tmp_entry;

	} else if (PyInt_Check(py_obj) || PyLong_Check(py_obj)) {
		as_integer * tmp_l = as_integer_fromval(*tmp_val);
		int64_t i = (int64_t) as_integer_get(tmp_l);
		as_exp_entry tmp_entry = AS_EXP_INT(i);
		*new_entry = tmp_entry;

	} else if (PyUnicode_Check(py_obj) || PyString_Check(py_obj)) {
		as_string * s = as_string_fromval(*tmp_val);
		char * str = as_string_get(s);
		as_exp_entry tmp_entry = AS_EXP_STR(str);
		*new_entry = tmp_entry;

	} else if (!strcmp(py_obj->ob_type->tp_name, "aerospike.Geospatial")) {
		as_geojson * gp = as_geojson_fromval(*tmp_val);
		char * locstr = as_geojson_get(gp);
		if (aerospike_has_geo(self->as)) {
			as_exp_entry tmp_entry = AS_EXP_GEO(locstr);
			*new_entry = tmp_entry;
		} else {
			as_bytes * bytes = as_bytes_fromval(*tmp_val);
			as_exp_entry tmp_entry = AS_EXP_BYTES( bytes->value, bytes->size);
			*new_entry = tmp_entry;
		}

	} else if (PyList_Check(py_obj)) {
		as_list * l = as_list_fromval(*tmp_val);
		if (l) {
			as_exp_entry tmp_entry = AS_EXP_VAL(l);
			*new_entry = tmp_entry;
		}

	} else if (PyDict_Check(py_obj)) {
		as_map * m = as_map_fromval(*tmp_val);
		if (m) {
			as_exp_entry tmp_entry = AS_EXP_VAL(m);
			*new_entry = tmp_entry;
		}

	} else if (Py_None == py_obj || !strcmp(py_obj->ob_type->tp_name, "aerospike.null")) {
		as_exp_entry tmp_entry = AS_EXP_NIL();
		*new_entry = tmp_entry;

	} else if (AS_Matches_Classname(py_obj, AS_CDT_WILDCARD_NAME)) {
		as_exp_entry tmp_entry = AS_EXP_VAL(*tmp_val);
		*new_entry = tmp_entry;

	} else if (AS_Matches_Classname(py_obj, AS_CDT_INFINITE_NAME)) {
		{
			as_exp_entry tmp_entry = AS_EXP_VAL(*tmp_val);
			*new_entry = tmp_entry;
		}

	} else {
		if (PyFloat_Check(py_obj)) {
			as_double * d = as_double_fromval(*tmp_val);
			{
				as_exp_entry tmp_entry = AS_EXP_FLOAT(as_double_get(d));
				*new_entry = tmp_entry;
			}
		} else {
			as_bytes * bytes = as_bytes_fromval(*tmp_val);
			as_exp_entry tmp_entry = AS_EXP_BYTES( bytes->value, bytes->size);
			*new_entry = tmp_entry;
		}
	}

	return err->code;
}

as_status add_pred_macros(AerospikeClient * self, as_static_pool * static_pool, int serializer_type, as_vector * unicodeStrVector, as_exp_entry ** expressions, pred_op * pred, as_error * err) {
	// PyObject * tuple_py_val = NULL;
	// PyObject * utf8_temp = NULL;
	// char* bin_name = NULL;
	int64_t lval1 = 0;
	int64_t lval2 = 0;
	int64_t lval3 = 0;
	int64_t lval4 = 0;
	as_cdt_ctx ctx;
	bool ctx_in_use;
	char * bin_name = NULL;

	if (get_bin(err, pred->pydict, unicodeStrVector, &bin_name) != AEROSPIKE_OK) {
		return err->code;
	}

	if (get_cdt_ctx(self, err, &ctx, pred->pydict, &ctx_in_use, static_pool, serializer_type) != AEROSPIKE_OK) {
		char * tmp_warn = err->message;
		return as_error_update(err, AEROSPIKE_ERR_PARAM, "Failed to convert cdt_ctx: %s", tmp_warn);
	}
	pred->ctx = ctx_in_use ? &ctx : NULL;
	
	switch (pred->op) {
		case BIN:
			{
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
				as_val * tmp_val;
				if (get_exp_val_from_pyval(self, static_pool, serializer_type, &tmp_expr, PyDict_GetItemString(pred->pydict, AS_PY_VAL_KEY), &tmp_val, err) != AEROSPIKE_OK) {
					return err->code;
				}

				// as_val * tmp_val;
				// if (pyobject_to_val(self, err, PyTuple_GetItem(pred->pyfixed, 0), &tmp_val, static_pool, serializer_type) != AEROSPIKE_OK) {
				// 	return err->code;
				// }
				as_exp_entry new_entries[] = {tmp_expr};
				append_array(sizeof(new_entries) / sizeof(as_exp_entry));
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

				as_exp_entry new_entries[] = {AS_EXP_META_DIGEST_MOD(lval1)};
				append_array(2);
			}
			break;
		case META_DEVICE_SIZE:;
			{
				as_exp_entry new_entries[] = {AS_EXP_META_DEVICE_SIZE()};
				append_array(1);
			}
			break;
		case META_LAST_UPDATE_TIME:;
			{
				as_exp_entry new_entries[] = {AS_EXP_META_LAST_UPDATE()};
				append_array(1);
			}
			break;
		case META_VOID_TIME:;
			{
				as_exp_entry new_entries[] = {AS_EXP_META_VOID_TIME()};
				append_array(1);
			}
			break;
		case META_TTL:;
			{
				as_exp_entry new_entries[] = {AS_EXP_META_TTL()};
				append_array(1);
			}
			break;
		case META_SET_NAME:;
			{
				as_exp_entry new_entries[] = {AS_EXP_META_SET_NAME()};
				append_array(sizeof(new_entries) / sizeof(as_exp_entry));
			}
			break;
		case META_KEY_EXISTS:;
			{
				as_exp_entry new_entries[] = {AS_EXP_META_KEY_EXIST()};
				append_array(sizeof(new_entries) / sizeof(as_exp_entry));
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

				printf("size is: %d\n", sizeof(new_entries) / sizeof(as_exp_entry));
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

				printf("size is: %d\n", sizeof(new_entries) / sizeof(as_exp_entry));
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

				printf("size is: %d\n", sizeof(new_entries) / sizeof(as_exp_entry));
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

				printf("size is: %d\n", sizeof(new_entries) / sizeof(as_exp_entry));
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
				printf("size is: %d\n", sizeof(new_entries) / sizeof(as_exp_entry));
				append_array(sizeof(new_entries) / sizeof(as_exp_entry));
			}
			break;
		// case OP_LIST_EXP_GET_BY_VALUE_RANK_RANGE_REL_TO_END:;
		// 	printf("in get_by_val_range\n");
		// 	{
		// 		as_exp_entry tmp_expr_b;
		// 		if (get_exp_val_from_pyval(self, static_pool, serializer_type, &tmp_expr_b, PyDict_GetItemString(pred->pyval1, AS_PY_VAL_BEGIN_KEY), err) != AEROSPIKE_OK) {
		// 			return err->code;
		// 		}

		// 		as_exp_entry tmp_expr_e;
		// 		if (get_exp_val_from_pyval(self, static_pool, serializer_type, &tmp_expr_e, PyDict_GetItemString(pred->pyval1, AS_PY_VAL_END_KEY), err) != AEROSPIKE_OK) {
		// 			return err->code;
		// 		}

		// 		if (get_int64_t(err, AS_PY_LIST_RETURN_KEY, pred->pyval1, &lval1) != AEROSPIKE_OK) {
		// 			return err->code;
		// 		}

		// 		as_exp_entry new_entries[] = {AS_EXP_LIST_GET_BY_VALUE_RANGE(
		// 			pred->ctx,
		// 			lval1,
		// 			tmp_expr_b,
		// 			tmp_expr_e,
		// 			AS_EXP_BIN_LIST(py_fixed_str)
		// 			)};
		// 		printf("size is: %d\n", sizeof(new_entries) / sizeof(as_exp_entry));
		// 		append_array(sizeof(new_entries) / sizeof(as_exp_entry));
		// 	}
		// 	break;
		// case OP_LIST_EXP_APPEND:;
		// 	{
		// 		as_exp_entry new_entries[] = {AS_EXP_LIST_APPEND(pred->fixed)};
		// 		append_array(2);
		// 	}
		// 	break;
	}		

	return AEROSPIKE_OK;
}

as_status convert_exp_list(AerospikeClient * self, PyObject* py_exp_list, as_exp** exp_list, as_error* err) {
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
	PyObject * py_pred_tuple = NULL;
	as_vector pred_queue;
	pred_op pred;
	as_exp_entry * c_pred_entries = NULL;

	as_vector * unicodeStrVector = as_vector_create(sizeof(char *), 128);

	as_static_pool static_pool;
	memset(&static_pool, 0, sizeof(static_pool));

	as_vector_inita(&pred_queue, sizeof(pred_op), size);

	c_pred_entries = (as_exp_entry*) calloc((size * MAX_ELEMENTS), sizeof(as_exp_entry)); // iter and count elem?
	if (c_pred_entries == NULL) {
		as_error_update(err, AEROSPIKE_ERR, "could not calloc mem for c_pred_entries");
	}

    for ( int i = 0; i < size; ++i ) {
		pred.ctx = NULL;
		pred.pydict = NULL;

		if (child_count == 0 && va_flag >= 1) { //this handles AND, OR
			pred.op=END_VA_ARGS;
			as_vector_append(&pred_queue, (void*) &pred);
			--va_flag;
			continue;
		}

        py_pred_tuple = PyList_GetItem(py_exp_list, (Py_ssize_t)i);
		pred.pytuple = py_pred_tuple;
		Py_INCREF(py_pred_tuple);
        op = PyInt_AsLong(PyTuple_GetItem(py_pred_tuple, 0));

        result_type = PyInt_AsLong(PyTuple_GetItem(py_pred_tuple, 1));
		if (result_type == -1 && PyErr_Occurred()) {
			PyErr_Clear();
		}

        pred.pydict = PyTuple_GetItem(py_pred_tuple, 2);
		if (pred.pydict != NULL && pred.pydict != Py_None) {
			Py_INCREF(pred.pydict);
		}

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
    }

	for ( int i = 0; i < size; ++i ) {
		pred_op * pred = (pred_op *) as_vector_get(&pred_queue, (uint32_t)i);
		if (add_pred_macros(self, &static_pool, SERIALIZER_PYTHON, unicodeStrVector, &c_pred_entries, pred, err) != AEROSPIKE_OK) {
			return err->code;
		}
	}

	*exp_list = as_exp_build(c_pred_entries, bottom);


CLEANUP:

	for (int i = 0; i < size; ++i) {
		printf("here\n");
		pred_op * pred = (pred_op *) as_vector_get(&pred_queue, (uint32_t)i);
		Py_XDECREF(pred->pydict);
		Py_XDECREF(pred->pytuple);
		pred->pydict = NULL;
		pred->pytuple = NULL;
	}

	POOL_DESTROY(&static_pool);
	as_vector_clear(&pred_queue);
	free(c_pred_entries);

	// Py_DECREF(fixed); //this needs more decrefs for each fixed
	// fixed = NULL;
	// Py_DECREF(py_pred_tuple);
	// py_pred_tuple = NULL;
	//TODO free ctx

	return AEROSPIKE_OK; //TODO change this to err->code and redirect other error returns to CLEANUP
}