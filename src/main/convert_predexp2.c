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

#include "client.h"
#include "conversions.h"
#include "exceptions.h"
#include "policy.h"
#include "cdt_operation_utils.h"

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
#define MAX_ELEMENTS 8 //TODO find largest macro and adjust this val
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
	PyObject * pyfixed;
	char * fixed_str;
	int64_t fixed_num;
	PyObject * pyval1;
	as_cdt_ctx * ctx;
	// PyObject * pyval2;
	// PyObject * pyval3;
	// PyObject * pyval4;
	PyObject * pytuple;

	long num_children;
	// uint8_t fixed_active;
	//as_policy_write * policy;// todo add a member for an as_val
} pred_op;

int bottom = -1;


#define CONVERT_VAL_TO_AS_VAL()\
	if (pyobject_to_astype_write(self, err, py_value, &put_val,\
		static_pool, SERIALIZER_PYTHON) != AEROSPIKE_OK) {\
		return err->code;\
	}

#define CONVERT_KEY_TO_AS_VAL()\
	if (pyobject_to_astype_write(self, err, py_key, &put_key,\
			static_pool, SERIALIZER_PYTHON) != AEROSPIKE_OK) {\
		return err->code;\
	}

#define CONVERT_PY_CTX_TO_AS_CTX()\
	if (get_cdt_ctx(self, err, &ctx, py_val, &ctx_in_use,\
			static_pool, SERIALIZER_PYTHON) != AEROSPIKE_OK) {\
		return err->code;\
	}

#define CONVERT_RANGE_TO_AS_VAL()\
	if (pyobject_to_astype_write(self, err, py_range, &put_range,\
			static_pool, SERIALIZER_PYTHON) != AEROSPIKE_OK) {\
		return err->code;\
	}


// IMP do error checking for memcpy below

#define append_array(ar_size) {\
	for (int i = 0; i < ar_size; ++i) {\
		memcpy(&((*expressions)[++bottom]), &new_entries[i], sizeof(as_exp_entry));\
	}\
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
as_status get_exp_val_from_pyval(as_exp_entry * new_entry, PyObject * py_obj, as_error * err) {
	//as_exp_entry new_entries[] = {AS_EXP_VAL_INT(pred->fixed_num)};
	//TODO why not pass in the array and do the copy in here
	as_error_reset(err);

	if (!py_obj) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "value is null");
	} else if (PyBool_Check(py_obj)) {
		// as_bytes *bytes;
		// GET_BYTES_POOL(bytes, static_pool, err);
		// if (err->code == AEROSPIKE_OK) {
		// 	if (serialize_based_on_serializer_policy(self, serializer_type,
		// 		&bytes, py_obj, err) != AEROSPIKE_OK) {
		// 		return err->code;
		// 	}
		// 	*val = (as_val *) bytes;
		// }
	} else if (PyInt_Check(py_obj)) {
		int64_t i = (int64_t) PyInt_AsLong(py_obj);
		if (i == -1 && PyErr_Occurred()) {
			if (PyErr_ExceptionMatches(PyExc_OverflowError)) {
				return as_error_update(err, AEROSPIKE_ERR_PARAM, "integer value exceeds sys.maxsize");
			}
		}
		as_exp_entry temp_exp = AS_EXP_VAL_INT(pred->fixed_num);
	} else if (PyLong_Check(py_obj)) {
		int64_t l = (int64_t) PyLong_AsLongLong(py_obj);
		if (l == -1 && PyErr_Occurred()) {
			if (PyErr_ExceptionMatches(PyExc_OverflowError)) {
				return as_error_update(err, AEROSPIKE_ERR_PARAM, "integer value exceeds sys.maxsize");
			}
		}
		*val = (as_val *) as_integer_new(l);
	} else if (PyUnicode_Check(py_obj)) {
		PyObject * py_ustr = PyUnicode_AsUTF8String(py_obj);
		char * str = PyBytes_AsString(py_ustr);
		*val = (as_val *) as_string_new(strdup(str), true);
		Py_DECREF(py_ustr);
	} else if (PyString_Check(py_obj)) {
		char * s = PyString_AsString(py_obj);
		*val = (as_val *) as_string_new(s, false);
	 } else if (PyBytes_Check(py_obj)) {
	 	uint8_t * b = (uint8_t *) PyBytes_AsString(py_obj);
	 	uint32_t b_len  = (uint32_t)  PyBytes_Size(py_obj);
	 	*val = (as_val *) as_bytes_new_wrap(b, b_len, false);
	} else if (!strcmp(py_obj->ob_type->tp_name, "aerospike.Geospatial")) {
		PyObject *py_parameter = PyString_FromString("geo_data");
		PyObject* py_data = PyObject_GenericGetAttr(py_obj, py_parameter);
		Py_DECREF(py_parameter);
		char *geo_value = PyString_AsString(AerospikeGeospatial_DoDumps(py_data, err));
		if (aerospike_has_geo(self->as)) {
			*val = (as_val *) as_geojson_new(geo_value, false);
		} else {
			as_bytes *bytes;
			GET_BYTES_POOL(bytes, static_pool, err);
			if (err->code == AEROSPIKE_OK) {
				if (serialize_based_on_serializer_policy(self, serializer_type,
					&bytes, py_data, err) != AEROSPIKE_OK) {
					return err->code;
				}
				*val = (as_val *) bytes;
			}
		}
	} else if (PyByteArray_Check(py_obj)) {
		as_bytes *bytes;
		GET_BYTES_POOL(bytes, static_pool, err);
		if (err->code == AEROSPIKE_OK) {
			if (serialize_based_on_serializer_policy(self, serializer_type,
					&bytes, py_obj, err) != AEROSPIKE_OK) {
				return err->code;
			}
			*val = (as_val *) bytes;
		}
	} else if (PyList_Check(py_obj)) {
		as_list * list = NULL;
		pyobject_to_list(self, err, py_obj, &list, static_pool, serializer_type);
		if (err->code == AEROSPIKE_OK) {
			*val = (as_val *) list;
		}
	} else if (PyDict_Check(py_obj)) {
		as_map * map = NULL;
		pyobject_to_map(self, err, py_obj, &map, static_pool, serializer_type);
		if (err->code == AEROSPIKE_OK) {
			*val = (as_val *) map;
		}
	} else if (Py_None == py_obj) {
		*val = as_val_reserve(&as_nil);
	} else if (!strcmp(py_obj->ob_type->tp_name, "aerospike.null")) {
		*val = (as_val *) as_val_reserve(&as_nil);
	} else if (AS_Matches_Classname(py_obj, AS_CDT_WILDCARD_NAME)) {
		*val = (as_val *) as_val_reserve(&as_cmp_wildcard);
	} else if (AS_Matches_Classname(py_obj, AS_CDT_INFINITE_NAME)) {
		*val = (as_val *) as_val_reserve(&as_cmp_inf);
	} else {
		if (PyFloat_Check(py_obj)) {
			double d = PyFloat_AsDouble(py_obj);
			*val = (as_val *) as_double_new(d);
		} else {
			as_bytes *bytes;
			GET_BYTES_POOL(bytes, static_pool, err);
			if (err->code == AEROSPIKE_OK) {
				if (serialize_based_on_serializer_policy(self, serializer_type,
					&bytes, py_obj, err) != AEROSPIKE_OK) {
					return err->code;
				}
				*val = (as_val *) bytes;
			}
		}
	}

	return err->code;
}

as_status add_pred_macros(as_exp_entry ** expressions, pred_op * pred, as_error * err) {
	// PyObject * tuple_py_val = NULL;
	// PyObject * utf8_temp = NULL;
	// char* bin_name = NULL;
	int64_t lval1 = 0;
	int64_t lval2 = 0;
	int64_t lval3 = 0;
	int64_t lval4 = 0;
	
	switch (pred->op) {
		case BIN:
			switch (pred->result_type) { //remove switch
				case INTEGER:;
					{
						as_exp_entry new_entries[] = {AS_EXP_BIN_INT(pred->fixed_str)};
						append_array(3);
					}
				break;
				case STRING:;
					{
						as_exp_entry new_entries[] = {AS_EXP_BIN_STR(pred->fixed_str)};
						append_array(3);
					}
				break;
				case LIST:;
					{
						as_exp_entry new_entries[] = {AS_EXP_BIN_LIST(pred->fixed_str)};
						append_array(3);
					}
				break;
				case MAP:;
					{
						as_exp_entry new_entries[] = {AS_EXP_BIN_MAP(pred->fixed_str)};
						append_array(3);
					}
				break;
				case BLOB:;
					{
						as_exp_entry new_entries[] = {AS_EXP_BIN_BLOB(pred->fixed_str)};
						append_array(3);
					}
				break;
				case FLOAT:;
					{
						as_exp_entry new_entries[] = {AS_EXP_BIN_FLOAT(pred->fixed_str)};
						append_array(3);
					}
				break;
				case GEOJSON:;
					{
						as_exp_entry new_entries[] = {AS_EXP_BIN_GEO(pred->fixed_str)};
						append_array(3);
					}
				break;
				case HLL:;
					{
						as_exp_entry new_entries[] = {AS_EXP_BIN_HLL(pred->fixed_str)};
						append_array(3);
					}
				break;
			}
			break;
		case VAL:;
			as_exp_entry new_entries[] = {AS_EXP_VAL_INT(pred->fixed_num)};
			append_array(1);
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
				as_exp_entry new_entries[] = {AS_EXP_META_DIGEST_MOD(pred->fixed_num)};
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
				append_array(1);
			}
			break;
		case META_KEY_EXISTS:;
			{
				as_exp_entry new_entries[] = {AS_EXP_META_KEY_EXIST()};
				append_array(1);
			}
			break;
		case REC_KEY:;
			if (pred->fixed_str) {
				{
					as_exp_entry new_entries[] = {AS_EXP_KEY_STR()};
					append_array(3);
				}
			} 
			else {
				{
					as_exp_entry new_entries[] = {AS_EXP_KEY_INT()};
					append_array(3);
				}
			}
			break;
		case BIN_TYPE:;
			{
				as_exp_entry new_entries[] = {AS_EXP_BIN_TYPE(pred->fixed_str)};
				append_array(2);
			}
			break;
		case OP_LIST_EXP_GET_BY_INDEX:;
			printf("in get_by_index\n");
			{
				get_int64_t(err, AS_PY_BIN_TYPE, pred->pyval1, &lval1);
				get_int64_t(err, AS_PY_LIST_RETURN_KEY, pred->pyval1, &lval2);
				get_int64_t(err, AS_PY_INDEX_KEY, pred->pyval1, &lval3);
				as_exp_entry new_entries[] = {AS_EXP_LIST_GET_BY_INDEX(
					lval1,
					pred->ctx,
					lval2,
					AS_EXP_VAL_INT(lval3),
					AS_EXP_BIN_LIST(pred->fixed_str) // bin name only
					)}; //why not convert here?
				printf("size is: %d\n", sizeof(new_entries) / sizeof(as_exp_entry));
				append_array(sizeof(new_entries) / sizeof(as_exp_entry));
			}
			break;
		case OP_LIST_EXP_SIZE:;
			printf("in list_size\n");
			{
				as_exp_entry new_entries[] = {AS_EXP_LIST_SIZE(
					pred->ctx,
					AS_EXP_BIN_LIST(pred->fixed_str)
					)};
				printf("size is: %d\n", sizeof(new_entries) / sizeof(as_exp_entry));
				append_array(sizeof(new_entries) / sizeof(as_exp_entry));
			}
			break;
		case OP_LIST_EXP_GET_BY_INDEX:;
			printf("in get_by_index\n");
			{
				get_int64_t(err, AS_PY_BIN_TYPE, pred->pyval1, &lval1);
				get_int64_t(err, AS_PY_LIST_RETURN_KEY, pred->pyval1, &lval2);
				get_int64_t(err, AS_PY_INDEX_KEY, pred->pyval1, &lval3);
				as_exp_entry new_entries[] = {AS_EXP_LIST_GET_BY_INDEX(
					lval1,
					pred->ctx,
					lval2,
					AS_EXP_VAL_INT(lval3),
					AS_EXP_BIN_LIST(pred->fixed_str) // bin name only
					)}; //why not convert here?
				printf("size is: %d\n", sizeof(new_entries) / sizeof(as_exp_entry));
				append_array(sizeof(new_entries) / sizeof(as_exp_entry));
			}
		// case OP_LIST_EXP_APPEND:;
		// 	{
		// 		as_exp_entry new_entries[] = {AS_EXP_LIST_APPEND(pred->fixed)};
		// 		append_array(2);
		// 	}
		// 	break;
	}		

	return AEROSPIKE_OK;
}

as_status convert_predexp2_list(AerospikeClient * self, PyObject* py_predexp_list, as_exp** predexp_list, as_error* err) {
	Py_ssize_t size = PyList_Size(py_predexp_list);
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
    PyObject * fixed = NULL;
	PyObject * py_strings = NULL;
	as_vector pred_queue;
	pred_op pred;
	pred_op * pred_p = NULL;
	as_exp_entry * c_pred_entries = NULL;

	as_static_pool static_pool;
	memset(&static_pool, 0, sizeof(static_pool));


	as_vector_inita(&pred_queue, sizeof(pred_op), size);

	pred_p = (pred_op*) calloc(1, sizeof(pred_op));
	if (pred_p == NULL) {
		as_error_update(err, AEROSPIKE_ERR, "could not calloc mem for pred_p");
	}

	py_strings = (PyObject*) calloc(size, sizeof(PyObject)); // iter and count elem?
	if (py_strings == NULL) {
		as_error_update(err, AEROSPIKE_ERR, "could not calloc mem for py_strings");
	}

	c_pred_entries = (as_exp_entry*) calloc((size * MAX_ELEMENTS), sizeof(as_exp_entry)); // iter and count elem?
	if (c_pred_entries == NULL) {
		as_error_update(err, AEROSPIKE_ERR, "could not calloc mem for c_pred_entries");
	}

    for ( int i = 0; i < size; ++i ) {
		pred.pyval1 = NULL;
		pred.ctx = NULL;
		// pred.pyval2 = NULL;
		// pred.pyval3 = NULL;
		// pred.pyval4 = NULL;
		pred.pyfixed = NULL;
		pred.pytuple = NULL;

		if (child_count == 0 && va_flag >= 1) {
			pred.op=END_VA_ARGS;
			memcpy(pred_p, &pred, sizeof(pred_op)); //TODO error check
			as_vector_append(&pred_queue, (void*) pred_p);
			--va_flag;
			continue;
		}

        py_pred_tuple = PyList_GetItem(py_predexp_list, (Py_ssize_t)i);
		pred.pytuple = py_pred_tuple;
		Py_INCREF(py_pred_tuple);
        op = PyInt_AsLong(PyTuple_GetItem(py_pred_tuple, 0));

        result_type = PyInt_AsLong(PyTuple_GetItem(py_pred_tuple, 1));
		if (result_type == -1 && PyErr_Occurred()) {
			PyErr_Clear();
		}

        fixed = PyTuple_GetItem(py_pred_tuple, 2);
		if (fixed != NULL && fixed != Py_None) {
			Py_INCREF(fixed);
			PyObject * fixed_arg0 = PyTuple_GetItem(fixed, 0);
			if (PyInt_Check(fixed_arg0)) {
				pred.fixed_num = (int64_t) PyInt_AsLong(fixed_arg0);
				if (pred.fixed_num == -1 && PyErr_Occurred()) {
					if (PyErr_ExceptionMatches(PyExc_OverflowError)) {
						return as_error_update(err, AEROSPIKE_ERR_PARAM, "integer value exceeds sys.maxsize");
					}
				}

				pred.pyfixed = fixed;
				pred.fixed_str = NULL;
			} 
			else if (PyLong_Check(fixed_arg0)) {
				pred.fixed_num = (int64_t) PyLong_AsLong(fixed_arg0);
				if (pred.fixed_num == -1 && PyErr_Occurred()) {
					if (PyErr_ExceptionMatches(PyExc_OverflowError)) {
						return as_error_update(err, AEROSPIKE_ERR_PARAM, "integer value exceeds sys.maxsize");
					}
				}

				pred.pyfixed = fixed;
				pred.fixed_str = NULL;
			}
			else {
				PyObject * py_ustr = PyUnicode_AsUTF8String(fixed_arg0); //TODO this needs py 2 string handling
				//py_strings[++str_bottom] = PyBytes_AsString(py_ustr);
				//pred.fixed_str = PyBytes_AsString(py_ustr);
				//Py_INCREF(fixed_str);
				pred.fixed_str = calloc(20, sizeof(char));
				char * tmp = PyBytes_AsString(py_ustr);
				memcpy(pred.fixed_str, tmp, strlen(tmp)); //TODO decref the new object from this
				pred.fixed_num = 0; // try storing these ^
				pred.pyfixed = py_ustr;
			}

			//get op args
			if (PyTuple_Size(fixed) >= 2) {
				pred.pyval1 = PyTuple_GetItem(fixed, 1);
				Py_INCREF(pred.pyval1);

				if (PyDict_Check(pred.pyval1)) { //TODO change serialzer type to user defnied via arg
					if (get_cdt_ctx(self, err, &ctx, pred.pyval1, &ctx_in_use, &static_pool, SERIALIZER_PYTHON) != AEROSPIKE_OK) {
						return as_error_update(err, AEROSPIKE_ERR_PARAM, "Failed to convert cdt_ctx");
					}
					pred.ctx = ctx_in_use ? &ctx : NULL;
				}

			}

		}

		if (op == AND || op == OR) {
			++va_flag;
			++size;
		}

        num_children = PyInt_AsLong(PyTuple_GetItem(py_pred_tuple, 3));
		pred.op = op;
		pred.result_type = result_type;
		pred.num_children = num_children;
		memcpy(pred_p, &pred, sizeof(pred_op)); //TODO error check
		as_vector_append(&pred_queue, (void*) pred_p);
		if (va_flag) {
			child_count += num_children - 1;
		}
    }

	for ( int i = 0; i < size; ++i ) {
		pred_op * pred = (pred_op *) as_vector_get(&pred_queue, (uint32_t)i);
		add_pred_macros(&c_pred_entries, pred, err);
	}

	*predexp_list = as_exp_build(c_pred_entries, bottom + 1);

	for (int i = 0; i < size; ++i) {
		printf("here\n");
		pred_op * pred = (pred_op *) as_vector_get(&pred_queue, (uint32_t)i);
		printf("got: %d\n", pred->pyval1);
		Py_XDECREF(pred->pyfixed);
		Py_XDECREF(pred->pyval1);
		// Py_XDECREF(pred->pyval2);
		// Py_XDECREF(pred->pyval3);
		// Py_XDECREF(pred->pyval4);
		Py_XDECREF(pred->pytuple);
		pred->pyval1 = NULL;
		// pred->pyval2 = NULL;
		// pred->pyval3 = NULL;
		// pred->pyval4 = NULL;
		pred->pyfixed = NULL;
		pred->pytuple = NULL;
	}
	// Py_DECREF(fixed); //this needs more decrefs for each fixed
	// fixed = NULL;
	// Py_DECREF(py_pred_tuple);
	// py_pred_tuple = NULL;

	return AEROSPIKE_OK;
}