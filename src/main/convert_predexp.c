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

#include <aerospike/aerospike_index.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_predexp.h>

#include "client.h"
#include "conversions.h"
#include "exceptions.h"

#define MAX_CONSTANT_STR_SIZE 512
#define AS_PREDEXP_AND 1
#define AS_PREDEXP_OR 2
#define AS_PREDEXP_NOT 3

#define AS_PREDEXP_INTEGER_VALUE 10
#define AS_PREDEXP_STRING_VALUE 11
#define AS_PREDEXP_GEOJSON_VALUE 12

#define AS_PREDEXP_INTEGER_BIN 100
#define AS_PREDEXP_STRING_BIN 101
#define AS_PREDEXP_GEOJSON_BIN 102
#define AS_PREDEXP_LIST_BIN 103
#define AS_PREDEXP_MAP_BIN 104

#define AS_PREDEXP_INTEGER_VAR 120
#define AS_PREDEXP_STRING_VAR 121
#define AS_PREDEXP_GEOJSON_VAR 122

#define AS_PREDEXP_REC_DEVICE_SIZE 150
#define AS_PREDEXP_REC_LAST_UPDATE 151
#define AS_PREDEXP_REC_VOID_TIME 152
#define AS_PREDEXP_REC_DIGEST_MODULO 153

#define AS_PREDEXP_INTEGER_EQUAL 200
#define AS_PREDEXP_INTEGER_UNEQUAL 201
#define AS_PREDEXP_INTEGER_GREATER 202
#define AS_PREDEXP_INTEGER_GREATEREQ 203
#define AS_PREDEXP_INTEGER_LESS 204
#define AS_PREDEXP_INTEGER_LESSEQ 205

#define AS_PREDEXP_STRING_EQUAL 210
#define AS_PREDEXP_STRING_UNEQUAL 211
#define AS_PREDEXP_STRING_REGEX 212

#define AS_PREDEXP_GEOJSON_WITHIN 220
#define AS_PREDEXP_GEOJSON_CONTAINS 221

#define AS_PREDEXP_LIST_ITERATE_OR 250
#define AS_PREDEXP_MAPKEY_ITERATE_OR 251
#define AS_PREDEXP_MAPVAL_ITERATE_OR 252
#define AS_PREDEXP_LIST_ITERATE_AND 253
#define AS_PREDEXP_MAPKEY_ITERATE_AND 254
#define AS_PREDEXP_MAPVAL_ITERATE_AND 255

typedef as_predexp_base *(single_string_predexp_constructor)(char const *);
typedef as_predexp_base *(no_arg_predexp_constructor)(void);

/* Predexp constructor function which takes a single char* argument */
as_status add_pred_single_string_arg_predicate(
	as_predexp_list *predexp, PyObject *predicate, as_error *err,
	single_string_predexp_constructor *constructor, const char *predicate_name);

/* Predexp constructor function which takes a single char* argument */
as_status add_pred_no_arg_predicate(
	as_predexp_list *predexp, PyObject *predicate, as_error *err,
	no_arg_predexp_constructor *no_arg_constructor, const char *predicate_name);

/* Dispatch to another function based on what kind of predicate we have */
as_status add_pred_predexp(as_predexp_list *predexp, PyObject *predicate,
						   as_error *err);

// Functions for converting a Python tuple into a predexp, and adding it to the as_predexp_list.
as_status add_pred_and(as_predexp_list *predexp, PyObject *predicate,
					   as_error *err);
as_status add_pred_or(as_predexp_list *predexp, PyObject *predicate,
					  as_error *err);
as_status add_pred_not(as_predexp_list *predexp, PyObject *predicate,
					   as_error *err);
as_status add_pred_integer_val(as_predexp_list *predexp, PyObject *predicate,
							   as_error *err);
as_status add_pred_string_val(as_predexp_list *predexp, PyObject *predicate,
							  as_error *err);
as_status add_pred_geojson_val(as_predexp_list *predexp, PyObject *predicate,
							   as_error *err);
as_status add_pred_int_bin(as_predexp_list *predexp, PyObject *predicate,
						   as_error *err);
as_status add_pred_string_bin(as_predexp_list *predexp, PyObject *predicate,
							  as_error *err);
as_status add_pred_geo_bin(as_predexp_list *predexp, PyObject *predicate,
						   as_error *err);
as_status add_pred_list_bin(as_predexp_list *predexp, PyObject *predicate,
							as_error *err);
as_status add_pred_map_bin(as_predexp_list *predexp, PyObject *predicate,
						   as_error *err);
as_status add_pred_integer_var(as_predexp_list *predexp, PyObject *predicate,
							   as_error *err);
as_status add_pred_string_var(as_predexp_list *predexp, PyObject *predicate,
							  as_error *err);
as_status add_pred_geojson_var(as_predexp_list *predexp, PyObject *predicate,
							   as_error *err);
as_status add_pred_rec_device_size(as_predexp_list *predexp,
								   PyObject *predicate, as_error *err);
as_status add_pred_rec_last_update(as_predexp_list *predexp,
								   PyObject *predicate, as_error *err);
as_status add_pred_rec_void_time(as_predexp_list *predexp, PyObject *predicate,
								 as_error *err);
as_status add_pred_rec_digest_modulo(as_predexp_list *predexp,
									 PyObject *predicate, as_error *err);
as_status add_pred_integer_equal(as_predexp_list *predexp, PyObject *predicate,
								 as_error *err);
as_status add_pred_integer_unequal(as_predexp_list *predexp,
								   PyObject *predicate, as_error *err);
as_status add_pred_integer_greater(as_predexp_list *predexp,
								   PyObject *predicate, as_error *err);
as_status add_pred_integer_greatereq(as_predexp_list *predexp,
									 PyObject *predicate, as_error *err);
as_status add_pred_integer_less(as_predexp_list *predexp, PyObject *predicate,
								as_error *err);
as_status add_pred_integer_lesseq(as_predexp_list *predexp, PyObject *predicate,
								  as_error *err);
as_status add_pred_string_equal(as_predexp_list *predexp, PyObject *predicate,
								as_error *err);
as_status add_pred_string_unequal(as_predexp_list *predexp, PyObject *predicate,
								  as_error *err);
as_status add_pred_string_regex(as_predexp_list *predexp, PyObject *predicate,
								as_error *err);
as_status add_pred_geojson_within(as_predexp_list *predexp, PyObject *predicate,
								  as_error *err);
as_status add_pred_geojson_contains(as_predexp_list *predexp,
									PyObject *predicate, as_error *err);
as_status add_pred_list_iterate_or(as_predexp_list *predexp,
								   PyObject *predicate, as_error *err);
as_status add_pred_list_iterate_and(as_predexp_list *predexp,
									PyObject *predicate, as_error *err);
as_status add_pred_mapkey_iterate_or(as_predexp_list *predexp,
									 PyObject *predicate, as_error *err);
as_status add_pred_mapkey_iterate_and(as_predexp_list *predexp,
									  PyObject *predicate, as_error *err);
as_status add_pred_mapval_iterate_or(as_predexp_list *predexp,
									 PyObject *predicate, as_error *err);
as_status add_pred_mapval_iterate_and(as_predexp_list *predexp,
									  PyObject *predicate, as_error *err);

as_status convert_predexp_list(PyObject *py_predexp_list,
							   as_predexp_list *predexp_list, as_error *err)
{
	long predicate_type = -1;
	int number_predexp = 0;
	PyObject *predicate = NULL;

	if (!py_predexp_list || !PyList_Check(py_predexp_list)) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
							   "Invalid predicate list");
	}

	if (!predexp_list) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
							   "Invalid as_predexp_list");
	}

	number_predexp = PyList_Size(py_predexp_list);
	for (int i = 0; i < number_predexp; i++) {
		predicate = PyList_GetItem(py_predexp_list, (Py_ssize_t)i);

		if (!predicate || !PyTuple_Check(predicate) ||
			PyTuple_Size(predicate) < 1) {
			return as_error_update(err, AEROSPIKE_ERR_PARAM,
								   "Invalid predicate");
		}

		PyObject *py_pred_type = PyTuple_GetItem(predicate, 0);
		if (!py_pred_type || !PyInt_Check(py_pred_type)) {
			return as_error_update(err, AEROSPIKE_ERR_PARAM,
								   "Invalid predicate type");
		}

		predicate_type = PyLong_AsLong(py_pred_type);
		switch (predicate_type) {
		case AS_PREDEXP_AND:
			add_pred_and(predexp_list, predicate, err);
			break;
		case AS_PREDEXP_OR:
			add_pred_or(predexp_list, predicate, err);
			break;
		case AS_PREDEXP_NOT:
			add_pred_not(predexp_list, predicate, err);
			break;
		case AS_PREDEXP_INTEGER_VALUE:
			add_pred_integer_val(predexp_list, predicate, err);
			break;
		case AS_PREDEXP_STRING_VALUE:
			add_pred_string_val(predexp_list, predicate, err);
			break;
		case AS_PREDEXP_GEOJSON_VALUE:
			add_pred_geojson_val(predexp_list, predicate, err);
			break;
		case AS_PREDEXP_INTEGER_BIN:
			add_pred_int_bin(predexp_list, predicate, err);
			break;
		case AS_PREDEXP_STRING_BIN:
			add_pred_string_bin(predexp_list, predicate, err);
			break;
		case AS_PREDEXP_GEOJSON_BIN:
			add_pred_geo_bin(predexp_list, predicate, err);
			break;
		case AS_PREDEXP_LIST_BIN:
			add_pred_list_bin(predexp_list, predicate, err);
			break;
		case AS_PREDEXP_MAP_BIN:
			add_pred_map_bin(predexp_list, predicate, err);
			break;
		case AS_PREDEXP_INTEGER_VAR:
			add_pred_integer_var(predexp_list, predicate, err);
			break;
		case AS_PREDEXP_STRING_VAR:
			add_pred_string_var(predexp_list, predicate, err);
			break;
		case AS_PREDEXP_GEOJSON_VAR:
			add_pred_geojson_var(predexp_list, predicate, err);
			break;
		case AS_PREDEXP_REC_DEVICE_SIZE:
			add_pred_rec_device_size(predexp_list, predicate, err);
			break;
		case AS_PREDEXP_REC_LAST_UPDATE:
			add_pred_rec_last_update(predexp_list, predicate, err);
			break;
		case AS_PREDEXP_REC_VOID_TIME:
			add_pred_rec_void_time(predexp_list, predicate, err);
			break;
		case AS_PREDEXP_REC_DIGEST_MODULO:
			add_pred_rec_digest_modulo(predexp_list, predicate, err);
			break;
		case AS_PREDEXP_INTEGER_EQUAL:
			add_pred_integer_equal(predexp_list, predicate, err);
			break;
		case AS_PREDEXP_INTEGER_UNEQUAL:
			add_pred_integer_unequal(predexp_list, predicate, err);
			break;
		case AS_PREDEXP_INTEGER_GREATER:
			add_pred_integer_greater(predexp_list, predicate, err);
			break;
		case AS_PREDEXP_INTEGER_GREATEREQ:
			add_pred_integer_greatereq(predexp_list, predicate, err);
			break;
		case AS_PREDEXP_INTEGER_LESS:
			add_pred_integer_less(predexp_list, predicate, err);
			break;
		case AS_PREDEXP_INTEGER_LESSEQ:
			add_pred_integer_lesseq(predexp_list, predicate, err);
			break;
		case AS_PREDEXP_STRING_EQUAL:
			add_pred_string_equal(predexp_list, predicate, err);
			break;
		case AS_PREDEXP_STRING_UNEQUAL:
			add_pred_string_unequal(predexp_list, predicate, err);
			break;
		case AS_PREDEXP_STRING_REGEX:
			add_pred_string_regex(predexp_list, predicate, err);
			break;
		case AS_PREDEXP_GEOJSON_WITHIN:
			add_pred_geojson_within(predexp_list, predicate, err);
			break;
		case AS_PREDEXP_GEOJSON_CONTAINS:
			add_pred_geojson_contains(predexp_list, predicate, err);
			break;
		case AS_PREDEXP_LIST_ITERATE_OR:
			add_pred_list_iterate_or(predexp_list, predicate, err);
			break;
		case AS_PREDEXP_MAPKEY_ITERATE_OR:
			add_pred_mapkey_iterate_or(predexp_list, predicate, err);
			break;
		case AS_PREDEXP_MAPVAL_ITERATE_OR:
			add_pred_mapval_iterate_or(predexp_list, predicate, err);
			break;
		case AS_PREDEXP_LIST_ITERATE_AND:
			add_pred_list_iterate_and(predexp_list, predicate, err);
			break;
		case AS_PREDEXP_MAPKEY_ITERATE_AND:
			add_pred_mapkey_iterate_and(predexp_list, predicate, err);
			break;
		case AS_PREDEXP_MAPVAL_ITERATE_AND:
			add_pred_mapval_iterate_and(predexp_list, predicate, err);
			break;
		default:
			return as_error_update(err, AEROSPIKE_ERR_PARAM,
								   "Unknown predicate type");
		}
	}
	return err->code;
}

as_status add_pred_and(as_predexp_list *predexp, PyObject *predicate,
					   as_error *err)
{
	if (PyTuple_Size(predicate) != 2) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
							   "Invalid and predicate");
	}
	PyObject *py_and_count = PyTuple_GetItem(predicate, 1);
	if (!py_and_count || !PyInt_Check(py_and_count)) {
		return as_error_update(
			err, AEROSPIKE_ERR_PARAM,
			"And predicate must contain an integer number of items");
	}
	uint16_t nitems = (uint16_t)PyInt_AsLong(py_and_count);
	if (PyErr_Occurred()) {
		if (nitems == (uint16_t)-1 &&
			PyErr_ExceptionMatches(PyExc_OverflowError)) {
			return as_error_update(
				err, AEROSPIKE_ERR_PARAM,
				"Number of items for predexp_and exceeds maximum");
		}
		else {
			return as_error_update(err, AEROSPIKE_ERR_PARAM,
								   "Invalid number of items for predexp_and");
		}
	}
	as_predexp_list_add(predexp, as_predexp_and(nitems));
	return err->code;
}

as_status add_pred_or(as_predexp_list *predexp, PyObject *predicate,
					  as_error *err)
{
	if (PyTuple_Size(predicate) != 2) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
							   "Invalid or predicate");
	}
	PyObject *py_or_count = PyTuple_GetItem(predicate, 1);
	if (!py_or_count || !PyInt_Check(py_or_count)) {
		return as_error_update(
			err, AEROSPIKE_ERR_PARAM,
			"Or predicate must contain an integer number of items");
	}
	uint16_t nitems = (uint16_t)PyInt_AsLong(py_or_count);
	// This conversion could have overflowed, if somebody did something odd like pass 1 << 64 as the number of items
	if (PyErr_Occurred()) {
		if (nitems == (uint16_t)-1 &&
			PyErr_ExceptionMatches(PyExc_OverflowError)) {
			return as_error_update(
				err, AEROSPIKE_ERR_PARAM,
				"Number of items for predexp_or exceeds maximum");
		}
		else {
			return as_error_update(err, AEROSPIKE_ERR_PARAM,
								   "Invalid number of items for predexp_or");
		}
	}
	as_predexp_list_add(predexp, as_predexp_or(nitems));
	return err->code;
}

as_status add_pred_not(as_predexp_list *predexp, PyObject *predicate,
					   as_error *err)
{
	return add_pred_no_arg_predicate(predexp, predicate, err, as_predexp_not,
									 "not");
}

as_status add_pred_integer_val(as_predexp_list *predexp, PyObject *predicate,
							   as_error *err)
{
	if (PyTuple_Size(predicate) != 2) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
							   "Invalid integer val predicate");
	}
	PyObject *py_int_val = PyTuple_GetItem(predicate, 1);
	if (!py_int_val || !PyInt_Check(py_int_val)) {
		return as_error_update(
			err, AEROSPIKE_ERR_PARAM,
			"Or predicate must contain an integer number of items");
	}
	int64_t int_val = PyLong_AsLong(py_int_val);
	if (int_val == -1 && PyErr_Occurred()) {
		PyErr_Clear();
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
							   "Failed to add integer_val predicate, due to "
							   "integer conversion failure");
	}
	as_predexp_list_add(predexp, as_predexp_integer_value(int_val));
	return err->code;
}

as_status add_pred_string_val(as_predexp_list *predexp, PyObject *predicate,
							  as_error *err)
{
	return add_pred_single_string_arg_predicate(
		predexp, predicate, err, as_predexp_string_value, "string value");
}

as_status add_pred_geojson_val(as_predexp_list *predexp, PyObject *predicate,
							   as_error *err)
{
	return add_pred_single_string_arg_predicate(
		predexp, predicate, err, as_predexp_geojson_value, "geojson value");
}

as_status add_pred_int_bin(as_predexp_list *predexp, PyObject *predicate,
						   as_error *err)
{
	return add_pred_single_string_arg_predicate(
		predexp, predicate, err, as_predexp_integer_bin, "integer bin");
}

as_status add_pred_string_bin(as_predexp_list *predexp, PyObject *predicate,
							  as_error *err)
{
	return add_pred_single_string_arg_predicate(
		predexp, predicate, err, as_predexp_string_bin, "string bin");
}

as_status add_pred_geo_bin(as_predexp_list *predexp, PyObject *predicate,
						   as_error *err)
{
	return add_pred_single_string_arg_predicate(
		predexp, predicate, err, as_predexp_geojson_bin, "geojson bin");
}

as_status add_pred_list_bin(as_predexp_list *predexp, PyObject *predicate,
							as_error *err)
{
	return add_pred_single_string_arg_predicate(
		predexp, predicate, err, as_predexp_list_bin, "list bin");
}

as_status add_pred_map_bin(as_predexp_list *predexp, PyObject *predicate,
						   as_error *err)
{
	return add_pred_single_string_arg_predicate(predexp, predicate, err,
												as_predexp_map_bin, "map bin");
}

as_status add_pred_integer_var(as_predexp_list *predexp, PyObject *predicate,
							   as_error *err)
{
	return add_pred_single_string_arg_predicate(
		predexp, predicate, err, as_predexp_integer_var, "integer var");
}

as_status add_pred_string_var(as_predexp_list *predexp, PyObject *predicate,
							  as_error *err)
{
	return add_pred_single_string_arg_predicate(
		predexp, predicate, err, as_predexp_string_var, "string var");
}

as_status add_pred_geojson_var(as_predexp_list *predexp, PyObject *predicate,
							   as_error *err)
{
	return add_pred_single_string_arg_predicate(
		predexp, predicate, err, as_predexp_geojson_var, "geojson var");
}

as_status add_pred_rec_device_size(as_predexp_list *predexp,
								   PyObject *predicate, as_error *err)
{
	return add_pred_no_arg_predicate(
		predexp, predicate, err, as_predexp_rec_device_size, "rec device size");
}

as_status add_pred_rec_last_update(as_predexp_list *predexp,
								   PyObject *predicate, as_error *err)
{
	return add_pred_no_arg_predicate(
		predexp, predicate, err, as_predexp_rec_last_update, "rec last update");
}

as_status add_pred_rec_void_time(as_predexp_list *predexp, PyObject *predicate,
								 as_error *err)
{
	return add_pred_no_arg_predicate(predexp, predicate, err,
									 as_predexp_rec_void_time, "rec void time");
}

as_status add_pred_rec_digest_modulo(as_predexp_list *predexp,
									 PyObject *predicate, as_error *err)
{
	if (PyTuple_Size(predicate) != 2) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
							   "Invalid digest modulo predicate");
	}
	PyObject *py_int_val = PyTuple_GetItem(predicate, 1);
	if (!py_int_val || !PyInt_Check(py_int_val)) {
		return as_error_update(
			err, AEROSPIKE_ERR_PARAM,
			"Digest modulo predicate must contain an integer modulo");
	}
	int32_t int_val = (int32_t)PyLong_AsLong(py_int_val);
	if (int_val == -1 && PyErr_Occurred()) {
		PyErr_Clear();
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
							   "Failed to add rec_digest_modulo predicate, due "
							   "to integer conversion failure");
	}
	as_predexp_list_add(predexp, as_predexp_rec_digest_modulo(int_val));
	return err->code;
}

as_status add_pred_integer_equal(as_predexp_list *predexp, PyObject *predicate,
								 as_error *err)
{
	return add_pred_no_arg_predicate(predexp, predicate, err,
									 as_predexp_integer_equal, "integer equal");
}

as_status add_pred_integer_unequal(as_predexp_list *predexp,
								   PyObject *predicate, as_error *err)
{
	return add_pred_no_arg_predicate(
		predexp, predicate, err, as_predexp_integer_unequal, "integer unequal");
}

as_status add_pred_integer_greater(as_predexp_list *predexp,
								   PyObject *predicate, as_error *err)
{
	return add_pred_no_arg_predicate(
		predexp, predicate, err, as_predexp_integer_greater, "integer greater");
}

as_status add_pred_integer_greatereq(as_predexp_list *predexp,
									 PyObject *predicate, as_error *err)
{
	return add_pred_no_arg_predicate(predexp, predicate, err,
									 as_predexp_integer_greatereq,
									 "integer greatereq");
}

as_status add_pred_integer_less(as_predexp_list *predexp, PyObject *predicate,
								as_error *err)
{
	return add_pred_no_arg_predicate(predexp, predicate, err,
									 as_predexp_integer_less, "integer less");
}

as_status add_pred_integer_lesseq(as_predexp_list *predexp, PyObject *predicate,
								  as_error *err)
{
	return add_pred_no_arg_predicate(
		predexp, predicate, err, as_predexp_integer_lesseq, "integer lesseq");
}

as_status add_pred_string_equal(as_predexp_list *predexp, PyObject *predicate,
								as_error *err)
{
	return add_pred_no_arg_predicate(predexp, predicate, err,
									 as_predexp_string_equal, "string equal");
}

as_status add_pred_string_unequal(as_predexp_list *predexp, PyObject *predicate,
								  as_error *err)
{
	return add_pred_no_arg_predicate(
		predexp, predicate, err, as_predexp_string_unequal, "string unequal");
}

as_status add_pred_string_regex(as_predexp_list *predexp, PyObject *predicate,
								as_error *err)
{
	if (PyTuple_Size(predicate) != 2) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
							   "Invalid string regex predicate");
	}
	PyObject *py_int_val = PyTuple_GetItem(predicate, 1);
	if (!py_int_val || !PyInt_Check(py_int_val)) {
		return as_error_update(
			err, AEROSPIKE_ERR_PARAM,
			"String regex predicate must contain an integer number of items");
	}
	uint32_t flags = PyInt_AsLong(py_int_val);
	if (PyErr_Occurred()) {
		if (flags == (uint32_t)-1 &&
			PyErr_ExceptionMatches(PyExc_OverflowError)) {
			return as_error_update(
				err, AEROSPIKE_ERR_PARAM,
				"Flags value exceeds maximum for string_regex.");
		}
		else {
			return as_error_update(err, AEROSPIKE_ERR_PARAM,
								   "Invalid flags for string_regex.");
		}
	}
	as_predexp_list_add(predexp, as_predexp_string_regex(flags));
	return err->code;
}

as_status add_pred_geojson_within(as_predexp_list *predexp, PyObject *predicate,
								  as_error *err)
{
	return add_pred_no_arg_predicate(
		predexp, predicate, err, as_predexp_geojson_within, "geojson within");
}

as_status add_pred_geojson_contains(as_predexp_list *predexp,
									PyObject *predicate, as_error *err)
{
	return add_pred_no_arg_predicate(predexp, predicate, err,
									 as_predexp_geojson_contains,
									 "geojson contains");
}

as_status add_pred_list_iterate_or(as_predexp_list *predexp,
								   PyObject *predicate, as_error *err)
{
	return add_pred_single_string_arg_predicate(
		predexp, predicate, err, as_predexp_list_iterate_or, "list_iterate_or");
}

as_status add_pred_list_iterate_and(as_predexp_list *predexp,
									PyObject *predicate, as_error *err)
{
	return add_pred_single_string_arg_predicate(predexp, predicate, err,
												as_predexp_list_iterate_and,
												"list_iterate_and");
}

as_status add_pred_mapkey_iterate_or(as_predexp_list *predexp,
									 PyObject *predicate, as_error *err)
{
	return add_pred_single_string_arg_predicate(predexp, predicate, err,
												as_predexp_mapkey_iterate_or,
												"mapkey_iterate_or");
}

as_status add_pred_mapkey_iterate_and(as_predexp_list *predexp,
									  PyObject *predicate, as_error *err)
{
	return add_pred_single_string_arg_predicate(predexp, predicate, err,
												as_predexp_mapkey_iterate_and,
												"mapkey_iterate_and");
}

as_status add_pred_mapval_iterate_or(as_predexp_list *predexp,
									 PyObject *predicate, as_error *err)
{
	return add_pred_single_string_arg_predicate(predexp, predicate, err,
												as_predexp_mapval_iterate_or,
												"mapval_iterate_or");
}

as_status add_pred_mapval_iterate_and(as_predexp_list *predexp,
									  PyObject *predicate, as_error *err)
{
	return add_pred_single_string_arg_predicate(predexp, predicate, err,
												as_predexp_mapval_iterate_and,
												"mapval_iterate_and");
}

as_status add_pred_single_string_arg_predicate(
	as_predexp_list *predexp, PyObject *predicate, as_error *err,
	single_string_predexp_constructor *constructor, const char *predicate_name)
{
	char *c_var_name = NULL;
	PyObject *py_uni = NULL;

	if (PyTuple_Size(predicate) != 2) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid %s predicate",
							   predicate_name);
	}

	PyObject *py_str_val = PyTuple_GetItem(predicate, 1);
	if (!py_str_val ||
		!(PyString_Check(py_str_val) || PyUnicode_Check(py_str_val))) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid %s predicate",
							   predicate_name);
	}

	/* Get a char* from the py string like*/
	if (string_and_pyuni_from_pystring(py_str_val, &py_uni, &c_var_name, err) !=
		AEROSPIKE_OK) {
		return err->code;
	}

	as_predexp_list_add(predexp, constructor(c_var_name));

	Py_XDECREF(py_uni);
	return err->code;
}

as_status add_pred_no_arg_predicate(
	as_predexp_list *predexp, PyObject *predicate, as_error *err,
	no_arg_predexp_constructor *no_arg_constructor, const char *predicate_name)
{

	if (PyTuple_Size(predicate) != 1) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid %s predicate",
							   predicate_name);
	}
	as_predexp_list_add(predexp, no_arg_constructor());
	return err->code;
}