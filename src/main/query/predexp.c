#include <Python.h>
#include <stdbool.h>

#include <aerospike/as_query.h>
#include <aerospike/aerospike_index.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_error.h>

#include "client.h"
#include "query.h"
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
as_status add_single_string_arg_predicate(
	as_query *query, PyObject *predicate, as_error *err,
	single_string_predexp_constructor *constructor, const char *predicate_name);

/* Predexp constructor function which takes a single char* argument */
as_status add_no_arg_predicate(as_query *query, PyObject *predicate,
							   as_error *err,
							   no_arg_predexp_constructor *no_arg_constructor,
							   const char *predicate_name);

/* Dispatch to another function based on what kind of predicate we have */
as_status add_predexp(as_query *query, PyObject *predicate, as_error *err);

/* Functions for converting a Python tuple into a predexp, and adding it to the query */
as_status add_and(as_query *query, PyObject *predicate, as_error *err);
as_status add_or(as_query *query, PyObject *predicate, as_error *err);
as_status add_not(as_query *query, PyObject *predicate, as_error *err);
as_status add_integer_val(as_query *query, PyObject *predicate, as_error *err);
as_status add_string_val(as_query *query, PyObject *predicate, as_error *err);
as_status add_geojson_val(as_query *query, PyObject *predicate, as_error *err);
as_status add_int_bin(as_query *query, PyObject *predicate, as_error *err);
as_status add_string_bin(as_query *query, PyObject *predicate, as_error *err);
as_status add_geo_bin(as_query *query, PyObject *predicate, as_error *err);
as_status add_list_bin(as_query *query, PyObject *predicate, as_error *err);
as_status add_map_bin(as_query *query, PyObject *predicate, as_error *err);
as_status add_integer_var(as_query *query, PyObject *predicate, as_error *err);
as_status add_string_var(as_query *query, PyObject *predicate, as_error *err);
as_status add_geojson_var(as_query *query, PyObject *predicate, as_error *err);
as_status add_rec_device_size(as_query *query, PyObject *predicate,
							  as_error *err);
as_status add_rec_last_update(as_query *query, PyObject *predicate,
							  as_error *err);
as_status add_rec_void_time(as_query *query, PyObject *predicate,
							as_error *err);
as_status add_rec_digest_modulo(as_query *query, PyObject *predicate,
								as_error *err);
as_status add_integer_equal(as_query *query, PyObject *predicate,
							as_error *err);
as_status add_integer_unequal(as_query *query, PyObject *predicate,
							  as_error *err);
as_status add_integer_greater(as_query *query, PyObject *predicate,
							  as_error *err);
as_status add_integer_greatereq(as_query *query, PyObject *predicate,
								as_error *err);
as_status add_integer_less(as_query *query, PyObject *predicate, as_error *err);
as_status add_integer_lesseq(as_query *query, PyObject *predicate,
							 as_error *err);
as_status add_string_equal(as_query *query, PyObject *predicate, as_error *err);
as_status add_string_unequal(as_query *query, PyObject *predicate,
							 as_error *err);
as_status add_string_regex(as_query *query, PyObject *predicate, as_error *err);
as_status add_geojson_within(as_query *query, PyObject *predicate,
							 as_error *err);
as_status add_geojson_contains(as_query *query, PyObject *predicate,
							   as_error *err);
as_status add_list_iterate_or(as_query *query, PyObject *predicate,
							  as_error *err);
as_status add_list_iterate_and(as_query *query, PyObject *predicate,
							   as_error *err);
as_status add_mapkey_iterate_or(as_query *query, PyObject *predicate,
								as_error *err);
as_status add_mapkey_iterate_and(as_query *query, PyObject *predicate,
								 as_error *err);
as_status add_mapval_iterate_or(as_query *query, PyObject *predicate,
								as_error *err);
as_status add_mapval_iterate_and(as_query *query, PyObject *predicate,
								 as_error *err);

/* MACROS For simple tuple return functions */
#define OneArgPredExpBuilderFunc(_name, _pyfunc_name, _predexp_code)           \
	static PyObject *AerospikePredExp_##_name(PyObject *self, PyObject *param) \
	{                                                                          \
		return Py_BuildValue("(iO)", _predexp_code, param);                    \
	}
/* Functions which return a tuple of the type (num_value,) */
#define NoArgPredExpBuilderFunc(_name, _pyfunc_name, _predexp_code)            \
	static PyObject *AerospikePredExp_##_name(PyObject *self,                  \
											  PyObject *unused)                \
	{                                                                          \
		return Py_BuildValue("(i)", _predexp_code);                            \
	}

#define OneArgPredExpFunctionEntry(_name, _pyfunc_name, _doc_var)              \
	{                                                                          \
		#_pyfunc_name, (PyCFunction)AerospikePredExp_##_name, METH_O, _doc_var \
	}
#define NoArgPredExpFunctionEntry(_name, _pyfunc_name, _doc_var)               \
	{                                                                          \
		#_pyfunc_name, (PyCFunction)AerospikePredExp_##_name, METH_NOARGS,     \
			_doc_var                                                           \
	}

AerospikeQuery *AerospikeQuery_Predexp(AerospikeQuery *self, PyObject *args)
{
	PyObject *predicates_list = NULL;

	as_error err;
	as_error_init(&err);
	Py_ssize_t predicate_count = 0;
	uint16_t predicates_stored = 0;
	bool predicates_initialized = false;

	if (PyArg_ParseTuple(args, "O", &predicates_list) == false) {
		return NULL;
	}

	if (!PyList_Check(predicates_list)) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Predicates must be a list");
		goto CLEANUP;
	}

	predicate_count = PyList_Size(predicates_list);

	if (predicate_count == 0) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"Predicates list must not be empty");
		goto CLEANUP;
	}

	as_query_predexp_init(&self->query, predicate_count);

	PyObject *predicateTuple = NULL;

	for (Py_ssize_t i = 0; i < predicate_count; i++) {
		predicateTuple = PyList_GetItem(predicates_list, i);
		if (add_predexp(&self->query, predicateTuple, &err) != AEROSPIKE_OK) {
			goto CLEANUP;
		}
		predicates_stored += 1;
	}

CLEANUP:

	if (err.code != AEROSPIKE_OK) {

		if (predicates_initialized) {

			for (uint16_t ndx = 0; ndx < self->query.predexp.size; ++ndx) {
				as_predexp_base *bp = self->query.predexp.entries[ndx];
				if (!bp) {
					break;
				}
				if (bp->dtor_fn) {
					(*bp->dtor_fn)(bp);
				}
			}

			if (self->query.predexp.entries && self->query.predexp._free) {
				cf_free(self->query.predexp.entries);
			}
			self->query.predexp.entries = NULL;
		}

		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	Py_INCREF(self);
	return self;

	//ERROR
}

/**
 * Apply a list of predicates to the query
 *
 *		query.predexp(predexps)
 *
 */

as_status add_predexp(as_query *query, PyObject *predicate, as_error *err)
{
	long predicate_type = -1;

	if (!predicate || !PyTuple_Check(predicate) ||
		PyTuple_Size(predicate) < 1) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid predicate");
	}

	PyObject *py_pred_type = PyTuple_GetItem(predicate, 0);
	if (!py_pred_type || !PyInt_Check(py_pred_type)) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
							   "Invalid predicate type");
	}

	predicate_type = PyLong_AsLong(py_pred_type);
	switch (predicate_type) {
	case AS_PREDEXP_AND:
		add_and(query, predicate, err);
		break;
	case AS_PREDEXP_OR:
		add_or(query, predicate, err);
		break;
	case AS_PREDEXP_NOT:
		add_not(query, predicate, err);
		break;
	case AS_PREDEXP_INTEGER_VALUE:
		add_integer_val(query, predicate, err);
		break;
	case AS_PREDEXP_STRING_VALUE:
		add_string_val(query, predicate, err);
		break;
	case AS_PREDEXP_GEOJSON_VALUE:
		add_geojson_val(query, predicate, err);
		break;
	case AS_PREDEXP_INTEGER_BIN:
		add_int_bin(query, predicate, err);
		break;
	case AS_PREDEXP_STRING_BIN:
		add_string_bin(query, predicate, err);
		break;
	case AS_PREDEXP_GEOJSON_BIN:
		add_geo_bin(query, predicate, err);
		break;
	case AS_PREDEXP_LIST_BIN:
		add_list_bin(query, predicate, err);
		break;
	case AS_PREDEXP_MAP_BIN:
		add_map_bin(query, predicate, err);
		break;
	case AS_PREDEXP_INTEGER_VAR:
		add_integer_var(query, predicate, err);
		break;
	case AS_PREDEXP_STRING_VAR:
		add_string_var(query, predicate, err);
		break;
	case AS_PREDEXP_GEOJSON_VAR:
		add_geojson_var(query, predicate, err);
		break;
	case AS_PREDEXP_REC_DEVICE_SIZE:
		add_rec_device_size(query, predicate, err);
		break;
	case AS_PREDEXP_REC_LAST_UPDATE:
		add_rec_last_update(query, predicate, err);
		break;
	case AS_PREDEXP_REC_VOID_TIME:
		add_rec_void_time(query, predicate, err);
		break;
	case AS_PREDEXP_REC_DIGEST_MODULO:
		add_rec_digest_modulo(query, predicate, err);
		break;
	case AS_PREDEXP_INTEGER_EQUAL:
		add_integer_equal(query, predicate, err);
		break;
	case AS_PREDEXP_INTEGER_UNEQUAL:
		add_integer_unequal(query, predicate, err);
		break;
	case AS_PREDEXP_INTEGER_GREATER:
		add_integer_greater(query, predicate, err);
		break;
	case AS_PREDEXP_INTEGER_GREATEREQ:
		add_integer_greatereq(query, predicate, err);
		break;
	case AS_PREDEXP_INTEGER_LESS:
		add_integer_less(query, predicate, err);
		break;
	case AS_PREDEXP_INTEGER_LESSEQ:
		add_integer_lesseq(query, predicate, err);
		break;
	case AS_PREDEXP_STRING_EQUAL:
		add_string_equal(query, predicate, err);
		break;
	case AS_PREDEXP_STRING_UNEQUAL:
		add_string_unequal(query, predicate, err);
		break;
	case AS_PREDEXP_STRING_REGEX:
		add_string_regex(query, predicate, err);
		break;
	case AS_PREDEXP_GEOJSON_WITHIN:
		add_geojson_within(query, predicate, err);
		break;
	case AS_PREDEXP_GEOJSON_CONTAINS:
		add_geojson_contains(query, predicate, err);
		break;
	case AS_PREDEXP_LIST_ITERATE_OR:
		add_list_iterate_or(query, predicate, err);
		break;
	case AS_PREDEXP_MAPKEY_ITERATE_OR:
		add_mapkey_iterate_or(query, predicate, err);
		break;
	case AS_PREDEXP_MAPVAL_ITERATE_OR:
		add_mapval_iterate_or(query, predicate, err);
		break;
	case AS_PREDEXP_LIST_ITERATE_AND:
		add_list_iterate_and(query, predicate, err);
		break;
	case AS_PREDEXP_MAPKEY_ITERATE_AND:
		add_mapkey_iterate_and(query, predicate, err);
		break;
	case AS_PREDEXP_MAPVAL_ITERATE_AND:
		add_mapval_iterate_and(query, predicate, err);
		break;
	default:
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
							   "Unknown predicate type");
	}
	return err->code;
}

/*
 * as_query_predexp_add(as_query* query, as_predexp_base * predexp)
 *
 */

as_status add_and(as_query *query, PyObject *predicate, as_error *err)
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
	if (!as_query_predexp_add(query, as_predexp_and(nitems))) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
							   "Failed to add and predicate");
	}
	return err->code;
}

as_status add_or(as_query *query, PyObject *predicate, as_error *err)
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
	if (!as_query_predexp_add(query, as_predexp_or(nitems))) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
							   "Failed to add or predicate");
	}
	return err->code;
}

as_status add_not(as_query *query, PyObject *predicate, as_error *err)
{
	return add_no_arg_predicate(query, predicate, err, as_predexp_not, "not");
}

as_status add_integer_val(as_query *query, PyObject *predicate, as_error *err)
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
	if (!as_query_predexp_add(query, as_predexp_integer_value(int_val))) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
							   "Failed to add interger_val");
	}
	return err->code;
}

as_status add_string_val(as_query *query, PyObject *predicate, as_error *err)
{
	return add_single_string_arg_predicate(
		query, predicate, err, as_predexp_string_value, "string value");
}

as_status add_geojson_val(as_query *query, PyObject *predicate, as_error *err)
{
	return add_single_string_arg_predicate(
		query, predicate, err, as_predexp_geojson_value, "geojson value");
}

as_status add_int_bin(as_query *query, PyObject *predicate, as_error *err)
{
	return add_single_string_arg_predicate(
		query, predicate, err, as_predexp_integer_bin, "integer bin");
}

as_status add_string_bin(as_query *query, PyObject *predicate, as_error *err)
{
	return add_single_string_arg_predicate(query, predicate, err,
										   as_predexp_string_bin, "string bin");
}

as_status add_geo_bin(as_query *query, PyObject *predicate, as_error *err)
{
	return add_single_string_arg_predicate(
		query, predicate, err, as_predexp_geojson_bin, "geojson bin");
}

as_status add_list_bin(as_query *query, PyObject *predicate, as_error *err)
{
	return add_single_string_arg_predicate(query, predicate, err,
										   as_predexp_list_bin, "list bin");
}

as_status add_map_bin(as_query *query, PyObject *predicate, as_error *err)
{
	return add_single_string_arg_predicate(query, predicate, err,
										   as_predexp_map_bin, "map bin");
}

as_status add_integer_var(as_query *query, PyObject *predicate, as_error *err)
{
	return add_single_string_arg_predicate(
		query, predicate, err, as_predexp_integer_var, "integer var");
}

as_status add_string_var(as_query *query, PyObject *predicate, as_error *err)
{
	return add_single_string_arg_predicate(query, predicate, err,
										   as_predexp_string_var, "string var");
}

as_status add_geojson_var(as_query *query, PyObject *predicate, as_error *err)
{
	return add_single_string_arg_predicate(
		query, predicate, err, as_predexp_geojson_var, "geojson var");
}

as_status add_rec_device_size(as_query *query, PyObject *predicate,
							  as_error *err)
{
	return add_no_arg_predicate(query, predicate, err,
								as_predexp_rec_device_size, "rec device size");
}

as_status add_rec_last_update(as_query *query, PyObject *predicate,
							  as_error *err)
{
	return add_no_arg_predicate(query, predicate, err,
								as_predexp_rec_last_update, "rec last update");
}

as_status add_rec_void_time(as_query *query, PyObject *predicate, as_error *err)
{
	return add_no_arg_predicate(query, predicate, err, as_predexp_rec_void_time,
								"rec void time");
}

as_status add_rec_digest_modulo(as_query *query, PyObject *predicate,
								as_error *err)
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
	if (!as_query_predexp_add(query, as_predexp_rec_digest_modulo(int_val))) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
							   "Failed to add digest modulo predicate");
	}
	return err->code;
}

as_status add_integer_equal(as_query *query, PyObject *predicate, as_error *err)
{
	return add_no_arg_predicate(query, predicate, err, as_predexp_integer_equal,
								"integer equal");
}

as_status add_integer_unequal(as_query *query, PyObject *predicate,
							  as_error *err)
{
	return add_no_arg_predicate(query, predicate, err,
								as_predexp_integer_unequal, "integer unequal");
}

as_status add_integer_greater(as_query *query, PyObject *predicate,
							  as_error *err)
{
	return add_no_arg_predicate(query, predicate, err,
								as_predexp_integer_greater, "integer greater");
}

as_status add_integer_greatereq(as_query *query, PyObject *predicate,
								as_error *err)
{
	return add_no_arg_predicate(query, predicate, err,
								as_predexp_integer_greatereq,
								"integer greatereq");
}

as_status add_integer_less(as_query *query, PyObject *predicate, as_error *err)
{
	return add_no_arg_predicate(query, predicate, err, as_predexp_integer_less,
								"integer less");
}

as_status add_integer_lesseq(as_query *query, PyObject *predicate,
							 as_error *err)
{
	return add_no_arg_predicate(query, predicate, err,
								as_predexp_integer_lesseq, "integer lesseq");
}

as_status add_string_equal(as_query *query, PyObject *predicate, as_error *err)
{
	return add_no_arg_predicate(query, predicate, err, as_predexp_string_equal,
								"string equal");
}

as_status add_string_unequal(as_query *query, PyObject *predicate,
							 as_error *err)
{
	return add_no_arg_predicate(query, predicate, err,
								as_predexp_string_unequal, "string unequal");
}

as_status add_string_regex(as_query *query, PyObject *predicate, as_error *err)
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
	if (!as_query_predexp_add(query, as_predexp_string_regex(flags))) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
							   "Failed to add string regex predicate");
	}
	return err->code;
}

as_status add_geojson_within(as_query *query, PyObject *predicate,
							 as_error *err)
{
	return add_no_arg_predicate(query, predicate, err,
								as_predexp_geojson_within, "geojson within");
}

as_status add_geojson_contains(as_query *query, PyObject *predicate,
							   as_error *err)
{
	return add_no_arg_predicate(
		query, predicate, err, as_predexp_geojson_contains, "geojson contains");
}

as_status add_list_iterate_or(as_query *query, PyObject *predicate,
							  as_error *err)
{
	return add_single_string_arg_predicate(
		query, predicate, err, as_predexp_list_iterate_or, "list_iterate_or");
}

as_status add_list_iterate_and(as_query *query, PyObject *predicate,
							   as_error *err)
{
	return add_single_string_arg_predicate(
		query, predicate, err, as_predexp_list_iterate_and, "list_iterate_and");
}

as_status add_mapkey_iterate_or(as_query *query, PyObject *predicate,
								as_error *err)
{
	return add_single_string_arg_predicate(query, predicate, err,
										   as_predexp_mapkey_iterate_or,
										   "mapkey_iterate_or");
}

as_status add_mapkey_iterate_and(as_query *query, PyObject *predicate,
								 as_error *err)
{
	return add_single_string_arg_predicate(query, predicate, err,
										   as_predexp_mapkey_iterate_and,
										   "mapkey_iterate_and");
}

as_status add_mapval_iterate_or(as_query *query, PyObject *predicate,
								as_error *err)
{
	return add_single_string_arg_predicate(query, predicate, err,
										   as_predexp_mapval_iterate_or,
										   "mapval_iterate_or");
}

as_status add_mapval_iterate_and(as_query *query, PyObject *predicate,
								 as_error *err)
{
	return add_single_string_arg_predicate(query, predicate, err,
										   as_predexp_mapval_iterate_and,
										   "mapval_iterate_and");
}

as_status add_single_string_arg_predicate(
	as_query *query, PyObject *predicate, as_error *err,
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

	if (!as_query_predexp_add(query, constructor(c_var_name))) {
		as_error_update(err, AEROSPIKE_ERR_PARAM, "Failed to add %s predicate",
						predicate_name);
	}

	Py_XDECREF(py_uni);
	return err->code;
}

as_status add_no_arg_predicate(as_query *query, PyObject *predicate,
							   as_error *err,
							   no_arg_predexp_constructor *no_arg_constructor,
							   const char *predicate_name)
{

	if (PyTuple_Size(predicate) != 1) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid %s predicate",
							   predicate_name);
	}
	if (!as_query_predexp_add(query, no_arg_constructor())) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
							   "Failed to add %s predicate", predicate_name);
	}
	return err->code;
}

/* Predicate functions */
OneArgPredExpBuilderFunc(PredexpAnd, predexp_and, AS_PREDEXP_AND) OneArgPredExpBuilderFunc(
	PredexpOr, predexp_or,
	AS_PREDEXP_OR) OneArgPredExpBuilderFunc(IntegerValue, integer_value,
											AS_PREDEXP_INTEGER_VALUE)
	OneArgPredExpBuilderFunc(
		StringValue, string_value,
		AS_PREDEXP_STRING_VALUE) OneArgPredExpBuilderFunc(GeojsonValue,
														  geojson_value,
														  AS_PREDEXP_GEOJSON_VALUE)
		OneArgPredExpBuilderFunc(
			IntegerBin, integer_bin,
			AS_PREDEXP_INTEGER_BIN) OneArgPredExpBuilderFunc(StringBin,
															 string_bin,
															 AS_PREDEXP_STRING_BIN)
			OneArgPredExpBuilderFunc(
				GeojsonBin, geojson_bin,
				AS_PREDEXP_GEOJSON_BIN) OneArgPredExpBuilderFunc(ListBin,
																 list_bin,
																 AS_PREDEXP_LIST_BIN)
				OneArgPredExpBuilderFunc(
					MapBin, map_bin,
					AS_PREDEXP_MAP_BIN) OneArgPredExpBuilderFunc(IntegerVar,
																 integer_var,
																 AS_PREDEXP_INTEGER_VAR)
					OneArgPredExpBuilderFunc(StringVar, string_var,
											 AS_PREDEXP_STRING_VAR)
						OneArgPredExpBuilderFunc(GeojsonVar, geojson_var,
												 AS_PREDEXP_GEOJSON_VAR)
							OneArgPredExpBuilderFunc(
								RecDigestModulo, rec_digest_modulo,
								AS_PREDEXP_REC_DIGEST_MODULO)
								OneArgPredExpBuilderFunc(
									ListIterateOr, list_iterate_or,
									AS_PREDEXP_LIST_ITERATE_OR)
									OneArgPredExpBuilderFunc(
										ListIterateAnd, list_iterate_and,
										AS_PREDEXP_LIST_ITERATE_AND)
										OneArgPredExpBuilderFunc(
											MapkeyIterateOr, mapkey_iterate_or,
											AS_PREDEXP_MAPKEY_ITERATE_OR)
											OneArgPredExpBuilderFunc(
												MapkeyIterateAnd,
												mapkey_iterate_and,
												AS_PREDEXP_MAPKEY_ITERATE_AND)
												OneArgPredExpBuilderFunc(
													MapvalIterateOr,
													mapval_iterate_or,
													AS_PREDEXP_MAPVAL_ITERATE_OR)
													OneArgPredExpBuilderFunc(
														MapvalIterateAnd,
														mapval_iterate_and,
														AS_PREDEXP_MAPVAL_ITERATE_AND)
	/* No argument Predexp Functions */
	NoArgPredExpBuilderFunc(
		PredexpNot, predexp_not,
		AS_PREDEXP_NOT) NoArgPredExpBuilderFunc(RecDeviceSize, rec_device_size,
												AS_PREDEXP_REC_DEVICE_SIZE)
		NoArgPredExpBuilderFunc(RecLastUpdate, rec_last_update,
								AS_PREDEXP_REC_LAST_UPDATE)
			NoArgPredExpBuilderFunc(RecVoidTime, rec_void_time,
									AS_PREDEXP_REC_VOID_TIME)

				NoArgPredExpBuilderFunc(IntegerEqual, integer_equal,
										AS_PREDEXP_INTEGER_EQUAL)
					NoArgPredExpBuilderFunc(IntegerUnequal, integer_unequal,
											AS_PREDEXP_INTEGER_UNEQUAL)
						NoArgPredExpBuilderFunc(IntegerGreater, integer_greater,
												AS_PREDEXP_INTEGER_GREATER)
							NoArgPredExpBuilderFunc(
								IntegerGreaterEq, integer_greatereq,
								AS_PREDEXP_INTEGER_GREATEREQ)
								NoArgPredExpBuilderFunc(IntegerLess,
														integer_less,
														AS_PREDEXP_INTEGER_LESS)
									NoArgPredExpBuilderFunc(
										IntegerLessEq, integer_lesseq,
										AS_PREDEXP_INTEGER_LESSEQ)
										NoArgPredExpBuilderFunc(
											StringEqual, string_equal,
											AS_PREDEXP_STRING_EQUAL)
											NoArgPredExpBuilderFunc(
												StringUnequal, string_unequal,
												AS_PREDEXP_STRING_UNEQUAL)
												NoArgPredExpBuilderFunc(
													GeojsonWithin,
													geojson_within,
													AS_PREDEXP_GEOJSON_WITHIN)
													NoArgPredExpBuilderFunc(
														GeojsonContains,
														geojson_contains,
														AS_PREDEXP_GEOJSON_CONTAINS)
	/* Predexp Method Docstrings */

	/*String regex is special*/
	static PyObject *AerospikePredExp_PredexpStringRegex(PyObject *self,
														 PyObject *args)
{
	int flag = 0;
	Py_ssize_t arg_count = PyTuple_Size(args);
	for (int i = 0; i < arg_count; i++) {
		PyObject *py_flag = PyTuple_GetItem(args, i);
		if (PyInt_Check(py_flag)) {
			flag |= PyInt_AsLong(py_flag);
		}
		else if (PyLong_Check(py_flag)) {
			flag |= PyLong_AsLong(py_flag);
		}
		else {
			PyErr_SetString(
				PyExc_TypeError,
				"Arguments to string_regex must be integers or longs");
			return NULL;
		}
	}
	return Py_BuildValue("(ii)", AS_PREDEXP_STRING_REGEX, flag);
}

PyDoc_STRVAR(predexp_and_doc,
			 "predexp_and(item_count) -> (predexp_code, value)\n\
\n\
Creates a tuple to be used with the query.predexp method");

PyDoc_STRVAR(predexp_or_doc, "predexp_or(item_count) -> (predexp_code, value)\n\
\n\
Creates a tuple to be used with the query.predexp method");

PyDoc_STRVAR(integer_value_doc,
			 "integer_value(value) -> (predexp_code, value)\n\
\n\
Creates a tuple to be used with the query.predexp method");

PyDoc_STRVAR(string_value_doc, "string_value(value) -> (predexp_code, value)\n\
\n\
Creates a tuple to be used with the query.predexp method");

PyDoc_STRVAR(geojson_value_doc,
			 "geojson_value(value) -> (predexp_code, value)\n\
\n\
Creates a tuple to be used with the query.predexp method");

PyDoc_STRVAR(integer_bin_doc, "integer_bin(value) -> (predexp_code, value)\n\
\n\
Creates a tuple to be used with the query.predexp method");

PyDoc_STRVAR(string_bin_doc, "string_bin(value) -> (predexp_code, value)\n\
\n\
Creates a tuple to be used with the query.predexp method");

PyDoc_STRVAR(geojson_bin_doc, "geojson_bin(value) -> (predexp_code, value)\n\
\n\
Creates a tuple to be used with the query.predexp method");

PyDoc_STRVAR(list_bin_doc, "list_bin(value) -> (predexp_code, value)\n\
\n\
Creates a tuple to be used with the query.predexp method");

PyDoc_STRVAR(map_bin_doc, "map_bin(value) -> (predexp_code, value)\n\
\n\
Creates a tuple to be used with the query.predexp method");

PyDoc_STRVAR(predexp_not_doc, "predexp_not() -> (predexp_code,)\n\
\n\
Creates a tuple to be used with the query.predexp method");

PyDoc_STRVAR(integer_var_doc, "integer_var(value) -> (predexp_code, value)\n\
\n\
Creates a tuple to be used with the query.predexp method");

PyDoc_STRVAR(string_var_doc, "string_var(value) -> (predexp_code, value)\n\
\n\
Creates a tuple to be used with the query.predexp method");

PyDoc_STRVAR(geojson_var_doc, "geojson_var(value) -> (predexp_code, value)\n\
\n\
Creates a tuple to be used with the query.predexp method");

PyDoc_STRVAR(rec_digest_modulo_doc,
			 "rec_digest_modulo(value) -> (predexp_code, value)\n\
\n\
Creates a tuple to be used with the query.predexp method");

PyDoc_STRVAR(list_iterate_or_doc,
			 "list_iterate_or(var_name) -> (predexp_code, var_name)\n\
\n\
Creates a tuple to be used with the query.predexp method");

PyDoc_STRVAR(list_iterate_and_doc,
			 "list_iterate_and(var_name) -> (predexp_code, var_name)\n\
\n\
Creates a tuple to be used with the query.predexp method");

PyDoc_STRVAR(mapkey_iterate_or_doc,
			 "mapkey_iterate_or(var_name) -> (predexp_code, var_name)\n\
\n\
Creates a tuple to be used with the query.predexp method");

PyDoc_STRVAR(mapkey_iterate_and_doc,
			 "mapkey_iterate_and(var_name) -> (predexp_code, var_name)\n\
\n\
Creates a tuple to be used with the query.predexp method");

PyDoc_STRVAR(mapval_iterate_or_doc,
			 "mapval_iterate_or(var_name) -> (predexp_code, var_name)\n\
\n\
Creates a tuple to be used with the query.predexp method");

PyDoc_STRVAR(mapval_iterate_and_doc,
			 "mapval_iterate_and(var_name) -> (predexp_code, var_name)\n\
\n\
Creates a tuple to be used with the query.predexp method");

PyDoc_STRVAR(string_regex_doc,
			 "geojson_within_doc(flags..) -> (predexp_code, flag)\n\
\n\
Creates a tuple to be used with the query.predexp method");
/* No arg Docstrings */
PyDoc_STRVAR(rec_device_size_doc, "rec_device_size() -> (predexp_code,)\n\
\n\
Creates a tuple to be used with the query.predexp method");

PyDoc_STRVAR(rec_last_update_doc, "rec_last_update() -> (predexp_code,)\n\
\n\
Creates a tuple to be used with the query.predexp method");

PyDoc_STRVAR(rec_void_time_doc, "rec_void_time() -> (predexp_code,)\n\
\n\
Creates a tuple to be used with the query.predexp method");

PyDoc_STRVAR(integer_equal_doc, "integer_equal() -> (predexp_code,)\n\
\n\
Creates a tuple to be used with the query.predexp method");

PyDoc_STRVAR(integer_unequal_doc, "integer_unequal() -> (predexp_code,)\n\
\n\
Creates a tuple to be used with the query.predexp method");

PyDoc_STRVAR(integer_greater_doc, "integer_greater() -> (predexp_code,)\n\
\n\
Creates a tuple to be used with the query.predexp method");

PyDoc_STRVAR(integer_greatereq_doc, "integer_greatereq() -> (predexp_code,)\n\
\n\
Creates a tuple to be used with the query.predexp method");

PyDoc_STRVAR(integer_less_doc, "integer_greater() -> (predexp_code,)\n\
\n\
Creates a tuple to be used with the query.predexp method");

PyDoc_STRVAR(integer_lesseq_doc, "integer_greatereq() -> (predexp_code,)\n\
\n\
Creates a tuple to be used with the query.predexp method");

PyDoc_STRVAR(string_equal_doc, "string_equal() -> (predexp_code,)\n\
\n\
Creates a tuple to be used with the query.predexp method");

PyDoc_STRVAR(string_unequal_doc, "string_unequal() -> (predexp_code,)\n\
\n\
Creates a tuple to be used with the query.predexp method");

PyDoc_STRVAR(geojson_contains_doc, "geojson_contains_doc() -> (predexp_code,)\n\
\n\
Creates a tuple to be used with the query.predexp method");

PyDoc_STRVAR(geojson_within_doc, "geojson_within_doc() -> (predexp_code,)\n\
\n\
Creates a tuple to be used with the query.predexp method");

static PyMethodDef AerospikePredExp_Methods[] = {
	/* One argument Predexp functions */
	OneArgPredExpFunctionEntry(PredexpAnd, predexp_and, predexp_and_doc),
	OneArgPredExpFunctionEntry(PredexpOr, predexp_or, predexp_or_doc),

	OneArgPredExpFunctionEntry(IntegerValue, integer_value, integer_value_doc),
	OneArgPredExpFunctionEntry(StringValue, string_value, string_value_doc),
	OneArgPredExpFunctionEntry(GeojsonValue, geojson_value, geojson_value_doc),

	OneArgPredExpFunctionEntry(IntegerBin, integer_bin, integer_bin_doc),
	OneArgPredExpFunctionEntry(StringBin, string_bin, string_bin_doc),
	OneArgPredExpFunctionEntry(GeojsonBin, geojson_bin, geojson_bin_doc),
	OneArgPredExpFunctionEntry(ListBin, list_bin, list_bin_doc),
	OneArgPredExpFunctionEntry(MapBin, map_bin, map_bin_doc),

	OneArgPredExpFunctionEntry(IntegerVar, integer_var, integer_var_doc),
	OneArgPredExpFunctionEntry(StringVar, string_var, string_var_doc),
	OneArgPredExpFunctionEntry(GeojsonVar, geojson_var, geojson_var_doc),

	OneArgPredExpFunctionEntry(RecDigestModulo, rec_digest_modulo,
							   rec_digest_modulo_doc),

	OneArgPredExpFunctionEntry(ListIterateOr, list_iterate_or,
							   list_iterate_or_doc),
	OneArgPredExpFunctionEntry(ListIterateAnd, list_iterate_and,
							   list_iterate_and_doc),
	OneArgPredExpFunctionEntry(MapkeyIterateOr, mapkey_iterate_or,
							   mapkey_iterate_or_doc),
	OneArgPredExpFunctionEntry(MapkeyIterateAnd, mapkey_iterate_and,
							   mapkey_iterate_and_doc),
	OneArgPredExpFunctionEntry(MapvalIterateOr, mapval_iterate_or,
							   mapval_iterate_or_doc),
	OneArgPredExpFunctionEntry(MapvalIterateAnd, mapval_iterate_and,
							   mapval_iterate_and_doc),

	/* No argument Predexp functions */
	NoArgPredExpFunctionEntry(PredexpNot, predexp_not, predexp_not_doc),
	NoArgPredExpFunctionEntry(RecDeviceSize, rec_device_size,
							  rec_device_size_doc),
	NoArgPredExpFunctionEntry(RecLastUpdate, rec_last_update,
							  rec_last_update_doc),
	NoArgPredExpFunctionEntry(RecVoidTime, rec_void_time, rec_void_time_doc),

	NoArgPredExpFunctionEntry(IntegerEqual, integer_equal, integer_equal_doc),
	NoArgPredExpFunctionEntry(IntegerUnequal, integer_unequal,
							  integer_unequal_doc),
	NoArgPredExpFunctionEntry(IntegerGreater, integer_greater,
							  integer_greater_doc),
	NoArgPredExpFunctionEntry(IntegerGreaterEq, integer_greatereq,
							  integer_greatereq_doc),
	NoArgPredExpFunctionEntry(IntegerLess, integer_less, integer_less_doc),
	NoArgPredExpFunctionEntry(IntegerLessEq, integer_lesseq,
							  integer_lesseq_doc),
	NoArgPredExpFunctionEntry(StringEqual, string_equal, string_equal_doc),
	NoArgPredExpFunctionEntry(StringUnequal, string_unequal,
							  string_unequal_doc),
	NoArgPredExpFunctionEntry(GeojsonContains, geojson_contains,
							  geojson_contains_doc),
	NoArgPredExpFunctionEntry(GeojsonWithin, geojson_within,
							  geojson_within_doc),
	/* String regex is special so it gets a non macro definition */
	{"string_regex", (PyCFunction)AerospikePredExp_PredexpStringRegex,
	 METH_VARARGS, string_regex_doc},
	{NULL, NULL, 0, NULL}};

as_status RegisterPredExpConstants(PyObject *aerospike)
{

	PyModule_AddIntConstant(aerospike, "REGEX_NONE", 0);
	PyModule_AddIntConstant(aerospike, "REGEX_EXTENDED", 1);
	PyModule_AddIntConstant(aerospike, "REGEX_ICASE", 2);
	PyModule_AddIntConstant(aerospike, "REGEX_NOSUB", 4);
	PyModule_AddIntConstant(aerospike, "REGEX_NEWLINE", 8);

	return AEROSPIKE_OK;
}

/* Register all of our methods */
PyObject *AerospikePredExp_New(void)
{
	PyObject *module;
	MOD_DEF(module, "aerospike.predexp", "Query Predexp",
			-1, AerospikePredExp_Methods, NULL);
	return module;
}
