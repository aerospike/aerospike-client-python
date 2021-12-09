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

#include <aerospike/as_query.h>
#include <aerospike/aerospike_index.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_error.h>

#include "client.h"
#include "query.h"
#include "conversions.h"
#include "exceptions.h"

#undef TRACE
#define TRACE()

int64_t pyobject_to_int64(PyObject *py_obj)
{
	if (PyInt_Check(py_obj)) {
		return PyInt_AsLong(py_obj);
	}
	else if (PyLong_Check(py_obj)) {
		return PyLong_AsLongLong(py_obj);
	}
	else {
		return 0;
	}
}

static int AerospikeQuery_Where_Add(AerospikeQuery *self,
									as_predicate_type predicate,
									as_index_datatype in_datatype,
									PyObject *py_bin, PyObject *py_val1,
									PyObject *py_val2, int index_type)
{
	as_error err;
	char *val = NULL, *bin = NULL;
	PyObject *py_ubin = NULL;

	switch (predicate) {
	case AS_PREDICATE_EQUAL: {
		if (in_datatype == AS_INDEX_STRING) {
			if (PyUnicode_Check(py_bin)) {
				py_ubin = PyUnicode_AsUTF8String(py_bin);
				bin = PyBytes_AsString(py_ubin);
			}
			else if (PyString_Check(py_bin)) {
				bin = PyString_AsString(py_bin);
			}
			else if (PyByteArray_Check(py_bin)) {
				bin = PyByteArray_AsString(py_bin);
			}
			else {
				return 1;
			}

			if (PyUnicode_Check(py_val1)) {
				PyObject *py_uval = PyUnicode_AsUTF8String(py_val1);
				val = strdup(PyBytes_AsString(py_uval));
				Py_DECREF(py_uval);
			}
			else if (PyString_Check(py_val1)) {
				val = strdup(PyString_AsString(py_val1));
			}
			else {
				return 1;
			}

			as_query_where_init(&self->query, 1);
			if (index_type == AS_INDEX_TYPE_DEFAULT) {
				as_query_where(&self->query, bin, as_equals(STRING, val));
			}
			else if (index_type == AS_INDEX_TYPE_LIST) {
				as_query_where(&self->query, bin,
							   as_contains(LIST, STRING, val));
			}
			else if (index_type == AS_INDEX_TYPE_MAPKEYS) {
				as_query_where(&self->query, bin,
							   as_contains(MAPKEYS, STRING, val));
			}
			else if (index_type == AS_INDEX_TYPE_MAPVALUES) {
				as_query_where(&self->query, bin,
							   as_contains(MAPVALUES, STRING, val));
			}
			else {
				return 1;
			}
			if (py_ubin) {
				Py_DECREF(py_ubin);
				py_ubin = NULL;
			}
		}
		else if (in_datatype == AS_INDEX_NUMERIC) {
			if (PyUnicode_Check(py_bin)) {
				py_ubin = PyUnicode_AsUTF8String(py_bin);
				bin = PyBytes_AsString(py_ubin);
			}
			else if (PyString_Check(py_bin)) {
				bin = PyString_AsString(py_bin);
			}
			else if (PyByteArray_Check(py_bin)) {
				bin = PyByteArray_AsString(py_bin);
			}
			else {
				return 1;
			}
			int64_t val = pyobject_to_int64(py_val1);

			as_query_where_init(&self->query, 1);
			if (index_type == AS_INDEX_TYPE_DEFAULT) {
				as_query_where(&self->query, bin, as_equals(NUMERIC, val));
			}
			else if (index_type == AS_INDEX_TYPE_LIST) {
				as_query_where(&self->query, bin,
							   as_contains(LIST, NUMERIC, val));
			}
			else if (index_type == AS_INDEX_TYPE_MAPKEYS) {
				as_query_where(&self->query, bin,
							   as_contains(MAPKEYS, NUMERIC, val));
			}
			else if (index_type == AS_INDEX_TYPE_MAPVALUES) {
				as_query_where(&self->query, bin,
							   as_contains(MAPVALUES, NUMERIC, val));
			}
			else {
				return 1;
			}
			if (py_ubin) {
				Py_DECREF(py_ubin);
				py_ubin = NULL;
			}
		}
		else {
			// If it ain't expected, raise and error
			as_error_update(
				&err, AEROSPIKE_ERR_PARAM,
				"predicate 'equals' expects a string or integer value.");
			PyObject *py_err = NULL;
			error_to_pyobject(&err, &py_err);
			PyErr_SetObject(PyExc_Exception, py_err);
			return 1;
		}

		break;
	}
	case AS_PREDICATE_RANGE: {
		if (in_datatype == AS_INDEX_NUMERIC) {
			if (PyUnicode_Check(py_bin)) {
				py_ubin = PyUnicode_AsUTF8String(py_bin);
				bin = PyBytes_AsString(py_ubin);
			}
			else if (PyString_Check(py_bin)) {
				bin = PyString_AsString(py_bin);
			}
			else if (PyByteArray_Check(py_bin)) {
				bin = PyByteArray_AsString(py_bin);
			}
			else {
				return 1;
			}
			int64_t min = pyobject_to_int64(py_val1);
			int64_t max = pyobject_to_int64(py_val2);

			as_query_where_init(&self->query, 1);
			if (index_type == 0) {
				as_query_where(&self->query, bin,
							   as_range(DEFAULT, NUMERIC, min, max));
			}
			else if (index_type == 1) {
				as_query_where(&self->query, bin,
							   as_range(LIST, NUMERIC, min, max));
			}
			else if (index_type == 2) {
				as_query_where(&self->query, bin,
							   as_range(MAPKEYS, NUMERIC, min, max));
			}
			else if (index_type == 3) {
				as_query_where(&self->query, bin,
							   as_range(MAPVALUES, NUMERIC, min, max));
			}
			else {
				return 1;
			}
			if (py_ubin) {
				Py_DECREF(py_ubin);
				py_ubin = NULL;
			}
		}
		else if (in_datatype == AS_INDEX_STRING) {
			// NOT IMPLEMENTED
		}
		else if (in_datatype == AS_INDEX_GEO2DSPHERE) {

			if (PyUnicode_Check(py_bin)) {
				py_ubin = PyUnicode_AsUTF8String(py_bin);
				bin = PyBytes_AsString(py_ubin);
			}
			else if (PyString_Check(py_bin)) {
				bin = PyString_AsString(py_bin);
			}
			else if (PyByteArray_Check(py_bin)) {
				bin = PyByteArray_AsString(py_bin);
			}
			else {
				return 1;
			}

			if (PyUnicode_Check(py_val1)) {
				PyObject *py_uval = PyUnicode_AsUTF8String(py_val1);
				val = strdup(PyBytes_AsString(py_uval));
				Py_DECREF(py_uval);
			}
			else if (PyString_Check(py_val1)) {
				val = strdup(PyString_AsString(py_val1));
			}
			else {
				return 1;
			}

			as_query_where_init(&self->query, 1);
			as_query_where(&self->query, bin, AS_PREDICATE_RANGE, index_type,
						   in_datatype, val);

			if (py_ubin) {
				Py_DECREF(py_ubin);
				py_ubin = NULL;
			}
		}
		else {
			// If it ain't right, raise and error
			as_error_update(&err, AEROSPIKE_ERR_PARAM,
							"range predicate type not supported");
			PyObject *py_err = NULL;
			error_to_pyobject(&err, &py_err);
			PyErr_SetObject(PyExc_Exception, py_err);
			return 1;
		}
		break;
	}
	default: {
		// If it ain't supported, raise and error
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "unknown predicate type");
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return 1;
	}
	}
	return 0;
}

AerospikeQuery *AerospikeQuery_Where(AerospikeQuery *self, PyObject *args)
{
	as_error err;
	int rc = 0;

	PyObject *py_arg1 = NULL;
	PyObject *py_arg2 = NULL;
	PyObject *py_arg3 = NULL;
	PyObject *py_arg4 = NULL;
	PyObject *py_arg5 = NULL;
	PyObject *py_arg6 = NULL;

	if (PyArg_ParseTuple(args, "O|OOOOO:where", &py_arg1, &py_arg2, &py_arg3,
						 &py_arg4, &py_arg5, &py_arg6) == false) {
		return NULL;
	}

	as_error_init(&err);

	if (!self || !self->client->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}

	if (!self->client->is_conn_16) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER,
						"No connection to aerospike cluster");
		goto CLEANUP;
	}

	if (PyTuple_Check(py_arg1) && PyTuple_Size(py_arg1) > 1 &&
		PyTuple_Size(py_arg1) <= 6) {

		Py_ssize_t size = PyTuple_Size(py_arg1);

		PyObject *py_op = PyTuple_GetItem(py_arg1, 0);
		PyObject *py_op_data = PyTuple_GetItem(py_arg1, 1);
		if (!py_op || !py_op_data) {
			as_error_update(&err, AEROSPIKE_ERR_CLIENT,
							"Failed to fetch predicate information");
			goto CLEANUP;
		}
		if (PyInt_Check(py_op) && PyInt_Check(py_op_data)) {
			as_predicate_type op = (as_predicate_type)PyInt_AsLong(py_op);
			as_index_datatype op_data =
				(as_index_datatype)PyInt_AsLong(py_op_data);
			rc = AerospikeQuery_Where_Add(
				self, op, op_data,
				size > 2 ? PyTuple_GetItem(py_arg1, 2) : Py_None,
				size > 3 ? PyTuple_GetItem(py_arg1, 3) : Py_None,
				size > 4 ? PyTuple_GetItem(py_arg1, 4) : Py_None,
				size > 5 ? PyInt_AsLong(PyTuple_GetItem(py_arg1, 5)) : 0);
			/* Failed to add the predicate for some reason */
			if (rc != 0) {
				as_error_update(&err, AEROSPIKE_ERR_PARAM,
								"Failed to add predicate");
				goto CLEANUP;
			}
			/* Incorrect predicate or index type */
		}
		else {
			as_error_update(&err, AEROSPIKE_ERR_PARAM, "predicate is invalid.");
			goto CLEANUP;
		}
		/* Predicate not a tuple, or too short or too long */
	}
	else {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "predicate is invalid.");
	}
CLEANUP:

	if (err.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	Py_INCREF(self);
	return self;
}
