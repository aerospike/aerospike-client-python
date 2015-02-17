/*******************************************************************************
 * Copyright 2013-2014 Aerospike, Inc.
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
#include <aerospike/as_error.h>

#include "client.h"
#include "query.h"
#include "conversions.h"

#undef TRACE
#define TRACE()

static int64_t pyobject_to_int64(PyObject * py_obj)
{
	if ( PyInt_Check(py_obj) ) {
		return PyInt_AsLong(py_obj);
	}
	else if ( PyLong_Check(py_obj) ) {
		return PyLong_AsLongLong(py_obj);
	}
	else {
		return 0;
	}
}
/*
static char * pyobject_to_str(PyObject * py_obj)
{
	if ( PyString_Check(py_obj) ) {
		return PyString_AsString(py_obj);
	}
	else {
		return NULL;
	}
}
*/
static int AerospikeQuery_Where_Add(AerospikeQuery * self, as_predicate_type predicate, as_index_datatype in_datatype, PyObject * py_bin, PyObject * py_val1, PyObject * py_val2)

{
	as_error err;
	char * val = NULL, * bin = NULL;
	PyObject * py_ubin = NULL;

	switch (predicate) {
		case AS_PREDICATE_EQUAL: {
			if ( in_datatype == AS_INDEX_STRING ){
				if (PyUnicode_Check(py_bin)){
					py_ubin = PyUnicode_AsUTF8String(py_bin);
					bin = PyString_AsString(py_ubin);
				} else if (PyString_Check(py_bin) ){
					bin = PyString_AsString(py_bin);
				}
				else {
					return 1;
				}

				if (PyUnicode_Check(py_val1)){ 
					val = PyString_AsString( 
							StoreUnicodePyObject( self,
								PyUnicode_AsUTF8String(py_val1) ));

				} else if (PyString_Check(py_val1) ){
					val = PyString_AsString(py_val1);
				}
				else {
					return 1;
				}

				as_query_where_init(&self->query, 1);
				as_query_where(&self->query, bin, as_equals( STRING, val ));
				if (py_ubin){
					Py_DECREF(py_ubin);
					py_ubin = NULL;
				}
			}
			else if ( in_datatype == AS_INDEX_NUMERIC ){
				if (PyUnicode_Check(py_bin)){
					py_ubin = PyUnicode_AsUTF8String(py_bin);
					bin = PyString_AsString(py_ubin);
				} else if (PyString_Check(py_bin) ){
					bin = PyString_AsString(py_bin);
				}
				else {
					return 1;
				}
				int64_t val = pyobject_to_int64(py_val1);

				as_query_where_init(&self->query, 1);
				as_query_where(&self->query, bin, as_equals( NUMERIC, val ));
				if (py_ubin){
					Py_DECREF(py_ubin);
					py_ubin = NULL;
				}
			}
			else {
				// If it ain't expected, raise and error
				as_error_update(&err, AEROSPIKE_ERR_PARAM, "predicate 'equals' expects a string or integer value.");
				PyObject * py_err = NULL;
				error_to_pyobject(&err, &py_err);
				PyErr_SetObject(PyExc_Exception, py_err);
				return 1;
			}

			break;
		}
		case AS_PREDICATE_RANGE: {
			if ( in_datatype == AS_INDEX_NUMERIC) {
				if (PyUnicode_Check(py_bin)){
					py_ubin = PyUnicode_AsUTF8String(py_bin);
					bin = PyString_AsString(py_ubin);
				} else if (PyString_Check(py_bin)){
					bin = PyString_AsString(py_bin);
				}
				else {
					return 1;
				}
				int64_t min = pyobject_to_int64(py_val1);
				int64_t max = pyobject_to_int64(py_val2);

				as_query_where_init(&self->query, 1);
				as_query_where(&self->query, bin, as_range( DEFAULT, NUMERIC, min, max ));
				if (py_ubin){
					Py_DECREF(py_ubin);
					py_ubin = NULL;
				}
			}
			else if ( in_datatype == AS_INDEX_STRING) {
				// NOT IMPLEMENTED
			}
			else {
				// If it ain't right, raise and error
				as_error_update(&err, AEROSPIKE_ERR_PARAM, "predicate 'between' expects two integer values.");
				PyObject * py_err = NULL;
				error_to_pyobject(&err, &py_err);
				PyErr_SetObject(PyExc_Exception, py_err);
				return 1;
			}
			break;
		}
		default: {
			// If it ain't supported, raise and error
			as_error_update(&err, AEROSPIKE_ERR_PARAM, "unknown predicate type");
			PyObject * py_err = NULL;
			error_to_pyobject(&err, &py_err);
			PyErr_SetObject(PyExc_Exception, py_err);
			return 1;
		}
	}
	return 0;
}

AerospikeQuery * AerospikeQuery_Where(AerospikeQuery * self, PyObject * args)
{
	as_error err;
	int rc = 0;

	PyObject * py_arg1 = NULL;
	PyObject * py_arg2 = NULL;
	PyObject * py_arg3 = NULL;
	PyObject * py_arg4 = NULL;

	if ( PyArg_ParseTuple(args, "O|OOO:where", &py_arg1, &py_arg2, &py_arg3, &py_arg4) == false ) {
		return NULL;
	}

	if ( PyTuple_Check(py_arg1) ) {

		Py_ssize_t size = PyTuple_Size(py_arg1);
		if ( size < 1 ) {
			// If it ain't atleast 1, then raise error
			return NULL;
		}

		PyObject * py_op = PyTuple_GetItem(py_arg1, 0);
		PyObject * py_op_data = PyTuple_GetItem(py_arg1, 1);

		if ( PyInt_Check(py_op) && PyInt_Check(py_op_data)) {
			as_predicate_type op = (as_predicate_type) PyInt_AsLong(py_op);
			as_index_datatype op_data = (as_index_datatype) PyInt_AsLong(py_op_data);
			rc = AerospikeQuery_Where_Add(
				self,
				op,
				op_data,
				size > 2 ? PyTuple_GetItem(py_arg1, 2) : Py_None,
				size > 3 ? PyTuple_GetItem(py_arg1, 3) : Py_None,
				size > 4 ? PyTuple_GetItem(py_arg1, 4) : Py_None
			);
		}
	}
	else if ( (py_arg1) && PyString_Check(py_arg1) && (py_arg2) && PyString_Check(py_arg2) ) {

		char * op = PyString_AsString(py_arg2);

		if ( strcmp(op, "equals") == 0 ) {
			if ( PyInt_Check(py_arg3) || PyLong_Check(py_arg3) ) {
				rc = AerospikeQuery_Where_Add(
					self,
					AS_PREDICATE_EQUAL,
					AS_INDEX_NUMERIC,
					py_arg1,
					py_arg3,
					Py_None
				);
			}
			else if ( PyString_Check(py_arg3) || PyUnicode_Check(py_arg3) ) {
				rc = AerospikeQuery_Where_Add(
					self,
					AS_PREDICATE_EQUAL,
					AS_INDEX_STRING,
					py_arg1,
					py_arg3,
					Py_None
				);
			}
			else {
				as_error_update(&err, AEROSPIKE_ERR_PARAM, "predicate 'equals' expects a bin and string value.");
				PyObject * py_err = NULL;
				error_to_pyobject(&err, &py_err);
				PyErr_SetObject(PyExc_Exception, py_err);
				rc = 1;
			}
		}
		else if ( strcmp(op, "between") == 0 ) {
			rc = AerospikeQuery_Where_Add(
				self,
				AS_PREDICATE_RANGE,
				AS_INDEX_NUMERIC,
				py_arg1,
				py_arg3,
				py_arg4
			);
		}
		else {
			as_error_update(&err, AEROSPIKE_ERR_PARAM, "predicate '%s' is invalid.", op);
			PyObject * py_err = NULL;
			error_to_pyobject(&err, &py_err);
			PyErr_SetObject(PyExc_Exception, py_err);
			rc = 1;
		}

		// if ( PyInt_Check(py_op) ) {
		// 	rc = AerospikeQuery_Where_Add(
		// 		&self->query,
		// 		PyInt_AsLong(py_op),
		// 		size > 1 ? PyTuple_GetItem(py_predicate, 1) : Py_None,
		// 		size > 2 ? PyTuple_GetItem(py_predicate, 2) : Py_None,
		// 		size > 3 ? PyTuple_GetItem(py_predicate, 3) : Py_None
		// 	);
		// }
	}
	else {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "predicate is invalid.");
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		Py_DECREF(py_err);
		as_query_destroy(&self->query);
		rc = 1;
	}

	if ( rc == 1 ) {
		return NULL;
	}

	Py_INCREF(self);
	return self;
}
