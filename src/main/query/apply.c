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

#include <aerospike/as_arraylist.h>
#include <aerospike/as_error.h>

#include "client.h"
#include "conversions.h"
#include "query.h"

AerospikeQuery * AerospikeQuery_Apply(AerospikeQuery * self, PyObject * args, PyObject * kwds)
{

	// Python function arguments
	PyObject * py_module = NULL;
	PyObject * py_function = NULL;
	PyObject * py_args = NULL;
	PyObject * py_policy = NULL;

	// Python function keyword arguments
	static char * kwlist[] = {"module", "function", "arguments", "policy", NULL};

	if ( PyArg_ParseTupleAndKeywords(args, kwds, "OO|OO:apply", kwlist, &py_module, &py_function, &py_args, &py_policy) == false ){
		return NULL;
	}

	// Aerospike error object
	as_error err;
	// Initialize error object
	as_error_init(&err);

	if ( !self || !self->client->as ){
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid query object");
		goto CLEANUP;
	}

	// Aerospike API Arguments
	char * module = NULL;
	char * function = NULL;
	as_arraylist * arglist = NULL;

	if ( PyString_Check(py_module) ) {
		module = PyString_AsString(py_module);
	}
	else {
		as_error_update(&err, AEROSPIKE_ERR_CLIENT, "udf module argument must be a string");
		goto CLEANUP;
	}

	if ( PyString_Check(py_function) ) {
		function = PyString_AsString(py_function);
	}
	else {
		as_error_update(&err, AEROSPIKE_ERR_CLIENT, "udf function argument must be a string");
		goto CLEANUP;
	}

	if ( py_args && PyList_Check(py_args) ){
		Py_ssize_t size = PyList_Size(py_args);

		arglist = as_arraylist_new(size, 0);

		for ( int i = 0; i < size; i++ ) {
			PyObject * py_val = PyList_GetItem(py_args, (Py_ssize_t)i);
			as_val * val = NULL;
			pyobject_to_val(&err, py_val, &val);
			if ( err.code != AEROSPIKE_OK ) {
				goto CLEANUP;
			}
			else {
				as_arraylist_append(arglist, val);
			}
		}
	}


	as_query_apply(&self->query, module, function, (as_list *) arglist);

CLEANUP:

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	Py_INCREF(self);
	return self;
}
