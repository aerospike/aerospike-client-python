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
	as_error err;

	// Initialize error
	as_error_init(&err);

	int nargs = (int) PyTuple_Size(args);

	// Aerospike API Arguments
	char * module = NULL;
	char * function = NULL;
	as_arraylist * arglist = NULL;

	// too few args
	if ( nargs < 2 ) {
		as_error_update(&err, AEROSPIKE_ERR_CLIENT, "udf module and function names are required.");
		goto CLEANUP;
	}

	// Python Arguments
	PyObject * py_module = PyTuple_GetItem(args, 0);
	PyObject * py_function = PyTuple_GetItem(args, 1);

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

	if ( nargs > 2 ) {
		arglist = as_arraylist_new(nargs-2, 0);
		for ( int i = 2; i < nargs; i++ ) {
			PyObject * py_val = PyTuple_GetItem(args, i);
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
		return NULL;
	}

	Py_INCREF(self);
	return self;
}