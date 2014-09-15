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

#include <aerospike/aerospike_scan.h>
#include <aerospike/as_error.h>
#include <aerospike/as_scan.h>

#include "client.h"
#include "conversions.h"
#include "scan.h"
#include "policy.h"

// Struct for Python User-Data for the Callback
typedef struct {
	as_error error;
	PyObject * callback;
} LocalData;


static bool each_result(const as_val * val, void * udata)
{
	if ( !val ) {
		return false;
	}

	// Extract callback user-data
	LocalData * data = (LocalData *) udata;
	as_error * err = &data->error;
	PyObject * py_callback = data->callback;

	// Python Function Arguments and Result Value
	PyObject * py_arglist = NULL; 
	PyObject * py_result = NULL;

	// Lock Python State
	PyGILState_STATE gstate;
	gstate = PyGILState_Ensure();

	// Convert as_val to a Python Object
	val_to_pyobject(err, val, &py_result);

	// Build Python Function Arguments
	py_arglist = PyTuple_New(1);
	PyTuple_SetItem(py_arglist, 0, py_result);

	// Invoke Python Callback
	PyEval_CallObject(py_callback, py_arglist);

	// Release Python Function Arguments
	Py_DECREF(py_arglist);

	// TODO: handle return value

	// Release Python State
	PyGILState_Release(gstate);

	return true;
}

PyObject * AerospikeScan_Foreach(AerospikeScan * self, PyObject * args, PyObject * kwds)
{
	// Python Function Arguments
	PyObject * py_callback = NULL;
	PyObject * py_policy = NULL;

	// Python Function Keyword Arguments
	static char * kwlist[] = {"callback", "policy", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "O|O:foreach", kwlist, &py_callback, &py_policy) == false ) {
		return NULL;
	}

	// Aerospike Client Arguments
	as_error err;
	as_policy_scan policy;
	as_policy_scan * policy_p = NULL;

	// Initialize error
	as_error_init(&err);

	// Convert python policy object to as_policy_exists
	pyobject_to_policy_scan(&err, py_policy, &policy, &policy_p);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

	// Create and initialize callback user-data
	LocalData data;
	data.callback = py_callback;
	as_error_init(&data.error);
	
	// We are spawning multiple threads
	PyThreadState * _save = PyEval_SaveThread();

	// Invoke operation
	aerospike_scan_foreach(self->client->as, &err, policy_p, &self->scan, each_result, &data);

	// We are done using multiple threads
	PyEval_RestoreThread(_save);
	
CLEANUP:

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return NULL;
	}
	
	Py_INCREF(Py_None);
	return Py_None;
}