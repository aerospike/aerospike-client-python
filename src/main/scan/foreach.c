/*******************************************************************************
 * Copyright 2013-2016 Aerospike, Inc.
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
#include "exceptions.h"
#include "scan.h"
#include "policy.h"

// Struct for Python User-Data for the Callback
typedef struct {
	as_error error;
	PyObject * callback;
	AerospikeClient * client;
} LocalData;


static bool each_result(const as_val * val, void * udata)
{
	bool rval = true;

	if (!val) {
		return false;
	}

	// Extract callback user-data
	LocalData * data = (LocalData *) udata;
	as_error * err = &data->error;
	PyObject * py_callback = data->callback;

	// Python Function Arguments and Result Value
	PyObject * py_arglist = NULL;
	PyObject * py_result = NULL;
	PyObject * py_return = NULL;

	// Lock Python State
	PyGILState_STATE gstate;
	gstate = PyGILState_Ensure();

	// Convert as_val to a Python Object
	val_to_pyobject(data->client, err, val, &py_result);

	if (!py_result) {
		PyGILState_Release(gstate);
		return true;
	}
	// Build Python Function Arguments
	py_arglist = PyTuple_New(1);
	PyTuple_SetItem(py_arglist, 0, py_result);

	// Invoke Python Callback
	py_return = PyEval_CallObject(py_callback, py_arglist);

	// Release Python Function Arguments
	Py_DECREF(py_arglist);

	// handle return value
	if (!py_return) {
		// an exception was raised, handle it (someday)
		// for now, we bail from the loop
		as_error_update(err, AEROSPIKE_ERR_CLIENT, "Callback function raised an exception");
		rval = false;
	}
	else if (PyBool_Check(py_return)) {
		if (Py_False == py_return) {
			rval = false;
		}
		else {
			rval = true;
		}
		Py_DECREF(py_return);
	}
	else {
		rval = true;
		Py_DECREF(py_return);
	}

	// Release Python State
	PyGILState_Release(gstate);

	return rval;
}

PyObject * AerospikeScan_Foreach(AerospikeScan * self, PyObject * args, PyObject * kwds)
{
	// Python Function Arguments
	PyObject * py_callback = NULL;
	PyObject * py_policy = NULL;
	PyObject * py_options = NULL;
	as_policy_scan scan_policy;
	as_policy_scan * scan_policy_p = NULL;

	// Python Function Keyword Arguments
	static char * kwlist[] = {"callback", "policy", "options", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "O|OO:foreach", kwlist, &py_callback, &py_policy, &py_options) == false) {
		return NULL;
	}

	// Create and initialize callback user-data
	LocalData data;
	data.callback = py_callback;
	data.client = self->client;
	as_error_init(&data.error);

	// Aerospike Client Arguments
	as_error err;

	// Initialize error
	as_error_init(&err);

	if (!self || !self->client->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}

	if (!self->client->is_conn_16) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER, "No connection to aerospike cluster");
		goto CLEANUP;
	}

	// Convert python policy object to as_policy_exists
	pyobject_to_policy_scan(&err, py_policy, &scan_policy, &scan_policy_p,
			&self->client->as->config.policies.scan);
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}
	if (py_options && PyDict_Check(py_options)) {
		set_scan_options(&err, &self->scan, py_options);
		if (err.code != AEROSPIKE_OK) {
			goto CLEANUP;
		}
	}

	// We are spawning multiple threads
	PyThreadState * _save = PyEval_SaveThread();

	// Invoke operation
	aerospike_scan_foreach(self->client->as, &err, scan_policy_p, &self->scan, each_result, &data);

	// We are done using multiple threads
	PyEval_RestoreThread(_save);
	if (data.error.code != AEROSPIKE_OK) {
		as_error_update(&data.error, data.error.code, NULL);
		goto CLEANUP;
	}

CLEANUP:

	if (err.code != AEROSPIKE_OK || data.error.code != AEROSPIKE_OK) {
		PyObject * py_err = NULL, *exception_type = NULL;
		if (err.code != AEROSPIKE_OK){
			error_to_pyobject(&err, &py_err);
			exception_type = raise_exception(&err);
		}
		if (data.error.code != AEROSPIKE_OK){
			error_to_pyobject(&data.error, &py_err);
			exception_type = raise_exception(&data.error);
		}
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	Py_INCREF(Py_None);
	return Py_None;
}
