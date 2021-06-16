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

#include <aerospike/aerospike_query.h>
#include <aerospike/as_error.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_query.h>
#include <aerospike/as_arraylist.h>

#include "client.h"
#include "conversions.h"
#include "exceptions.h"
#include "query.h"
#include "policy.h"

// Struct for Python User-Data for the Callback
typedef struct {
	as_error error;
	PyObject *callback;
	AerospikeClient *client;
} LocalData;

static bool each_result(const as_val *val, void *udata)
{
	bool rval = true;

	if (!val) {
		return false;
	}

	// Extract callback user-data
	LocalData *data = (LocalData *)udata;
	as_error *err = &data->error;
	PyObject *py_callback = data->callback;

	// Python Function Arguments and Result Value
	PyObject *py_arglist = NULL;
	PyObject *py_result = NULL;
	PyObject *py_return = NULL;

	// Lock Python State
	PyGILState_STATE gstate;
	gstate = PyGILState_Ensure();

	// Convert as_val to a Python Object
	val_to_pyobject(data->client, err, val, &py_result);

	// The record could not be converted to a python object
	if (!py_result) {
		//TBD set error here
		// Must release the interpreter lock before returning
		PyGILState_Release(gstate);
		return true;
	}
	// Build Python Function Arguments
	py_arglist = PyTuple_New(1);
	PyTuple_SetItem(py_arglist, 0, py_result);

	// Invoke Python Callback
	py_return = PyObject_Call(py_callback, py_arglist, NULL);

	// Release Python Function Arguments
	Py_DECREF(py_arglist);
	// handle return value
	if (!py_return) {
		// an exception was raised, handle it (someday)
		// for now, we bail from the loop
		as_error_update(err, AEROSPIKE_ERR_CLIENT,
						"Callback function contains an error");
		rval = true;
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

PyObject *AerospikeQuery_Foreach(AerospikeQuery *self, PyObject *args,
								 PyObject *kwds)
{
	// Python Function Arguments
	PyObject *py_callback = NULL;
	PyObject *py_policy = NULL;
	PyObject *py_options = NULL;
	// Python Function Keyword Arguments
	static char *kwlist[] = {"callback", "policy", "options", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "O|OO:foreach", kwlist,
									&py_callback, &py_policy,
									&py_options) == false) {
		as_query_destroy(&self->query);
		return NULL;
	}

	// Initialize callback user data
	LocalData data;
	data.callback = py_callback;
	data.client = self->client;
	as_error_init(&data.error);

	// Aerospike Client Arguments
	as_error err;
	as_policy_query query_policy;
	as_policy_query *query_policy_p = NULL;

	// For converting expressions.
	as_exp exp_list;
	as_exp *exp_list_p = NULL;

	// For converting predexp.
	as_predexp_list predexp_list;
	as_predexp_list *predexp_list_p = NULL;

	// Initialize error
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

	// Convert python policy object to as_policy_exists
	pyobject_to_policy_query(
		self->client, &err, py_policy, &query_policy, &query_policy_p,
		&self->client->as->config.policies.query, &predexp_list,
		&predexp_list_p, &exp_list, &exp_list_p);
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	if (set_query_options(&err, py_options, &self->query) != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	// We are spawning multiple threads
	PyThreadState *_save = PyEval_SaveThread();

	// Invoke operation
	aerospike_query_foreach(self->client->as, &err, query_policy_p,
							&self->query, each_result, &data);

	// We are done using multiple threads
	PyEval_RestoreThread(_save);
	if (data.error.code != AEROSPIKE_OK) {
		as_error_update(&data.error, data.error.code, NULL);
		goto CLEANUP;
	}

CLEANUP:
	if (exp_list_p) {
		as_exp_destroy(exp_list_p);
		;
	}

	if (predexp_list_p) {
		as_predexp_list_destroy(&predexp_list);
	}

	if (self->query.apply.arglist) {
		as_arraylist_destroy((as_arraylist *)self->query.apply.arglist);
	}
	self->query.apply.arglist = NULL;

	if (err.code != AEROSPIKE_OK || data.error.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		PyObject *exception_type = NULL;
		if (err.code != AEROSPIKE_OK) {
			error_to_pyobject(&err, &py_err);
			exception_type = raise_exception(&err);
		}
		if (data.error.code != AEROSPIKE_OK) {
			error_to_pyobject(&data.error, &py_err);
			exception_type = raise_exception(&data.error);
		}
		if (PyObject_HasAttrString(exception_type, "name")) {
			PyObject_SetAttrString(exception_type, "name", Py_None);
		}
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	Py_INCREF(Py_None);
	return Py_None;
}
