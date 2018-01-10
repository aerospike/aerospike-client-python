/*******************************************************************************
 * Copyright 2013-2017 Aerospike, Inc.
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
#include <pthread.h>
#include <stdbool.h>

#include <aerospike/aerospike_scan.h>
#include <aerospike/as_error.h>
#include <aerospike/as_scan.h>

#include "client.h"
#include "conversions.h"
#include "exceptions.h"
#include "policy.h"
#include "scan.h"

#undef TRACE
#define TRACE()

typedef struct {
	PyObject * py_results;
	AerospikeClient * client;
} LocalData;

static bool each_result(const as_val * val, void * udata)
{
	if (!val) {
		return false;
	}

	PyObject * py_results = NULL;
	LocalData *data = (LocalData *) udata;
	py_results = data->py_results;
	PyObject * py_result = NULL;

	as_error err;

	PyGILState_STATE gstate;
	gstate = PyGILState_Ensure();

	val_to_pyobject(data->client, &err, val, &py_result);

	if (py_result) {
		PyList_Append(py_results, py_result);
		Py_DECREF(py_result);
	}

	PyGILState_Release(gstate);

	return true;
}

PyObject * AerospikeScan_Results(AerospikeScan * self, PyObject * args, PyObject * kwds)
{
	PyObject * py_policy = NULL;
	PyObject * py_results = NULL;
	PyObject * py_nodename = NULL;
	PyObject* py_ustr = NULL;

	as_policy_scan scan_policy;
	as_policy_scan * scan_policy_p = NULL;

	char* nodename = NULL;
	LocalData data;
	data.client = self->client;
	static char * kwlist[] = {"policy", "nodename", NULL};

	if (PyArg_ParseTupleAndKeywords(args, kwds, "|OO:results", kwlist, &py_policy, &py_nodename) == false) {
		return NULL;
	}

	as_error err;
	as_error_init(&err);

	if (!self || !self->client->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}
	if (!self->client->is_conn_16) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER, "No connection to aerospike cluster");
		goto CLEANUP;
	}

	// Convert python policy object to as_policy_scan
	pyobject_to_policy_scan(&err, py_policy, &scan_policy, &scan_policy_p,
			&self->client->as->config.policies.scan);
	if (err.code != AEROSPIKE_OK) {
		as_error_update(&err, err.code, NULL);
		goto CLEANUP;
	}

	/*
	 * If the user specified a nodename, validate and convert it to a char*
	 */
	if (py_nodename) {
		if (PyString_Check(py_nodename)) {
			nodename = PyString_AsString(py_nodename);
		} else if (PyUnicode_Check(py_nodename)) {
			/* The decoding could fail, so we need to check for null */
			py_ustr = PyUnicode_AsUTF8String(py_nodename);
			if (!py_ustr) {
				as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid unicode nodename");
				goto CLEANUP;
			}
			nodename = PyBytes_AsString(py_ustr);
		}
		else {
			as_error_update(&err, AEROSPIKE_ERR_PARAM, "nodename must be a string");
			goto CLEANUP;
		}
	}

	py_results = PyList_New(0);
	data.py_results = py_results;

	Py_BEGIN_ALLOW_THREADS

	if (nodename) {
		aerospike_scan_node(self->client->as, &err, scan_policy_p, &self->scan, nodename, each_result, &data);
	} else {
		aerospike_scan_foreach(self->client->as, &err, scan_policy_p, &self->scan, each_result, &data);
	}


	Py_END_ALLOW_THREADS


CLEANUP:

	Py_XDECREF(py_ustr);

	if (err.code != AEROSPIKE_OK) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return py_results;
}
