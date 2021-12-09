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

#include <aerospike/aerospike_scan.h>
#include <aerospike/as_error.h>
#include <aerospike/as_scan.h>
#include <aerospike/as_partition.h>

#include "client.h"
#include "conversions.h"
#include "exceptions.h"
#include "scan.h"
#include "policy.h"

// Struct for Python User-Data for the Callback
typedef struct {
	as_error error;
	PyObject *callback;
	AerospikeClient *client;
	int partition_scan;
} LocalData;

static bool each_result(const as_val *val, void *udata)
{
	bool rval = true;

	if (!val) {
		return false;
	}

	uint32_t part_id = 0;

	as_record* rec = as_record_fromval(val);
 
	if (rec->key.digest.init) {
		part_id = as_partition_getid(rec->key.digest.value, CLUSTER_NPARTITIONS);
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

	if (!py_result) {
		PyGILState_Release(gstate);
		return true;
	}

	if (data->partition_scan) {
		// Build Python Function Arguments
		py_arglist = PyTuple_New(2);
		PyTuple_SetItem(py_arglist, 0, PyInt_FromLong(part_id));
		PyTuple_SetItem(py_arglist, 1, py_result);
	} else {
		// Build Python Function Arguments
		py_arglist = PyTuple_New(1);
		PyTuple_SetItem(py_arglist, 0, py_result);
	}
	// Invoke Python Callback
	py_return = PyObject_Call(py_callback, py_arglist, NULL);

	// Release Python Function Arguments
	Py_DECREF(py_arglist);

	// handle return value
	if (!py_return) {
		// an exception was raised, handle it (someday)
		// for now, we bail from the loop
		as_error_update(err, AEROSPIKE_ERR_CLIENT,
						"Callback function raised an exception");
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

PyObject *AerospikeScan_Foreach(AerospikeScan *self, PyObject *args,
								PyObject *kwds)
{
	// Python Function Arguments
	PyObject *py_callback = NULL;
	PyObject *py_policy = NULL;
	PyObject *py_options = NULL;
	PyObject *py_nodename = NULL;
	PyObject *py_ustr = NULL;

	char *nodename = NULL;

	as_policy_scan scan_policy;
	as_policy_scan *scan_policy_p = NULL;

	// For converting expressions.
	as_exp exp_list;
	as_exp *exp_list_p = NULL;

	// For converting predexp.
	as_predexp_list predexp_list;
	as_predexp_list *predexp_list_p = NULL;

	as_partition_filter partition_filter = {0};
	as_partition_filter *partition_filter_p = NULL;
	as_partitions_status *ps = NULL;

	// Python Function Keyword Arguments
	static char *kwlist[] = {"callback", "policy", "options", "nodename", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "O|OOO:foreach", kwlist,
									&py_callback, &py_policy, &py_options,
									&py_nodename) == false) {
		return NULL;
	}

	// Create and initialize callback user-data
	LocalData data;
	data.callback = py_callback;
	data.client = self->client;
	data.partition_scan = 0;

	as_error_init(&data.error);

	if (!self || !self->client->as) {
		as_error_update(&data.error, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}

	if (!self->client->is_conn_16) {
		as_error_update(&data.error, AEROSPIKE_ERR_CLUSTER,
						"No connection to aerospike cluster");
		goto CLEANUP;
	}

	// Convert python policy object to as_policy_exists
	pyobject_to_policy_scan(
		self->client, &data.error, py_policy, &scan_policy, &scan_policy_p,
		&self->client->as->config.policies.scan, &predexp_list, &predexp_list_p,
		&exp_list, &exp_list_p);
	if (data.error.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	if (py_policy) {
		PyObject *py_partition_filter =
			PyDict_GetItemString(py_policy, "partition_filter");
		if (py_partition_filter) {
			if (convert_partition_filter(self->client, py_partition_filter,
										 &partition_filter,
										 &ps,
										 &data.error) == AEROSPIKE_OK) {
				partition_filter_p = &partition_filter;
			}
			data.partition_scan = 1;
		}
	}
	as_error_reset(&data.error);

	if (py_options && PyDict_Check(py_options)) {
		set_scan_options(&data.error, &self->scan, py_options);
		if (data.error.code != AEROSPIKE_OK) {
			goto CLEANUP;
		}
	}

	if (py_nodename) {
		if (PyString_Check(py_nodename)) {
			nodename = PyString_AsString(py_nodename);
		}
		else if (PyUnicode_Check(py_nodename)) {
			py_ustr = PyUnicode_AsUTF8String(py_nodename);
			if (!py_ustr) {
				as_error_update(&data.error, AEROSPIKE_ERR_PARAM,
								"Invalid unicode nodename");
				goto CLEANUP;
			}
			nodename = PyBytes_AsString(py_ustr);
		}
		else {
			as_error_update(&data.error, AEROSPIKE_ERR_PARAM,
							"nodename must be a string");
			goto CLEANUP;
		}
	}

	// We are spawning multiple threads
	Py_BEGIN_ALLOW_THREADS
	// Invoke operation
	if (partition_filter_p) {
		if	(ps) {
			as_partition_filter_set_partitions(partition_filter_p, ps);
		}
		aerospike_scan_partitions(self->client->as, &data.error, scan_policy_p,
								  &self->scan, partition_filter_p, each_result,
								  &data);
		if	(ps) {
			as_partitions_status_release(ps);
		}
	}
	else if (nodename) {
		aerospike_scan_node(self->client->as, &data.error, scan_policy_p, &self->scan,
							nodename, each_result, &data);
	}
	else {
		aerospike_scan_foreach(self->client->as, &data.error, scan_policy_p,
							   &self->scan, each_result, &data);
	}
	// We are done using multiple threads
	Py_END_ALLOW_THREADS

	if (data.error.code != AEROSPIKE_OK) {
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

	Py_XDECREF(py_ustr);

	if (data.error.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL, *exception_type = NULL;
		if (data.error.code != AEROSPIKE_OK) {
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
