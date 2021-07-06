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

#include <aerospike/as_error.h>
#include <aerospike/aerospike.h>
#include "client.h"
#include "conversions.h"
#include "exceptions.h"
#include "policy.h"

static PyObject *AerospikeClient_TruncateInvoke(AerospikeClient *self,
												char *namespace, char *set,
												uint64_t nanos,
												PyObject *py_policy,
												as_error *err)
{
	as_policy_info info_policy;
	as_policy_info *info_policy_p = NULL;
	as_status status = AEROSPIKE_OK;

	pyobject_to_policy_info(err, py_policy, &info_policy, &info_policy_p,
							&self->as->config.policies.info);

	if (err->code != AEROSPIKE_OK) {
		as_error_update(err, AEROSPIKE_ERR_CLIENT, "Incorrect Policy");
		goto CLEANUP;
	}

	status =
		aerospike_truncate(self->as, err, info_policy_p, namespace, set, nanos);
	if (status != AEROSPIKE_OK) {
		// The truncate operation failed. Update the err->code and return
		as_error_update(err, AEROSPIKE_ERR_CLIENT, "Truncate operation failed");
		return NULL;
	}

CLEANUP:

	if (err->code != AEROSPIKE_OK) {
		return NULL;
	}
	return PyLong_FromLong(0);
}

PyObject *AerospikeClient_Truncate(AerospikeClient *self, PyObject *args,
								   PyObject *kwds)
{
	PyObject *py_set = NULL;
	PyObject *py_ns = NULL;
	PyObject *py_nanos = NULL;
	PyObject *py_policy = NULL;
	PyObject *py_ustr = NULL;
	PyObject *ret_val = NULL;
	long long temp_long;
	as_error err;
	uint64_t nanos = 1; // If assignment fails, this will cause an error
	char *namespace = NULL;
	char *set = NULL;

	as_error_init(&err);

	static char *kwlist[] = {"namespace", "set", "nanos", "policy", NULL};

	if (PyArg_ParseTupleAndKeywords(args, kwds, "OOO|O:truncate", kwlist,
									&py_ns, &py_set, &py_nanos,
									&py_policy) == false) {
		return NULL;
	}

	// Start conversion of the namespace parameter
	if (PyString_Check(py_ns)) {
		namespace = strdup(PyString_AsString(py_ns));
		// If we failed to copy the string, exit
		if (!namespace) {
			as_error_update(&err, AEROSPIKE_ERR_CLIENT,
							"Memory allocation failed");
			goto CLEANUP;
		}
	}
	else if (PyUnicode_Check(py_ns)) {
		py_ustr = PyUnicode_AsUTF8String(py_ns);
		namespace = strdup(PyBytes_AsString(py_ustr));
		Py_DECREF(py_ustr);

		// If we failed to copy the string, exit
		if (!namespace) {
			as_error_update(&err, AEROSPIKE_ERR_CLIENT,
							"Memory allocation failed");
			goto CLEANUP;
		}
	}
	else {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"Namespace must be unicode or string type");
		goto CLEANUP;
	}

	// Start conversion of the set parameter
	if (PyString_Check(py_set)) {
		set = strdup(PyString_AsString(py_set));
		// If we called strdup, and it failed we need to exit
		if (!set) {
			as_error_update(&err, AEROSPIKE_ERR_CLIENT,
							"Memory allocation failed");
			goto CLEANUP;
		}
	}
	else if (PyUnicode_Check(py_set)) {
		py_ustr = PyUnicode_AsUTF8String(py_set);
		set = strdup(PyBytes_AsString(py_ustr));
		Py_DECREF(py_ustr);
		// If we called strdup, and it failed we need to exit
		if (!set) {
			as_error_update(&err, AEROSPIKE_ERR_CLIENT,
							"Memory allocation failed");
			goto CLEANUP;
		}
	}
	else if (py_set != Py_None) {
		// If the set is none, this is fine
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"Set must be None, or unicode or string type");
		goto CLEANUP;
	}

	// Start conversion of the nanosecond parameter
	if (PyLong_Check(py_nanos)) {

		temp_long = PyLong_AsLongLong(py_nanos);
		// There was a negative number outside of the range of - 2 ^ 63
		if (temp_long < 0 && !PyErr_Occurred()) {
			as_error_update(&err, AEROSPIKE_ERR_PARAM,
							"Nanoseconds must be a positive value");
			goto CLEANUP;
		}
		// Its possible that this is a valid uint64 between 2 ^ 63 and 2^64 -1
		PyErr_Clear();
		nanos = (uint64_t)PyLong_AsUnsignedLongLong(py_nanos);

		if (PyErr_Occurred()) {
			as_error_update(&err, AEROSPIKE_ERR_PARAM,
							"Nanoseconds value too large");
			goto CLEANUP;
		}
	}
	else if (PyInt_Check(py_nanos)) {
		long tempInt;
		tempInt = PyInt_AsLong(py_nanos);

		if (tempInt == -1 && PyErr_Occurred()) {
			as_error_update(&err, AEROSPIKE_ERR_PARAM,
							"Nanoseconds value out of range for long");
			goto CLEANUP;
		}

		if (tempInt < 0) {
			as_error_update(&err, AEROSPIKE_ERR_PARAM,
							"Nanoseconds value must be a positive value");
			goto CLEANUP;
		}

		nanos = (uint64_t)tempInt;
	}
	else {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"Nanoseconds must be a long type");
		goto CLEANUP;
	}

	ret_val = AerospikeClient_TruncateInvoke(self, namespace, set, nanos,
											 py_policy, &err);

CLEANUP:
	if (namespace) {
		free(namespace);
	}

	if (set) {
		free(set);
	}

	if (err.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);

		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		// If there was a returned value, we need to lower it's ref count here
		Py_XDECREF(ret_val);
		return NULL;
	}

	return ret_val;
}
