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

#include <aerospike/aerospike_info.h>
#include <aerospike/as_error.h>
#include <aerospike/as_node.h>
#include <aerospike/as_cluster.h>

#include "client.h"
#include "conversions.h"
#include "exceptions.h"
#include "policy.h"

/**
 ******************************************************************************************************
 * Returns data for a particular request string to AerospikeClient_InfoRandomNode.
 *
 * @param self                  AerospikeClient object.
 * @param request_str_p         Request string sent from the Python client.
 * @param py_policy             The policy sent from the Python client.
 *
 * Returns information about a random host.
 ********************************************************************************************************/
static PyObject *AerospikeClient_InfoRandomNode_Invoke(as_error *err,
													   AerospikeClient *self,
													   PyObject *py_request_str,
													   PyObject *py_policy)
{

	// vars used in cleanup
	char *response_p = NULL;

	if (!self || !self->as) {
		as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object.");
		goto CLEANUP;
	}

	if (!self->is_conn_16) {
		as_error_update(err, AEROSPIKE_ERR_CLUSTER,
						"No connection to aerospike cluster.");
		goto CLEANUP;
	}

	as_policy_info info_policy;
	as_policy_info *info_policy_p = NULL;
	if (py_policy) {
		if (pyobject_to_policy_info(
				err, py_policy, &info_policy, &info_policy_p,
				&self->as->config.policies.info) != AEROSPIKE_OK) {
			goto CLEANUP;
		}
	}

	const char *request_str_p = NULL;
	if (PyUnicode_Check(py_request_str)) {
		request_str_p = PyUnicode_AsUTF8(py_request_str);
	}
	else {
		as_error_update(err, AEROSPIKE_ERR_PARAM,
						"Request should be a string.");
		goto CLEANUP;
	}

	as_status status = AEROSPIKE_OK;
	Py_BEGIN_ALLOW_THREADS
	status = aerospike_info_any(self->as, err, info_policy_p, request_str_p,
								&response_p);
	Py_END_ALLOW_THREADS

	PyObject *py_response = NULL;
	if (err->code == AEROSPIKE_OK) {
		if (response_p != NULL && status == AEROSPIKE_OK) {
			py_response = PyUnicode_FromString(response_p);
		}
		else if (response_p == NULL) {
			as_error_update(err, AEROSPIKE_ERR_CLIENT,
							"Invalid info operation.");
			goto CLEANUP;
		}
		else if (status != AEROSPIKE_OK) {
			as_error_update(err, status, "Info operation failed.");
			goto CLEANUP;
		}
	}
	else {
		goto CLEANUP;
	}

CLEANUP:

	if (response_p != NULL) {
		cf_free(response_p);
	}

	if (err->code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(err, &py_err);
		PyObject *exception_type = raise_exception(err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return py_response;
}

/**
 ******************************************************************************************************
 * Returns data from a random node in the database depending upon the request string.
 *
 * @param self                  AerospikeClient object.
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function.
 * @param kwds                  Dictionary of keywords.
 *
 * Returns information about a random host.
 ********************************************************************************************************/
PyObject *AerospikeClient_InfoRandomNode(AerospikeClient *self, PyObject *args,
										 PyObject *kwds)
{
	PyObject *py_policy = NULL;
	PyObject *py_command = NULL;

	as_error err;
	as_error_init(&err);

	static char *kwlist[] = {"command", "policy", NULL};

	if (PyArg_ParseTupleAndKeywords(args, kwds, "O|O:info_random_node", kwlist,
									&py_command, &py_policy) == false) {
		return NULL;
	}

	return AerospikeClient_InfoRandomNode_Invoke(&err, self, py_command,
												 py_policy);
}