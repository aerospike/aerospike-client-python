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
 *******************************************************************************************************
 * Set the cluster's xdr filter using an Aerospike expression.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns a server response for the particular request string.
 * In case of error, appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_SetXDRFilter(AerospikeClient *self, PyObject *args,
									   PyObject *kwds)
{
	// function args
	PyObject *py_data_center = NULL;
	PyObject *py_namespace = NULL;
	PyObject *py_expression_filter = NULL;
	PyObject *py_policy = NULL;

	// utility vars
	char *fmt_str = "xdr-set-filter:dc=%s;namespace=%s;exp=%s";
	const char *DELETE_CURRENT_XDR_FILTER = "null";
	char *base64_filter = NULL;
	char *base64_filter_to_free = NULL;
	as_exp *exp_list_p = NULL;
	char *request_str_p = NULL;
	char *response_p = NULL;

	PyObject *py_response = NULL;

	as_error err;
	as_error_init(&err);

	static char *kwlist[] = {"data_center", "namespace", "expression_filter",
							 "policy", NULL};
	if (PyArg_ParseTupleAndKeywords(
			args, kwds, "OOO|O:set_xdr_filter", kwlist, &py_data_center,
			&py_namespace, &py_expression_filter, &py_policy) == false) {
		return NULL;
	}

	const char *data_center_str_p = NULL;
	if (PyUnicode_Check(py_data_center)) {
		data_center_str_p = PyUnicode_AsUTF8(py_data_center);
	}
	else {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"Data_center should be a string.");
		goto CLEANUP;
	}

	const char *namespace_str_p = NULL;
	if (PyUnicode_Check(py_namespace)) {
		namespace_str_p = PyUnicode_AsUTF8(py_namespace);
	}
	else {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"Namespace should be a string.");
		goto CLEANUP;
	}

	//convert filter to base64
	if (py_expression_filter == Py_None) {
		base64_filter = (char *)DELETE_CURRENT_XDR_FILTER;
	}
	else {
		if (convert_exp_list(self, py_expression_filter, &exp_list_p, &err) !=
			AEROSPIKE_OK) {
			goto CLEANUP;
		}

		base64_filter = as_exp_compile_b64(exp_list_p);
		base64_filter_to_free = base64_filter;
		as_exp_destroy(exp_list_p);
	}

	as_policy_info info_policy;
	as_policy_info *info_policy_p = NULL;
	if (py_policy) {
		if (pyobject_to_policy_info(
				&err, py_policy, &info_policy, &info_policy_p,
				&self->as->config.policies.info) != AEROSPIKE_OK) {
			goto CLEANUP;
		}
	}

	// - 6 for format char
	uint request_length = strlen(fmt_str) + strlen(data_center_str_p) +
						  strlen(namespace_str_p) + strlen(base64_filter) + 1 -
						  6;
	request_str_p = cf_malloc(request_length * sizeof(char));
	if (request_str_p == NULL) {
		as_error_update(&err, AEROSPIKE_ERR_CLIENT,
						"Failed to allocate memory for request.");
		goto CLEANUP;
	}

	sprintf(request_str_p, fmt_str, data_center_str_p, namespace_str_p,
			base64_filter);

	as_status status = AEROSPIKE_OK;
	Py_BEGIN_ALLOW_THREADS
	status = aerospike_info_any(self->as, &err, info_policy_p, request_str_p,
								&response_p);
	Py_END_ALLOW_THREADS

	if (err.code == AEROSPIKE_OK) {
		if (response_p != NULL && status == AEROSPIKE_OK) {
			py_response = PyUnicode_FromString(response_p);
		}
		else if (response_p == NULL) {
			as_error_update(&err, AEROSPIKE_ERR_CLIENT,
							"Invalid info operation.");
			goto CLEANUP;
		}
		else if (status != AEROSPIKE_OK) {
			as_error_update(&err, status, "Info operation failed.");
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

	if (request_str_p != NULL) {
		cf_free(request_str_p);
	}

	if (base64_filter_to_free != NULL) {
		cf_free(base64_filter_to_free);
	}

	if (err.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return py_response;
}