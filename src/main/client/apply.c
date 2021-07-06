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

#include <aerospike/aerospike_key.h>
#include <aerospike/as_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_record.h>

#include "client.h"
#include "conversions.h"
#include "exceptions.h"
#include "policy.h"

/**
 *******************************************************************************************************
 * This function applies a registered udf module on a particular record.
 *
 * @param self                  AerospikeClient object
 * @param py_key                The key under which to store the record.
 * @param py_module             The module name.
 * @param py_function           The UDF function to be applied on a record.
 * @param py_arglist            The arguments to the UDF function
 * @param py_policy             The optional policy parameters
 *
 * Returns the result of UDF function.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_Apply_Invoke(AerospikeClient *self, PyObject *py_key,
									   PyObject *py_module,
									   PyObject *py_function,
									   PyObject *py_arglist,
									   PyObject *py_policy)
{
	// Python Return Value
	PyObject *py_result = NULL;

	// Aerospike Client Arguments
	as_error err;
	as_policy_apply apply_policy;
	as_policy_apply *apply_policy_p = NULL;
	as_key key;
	char *module = NULL;
	char *function = NULL;
	as_list *arglist = NULL;
	as_val *result = NULL;

	PyObject *py_umodule = NULL;
	PyObject *py_ufunction = NULL;

	// For converting expressions.
	as_exp exp_list;
	as_exp *exp_list_p = NULL;

	// For converting predexp.
	as_predexp_list predexp_list;
	as_predexp_list *predexp_list_p = NULL;

	as_static_pool static_pool;
	memset(&static_pool, 0, sizeof(static_pool));
	// Initialisation flags
	bool key_initialised = false;

	// Initialize error
	as_error_init(&err);

	if (!PyList_Check(py_arglist)) {
		PyErr_SetString(PyExc_TypeError,
						"expected UDF method arguments in a 'list'");
		return NULL;
	}

	if (!self || !self->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}

	if (!self->is_conn_16) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER,
						"No connection to aerospike cluster");
		goto CLEANUP;
	}

	self->is_client_put_serializer = false;
	// Convert python key object to as_key
	pyobject_to_key(&err, py_key, &key);
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}
	// Key is initialiased successfully
	key_initialised = true;

	// Convert python list to as_list
	pyobject_to_list(self, &err, py_arglist, &arglist, &static_pool,
					 SERIALIZER_PYTHON);
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	// Convert python policy object to as_policy_apply
	pyobject_to_policy_apply(self, &err, py_policy, &apply_policy,
							 &apply_policy_p, &self->as->config.policies.apply,
							 &predexp_list, &predexp_list_p, &exp_list,
							 &exp_list_p);
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	if (PyUnicode_Check(py_module)) {
		py_umodule = PyUnicode_AsUTF8String(py_module);
		module = PyBytes_AsString(py_umodule);
	}
	else if (PyString_Check(py_module)) {
		module = PyString_AsString(py_module);
	}
	else {
		as_error_update(
			&err, AEROSPIKE_ERR_CLIENT,
			"udf module argument must be a string or unicode string");
		goto CLEANUP;
	}

	if (PyUnicode_Check(py_function)) {
		py_ufunction = PyUnicode_AsUTF8String(py_function);
		function = PyBytes_AsString(py_ufunction);
	}
	else if (PyString_Check(py_function)) {
		function = PyString_AsString(py_function);
	}
	else {
		as_error_update(&err, AEROSPIKE_ERR_CLIENT,
						"function name must be a string or unicode string");
		goto CLEANUP;
	}

	// Invoke operation
	Py_BEGIN_ALLOW_THREADS
	aerospike_key_apply(self->as, &err, apply_policy_p, &key, module, function,
						arglist, &result);
	Py_END_ALLOW_THREADS

	if (err.code == AEROSPIKE_OK) {
		val_to_pyobject(self, &err, result, &py_result);
	}
	else {
		as_error_update(&err, err.code, NULL);
	}

CLEANUP:
	if (exp_list_p) {
		as_exp_destroy(exp_list_p);
	}

	if (predexp_list_p) {
		as_predexp_list_destroy(&predexp_list);
	}

	if (py_umodule) {
		Py_DECREF(py_umodule);
	}

	if (py_ufunction) {
		Py_DECREF(py_ufunction);
	}

	if (key_initialised == true) {
		// Destroy the key if it is initialised successfully.
		as_key_destroy(&key);
	}
	as_list_destroy(arglist);
	as_val_destroy(result);

	if (err.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		if (PyObject_HasAttrString(exception_type, "key")) {
			PyObject_SetAttrString(exception_type, "key", py_key);
		}
		if (PyObject_HasAttrString(exception_type, "bin")) {
			PyObject_SetAttrString(exception_type, "bin", Py_None);
		}
		if (PyObject_HasAttrString(exception_type, "module")) {
			PyObject_SetAttrString(exception_type, "module", py_module);
		}
		if (PyObject_HasAttrString(exception_type, "func")) {
			PyObject_SetAttrString(exception_type, "func", py_function);
		}
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return py_result;
}

/**
 *******************************************************************************************************
 * Applies a registered UDF module on a particular record.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns the result of the udf function applied on the record.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_Apply(AerospikeClient *self, PyObject *args,
								PyObject *kwds)
{
	// Python Function Arguments
	PyObject *py_key = NULL;
	PyObject *py_module = NULL;
	PyObject *py_function = NULL;
	PyObject *py_arglist = NULL;
	PyObject *py_policy = NULL;

	// Python Function Keyword Arguments
	static char *kwlist[] = {"key",	 "module", "function",
							 "args", "policy", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OOOO|O:apply", kwlist, &py_key,
									&py_module, &py_function, &py_arglist,
									&py_policy) == false) {
		return NULL;
	}

	// Invoke Operation
	return AerospikeClient_Apply_Invoke(self, py_key, py_module, py_function,
										py_arglist, py_policy);
}
