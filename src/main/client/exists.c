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
 * @param py_key                The key under which the record is stored.
 * @param py_policy             The dictionary of policies
 *
 * Returns a tuple of record having key and meta sequentially.
 *******************************************************************************************************
 */
extern PyObject *AerospikeClient_Exists_Invoke(AerospikeClient *self,
											   PyObject *py_key,
											   PyObject *py_policy)
{
	// Python Return Value
	PyObject *py_result = NULL;

	// Aerospike Client Arguments
	as_error err;
	as_policy_read read_policy;
	as_policy_read *read_policy_p = NULL;
	as_key key;
	as_record *rec = NULL;

	// For converting expressions.
	as_exp exp_list;
	as_exp *exp_list_p = NULL;

	// For converting predexp.
	as_predexp_list predexp_list;
	as_predexp_list *predexp_list_p = NULL;

	// Initialisation flags
	bool key_initialised = false;

	// Initialize error
	as_error_init(&err);

	if (!self || !self->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}

	if (!self->is_conn_16) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER,
						"No connection to aerospike cluster");
		goto CLEANUP;
	}

	// Convert python key object to as_key
	pyobject_to_key(&err, py_key, &key);
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}
	// key is initialised successfully
	key_initialised = true;

	// Convert python policy object to as_policy_exists
	pyobject_to_policy_read(self, &err, py_policy, &read_policy, &read_policy_p,
							&self->as->config.policies.read, &predexp_list,
							&predexp_list_p, &exp_list, &exp_list_p);
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	// Invoke operation
	Py_BEGIN_ALLOW_THREADS
	aerospike_key_exists(self->as, &err, read_policy_p, &key, &rec);
	Py_END_ALLOW_THREADS

	if (err.code == AEROSPIKE_OK) {
		PyObject *py_result_key = NULL;
		PyObject *py_result_meta = NULL;

		key_to_pyobject(&err, &key, &py_result_key);
		metadata_to_pyobject(&err, rec, &py_result_meta);

		py_result = PyTuple_New(2);
		PyTuple_SetItem(py_result, 0, py_result_key);
		PyTuple_SetItem(py_result, 1, py_result_meta);
	}
	else if (err.code == AEROSPIKE_ERR_RECORD_NOT_FOUND) {
		as_error_reset(&err);

		PyObject *py_result_key = NULL;
		PyObject *py_result_meta = Py_None;

		key_to_pyobject(&err, &key, &py_result_key);

		py_result = PyTuple_New(2);
		PyTuple_SetItem(py_result, 0, py_result_key);
		PyTuple_SetItem(py_result, 1, py_result_meta);

		Py_INCREF(py_result_meta);
	}
	else {
		as_error_update(&err, err.code, NULL);
	}

CLEANUP:

	if (exp_list_p) {
		as_exp_destroy(exp_list_p);
		;
	}

	if (predexp_list_p) {
		as_predexp_list_destroy(&predexp_list);
	}

	if (key_initialised == true) {
		// Destroy the key if it is initialised successfully.
		as_key_destroy(&key);
	}

	if (rec) {
		as_record_destroy(rec);
	}

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
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
	}

	return py_result;
}

/**
 *******************************************************************************************************
 * Checks if a record exists in the Aerospike DB.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns a tuple of record having key and meta sequentially.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_Exists(AerospikeClient *self, PyObject *args,
								 PyObject *kwds)
{
	// Python Function Arguments
	PyObject *py_key = NULL;
	PyObject *py_policy = NULL;

	// Python Function Keyword Arguments
	static char *kwlist[] = {"key", "policy", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "O|O:exists", kwlist, &py_key,
									&py_policy) == false) {
		return NULL;
	}

	// Invoke Operation
	return AerospikeClient_Exists_Invoke(self, py_key, py_policy);
}
