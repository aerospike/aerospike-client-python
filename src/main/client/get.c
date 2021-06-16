/*******************************************************************************
 * Copyright 2013-2020 Aerospike, Inc.
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
 * This function read a record with a given key, and return the record.
 *
 * @param self                  AerospikeClient object
 * @param py_key                The key under which to store the record.
 * @param py_policy             The dictionary of policies to be given while
 *                              reading a record.
 *
 * Returns the record on success.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_Get_Invoke(AerospikeClient *self, PyObject *py_key,
									 PyObject *py_policy)
{
	// Python Return Value
	PyObject *py_rec = NULL;

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

	// Initialised flags
	bool key_initialised = false;
	bool record_initialised = false;

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
	// Key is successfully initialised.
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
	aerospike_key_get(self->as, &err, read_policy_p, &key, &rec);
	Py_END_ALLOW_THREADS
	if (err.code == AEROSPIKE_OK) {
		record_initialised = true;

		if (record_to_pyobject(self, &err, rec, &key, &py_rec) !=
			AEROSPIKE_OK) {
			goto CLEANUP;
		}
		if (!read_policy_p ||
			(read_policy_p && read_policy_p->key == AS_POLICY_KEY_DIGEST)) {
			// This is a special case.
			// C-client returns NULL key, so to the user
			// response will be (<ns>, <set>, None, <digest>)
			// Using the same input key, just making primary key part to be None
			// Only in case of POLICY_KEY_DIGEST or no policy specified
			PyObject *p_key = PyTuple_GetItem(py_rec, 0);
			Py_INCREF(Py_None);
			PyTuple_SetItem(p_key, 2, Py_None);
		}
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
		// Destroy key only if it is initialised.
		as_key_destroy(&key);
	}

	if (rec && record_initialised) {
		// Destroy record only if it is initialised.
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
		return NULL;
	}

	return py_rec;
}

/**
 *******************************************************************************************************
 * Gets a record from the Aerospike DB.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns a tuple of record having key, meata and bins sequentially.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_Get(AerospikeClient *self, PyObject *args,
							  PyObject *kwds)
{
	// Python Function Arguments
	PyObject *py_key = NULL;
	PyObject *py_policy = NULL;

	// Python Function Keyword Arguments
	static char *kwlist[] = {"key", "policy", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "O|O:get", kwlist, &py_key,
									&py_policy) == false) {
		return NULL;
	}

	// Invoke Operation
	return AerospikeClient_Get_Invoke(self, py_key, py_policy);
}
