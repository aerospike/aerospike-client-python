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
 * This function invokes csdk's API to remove particular record.
 *
 * @param self                  AerospikeClient object
 * @param py_key                The key under which to store the record
 * @param generation            The generation value
 * @param py_policy             The optional policy parameters
 *
 * Returns 0 on success.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_Remove_Invoke(AerospikeClient *self, PyObject *py_key,
										PyObject *py_meta, PyObject *py_policy)
{

	// Aerospike Client Arguments
	as_error err;
	as_policy_remove remove_policy;
	as_policy_remove *remove_policy_p = NULL;
	as_key key;

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
	// Key is initialised successfully
	key_initialised = true;

	// Convert python policy object to as_policy_exists
	if (py_policy) {
		pyobject_to_policy_remove(
			self, &err, py_policy, &remove_policy, &remove_policy_p,
			&self->as->config.policies.remove, &predexp_list, &predexp_list_p,
			&exp_list, &exp_list_p);
		if (err.code != AEROSPIKE_OK) {
			goto CLEANUP;
		}
		else {
			if (py_meta && PyDict_Check(py_meta)) {
				PyObject *py_gen = PyDict_GetItemString(py_meta, "gen");

				if (py_gen) {
					if (PyInt_Check(py_gen)) {
						remove_policy_p->generation =
							(uint16_t)PyInt_AsLong(py_gen);
					}
					else if (PyLong_Check(py_gen)) {
						remove_policy_p->generation =
							(uint16_t)PyLong_AsLongLong(py_gen);
						if ((uint16_t)-1 == remove_policy_p->generation &&
							PyErr_Occurred()) {
							as_error_update(
								&err, AEROSPIKE_ERR_PARAM,
								"integer value for gen exceeds sys.maxsize");
							goto CLEANUP;
						}
					}
					else {
						as_error_update(&err, AEROSPIKE_ERR_PARAM,
										"Generation should be an int or long");
					}
				}
			}
		}
	}

	// Invoke operation
	Py_BEGIN_ALLOW_THREADS
	aerospike_key_remove(self->as, &err, remove_policy_p, &key);
	Py_END_ALLOW_THREADS
	if (err.code != AEROSPIKE_OK) {
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

	return PyLong_FromLong(0);
}

/**
 *******************************************************************************************************
 * Removes a particular record matching with the given key.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns an integer status. 0(Zero) is success value.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_Remove(AerospikeClient *self, PyObject *args,
								 PyObject *kwds)
{
	// Python Function Arguments
	PyObject *py_key = NULL;
	PyObject *py_policy = NULL;
	PyObject *py_meta = NULL;

	// Python Function Keyword Arguments
	static char *kwlist[] = {"key", "meta", "policy", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "O|OO:remove", kwlist, &py_key,
									&py_meta, &py_policy) == false) {
		return NULL;
	}

	// Invoke Operation
	return AerospikeClient_Remove_Invoke(self, py_key, py_meta, py_policy);
}
