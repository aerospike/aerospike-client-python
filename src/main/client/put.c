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
 * This function will put record to the Aerospike DB.
 *
 * @param self                  AerospikeClient object
 * @param py_key                The key under which to store the record.
 * @param py_bins               The data to write to the Aerospike DB.
 * @param py_meta               The meatadata for the record.
 * @param py_policy             The dictionary of policies to be given while
 *                              reading a record.
 *
 * Returns an integer status. 0(Zero) is success value.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_Put_Invoke(AerospikeClient *self, PyObject *py_key,
									 PyObject *py_bins, PyObject *py_meta,
									 PyObject *py_policy,
									 long serializer_option)
{
	// Aerospike Client Arguments
	as_error err;
	as_policy_write write_policy;
	as_policy_write *write_policy_p = NULL;
	as_key key;
	as_record rec;

	// For converting predexp.
	as_exp exp_list;
	as_exp *exp_list_p = NULL;

	// For converting predexp.
	as_predexp_list predexp_list;
	as_predexp_list *predexp_list_p = NULL;

	// Initialisation flags
	bool key_initialised = false;
	bool record_initialised = false;

	// Initialize record
	as_record_init(&rec, 0);
	record_initialised = true;

	as_static_pool static_pool;
	memset(&static_pool, 0, sizeof(static_pool));

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
	// Key is initialised successfully.
	key_initialised = true;

	// Convert python bins and metadata objects to as_record
	pyobject_to_record(self, &err, py_bins, py_meta, &rec, serializer_option,
					   &static_pool);
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	// Convert python policy object to as_policy_write
	pyobject_to_policy_write(self, &err, py_policy, &write_policy,
							 &write_policy_p, &self->as->config.policies.write,
							 &predexp_list, &predexp_list_p, &exp_list,
							 &exp_list_p);
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	// Invoke operation
	Py_BEGIN_ALLOW_THREADS
	aerospike_key_put(self->as, &err, write_policy_p, &key, &rec);
	Py_END_ALLOW_THREADS
	if (err.code != AEROSPIKE_OK) {
		as_error_update(&err, err.code, NULL);
	}

CLEANUP:
	POOL_DESTROY(&static_pool);

	if (exp_list_p) {
		as_exp_destroy(exp_list_p);
		;
	}

	if (predexp_list_p) {
		as_predexp_list_destroy(&predexp_list);
	}

	if (key_initialised == true) {
		// Destroy the key if it is initialised.
		as_key_destroy(&key);
	}
	if (record_initialised == true) {
		// Destroy the record if it is initialised.
		as_record_destroy(&rec);
	}

	// If an error occurred, tell Python.
	if (err.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		if (PyObject_HasAttrString(exception_type, "key")) {
			PyObject_SetAttrString(exception_type, "key", py_key);
		}
		if (PyObject_HasAttrString(exception_type, "bin")) {
			PyObject_SetAttrString(exception_type, "bin", py_bins);
		}
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return PyLong_FromLong(0);
}

/**
 *******************************************************************************************************
 * Puts a record to the Aerospike DB.
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
PyObject *AerospikeClient_Put(AerospikeClient *self, PyObject *args,
							  PyObject *kwds)
{
	// Python Function Arguments
	PyObject *py_key = NULL;
	PyObject *py_bins = NULL;
	PyObject *py_meta = NULL;
	PyObject *py_policy = NULL;
	PyObject *py_serializer_option = NULL;
	long serializer_option = SERIALIZER_PYTHON;

	// Python Function Keyword Arguments
	static char *kwlist[] = {"key",	   "bins",		 "meta",
							 "policy", "serializer", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OO|OOO:put", kwlist, &py_key,
									&py_bins, &py_meta, &py_policy,
									&py_serializer_option) == false) {
		return NULL;
	}

	if (py_serializer_option) {
		if (PyInt_Check(py_serializer_option) ||
			PyLong_Check(py_serializer_option)) {
			self->is_client_put_serializer = true;
			serializer_option = PyLong_AsLong(py_serializer_option);
		}
	}
	else {
		self->is_client_put_serializer = false;
	}
	// Invoke Operation
	return AerospikeClient_Put_Invoke(self, py_key, py_bins, py_meta, py_policy,
									  serializer_option);
}
