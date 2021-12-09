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
 ******************************************************************************************************
 * Removes a bin from a record.
 *
 * @param self                  AerospikeClient object
 * @prama py_key                The key for the record.
 * @pram py_binList             The name of the bins to be removed from the record.
 * @param py_policy             The optional policies.
 * @param err                   The C client's as_error to be set to the encountered error.
 *
 * Returns an integer status. 0(Zero) is success value.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
static PyObject *
AerospikeClient_RemoveBin_Invoke(AerospikeClient *self, PyObject *py_key,
								 PyObject *py_binList, PyObject *py_policy,
								 PyObject *py_meta, as_error *err)
{

	// Aerospike Client Arguments
	as_policy_write write_policy;
	as_policy_write *write_policy_p = NULL;
	as_key key;
	bool key_initialized = false;
	as_record rec;
	char *binName = NULL;
	int count = 0;
	PyObject *py_ustr = NULL;

	// For converting expressions.
	as_exp exp_list;
	as_exp *exp_list_p = NULL;

	// For converting predexp.
	as_predexp_list predexp_list;
	as_predexp_list *predexp_list_p = NULL;

	// Get the bin list size;
	Py_ssize_t size = PyList_Size(py_binList);
	// Initialize record
	as_record_inita(&rec, size);

	// Convert python key object to as_key
	pyobject_to_key(err, py_key, &key);
	if (err->code != AEROSPIKE_OK) {
		goto CLEANUP;
	}
	key_initialized = true;

	// Convert python policy object to as_policy_write
	pyobject_to_policy_write(self, err, py_policy, &write_policy,
							 &write_policy_p, &self->as->config.policies.write,
							 &predexp_list, &predexp_list_p, &exp_list,
							 &exp_list_p);
	if (err->code != AEROSPIKE_OK) {
		as_error_update(err, AEROSPIKE_ERR_CLIENT, "Incorrect policy");
		goto CLEANUP;
	}

	// Invoke operation

	for (count = 0; count < size; count++) {
		PyObject *py_val = PyList_GetItem(py_binList, count);
		if (PyUnicode_Check(py_val)) {
			py_ustr = PyUnicode_AsUTF8String(py_val);
			binName = PyBytes_AsString(py_ustr);
		}
		else if (PyString_Check(py_val)) {
			binName = PyString_AsString(py_val);
		}
		else {
			as_error_update(err, AEROSPIKE_ERR_CLIENT,
							"Invalid bin name, bin name should be a string or "
							"unicode string") goto CLEANUP;
		}
		if (!as_record_set_nil(&rec, binName)) {
			goto CLEANUP;
		}
		if (py_ustr) {
			Py_DECREF(py_ustr);
			py_ustr = NULL;
		}
	}

	if (py_meta && PyDict_Check(py_meta)) {
		PyObject *py_gen = PyDict_GetItemString(py_meta, "gen");
		PyObject *py_ttl = PyDict_GetItemString(py_meta, "ttl");

		if (py_ttl) {
			if (PyInt_Check(py_ttl)) {
				rec.ttl = (uint32_t)PyInt_AsLong(py_ttl);
			}
			else if (PyLong_Check(py_ttl)) {
				rec.ttl = (uint32_t)PyLong_AsLongLong(py_ttl);
				if ((uint32_t)-1 == rec.ttl && PyErr_Occurred()) {
					as_error_update(
						err, AEROSPIKE_ERR_PARAM,
						"integer value for ttl exceeds sys.maxsize");
					goto CLEANUP;
				}
			}
			else {
				as_error_update(err, AEROSPIKE_ERR_PARAM,
								"Ttl should be an int or long");
				goto CLEANUP;
			}
		}

		if (py_gen) {
			if (PyInt_Check(py_gen)) {
				rec.gen = (uint16_t)PyInt_AsLong(py_gen);
			}
			else if (PyLong_Check(py_gen)) {
				rec.gen = (uint16_t)PyLong_AsLongLong(py_gen);
				if ((uint16_t)-1 == rec.gen && PyErr_Occurred()) {
					as_error_update(
						err, AEROSPIKE_ERR_PARAM,
						"integer value for gen exceeds sys.maxsize");
					goto CLEANUP;
				}
			}
			else {
				as_error_update(err, AEROSPIKE_ERR_PARAM,
								"Generation should be an int or long");
				goto CLEANUP;
			}
		}
	}

	Py_BEGIN_ALLOW_THREADS
	aerospike_key_put(self->as, err, write_policy_p, &key, &rec);
	Py_END_ALLOW_THREADS
	if (err->code != AEROSPIKE_OK) {
		as_error_update(err, err->code, NULL);
		goto CLEANUP;
	}

CLEANUP:

	as_record_destroy(&rec);

	if (exp_list_p) {
		as_exp_destroy(exp_list_p);
		;
	}

	if (predexp_list_p) {
		as_predexp_list_destroy(&predexp_list);
	}

	if (key_initialized) {
		as_key_destroy(&key);
	}

	if (err->code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(err, &py_err);
		PyObject *exception_type = raise_exception(err);
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
 ******************************************************************************************************
 * Removes a bin from a record.
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
PyObject *AerospikeClient_RemoveBin(AerospikeClient *self, PyObject *args,
									PyObject *kwds)
{
	// Python Function Arguments
	PyObject *py_key = NULL;
	PyObject *py_policy = NULL;
	PyObject *py_binList = NULL;
	PyObject *py_result = NULL;
	PyObject *py_meta = NULL;

	as_error err;
	// Initialize error
	as_error_init(&err);

	// Python Function Keyword Arguments
	static char *kwlist[] = {"key", "list", "meta", "policy", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OO|OO:remove_bin", kwlist,
									&py_key, &py_binList, &py_meta,
									&py_policy) == false) {
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

	if (!PyList_Check(py_binList)) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Bins should be a list");
		goto CLEANUP;
	}

	// Invoke Operation
	return AerospikeClient_RemoveBin_Invoke(self, py_key, py_binList, py_policy,
											py_meta, &err);

CLEANUP:

	if (err.code != AEROSPIKE_OK || !py_result) {
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
	return NULL;
}
