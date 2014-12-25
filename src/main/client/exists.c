/*******************************************************************************
 * Copyright 2013-2014 Aerospike, Inc.
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
#include "key.h"
#include "policy.h"

PyObject * AerospikeClient_Exists_Invoke(
	AerospikeClient * self,
	PyObject * py_key, PyObject * py_policy)
{
	// Python Return Value
	PyObject * py_result = NULL;

	// Aerospike Client Arguments
	as_error err;
	as_policy_read read_policy;
	as_policy_read * read_policy_p = NULL;
	as_key key;
	as_record * rec = NULL;

	// Initialize error
	as_error_init(&err);

	if (!self || !self->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}

	// Convert python key object to as_key
	pyobject_to_key(&err, py_key, &key);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

	// Convert python policy object to as_policy_exists
	pyobject_to_policy_read(&err, py_policy, &read_policy, &read_policy_p);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

	// Invoke operation
	aerospike_key_exists(self->as, &err, read_policy_p, &key, &rec);

	if ( err.code == AEROSPIKE_OK ) {

		PyObject * py_result_key = NULL;
		PyObject * py_result_meta = NULL;

		key_to_pyobject(&err, &key, &py_result_key);
		metadata_to_pyobject(&err, rec, &py_result_meta);

		py_result = PyTuple_New(2);
		PyTuple_SetItem(py_result, 0, py_result_key);
		PyTuple_SetItem(py_result, 1, py_result_meta);
	}
	else if ( err.code == AEROSPIKE_ERR_RECORD_NOT_FOUND ) {
		as_error_reset(&err);

		PyObject * py_result_key = NULL;
		PyObject * py_result_meta = Py_None;

		key_to_pyobject(&err, &key, &py_result_key);

		py_result = PyTuple_New(2);
		PyTuple_SetItem(py_result, 0, py_result_key);
		PyTuple_SetItem(py_result, 1, py_result_meta);

		Py_INCREF(py_result_meta);
	}

CLEANUP:

	as_record_destroy(rec);

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return py_result;
}

PyObject * AerospikeClient_Exists(AerospikeClient * self, PyObject * args, PyObject * kwds)
{
	// Python Function Arguments
	PyObject * py_key = NULL;
	PyObject * py_policy = NULL;

	// Python Function Keyword Arguments
	static char * kwlist[] = {"key", "policy", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "O|O:exists", kwlist,
			&py_key, &py_policy) == false ) {
		return NULL;
	}

	// Invoke Operation
	return AerospikeClient_Exists_Invoke(self, py_key, py_policy);
}
