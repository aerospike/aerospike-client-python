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

PyObject * AerospikeClient_Put_Invoke(
	AerospikeClient * self,
	PyObject * py_key, PyObject * py_bins, PyObject * py_meta, PyObject * py_policy)
{
	// Aerospike Client Arguments
	as_error err;
	as_policy_write write_policy;
	as_policy_write * write_policy_p = NULL;
	as_key key;
	as_record rec;
	// Initialize record
	as_record_init(&rec, 0);

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

	// Convert python bins and metadata objects to as_record
	pyobject_to_record(&err, py_bins, py_meta, &rec);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

	// Convert python policy object to as_policy_write
	pyobject_to_policy_write(&err, py_policy, &write_policy, &write_policy_p);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

	// Invoke operation
	aerospike_key_put(self->as, &err, write_policy_p, &key, &rec);

CLEANUP:

	as_record_destroy(&rec);

	// If an error occurred, tell Python.
	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return PyLong_FromLong(0);
}

PyObject * AerospikeClient_Put(AerospikeClient * self, PyObject * args, PyObject * kwds)
{
	// Python Function Arguments
	PyObject * py_key = NULL;
	PyObject * py_bins = NULL;
	PyObject * py_meta = NULL;
	PyObject * py_policy = NULL;

	// Python Function Keyword Arguments
	static char * kwlist[] = {"key", "record", "metadata", "policy", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "OO|OO:put", kwlist,
			&py_key, &py_bins, &py_meta, &py_policy) == false ) {
		return NULL;
	}

	// Invoke Operation
	return AerospikeClient_Put_Invoke(self,
		py_key, py_bins, py_meta, py_policy
		);
}
