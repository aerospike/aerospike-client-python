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

PyObject * AerospikeClient_Apply_Invoke(
	AerospikeClient * self,
	PyObject * py_key, PyObject * py_module, PyObject * py_function,
	PyObject * py_arglist, PyObject * py_policy)
{
	// Python Return Value
	PyObject * py_result = NULL;

	// Aerospike Client Arguments
	as_error err;
	as_policy_apply apply_policy;
	as_policy_apply * apply_policy_p = NULL;
	as_key key;
	char * module = NULL;
	char * function = NULL;
	as_list * arglist = NULL;
	as_val * result = NULL;

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

	// Convert python list to as_list
	pyobject_to_list(&err, py_arglist, &arglist);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

	// Convert python policy object to as_policy_apply
	pyobject_to_policy_apply(&err, py_policy, &apply_policy, &apply_policy_p);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

	module = PyString_AsString(py_module);
	function = PyString_AsString(py_function);

	// Invoke operation
	aerospike_key_apply(self->as, &err, apply_policy_p, &key, module, function, arglist, &result);

	if ( err.code == AEROSPIKE_OK ) {
		val_to_pyobject(&err, result, &py_result);
	}

CLEANUP:

	as_list_destroy(arglist);
	as_val_destroy(result);

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return py_result;
}


PyObject * AerospikeClient_Apply(AerospikeClient * self, PyObject * args, PyObject * kwds)
{
	// Python Function Arguments
	PyObject * py_key = NULL;
	PyObject * py_module = NULL;
	PyObject * py_function = NULL;
	PyObject * py_arglist = NULL;
	PyObject * py_policy = NULL;

	// Python Function Keyword Arguments
	static char * kwlist[] = {"key", "module", "function", "args", "policy", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "OOOO|O:apply", kwlist,
			&py_key, &py_module, &py_function, &py_arglist, &py_policy) == false ) {
		return NULL;
	}

	// Invoke Operation
	return AerospikeClient_Apply_Invoke(self, py_key, py_module, py_function,
		py_arglist, py_policy);
}
