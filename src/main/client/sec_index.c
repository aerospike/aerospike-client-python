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

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_index.h>
#include <aerospike/as_bin.h>
#include <aerospike/as_config.h>
#include <aerospike/as_error.h>
#include <aerospike/as_policy.h>

#include "client.h"
#include "conversions.h"
#include "policy.h"


PyObject * AerospikeClient_Index_Integer_Create(AerospikeClient * self, PyObject *args, PyObject * kwds)
{
	// Initialize error
	as_error err;
	as_error_init(&err);

	// Python Function Arguments
	PyObject * py_policy = NULL;
	PyObject * py_ns = NULL;
	PyObject * py_set = NULL;
	PyObject * py_bin = NULL;
	PyObject * py_name = NULL;
	as_policy_info info_policy;
	as_policy_info *info_policy_p = NULL;

	// Python Function Keyword Arguments
	static char * kwlist[] = {"policy", "ns", "set", "bin", "name", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "OOOOO:index_integer_create", kwlist,
				&py_policy, &py_ns, &py_set, &py_bin, &py_name) == false ) {
		return NULL;
	}

	if (!self || !self->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}

	// Convert python object to policy_info
	pyobject_to_policy_info( &err, py_policy, &info_policy, &info_policy_p);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

	// Convert python object into namespace string
	char ns[AS_NAMESPACE_MAX_SIZE];
	if( !PyString_Check(py_ns) ) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Namespace should be a string");
		goto CLEANUP;
	}
	char *namespace = PyString_AsString(py_ns);
	strncpy(ns, namespace, AS_NAMESPACE_MAX_SIZE);

	// Convert python object into set string
	char set[AS_SET_MAX_SIZE];
	if( !PyString_Check(py_set) ) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Set should be a string");
		goto CLEANUP;
	}
	char *set_ptr = PyString_AsString(py_set);
	strncpy(set, set_ptr, AS_SET_MAX_SIZE);

	// Convert python object into bin string
	char bin[AS_BIN_NAME_MAX_SIZE];
	if( !PyString_Check(py_bin) ) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Bin should be a string");
		goto CLEANUP;
	}
	char *bin_ptr = PyString_AsString(py_bin);
	strncpy(bin, bin_ptr, AS_BIN_NAME_MAX_SIZE);

	// Convert PyObject into the name of the index
	char *name;
	if( !PyString_Check(py_name) ) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Index name should be a string");
		goto CLEANUP;
	}

	name = PyString_AsString(py_name);

	// Invoke operation
	aerospike_index_integer_create(self->as, &err, info_policy_p, ns, set, bin, name);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

CLEANUP:

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return PyLong_FromLong(0);
}


PyObject * AerospikeClient_Index_String_Create(AerospikeClient * self, PyObject *args, PyObject * kwds)
{
	// Initialize error
	as_error err;
	as_error_init(&err);

	// Python Function Arguments
	PyObject * py_policy = NULL;
	PyObject * py_ns = NULL;
	PyObject * py_set = NULL;
	PyObject * py_bin = NULL;
	PyObject * py_name = NULL;

	as_policy_info info_policy;
	as_policy_info *info_policy_p = NULL;

	// Python Function Keyword Arguments
	static char * kwlist[] = {"policy", "ns", "set", "bin", "name", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "OOOOO:index_string_create", kwlist,
				&py_policy, &py_ns, &py_set, &py_bin, &py_name) == false ) {
		return NULL;
	}

	if (!self || !self->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}

	// Convert python object to policy_info
	pyobject_to_policy_info( &err, py_policy, &info_policy, &info_policy_p);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

	// Convert python object into namespace string
	char ns[AS_NAMESPACE_MAX_SIZE];
	if( !PyString_Check(py_ns) ) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Namespace should be a string");
		goto CLEANUP;
	}
	char *namespace = PyString_AsString(py_ns);
	strncpy(ns, namespace, AS_NAMESPACE_MAX_SIZE);

	// Convert python object into set string
	char set[AS_SET_MAX_SIZE];
	if( !PyString_Check(py_set) ) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Set should be a string");
		goto CLEANUP;
	}
	char *set_ptr = PyString_AsString(py_set);
	strncpy(set, set_ptr, AS_SET_MAX_SIZE);

	// Convert python object into bin string
	char bin[AS_BIN_NAME_MAX_SIZE];
	if( !PyString_Check(py_bin) ) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Bin should be a string");
		goto CLEANUP;
	}
	char *bin_ptr = PyString_AsString(py_bin);
	strncpy(bin, bin_ptr, AS_BIN_NAME_MAX_SIZE);

	// Convert PyObject into the name of the index
	char *name;
	if( !PyString_Check(py_name) ) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Index name should be a string");
		goto CLEANUP;
	}

	name = PyString_AsString(py_name);

	// Invoke operation
	aerospike_index_string_create(self->as, &err, info_policy_p, ns, set, bin, name);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

CLEANUP:

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return PyLong_FromLong(0);
}


PyObject * AerospikeClient_Index_Remove(AerospikeClient * self, PyObject *args, PyObject * kwds)
{
	// Initialize error
	as_error err;
	as_error_init(&err);

	// Python Function Arguments
	PyObject * py_policy = NULL;
	PyObject * py_ns = NULL;
	PyObject * py_name = NULL;

	as_policy_info info_policy;
	as_policy_info *info_policy_p = NULL;

	// Python Function Keyword Arguments
	static char * kwlist[] = {"policy", "ns", "name", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "OOO:index_remove", kwlist,
				&py_policy, &py_ns, &py_name) == false ) {
		return NULL;
	}

	if (!self || !self->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}

	// Convert python object to policy_info
	pyobject_to_policy_info( &err, py_policy, &info_policy, &info_policy_p);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

	// Convert python object into namespace string
	char ns[AS_NAMESPACE_MAX_SIZE];
	if( !PyString_Check(py_ns) ) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Namespace should be a string");
		goto CLEANUP;
	}
	char *namespace = PyString_AsString(py_ns);
	strncpy(ns, namespace, AS_NAMESPACE_MAX_SIZE);

	// Convert PyObject into the name of the index
	char *name;
	if( !PyString_Check(py_name) ) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Index name should be a string");
		goto CLEANUP;
	}

	name = PyString_AsString(py_name);

	// Invoke operation
	aerospike_index_remove(self->as, &err, info_policy_p, ns, name);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

CLEANUP:

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return PyLong_FromLong(0);
}
