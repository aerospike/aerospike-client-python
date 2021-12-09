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

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_index.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_bin.h>
#include <aerospike/as_config.h>
#include <aerospike/as_error.h>
#include <aerospike/as_policy.h>

#include "client.h"
#include "conversions.h"
#include "exceptions.h"
#include "policy.h"

static bool getDataTypeFromPyObject(PyObject *py_datatype,
									as_index_datatype *idx_datatype,
									as_error *err);

static PyObject *
createIndexWithCollectionType(AerospikeClient *self, PyObject *py_policy,
							  PyObject *py_ns, PyObject *py_set,
							  PyObject *py_bin, PyObject *py_name,
							  PyObject *py_datatype, as_index_type index_type);

static PyObject *createIndexWithDataAndCollectionType(
	AerospikeClient *self, PyObject *py_policy, PyObject *py_ns,
	PyObject *py_set, PyObject *py_bin, PyObject *py_name,
	as_index_type index_type, as_index_datatype data_type);

/**
 *******************************************************************************************************
 * Creates an integer index for a bin in the Aerospike DB.
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
PyObject *AerospikeClient_Index_Integer_Create(AerospikeClient *self,
											   PyObject *args, PyObject *kwds)
{
	// Initialize error
	as_error err;
	as_error_init(&err);

	// Python Function Arguments
	PyObject *py_policy = NULL;
	PyObject *py_ns = NULL;
	PyObject *py_set = NULL;
	PyObject *py_bin = NULL;
	PyObject *py_name = NULL;

	// Python Function Keyword Arguments
	static char *kwlist[] = {"ns", "set", "bin", "name", "policy", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OOOO|O:index_integer_create",
									kwlist, &py_ns, &py_set, &py_bin, &py_name,
									&py_policy) == false) {
		return NULL;
	}

	return createIndexWithDataAndCollectionType(
		self, py_policy, py_ns, py_set, py_bin, py_name, AS_INDEX_TYPE_DEFAULT,
		AS_INDEX_NUMERIC);
}

/**
 *******************************************************************************************************
 * Creates a string index for a bin in the Aerospike DB.
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
PyObject *AerospikeClient_Index_String_Create(AerospikeClient *self,
											  PyObject *args, PyObject *kwds)
{
	// Initialize error
	as_error err;
	as_error_init(&err);

	// Python Function Arguments
	PyObject *py_policy = NULL;
	PyObject *py_ns = NULL;
	PyObject *py_set = NULL;
	PyObject *py_bin = NULL;
	PyObject *py_name = NULL;

	// Python Function Keyword Arguments
	static char *kwlist[] = {"ns", "set", "bin", "name", "policy", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OOOO|O:index_string_create",
									kwlist, &py_ns, &py_set, &py_bin, &py_name,
									&py_policy) == false) {
		return NULL;
	}

	return createIndexWithDataAndCollectionType(
		self, py_policy, py_ns, py_set, py_bin, py_name, AS_INDEX_TYPE_DEFAULT,
		AS_INDEX_STRING);
}

/**
 *******************************************************************************************************
 * Removes an index in the Aerospike database.
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
PyObject *AerospikeClient_Index_Remove(AerospikeClient *self, PyObject *args,
									   PyObject *kwds)
{
	// Initialize error
	as_error err;
	as_error_init(&err);

	// Python Function Arguments
	PyObject *py_policy = NULL;
	PyObject *py_ns = NULL;
	PyObject *py_name = NULL;
	PyObject *py_ustr_name = NULL;

	as_policy_info info_policy;
	as_policy_info *info_policy_p = NULL;

	// Python Function Keyword Arguments
	static char *kwlist[] = {"ns", "name", "policy", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OO|O:index_remove", kwlist,
									&py_ns, &py_name, &py_policy) == false) {
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

	// Convert python object to policy_info
	pyobject_to_policy_info(&err, py_policy, &info_policy, &info_policy_p,
							&self->as->config.policies.info);
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	// Convert python object into namespace string
	if (!PyString_Check(py_ns)) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"Namespace should be a string");
		goto CLEANUP;
	}
	char *namespace = PyString_AsString(py_ns);

	// Convert PyObject into the name of the index
	char *name = NULL;
	if (PyUnicode_Check(py_name)) {
		py_ustr_name = PyUnicode_AsUTF8String(py_name);
		name = PyBytes_AsString(py_ustr_name);
	}
	else if (PyString_Check(py_name)) {
		name = PyString_AsString(py_name);
	}
	else {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"Index name should be string or unicode");
		goto CLEANUP;
	}

	// Invoke operation
	Py_BEGIN_ALLOW_THREADS
	aerospike_index_remove(self->as, &err, info_policy_p, namespace, name);
	Py_END_ALLOW_THREADS
	if (err.code != AEROSPIKE_OK) {
		as_error_update(&err, err.code, NULL);
		goto CLEANUP;
	}

CLEANUP:

	if (py_ustr_name) {
		Py_DECREF(py_ustr_name);
	}
	if (err.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		if (PyObject_HasAttrString(exception_type, "name")) {
			PyObject_SetAttrString(exception_type, "name", py_name);
		}
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return PyLong_FromLong(0);
}

PyObject *AerospikeClient_Index_List_Create(AerospikeClient *self,
											PyObject *args, PyObject *kwds)
{
	// Initialize error
	as_error err;
	as_error_init(&err);

	// Python Function Arguments
	PyObject *py_policy = NULL;
	PyObject *py_ns = NULL;
	PyObject *py_set = NULL;
	PyObject *py_bin = NULL;
	PyObject *py_name = NULL;
	PyObject *py_datatype = NULL;

	// Python Function Keyword Arguments
	static char *kwlist[] = {"ns",	 "set",	   "bin", "index_datatype",
							 "name", "policy", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(
			args, kwds, "OOOOO|O:index_list_create", kwlist, &py_ns, &py_set,
			&py_bin, &py_datatype, &py_name, &py_policy) == false) {
		return NULL;
	}

	return createIndexWithCollectionType(self, py_policy, py_ns, py_set, py_bin,
										 py_name, py_datatype,
										 AS_INDEX_TYPE_LIST);
}

PyObject *AerospikeClient_Index_Map_Keys_Create(AerospikeClient *self,
												PyObject *args, PyObject *kwds)
{
	// Initialize error
	as_error err;
	as_error_init(&err);

	// Python Function Arguments
	PyObject *py_policy = NULL;
	PyObject *py_ns = NULL;
	PyObject *py_set = NULL;
	PyObject *py_bin = NULL;
	PyObject *py_name = NULL;
	PyObject *py_datatype = NULL;

	// Python Function Keyword Arguments
	static char *kwlist[] = {"ns",	 "set",	   "bin", "index_datatype",
							 "name", "policy", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(
			args, kwds, "OOOOO|O:index_map_keys_create", kwlist, &py_ns,
			&py_set, &py_bin, &py_datatype, &py_name, &py_policy) == false) {
		return NULL;
	}

	return createIndexWithCollectionType(self, py_policy, py_ns, py_set, py_bin,
										 py_name, py_datatype,
										 AS_INDEX_TYPE_MAPKEYS);
}

PyObject *AerospikeClient_Index_Map_Values_Create(AerospikeClient *self,
												  PyObject *args,
												  PyObject *kwds)
{
	// Initialize error
	as_error err;
	as_error_init(&err);

	// Python Function Arguments
	PyObject *py_policy = NULL;
	PyObject *py_ns = NULL;
	PyObject *py_set = NULL;
	PyObject *py_bin = NULL;
	PyObject *py_name = NULL;
	PyObject *py_datatype = NULL;

	// Python Function Keyword Arguments
	static char *kwlist[] = {"ns",	 "set",	   "bin", "index_datatype",
							 "name", "policy", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(
			args, kwds, "OOOOO|O:index_map_values_create", kwlist, &py_ns,
			&py_set, &py_bin, &py_datatype, &py_name, &py_policy) == false) {
		return NULL;
	}

	return createIndexWithCollectionType(self, py_policy, py_ns, py_set, py_bin,
										 py_name, py_datatype,
										 AS_INDEX_TYPE_MAPVALUES);
}

PyObject *AerospikeClient_Index_2dsphere_Create(AerospikeClient *self,
												PyObject *args, PyObject *kwds)
{
	// Initialize error
	as_error err;
	as_error_init(&err);

	// Python Function Arguments
	PyObject *py_policy = NULL;
	PyObject *py_ns = NULL;
	PyObject *py_set = NULL;
	PyObject *py_bin = NULL;
	PyObject *py_name = NULL;

	// Python Function Keyword Arguments
	static char *kwlist[] = {"ns", "set", "bin", "name", "policy", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(
			args, kwds, "OOOO|O:index_geo2dsphere_create", kwlist, &py_ns,
			&py_set, &py_bin, &py_name, &py_policy) == false) {
		return NULL;
	}

	return createIndexWithDataAndCollectionType(
		self, py_policy, py_ns, py_set, py_bin, py_name, AS_INDEX_TYPE_DEFAULT,
		AS_INDEX_GEO2DSPHERE);
}

/*
 * Convert a PyObject into an as_index_datatype, return False if the conversion fails for any reason.
 */
static bool getDataTypeFromPyObject(PyObject *py_datatype,
									as_index_datatype *idx_datatype,
									as_error *err)
{

	long type = 0;
	if (PyInt_Check(py_datatype)) {
		type = PyInt_AsLong(py_datatype);
	}
	else if (PyLong_Check(py_datatype)) {
		type = PyLong_AsLong(py_datatype);
		if (type == -1 && PyErr_Occurred()) {
			if (PyErr_ExceptionMatches(PyExc_OverflowError)) {
				as_error_update(err, AEROSPIKE_ERR_PARAM,
								"integer value exceeds sys.maxsize");
				goto CLEANUP;
			}
		}
	}
	else {
		as_error_update(err, AEROSPIKE_ERR_PARAM,
						"Index type must be an integer");
		goto CLEANUP;
	}

	*idx_datatype = type;

CLEANUP:
	if (err->code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(err, &py_err);
		PyObject *exception_type = raise_exception(err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return false;
	}
	return true;
}

/*
 * Figure out the data_type from a PyObject and call createIndexWithDataAndCollectionType.
 */
static PyObject *
createIndexWithCollectionType(AerospikeClient *self, PyObject *py_policy,
							  PyObject *py_ns, PyObject *py_set,
							  PyObject *py_bin, PyObject *py_name,
							  PyObject *py_datatype, as_index_type index_type)
{

	as_index_datatype data_type = AS_INDEX_STRING;

	as_error err;
	as_error_init(&err);

	if (!getDataTypeFromPyObject(py_datatype, &data_type, &err)) {
		return NULL;
	}

	return createIndexWithDataAndCollectionType(
		self, py_policy, py_ns, py_set, py_bin, py_name, index_type, data_type);
}

/*
 * Create a complex index on the specified ns/set/bin with the given name and index and data_type. Return PyObject(0) on success
 * else return NULL with an error raised.
 */

static PyObject *createIndexWithDataAndCollectionType(
	AerospikeClient *self, PyObject *py_policy, PyObject *py_ns,
	PyObject *py_set, PyObject *py_bin, PyObject *py_name,
	as_index_type index_type, as_index_datatype data_type)
{

	// Initialize error
	as_error err;
	as_error_init(&err);

	PyObject *py_ustr_set = NULL;
	PyObject *py_ustr_bin = NULL;
	PyObject *py_ustr_name = NULL;

	as_policy_info info_policy;
	as_policy_info *info_policy_p = NULL;
	as_index_task task;

	if (!self || !self->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		//raise_exception(&err, -2, "Invalid aerospike object");
		goto CLEANUP;
	}

	if (!self->is_conn_16) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER,
						"No connection to aerospike cluster");
		goto CLEANUP;
	}

	// Convert python object to policy_info
	pyobject_to_policy_info(&err, py_policy, &info_policy, &info_policy_p,
							&self->as->config.policies.info);
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	// Convert python object into namespace string
	if (!PyString_Check(py_ns)) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"Namespace should be a string");
		goto CLEANUP;
	}
	char *namespace = PyString_AsString(py_ns);

	// Convert python object into set string
	char *set_ptr = NULL;
	if (PyUnicode_Check(py_set)) {
		py_ustr_set = PyUnicode_AsUTF8String(py_set);
		set_ptr = PyBytes_AsString(py_ustr_set);
	}
	else if (PyString_Check(py_set)) {
		set_ptr = PyString_AsString(py_set);
	}
	else if (py_set != Py_None) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"Set should be string, unicode or None");
		goto CLEANUP;
	}

	// Convert python object into bin string
	char *bin_ptr = NULL;
	if (PyUnicode_Check(py_bin)) {
		py_ustr_bin = PyUnicode_AsUTF8String(py_bin);
		bin_ptr = PyBytes_AsString(py_ustr_bin);
	}
	else if (PyString_Check(py_bin)) {
		bin_ptr = PyString_AsString(py_bin);
	}
	else if (PyByteArray_Check(py_bin)) {
		bin_ptr = PyByteArray_AsString(py_bin);
	}
	else {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Bin should be a string");
		goto CLEANUP;
	}

	// Convert PyObject into the name of the index
	char *name = NULL;
	if (PyUnicode_Check(py_name)) {
		py_ustr_name = PyUnicode_AsUTF8String(py_name);
		name = PyBytes_AsString(py_ustr_name);
	}
	else if (PyString_Check(py_name)) {
		name = PyString_AsString(py_name);
	}
	else {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"Index name should be string or unicode");
		goto CLEANUP;
	}

	// Invoke operation
	Py_BEGIN_ALLOW_THREADS
	aerospike_index_create_complex(self->as, &err, &task, info_policy_p,
								   namespace, set_ptr, bin_ptr, name,
								   index_type, data_type);
	Py_END_ALLOW_THREADS
	if (err.code != AEROSPIKE_OK) {
		as_error_update(&err, err.code, NULL);
		goto CLEANUP;
	}
	else {
		Py_BEGIN_ALLOW_THREADS
		aerospike_index_create_wait(&err, &task, 2000);
		Py_END_ALLOW_THREADS
	}

CLEANUP:
	if (py_ustr_set) {
		Py_DECREF(py_ustr_set);
	}
	if (py_ustr_bin) {
		Py_DECREF(py_ustr_bin);
	}
	if (py_ustr_name) {
		Py_DECREF(py_ustr_name);
	}
	if (err.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return PyLong_FromLong(0);
}
