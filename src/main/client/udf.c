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
#include <aerospike/aerospike_udf.h>
#include <aerospike/as_config.h>
#include <aerospike/as_error.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_udf.h>

#include "client.h"
#include "conversions.h"
#include "policy.h"

#define SCRIPT_LEN_MAX 1048576

PyObject * AerospikeClient_UDF_Put(AerospikeClient * self, PyObject *args, PyObject * kwds)
{
	// Initialize error
	as_error err;
	as_error_init(&err);

	// Python Function Arguments
	PyObject * py_policy = NULL;
	PyObject * py_filename = NULL;
	PyObject * py_udf_type = NULL;
	uint8_t * bytes = NULL;
	as_policy_info info_policy;
	as_policy_info *info_policy_p = NULL;

	// Python Function Keyword Arguments
	static char * kwlist[] = {"policy", "filename", "udf_type", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "OOO:udf_put", kwlist,
				&py_policy, &py_filename, &py_udf_type) == false ) {
		return NULL;
	}

	if (!self || !self->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}

	// Convert PyObject into a filename string
	char *filename;
	if( !PyString_Check(py_filename) ) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Filename should be a string");
		goto CLEANUP;
	}

	filename = PyString_AsString(py_filename);

	// Convert python object to policy_info
	pyobject_to_policy_info( &err, py_policy, &info_policy, &info_policy_p);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}
	as_udf_type udf_type = (as_udf_type)PyInt_AsLong(py_udf_type);

	// Convert lua file to content
	as_bytes content;
	FILE * file = fopen(filename,"r");

	if ( !file ) {
		as_error_update(&err, errno, "cannot open script file");
		goto CLEANUP;
	}

	bytes = (uint8_t *) malloc(SCRIPT_LEN_MAX);
	if ( bytes == NULL ) {
		as_error_update(&err, errno, "malloc failed");
		goto CLEANUP;
	}

	int size = 0;

	uint8_t * buff = bytes;
	int read = (int)fread(buff, 1, 512, file);
	while ( read ) {
		size += read;
		buff += read;
		read = (int)fread(buff, 1, 512, file);
	}
	fclose(file);

	as_bytes_init_wrap(&content, bytes, size, true);

	// Invoke operation
	aerospike_udf_put(self->as, &err, info_policy_p, filename, udf_type, &content);
	if( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

CLEANUP:
	if(bytes)
		free(bytes);

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return PyLong_FromLong(0);
}


PyObject * AerospikeClient_UDF_Remove(AerospikeClient * self, PyObject *args, PyObject * kwds)
{
	// Initialize error
	as_error err;
	as_error_init(&err);

	// Python Function Arguments
	PyObject * py_policy = NULL;
	PyObject * py_filename = NULL;
	as_policy_info info_policy;
	as_policy_info *info_policy_p = NULL;

	// Python Function Keyword Arguments
	static char * kwlist[] = {"policy", "filename", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "OO:udf_remove", kwlist,
				&py_policy, &py_filename) == false ) {
		return NULL;
	}

	if (!self || !self->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}

	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

	// Convert PyObject into a filename string
	char *filename;
	if( !PyString_Check(py_filename) ) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Filename should be a string");
		goto CLEANUP;
	}

	filename = PyString_AsString(py_filename);

	// Convert python object to policy_info
	pyobject_to_policy_info( &err, py_policy, &info_policy, &info_policy_p);

	// Invoke operation
	aerospike_udf_remove(self->as, &err, info_policy_p, filename);
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

PyObject * AerospikeClient_UDF_List(AerospikeClient * self, PyObject *args, PyObject * kwds)
{
	// Initialize error
	as_error err;
	as_error_init(&err);
	int init_udf_files = 0;

	// Python Function Arguments
	PyObject * py_policy = NULL;
	as_policy_info info_policy;
	as_policy_info *info_policy_p = NULL;

	// Python Function Keyword Arguments
	static char * kwlist[] = {"policy", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "O:udf_list", kwlist, &py_policy) == false ) {
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

	as_udf_files files;
	as_udf_files_init(&files, 0);
	init_udf_files = 1;

	// Invoke operation
	aerospike_udf_list(self->as, &err, info_policy_p, &files);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

	// Convert as_udf_files struct into python object
	PyObject * py_files;
	as_udf_files_to_pyobject(&err, &files, &py_files);

	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

CLEANUP:

	if (init_udf_files) {
		as_udf_files_destroy(&files);
	}

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return py_files;
}
