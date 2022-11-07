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

// define PY_SSIZE_T_CLEAN is for py3.8 comptaibility see: https://bugs.python.org/issue36381
#define PY_SSIZE_T_CLEAN
#define SCRIPT_LEN_MAX 1048576

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
#include "exceptions.h"
#include "policy.h"

/**
 *******************************************************************************************************
 * Registers a UDF module with the Aerospike DB.
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns an integer status. 0(Zero) is success value.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_UDF_Put(AerospikeClient *self, PyObject *args,
								  PyObject *kwds)
{
	// Initialize error
	as_error err;
	as_error_init(&err);

	// Python Function Arguments
	PyObject *py_filename = NULL;
	long language = 0;
	PyObject *py_udf_type = NULL;
	PyObject *py_policy = NULL;
	PyObject *py_ustr = NULL;
	// This lets each component be 255 characters, and allows a '/'' in between them
	uint32_t max_copy_path_length = AS_CONFIG_PATH_MAX_SIZE * 2 - 1;
	uint32_t filename_length = 0;
	uint8_t *bytes = NULL;
	as_policy_info info_policy;
	as_policy_info *info_policy_p = NULL;
	FILE *file_p = NULL;
	FILE *copy_file_p = NULL;
	// Python Function Keyword Arguments
	static char *kwlist[] = {"filename", "udf_type", "policy", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "O|lO:udf_put", kwlist,
									&py_filename, &language,
									&py_policy) == false) {
		return NULL;
	}

	if (language != AS_UDF_TYPE_LUA) {
		as_error_update(&err, AEROSPIKE_ERR_CLIENT, "Invalid UDF language");
		goto CLEANUP;
	}
	py_udf_type = PyLong_FromLong(language);

	if (!self || !self->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}

	if (!self->is_conn_16) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER,
						"No connection to aerospike cluster");
		goto CLEANUP;
	}

	// Convert PyObject into a filename string
	char *filename = NULL;
	if (PyUnicode_Check(py_filename)) {
		py_ustr = PyUnicode_AsUTF8String(py_filename);
		filename = PyBytes_AsString(py_ustr);
	}
	else if (PyString_Check(py_filename)) {
		filename = PyString_AsString(py_filename);
	}
	else {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"Filename should be a string");
		goto CLEANUP;
	}

	// Convert python object to policy_info
	pyobject_to_policy_info(&err, py_policy, &info_policy, &info_policy_p,
							&self->as->config.policies.info);
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}
	as_udf_type udf_type = (as_udf_type)PyInt_AsLong(py_udf_type);

	// Convert lua file to content
	as_bytes content;
	file_p = fopen(filename, "r");

	// Make this equal to twice the path size, so the path and the filename
	// may be 255 characters each. The max size should then be 255 + 255 + 1 + 1
	char copy_filepath[AS_CONFIG_PATH_MAX_SIZE * 2] = {0};
	uint32_t user_path_len = strlen(self->as->config.lua.user_path);
	memcpy(copy_filepath, self->as->config.lua.user_path, user_path_len);
	if (self->as->config.lua.user_path[user_path_len - 1] != '/') {
		memcpy(copy_filepath + user_path_len, "/", 1);
		user_path_len = user_path_len + 1;
	}
	char *extracted_filename = strrchr(filename, '/');
	if (extracted_filename) {
		filename_length = strlen(extracted_filename) -
						  1; // Length of the filename after the last '/'
		if (!filename_length) {
			as_error_update(&err, AEROSPIKE_ERR_PARAM, "Empty udf filename");
			goto CLEANUP;
		}
		if (user_path_len + filename_length > max_copy_path_length) {
			as_error_update(&err, AEROSPIKE_ERR_PARAM,
							"Lua file pathname too long");
			goto CLEANUP;
		}
		memcpy(copy_filepath + user_path_len, extracted_filename + 1,
			   strlen(extracted_filename) - 1);
		copy_filepath[user_path_len + strlen(extracted_filename) - 1] = '\0';
	}
	else {
		filename_length = strlen(filename);
		if (!filename_length) {
			as_error_update(&err, AEROSPIKE_ERR_PARAM, "Empty udf filename");
			goto CLEANUP;
		}
		if (user_path_len + filename_length > max_copy_path_length) {
			as_error_update(&err, AEROSPIKE_ERR_PARAM,
							"Lua file pathname too long");
			goto CLEANUP;
		}
		memcpy(copy_filepath + user_path_len, filename, strlen(filename));
		copy_filepath[user_path_len + strlen(filename)] = '\0';
	}

	if (!file_p) {
		as_error_update(&err, AEROSPIKE_ERR_LUA_FILE_NOT_FOUND,
						"cannot open script file");
		goto CLEANUP;
	}

	fseek(file_p, 0, SEEK_END);
	int fileSize = ftell(file_p);
	fseek(file_p, 0, SEEK_SET);
	if (fileSize <= 0) {
		as_error_update(&err, AEROSPIKE_ERR_LUA_FILE_NOT_FOUND,
						"Script file is empty");
		fclose(file_p);
		file_p = NULL;
		goto CLEANUP;
	}

	if (fileSize >= SCRIPT_LEN_MAX) {
		as_error_update(&err, AEROSPIKE_ERR_LUA_FILE_NOT_FOUND,
						"Script File is too large");
		fclose(file_p);
		file_p = NULL;
		goto CLEANUP;
	}

	bytes = (uint8_t *)malloc(SCRIPT_LEN_MAX);
	if (!bytes) {
		as_error_update(&err, errno, "malloc failed");
		goto CLEANUP;
	}

	int size = 0;

	uint8_t *buff = bytes;

	if (access(self->as->config.lua.user_path, W_OK) == 0) {

		copy_file_p = fopen(copy_filepath, "w+");
		if (copy_file_p) {
			int read = (int)fread(buff, 1, LUA_FILE_BUFFER_FRAME, file_p);
			if (read && fwrite(buff, 1, read, copy_file_p)) {
				while (read) {
					size += read;
					buff += read;
					read = (int)fread(buff, 1, LUA_FILE_BUFFER_FRAME, file_p);
					if (!fwrite(buff, 1, read, copy_file_p)) {
						break;
					}
				}
			}
			else {
				as_error_update(&err, AEROSPIKE_ERR_CLIENT,
								"Write of lua file to user path failed");
				goto CLEANUP;
			}
		}
		else {
			as_error_update(&err, AEROSPIKE_ERR_CLIENT,
							"Write of lua file to user path failed");
			goto CLEANUP;
		}
	}
	else {
		as_error_update(&err, AEROSPIKE_ERR_CLIENT,
						"No permissions to write lua file to user path");
		goto CLEANUP;
	}

	as_bytes_init_wrap(&content, bytes, size, true);

	// Invoke operation
	Py_BEGIN_ALLOW_THREADS
	aerospike_udf_put(self->as, &err, info_policy_p, filename, udf_type,
					  &content);
	Py_END_ALLOW_THREADS
	if (err.code != AEROSPIKE_OK) {
		as_error_update(&err, err.code, NULL);
		goto CLEANUP;
	}
	else {
		aerospike_udf_put_wait(self->as, &err, info_policy_p,
							   as_basename(NULL, filename), 2000);
	}

CLEANUP:
	if (bytes) {
		free(bytes);
	}

	if (py_ustr) {
		Py_DECREF(py_ustr);
	}

	if (file_p) {
		fclose(file_p);
	}
	if (copy_file_p) {
		fclose(copy_file_p);
	}

	if (err.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		if (PyObject_HasAttrString(exception_type, "module")) {
			PyObject_SetAttrString(exception_type, "module", Py_None);
		}
		if (PyObject_HasAttrString(exception_type, "func")) {
			PyObject_SetAttrString(exception_type, "func", Py_None);
		}
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return PyLong_FromLong(0);
}

/**
 *******************************************************************************************************
 * Removes a UDF module from the Aerospike DB
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
PyObject *AerospikeClient_UDF_Remove(AerospikeClient *self, PyObject *args,
									 PyObject *kwds)
{
	// Initialize error
	as_error err;
	as_error_init(&err);

	// Python Function Arguments
	PyObject *py_policy = NULL;
	PyObject *py_filename = NULL;
	PyObject *py_ustr = NULL;

	as_policy_info info_policy;
	as_policy_info *info_policy_p = NULL;

	// Python Function Keyword Arguments
	static char *kwlist[] = {"filename", "policy", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "O|O:udf_remove", kwlist,
									&py_filename, &py_policy) == false) {
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

	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	// Convert PyObject into a filename string
	char *filename = NULL;
	if (PyUnicode_Check(py_filename)) {
		py_ustr = PyUnicode_AsUTF8String(py_filename);
		filename = PyBytes_AsString(py_ustr);
	}
	else if (PyString_Check(py_filename)) {
		filename = PyString_AsString(py_filename);
	}
	else {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"Filename should be a string");
		goto CLEANUP;
	}

	// Convert python object to policy_info
	pyobject_to_policy_info(&err, py_policy, &info_policy, &info_policy_p,
							&self->as->config.policies.info);

	// Invoke operation
	Py_BEGIN_ALLOW_THREADS
	aerospike_udf_remove(self->as, &err, info_policy_p, filename);
	Py_END_ALLOW_THREADS
	if (err.code != AEROSPIKE_OK) {
		as_error_update(&err, err.code, NULL);
		goto CLEANUP;
	}

CLEANUP:

	if (py_ustr) {
		Py_DECREF(py_ustr);
	}
	if (err.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		if (PyObject_HasAttrString(exception_type, "module")) {
			PyObject_SetAttrString(exception_type, "module", py_filename);
		}
		if (PyObject_HasAttrString(exception_type, "func")) {
			PyObject_SetAttrString(exception_type, "func", Py_None);
		}
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return PyLong_FromLong(0);
}

/**
 *******************************************************************************************************
 * Lists the UDF modules registered with the server
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns list of modules that are registered with Aerospike DB.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_UDF_List(AerospikeClient *self, PyObject *args,
								   PyObject *kwds)
{
	// Initialize error
	as_error err;
	as_error_init(&err);
	int init_udf_files = 0;

	// Python Function Arguments
	PyObject *py_policy = NULL;
	as_policy_info info_policy;
	as_policy_info *info_policy_p = NULL;

	// Python Function Keyword Arguments
	static char *kwlist[] = {"policy", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "|O:udf_list", kwlist,
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

	// Convert python object to policy_info
	pyobject_to_policy_info(&err, py_policy, &info_policy, &info_policy_p,
							&self->as->config.policies.info);
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	as_udf_files files;
	as_udf_files_init(&files, 0);
	init_udf_files = 1;

	// Invoke operation
	Py_BEGIN_ALLOW_THREADS
	aerospike_udf_list(self->as, &err, info_policy_p, &files);
	Py_END_ALLOW_THREADS
	if (err.code != AEROSPIKE_OK) {
		as_error_update(&err, err.code, NULL);
		goto CLEANUP;
	}

	// Convert as_udf_files struct into python object
	PyObject *py_files;
	as_udf_files_to_pyobject(&err, &files, &py_files);

	if (err.code != AEROSPIKE_OK) {
		as_error_update(&err, err.code, NULL);
		goto CLEANUP;
	}

CLEANUP:

	if (init_udf_files) {
		as_udf_files_destroy(&files);
	}

	if (err.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		if (PyObject_HasAttrString(exception_type, "module")) {
			PyObject_SetAttrString(exception_type, "module", Py_None);
		}
		if (PyObject_HasAttrString(exception_type, "func")) {
			PyObject_SetAttrString(exception_type, "func", Py_None);
		}
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return py_files;
}

/**
 *******************************************************************************************************
 * Gets the code for a UDF module registered with the server
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns the content of the UDF module.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_UDF_Get_UDF(AerospikeClient *self, PyObject *args,
									  PyObject *kwds)
{
	// Initialize error
	as_error err;
	as_error_init(&err);

	// Python Function Arguments
	PyObject *py_module = NULL;
	PyObject *py_policy = NULL;
	long language = 0;
	bool init_udf_file = false;
	PyObject *udf_content = NULL;
	PyObject *py_ustr = NULL;

	// Python Function Keyword Arguments
	static char *kwlist[] = {"module", "language", "policy", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "O|lO:udf_get", kwlist,
									&py_module, &language,
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

	if (language != AS_UDF_TYPE_LUA) {
		as_error_update(&err, AEROSPIKE_ERR_CLIENT, "Invalid language");
		goto CLEANUP;
	}
	char *strModule = NULL;
	if (PyUnicode_Check(py_module)) {
		py_ustr = PyUnicode_AsUTF8String(py_module);
		strModule = PyBytes_AsString(py_ustr);
	}
	else if (PyString_Check(py_module)) {
		strModule = PyString_AsString(py_module);
	}
	else {
		as_error_update(&err, AEROSPIKE_ERR_CLIENT,
						"Module name should be a string or unicode string.");
		goto CLEANUP;
	}

	// Convert python object to policy_info
	as_policy_info *info_policy_p = NULL, info_policy;

	pyobject_to_policy_info(&err, py_policy, &info_policy, &info_policy_p,
							&self->as->config.policies.info);
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	as_udf_file file;
	as_udf_file_init(&file);
	init_udf_file = true;

	// Invoke operation
	Py_BEGIN_ALLOW_THREADS
	aerospike_udf_get(self->as, &err, info_policy_p, strModule,
					  (language - AS_UDF_TYPE_LUA), &file);
	Py_END_ALLOW_THREADS
	if (err.code != AEROSPIKE_OK) {
		as_error_update(&err, err.code, NULL);
		goto CLEANUP;
	}
	udf_content = Py_BuildValue("s#", file.content.bytes, file.content.size);

CLEANUP:

	if (py_ustr) {
		Py_DECREF(py_ustr);
	}
	if (init_udf_file) {
		as_udf_file_destroy(&file);
	}
	if (err.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		if (PyObject_HasAttrString(exception_type, "module")) {
			PyObject_SetAttrString(exception_type, "module", py_module);
		}
		if (PyObject_HasAttrString(exception_type, "func")) {
			PyObject_SetAttrString(exception_type, "func", Py_None);
		}
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return udf_content;
}
