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

#include <aerospike/as_arraylist.h>
#include <aerospike/as_error.h>
#include "cdt_types.h"

#include "client.h"
#include "conversions.h"
#include "exceptions.h"
#include "scan.h"
#include "policy.h"

bool Scan_Illegal_UDF_Args_Check(PyObject *py_args);

AerospikeScan *AerospikeScan_Apply(AerospikeScan *self, PyObject *args,
								   PyObject *kwds)
{

	// Python function arguments.
	PyObject *py_module = NULL;
	PyObject *py_function = NULL;
	PyObject *py_args = NULL;
	PyObject *py_policy = NULL;

	PyObject *py_umodule = NULL;
	PyObject *py_ufunction = NULL;
	// Python function keyword arguments.
	static char *kwlist[] = {"module", "function", "arguments", "policy", NULL};

	if (PyArg_ParseTupleAndKeywords(args, kwds, "OO|OO:apply", kwlist,
									&py_module, &py_function, &py_args,
									&py_policy) == false) {
		return NULL;
	}

	as_static_pool static_pool;
	memset(&static_pool, 0, sizeof(static_pool));

	as_error err;
	as_error_init(&err);

	if (!self || !self->client->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid scan object.");
		goto CLEANUP;
	}

	if (!self->client->is_conn_16) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER,
						"No connection to aerospike cluster.");
		goto CLEANUP;
	}

	self->client->is_client_put_serializer = false;

	// Aerospike API Arguments.
	char *module = NULL;
	char *function = NULL;
	as_arraylist *arglist = NULL;

	if (PyUnicode_Check(py_module)) {
		py_umodule = PyUnicode_AsUTF8String(py_module);
		module = PyBytes_AsString(py_umodule);
	}
	else if (PyString_Check(py_module)) {
		module = PyString_AsString(py_module);
	}
	else {
		as_error_update(
			&err, AEROSPIKE_ERR_CLIENT,
			"udf module argument must be a string or unicode string");
		goto CLEANUP;
	}

	if (PyUnicode_Check(py_function)) {
		py_ufunction = PyUnicode_AsUTF8String(py_function);
		function = PyBytes_AsString(py_ufunction);
	}
	else if (PyString_Check(py_function)) {
		function = PyString_AsString(py_function);
	}
	else {
		as_error_update(
			&err, AEROSPIKE_ERR_CLIENT,
			"udf function argument must be a string or unicode string");
		goto CLEANUP;
	}

	if (py_args && PyList_Check(py_args)) {
		Py_ssize_t size = PyList_Size(py_args);

		if (Scan_Illegal_UDF_Args_Check(py_args)) {
			as_error_update(
				&err, AEROSPIKE_ERR_CLIENT,
				"udf function argument type must be supported by Aerospike");
			goto CLEANUP;
		}

		arglist = as_arraylist_new(size, 0);
		for (int i = 0; i < size; i++) {
			PyObject *py_val = PyList_GetItem(py_args, (Py_ssize_t)i);
			as_val *val = NULL;
			pyobject_to_val(self->client, &err, py_val, &val, &static_pool,
							SERIALIZER_PYTHON);
			if (err.code != AEROSPIKE_OK) {
				as_error_update(&err, err.code, NULL);
				as_arraylist_destroy(arglist);
				goto CLEANUP;
			}
			else {
				as_arraylist_append(arglist, val);
			}
		}
	}
	else {
		as_error_update(&err, AEROSPIKE_ERR_CLIENT,
						"udf function arguments must be enclosed in a list");
		as_arraylist_destroy(arglist);
		goto CLEANUP;
	}

	Py_BEGIN_ALLOW_THREADS
	as_scan_apply_each(&self->scan, module, function, (as_list *)arglist);
	Py_END_ALLOW_THREADS

CLEANUP:
	POOL_DESTROY(&static_pool);

	if (py_ufunction) {
		Py_DECREF(py_ufunction);
	}

	if (py_umodule) {
		Py_DECREF(py_umodule);
	}

	if (err.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		if (PyObject_HasAttrString(exception_type, "module")) {
			PyObject_SetAttrString(exception_type, "module", py_module);
		}
		if (PyObject_HasAttrString(exception_type, "func")) {
			PyObject_SetAttrString(exception_type, "func", py_function);
		}
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	Py_INCREF(self);
	return self;
}

bool Scan_Illegal_UDF_Args_Check(PyObject *py_args)
{
	Py_ssize_t size = PyList_Size(py_args);
	PyObject *py_args_copy =
		PyList_GetSlice(py_args, (Py_ssize_t)0, (Py_ssize_t)size);
	for (int i = 0; i < size; i++) {
		PyObject *py_val = PyList_GetItem(py_args_copy, (Py_ssize_t)i);
		if (PyList_Check(py_val)) {
			Py_ssize_t nested_size = PyList_Size(py_val);
			for (int j = 0; j < nested_size; j++, size++) {
				PyList_Append(py_args_copy,
							  PyList_GetItem(py_val, (Py_ssize_t)j));
			}
		}
		else if (PyDict_Check(py_val)) {
			PyObject *dict_values = PyDict_Values(py_val);
			Py_ssize_t nested_size = PyList_Size(dict_values);
			for (int j = 0; j < nested_size; j++, size++) {
				PyList_Append(py_args_copy,
							  PyList_GetItem(dict_values, (Py_ssize_t)j));
			}
			Py_DECREF(dict_values);
		}
		else if (!(PyInt_Check(py_val) || PyLong_Check(py_val) ||
				   PyFloat_Check(py_val) || PyString_Check(py_val) ||
				   PyBool_Check(py_val) || PyUnicode_Check(py_val) ||
				   !strcmp(py_val->ob_type->tp_name, "aerospike.Geospatial") ||
				   PyByteArray_Check(py_val) || (Py_None == py_val) ||
				   (!strcmp(py_val->ob_type->tp_name, "aerospike.null")) ||
				   AS_Matches_Classname(py_val, AS_CDT_WILDCARD_NAME) ||
				   AS_Matches_Classname(py_val, AS_CDT_INFINITE_NAME) ||
				   PyBytes_Check(py_val))) {
			return true;
		}
	}
	Py_DECREF(py_args_copy);
	return false;
}
