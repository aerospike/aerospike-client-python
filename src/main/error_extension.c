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

#include <aerospike/as_error.h>

#include "error_extension.h"

void capture_python_exception(as_error * err, as_status err_code) {
	PyObject * type = NULL;
	PyObject * value = NULL;
	PyObject * traceback = NULL;
	const char * exception_str = NULL;

	PyErr_Fetch(&type, &value, &traceback);

	PyObject * py_exception_str = PyObject_Str(value);
	if (value == NULL) {
		exception_str = "Failed to get string from exception value.";
	}

	exception_str = PyUnicode_AsUTF8(py_exception_str);
	if (exception_str == NULL) {
		exception_str = "Failed to decode string exception value string.";
	}

	as_error_update(err, err_code, exception_str);

	Py_XDECREF(type);
	Py_XDECREF(value);
	Py_XDECREF(traceback);
	Py_XDECREF(py_exception_str);
}