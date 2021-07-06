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

#include "client.h"
#include "conversions.h"
#include "exceptions.h"
#include "geo.h"
#include "policy.h"

PyObject *AerospikeGeospatial_Unwrap(AerospikeGeospatial *self, PyObject *args,
									 PyObject *kwds)
{

	// Python function arguments
	// Python function keyword arguments
	static char *kwlist[] = {NULL};

	if (PyArg_ParseTupleAndKeywords(args, kwds, ":unwrap", kwlist) == false) {
		return NULL;
	}

	// Aerospike error object
	as_error err;
	// Initialize error object
	as_error_init(&err);

	if (!self) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid geospatial object");
		goto CLEANUP;
	}

CLEANUP:

	// If an error occurred, tell Python.
	if (err.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}
	Py_INCREF(self->geo_data);
	return self->geo_data;
}
