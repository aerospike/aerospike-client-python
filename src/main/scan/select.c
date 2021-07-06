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

#include <aerospike/as_query.h>

#include "client.h"
#include "scan.h"
#include "conversions.h"
#include "exceptions.h"

#undef TRACE
#define TRACE()

AerospikeScan *AerospikeScan_Select(AerospikeScan *self, PyObject *args,
									PyObject *kwds)
{
	TRACE();

	char *bin = NULL;
	PyObject *py_ustr = NULL;
	as_error err;
	as_error_init(&err);

	if (!self || !self->client->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}

	if (!self->client->is_conn_16) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER,
						"No connection to aerospike cluster");
		goto CLEANUP;
	}

	int nbins = (int)PyTuple_Size(args);
	as_scan_select_init(&self->scan, nbins);

	for (int i = 0; i < nbins; i++) {
		PyObject *py_bin = PyTuple_GetItem(args, i);
		if (py_bin) {
			TRACE();
			if (PyUnicode_Check(py_bin)) {
				py_ustr = PyUnicode_AsUTF8String(py_bin);
				bin = PyBytes_AsString(py_ustr);
			}
			else if (PyString_Check(py_bin)) {
				bin = PyString_AsString(py_bin);
			}
			else if (PyByteArray_Check(py_bin)) {
				bin = PyByteArray_AsString(py_bin);
			}
			else {
				as_error_update(&err, AEROSPIKE_ERR_PARAM,
								"Bin name should be of type string");
				PyObject *py_err = NULL;
				error_to_pyobject(&err, &py_err);
				PyObject *exception_type = raise_exception(&err);
				PyErr_SetObject(exception_type, py_err);
				Py_DECREF(py_err);
				return NULL;
			}
		}
		else {
			TRACE();
		}
		as_scan_select(&self->scan, bin);
		if (py_ustr) {
			Py_DECREF(py_ustr);
			py_ustr = NULL;
		}
	}

CLEANUP:

	if (err.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	Py_INCREF(self);
	return self;
}
