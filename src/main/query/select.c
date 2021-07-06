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
#include <aerospike/as_error.h>

#include "client.h"
#include "query.h"
#include "conversions.h"
#include "exceptions.h"

#undef TRACE
#define TRACE()

AerospikeQuery *AerospikeQuery_Select(AerospikeQuery *self, PyObject *args,
									  PyObject *kwds)
{
	TRACE();

	int nbins = (int)PyTuple_Size(args);
	char *bin = NULL;
	PyObject *py_ubin = NULL;
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

	as_query_select_init(&self->query, nbins);

	for (int i = 0; i < nbins; i++) {
		PyObject *py_bin = PyTuple_GetItem(args, i);
		if (PyUnicode_Check(py_bin)) {
			py_ubin = PyUnicode_AsUTF8String(py_bin);
			bin = PyBytes_AsString(py_ubin);
		}
		else if (PyString_Check(py_bin)) {
			// TRACE();
			bin = PyString_AsString(py_bin);
		}
		else if (PyByteArray_Check(py_bin)) {
			bin = PyByteArray_AsString(py_bin);
		}
		else {
			// TRACE();
			as_error_update(&err, AEROSPIKE_ERR_PARAM,
							"Bin name should be of type string");
			PyObject *py_err = NULL;
			error_to_pyobject(&err, &py_err);
			PyObject *exception_type = raise_exception(&err);
			PyErr_SetObject(exception_type, py_err);
			Py_DECREF(py_err);
			return NULL;
		}

		as_query_select(&self->query, bin);

		if (py_ubin) {
			Py_DECREF(py_ubin);
			py_ubin = NULL;
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
