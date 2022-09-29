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
#include "operate.h"

AerospikeScan *AerospikeScan_Add_Ops(AerospikeScan *self, PyObject *args,
									 PyObject *kwds)
{
	// Python function arguments.
	PyObject *py_ops = NULL;
	// Python function keyword arguments.
	static char *kwlist[] = {"ops", NULL};

	if (!PyArg_ParseTupleAndKeywords(args, kwds, "O:ops", kwlist, &py_ops)) {
		return NULL;
	}

	// Aerospike API arguments.
	long return_type = -1;
	long operation;
	self->unicodeStrVector = as_vector_create(sizeof(char *), 128);

	as_static_pool static_pool;
	memset(&static_pool, 0, sizeof(static_pool));
	self->static_pool = &static_pool;

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

	if (PyList_Check(py_ops)) {
		Py_ssize_t size = PyList_Size(py_ops);
		self->scan.ops = as_operations_new((uint16_t)size);

		for (int i = 0; i < size; i++) {
			PyObject *py_val = PyList_GetItem(py_ops, (Py_ssize_t)i);

			if (PyDict_Check(py_val)) {
				if (add_op(self->client, &err, py_val, self->unicodeStrVector,
						   self->static_pool, self->scan.ops, &operation,
						   &return_type) != AEROSPIKE_OK) {
					as_error_update(&err, AEROSPIKE_ERR_PARAM,
									"Failed to convert ops.");
					goto CLEANUP;
				}
			}
			else {
				as_error_update(&err, AEROSPIKE_ERR_PARAM,
								"Failed to convert ops.");
				goto CLEANUP;
			}
		}
	}
	else {
		as_error_update(&err, AEROSPIKE_ERR_CLIENT, "Ops must be list.");
		goto CLEANUP;
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
