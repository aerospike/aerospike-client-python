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
#include <pthread.h>
#include <stdbool.h>

#include <aerospike/aerospike_scan.h>
#include <aerospike/as_error.h>
#include <aerospike/as_scan.h>

#include "client.h"
#include "conversions.h"
#include "scan.h"

#undef TRACE
#define TRACE()

static bool each_result(const as_val * val, void * udata)
{
	if ( !val ) {
		return false;
	}

	PyObject * py_results = (PyObject *) udata;
	PyObject * py_result = NULL;

	as_error err;

	PyGILState_STATE gstate;
	gstate = PyGILState_Ensure();

	val_to_pyobject(&err, val, &py_result);

	if ( py_result ) {
		PyList_Append(py_results, py_result);
		Py_DECREF(py_result);
	}

	PyGILState_Release(gstate);

	return true;
}

PyObject * AerospikeScan_Results(AerospikeScan * self, PyObject * args, PyObject * kwds)
{
	PyObject * py_policy = NULL;

	static char * kwlist[] = {"policy", NULL};

	if ( PyArg_ParseTupleAndKeywords(args, kwds, "|O:foreach", kwlist, &py_policy) == false ) {
		return NULL;
	}

	as_error err;
	as_error_init(&err);

	PyObject * py_results = PyList_New(0);

	PyThreadState * _save = PyEval_SaveThread();

	aerospike_scan_foreach(self->client->as, &err, NULL, &self->scan, each_result, py_results);

	PyEval_RestoreThread(_save);

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return NULL;
	}

	return py_results;
}
