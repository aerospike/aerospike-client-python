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

#include <aerospike/aerospike_scan.h>
#include <aerospike/as_error.h>
#include <aerospike/as_scan.h>

#include "client.h"
#include "conversions.h"
#include "scan.h"

#undef TRACE
#define TRACE()

typedef struct {
	as_error error;
	PyObject * callback;
} LocalData;


static bool each_result(const as_val * val, void * udata)
{
	if ( !val ) {
		return false;
	}

	LocalData * data = (LocalData *) udata;
	as_error * err = &data->error;
	PyObject * py_callback = data->callback;

	PyObject * py_arglist = NULL; 
	PyObject * py_result = NULL;

	PyGILState_STATE gstate;
	gstate = PyGILState_Ensure();

	val_to_pyobject(err, val, &py_result);

	py_arglist = Py_BuildValue("(O)", py_result);

	PyEval_CallObject(py_callback, py_arglist);

	Py_DECREF(py_arglist);

	PyGILState_Release(gstate);

	return true;
}

PyObject * AerospikeScan_Foreach(AerospikeScan * self, PyObject * args, PyObject * kwds)
{
	TRACE();
	
	PyObject * py_callback = NULL;
	PyObject * py_policy = NULL;

	TRACE();
	
	static char * kwlist[] = {"callback", "policy", NULL};

	TRACE();
	
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "O|O:foreach", kwlist, &py_callback, &py_policy) == false ) {
		return NULL;
	}

	as_error err;
	as_error_init(&err);

	LocalData data;
	data.callback = py_callback;
	as_error_init(&data.error);
	
	PyThreadState * _save = PyEval_SaveThread();

	aerospike_scan_foreach(self->client->as, &err, NULL, &self->scan, each_result, &data);

	PyEval_RestoreThread(_save);
	
	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return NULL;
	}
	
	Py_INCREF(Py_None);
	return Py_None;
}