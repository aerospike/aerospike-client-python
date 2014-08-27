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

#include <aerospike/aerospike_info.h>
#include <aerospike/as_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_node.h>
#include <aerospike/as_record.h>

#include "client.h"
#include "conversions.h"

static bool AerospikeClient_Info_each(const as_error * err, const as_node * node, const char * req, char * res, void * udata)
{
	PyObject * py_err = NULL;
	PyObject * py_out = NULL;

	if ( err && err->code != AEROSPIKE_OK ) {
		error_to_pyobject(err, &py_err);
	}
	else if ( res != NULL ) {
		char * out = strchr(res,'\t');
		if ( out != NULL ) {
			out++;
			py_out = PyString_FromString(out);
		}
		else {
			py_out = PyString_FromString(res);
		}
	}

	if ( py_err == NULL ) {
		Py_INCREF(Py_None);
		py_err = Py_None;
	}

	if ( py_out == NULL ) {
		Py_INCREF(Py_None);
		py_out = Py_None;
	}

	PyObject * py_res = PyTuple_New(2);
	PyTuple_SetItem(py_res, 0, py_err);
	PyTuple_SetItem(py_res, 1, py_out);

	PyObject * py_nodes = (PyObject *) udata;
	PyDict_SetItemString(py_nodes, node->name, py_res);

	return true;
}

PyObject * AerospikeClient_Info(AerospikeClient * self, PyObject * args, PyObject * kwds)
{
	PyObject * py_req = NULL;
	PyObject * py_policy = NULL;

	static char * kwlist[] = {"req", "policy", NULL};

	if ( PyArg_ParseTupleAndKeywords(args, kwds, "O|O:info", kwlist, &py_req, &py_policy) == false ) {
		return NULL;
	}

	as_error err;
	as_error_init(&err);

	char * req = PyString_AsString(py_req);

	PyObject * py_nodes = PyDict_New();

	aerospike_info_foreach(self->as, &err, NULL, req, AerospikeClient_Info_each, py_nodes);

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return NULL;
	}

	return py_nodes;
}
