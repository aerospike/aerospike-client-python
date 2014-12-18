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

#include <aerospike/aerospike.h>
#include <aerospike/as_error.h>

#include "client.h"
#include "conversions.h"

PyObject * AerospikeClient_Connect(AerospikeClient * self, PyObject * args, PyObject * kwds)
{
	as_error err;
	as_error_init(&err);

	PyObject * py_username = NULL;
	PyObject * py_password = NULL;

	if ( PyArg_ParseTuple(args, "|OO:connect", &py_username, &py_password) == false ) {
		return NULL;
	}

	if ( py_username && PyString_Check(py_username) && py_password && PyString_Check(py_password) ) {
		char * username = PyString_AsString(py_username);
		char * password = PyString_AsString(py_password);
		as_config_set_user(&self->as->config, username, password);
	}

	aerospike_connect(self->as, &err);

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	Py_INCREF(self);
	return (PyObject *) self;
}
