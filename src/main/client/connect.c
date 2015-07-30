/*******************************************************************************
 * Copyright 2013-2015 Aerospike, Inc.
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
#include "exceptions.h"

/**
 *******************************************************************************************************
 * Establishes a connection to the Aerospike DB instance.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns an instance of aerospike.Client, Which can be used later to do usual
 * database operations.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
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
		PyObject *exception_type = raise_exception(&err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}
	self->is_conn_16 = true;
	Py_INCREF(self);
	return (PyObject *) self;
}

/**
 *******************************************************************************************************
 * Tests the connection to the Aerospike DB
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns true or false.
 *******************************************************************************************************
 */
PyObject * AerospikeClient_is_connected(AerospikeClient * self, PyObject * args, PyObject * kwds)
{

	if (1 == self->is_conn_16) //Need to define a macro AEROSPIKE_CONN_STATE
	{
      Py_INCREF(Py_True);
	  return Py_True;
	}

    Py_INCREF(Py_False);
	return Py_False;

}
