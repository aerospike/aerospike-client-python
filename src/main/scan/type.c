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
#include <structmember.h>
#include <stdbool.h>

#include <aerospike/aerospike.h>
#include <aerospike/as_config.h>
#include <aerospike/as_error.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_scan.h>

#include "client.h"
#include "scan.h"
#include "conversions.h"

/*******************************************************************************
 * PYTHON TYPE METHODS
 ******************************************************************************/

static PyMethodDef AerospikeScan_Type_Methods[] = {

    {"foreach",	(PyCFunction) AerospikeScan_Foreach,	METH_VARARGS | METH_KEYWORDS,
    			"Iterate over each result and call the callback function."},

    {"select",	(PyCFunction) AerospikeScan_Select,		METH_VARARGS | METH_KEYWORDS,
    			"Add bins to select in the query."},

    {"results",	(PyCFunction) AerospikeScan_Results,	METH_VARARGS | METH_KEYWORDS,
    			"Get a record."},
	{NULL}
};

/*******************************************************************************
 * PYTHON TYPE HOOKS
 ******************************************************************************/

static PyObject * AerospikeScan_Type_New(PyTypeObject * type, PyObject * args, PyObject * kwds)
{
	AerospikeScan * self = NULL;

    self = (AerospikeScan *) type->tp_alloc(type, 0);

    if ( self == NULL ) {
    	return NULL;
    }

	return (PyObject *) self;
}

static int AerospikeScan_Type_Init(AerospikeScan * self, PyObject * args, PyObject * kwds)
{
	PyObject * py_namespace = NULL;
	PyObject * py_set = NULL;

	static char * kwlist[] = {"namespace", "set", NULL};

	if ( PyArg_ParseTupleAndKeywords(args, kwds, "O|O:key", kwlist,
		&py_namespace, &py_set) == false ) {
		return 0;
	}

	char * namespace = NULL;
	char * set = NULL;

	if (py_namespace && PyString_Check(py_namespace) ) {
		namespace = PyString_AsString(py_namespace);
	} else {
		return -1;
	}

	if ( py_set && PyString_Check(py_set) ) {
		set = PyString_AsString(py_set);
	} else {
		return -1;
	}

	as_scan_init(&self->scan, namespace, set);

    return 0;
}

static void AerospikeScan_Type_Dealloc(PyObject * self)
{
    self->ob_type->tp_free((PyObject *) self);
}

/*******************************************************************************
 * PYTHON TYPE DESCRIPTOR
 ******************************************************************************/

static PyTypeObject AerospikeScan_Type = {
	PyObject_HEAD_INIT(NULL)

    .ob_size			= 0,
    .tp_name			= "aerospike.Scan",
    .tp_basicsize		= sizeof(AerospikeScan),
    .tp_itemsize		= 0,
    .tp_dealloc			= (destructor) AerospikeScan_Type_Dealloc,
    .tp_print			= 0,
    .tp_getattr			= 0,
    .tp_setattr			= 0,
    .tp_compare			= 0,
    .tp_repr			= 0,
    .tp_as_number		= 0,
    .tp_as_sequence		= 0,
    .tp_as_mapping		= 0,
    .tp_hash			= 0,
    .tp_call			= 0,
    .tp_str				= 0,
    .tp_getattro		= 0,
    .tp_setattro		= 0,
    .tp_as_buffer		= 0,
    .tp_flags			= Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    .tp_doc				=
    		"The Scan class assists in populating the parameters of a scan\n"
    		"operation. To create a new instance of the Scan class, call the\n"
    		"scan() method on an instance of a Client class.\n",
    .tp_traverse		= 0,
    .tp_clear			= 0,
    .tp_richcompare		= 0,
    .tp_weaklistoffset	= 0,
    .tp_iter			= 0,
    .tp_iternext		= 0,
    .tp_methods			= AerospikeScan_Type_Methods,
    .tp_members			= 0,
    .tp_getset			= 0,
    .tp_base			= 0,
    .tp_dict			= 0,
    .tp_descr_get		= 0,
    .tp_descr_set		= 0,
    .tp_dictoffset		= 0,
    .tp_init			= (initproc) AerospikeScan_Type_Init,
    .tp_alloc			= 0,
    .tp_new				= AerospikeScan_Type_New
};

/*******************************************************************************
 * PUBLIC FUNCTIONS
 ******************************************************************************/

PyTypeObject * AerospikeScan_Ready()
{
	return PyType_Ready(&AerospikeScan_Type) == 0 ? &AerospikeScan_Type : NULL;
}

AerospikeScan * AerospikeScan_New(AerospikeClient * client, PyObject * args, PyObject * kwds)
{
	AerospikeScan * self  = (AerospikeScan *) AerospikeScan_Type.tp_new(&AerospikeScan_Type, args, kwds);
	self->client = client;
	Py_INCREF(client);
	if ( AerospikeScan_Type.tp_init((PyObject *) self, args, kwds) != -1 ) {
		return self;
	}
	else {
		Py_DECREF(self);
		as_error err;
		as_error_init(&err);
		as_error_update(&err, AEROSPIKE_ERR, "Parameters are incorrect");
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		Py_DECREF(py_err);
		return NULL;
	}
}
