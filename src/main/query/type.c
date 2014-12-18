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
#include <aerospike/as_query.h>

#include "client.h"
#include "query.h"
#include "conversions.h"

/*******************************************************************************
 * PYTHON TYPE METHODS
 ******************************************************************************/

static PyMethodDef AerospikeQuery_Type_Methods[] = {

    {"apply",	(PyCFunction) AerospikeQuery_Apply,		METH_VARARGS | METH_KEYWORDS,
    			"Apply a Stream UDF on the resultset of the query."},

    {"foreach",	(PyCFunction) AerospikeQuery_Foreach,	METH_VARARGS | METH_KEYWORDS,
    			"Iterate over each record in the resultset and call the callback function."},

    {"results",	(PyCFunction) AerospikeQuery_Results,	METH_VARARGS | METH_KEYWORDS,
    			"Return a list of all records in the resultset."},

    {"select",	(PyCFunction) AerospikeQuery_Select,	METH_VARARGS | METH_KEYWORDS,
    			"Bins to project in the query."},

    {"where",	(PyCFunction) AerospikeQuery_Where,		METH_VARARGS,
    			"Predicate to be applied to the query."},

	{NULL}
};

/*******************************************************************************
 * PYTHON TYPE HOOKS
 ******************************************************************************/

static PyObject * AerospikeQuery_Type_New(PyTypeObject * type, PyObject * args, PyObject * kwds)
{
	AerospikeQuery * self = NULL;

    self = (AerospikeQuery *) type->tp_alloc(type, 0);

    if ( self == NULL ) {
    	return NULL;
    }

	return (PyObject *) self;
}

static int AerospikeQuery_Type_Init(AerospikeQuery * self, PyObject * args, PyObject * kwds)
{
	PyObject * py_namespace = NULL;
	PyObject * py_set = NULL;

	static char * kwlist[] = {"namespace", "set", NULL};

	if ( PyArg_ParseTupleAndKeywords(args, kwds, "O|O:key", kwlist,
		&py_namespace, &py_set) == false ) {
		as_query_destroy(&self->query);
		return -1;
	}

	char * namespace = NULL;
	char * set = NULL;

	if ( PyString_Check(py_namespace) ) {
		namespace = PyString_AsString(py_namespace);
	}

	if ( PyString_Check(py_set) ) {
		set = PyString_AsString(py_set);
	}

	as_query_init(&self->query, namespace, set);

    return 0;
}

static void AerospikeQuery_Type_Dealloc(PyObject * self)
{
    self->ob_type->tp_free((PyObject *) self);
}

/*******************************************************************************
 * PYTHON TYPE DESCRIPTOR
 ******************************************************************************/

static PyTypeObject AerospikeQuery_Type = {
	PyObject_HEAD_INIT(NULL)

    .ob_size			= 0,
    .tp_name			= "aerospike.Query",
    .tp_basicsize		= sizeof(AerospikeQuery),
    .tp_itemsize		= 0,
    .tp_dealloc			= (destructor) AerospikeQuery_Type_Dealloc,
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
    		"The Query class assists in populating the parameters of a query\n"
    		"operation. To create a new instance of the Query class, call the\n"
    		"query() method on an instance of a Client class.\n",
    .tp_traverse		= 0,
    .tp_clear			= 0,
    .tp_richcompare		= 0,
    .tp_weaklistoffset	= 0,
    .tp_iter			= 0,
    .tp_iternext		= 0,
    .tp_methods			= AerospikeQuery_Type_Methods,
    .tp_members			= 0,
    .tp_getset			= 0,
    .tp_base			= 0,
    .tp_dict			= 0,
    .tp_descr_get		= 0,
    .tp_descr_set		= 0,
    .tp_dictoffset		= 0,
    .tp_init			= (initproc) AerospikeQuery_Type_Init,
    .tp_alloc			= 0,
    .tp_new				= AerospikeQuery_Type_New
};

/*******************************************************************************
 * PUBLIC FUNCTIONS
 ******************************************************************************/

PyTypeObject * AerospikeQuery_Ready()
{
	return PyType_Ready(&AerospikeQuery_Type) == 0 ? &AerospikeQuery_Type : NULL;
}

AerospikeQuery * AerospikeQuery_New(AerospikeClient * client, PyObject * args, PyObject * kwds)
{
    AerospikeQuery * self = (AerospikeQuery *) AerospikeQuery_Type.tp_new(&AerospikeQuery_Type, args, kwds);
    self->client = client;
	Py_INCREF(client);
	if (AerospikeQuery_Type.tp_init((PyObject *) self, args, kwds) == 0) {
		return self;
	} else {
		as_error err;
		as_error_init(&err);
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "query() expects atleast 1 parameter");
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		Py_DECREF(py_err);
		return NULL;
	}
}
