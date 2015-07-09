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
#include <structmember.h>
#include <stdbool.h>

#include <aerospike/aerospike.h>
#include <aerospike/as_config.h>
#include <aerospike/as_error.h>
#include <aerospike/as_key.h>
#include <aerospike/as_policy.h>

#include "client.h"
#include "key.h"

/*******************************************************************************
 * PYTHON TYPE METHODS
 ******************************************************************************/

static PyMethodDef AerospikeKey_Type_Methods[] = {

    {"apply",	(PyCFunction) AerospikeKey_Apply,	METH_VARARGS | METH_KEYWORDS, 
    			"Apply a UDF on a record."},
    
    {"exists",	(PyCFunction) AerospikeKey_Exists,	METH_VARARGS | METH_KEYWORDS, 
    			"Check existence of the record."},

    {"get",		(PyCFunction) AerospikeKey_Get,		METH_VARARGS | METH_KEYWORDS, 
    			"Get all bins of the record."},
    
    {"put",		(PyCFunction) AerospikeKey_Put,		METH_VARARGS | METH_KEYWORDS,
    			"Update a record."},
    
    {"remove",	(PyCFunction) AerospikeKey_Remove,	METH_VARARGS | METH_KEYWORDS,
    			"Remove a record."},
    
    // {"select",	(PyCFunction) AerospikeKey_Select,	METH_VARARGS | METH_KEYWORDS, "Select specific bins of the record."},
	
	{NULL}
};

/*******************************************************************************
 * PYTHON TYPE HOOKS
 ******************************************************************************/

static PyObject * AerospikeKey_Type_New(PyTypeObject * type, PyObject * args, PyObject * kwds)
{
	AerospikeKey * self = NULL;

    self = (AerospikeKey *) type->tp_alloc(type, 0);

    if ( self == NULL ) {
		return NULL;
    }
	
	return (PyObject *) self;
}

static int AerospikeKey_Type_Init(AerospikeKey * self, PyObject * args, PyObject * kwds)
{
    return 0;
}

static void AerospikeKey_Type_Dealloc(AerospikeKey * self)
{
	// as_key_destroy(&self->key);
    self->ob_type->tp_free((PyObject *) self);
}

/*******************************************************************************
 * PYTHON TYPE DESCRIPTOR
 ******************************************************************************/

static PyTypeObject AerospikeKey_Type = {
	PyObject_HEAD_INIT(NULL)

    .ob_size			= 0,
    .tp_name			= "aerospike.Key",
    .tp_basicsize		= sizeof(AerospikeKey),
    .tp_itemsize		= 0,
    .tp_dealloc			= (destructor) AerospikeKey_Type_Dealloc,
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
    		"[DEPRECATED] The Key class assists in creating a key object for use with kvs\n"
    		"operations. To create a new instance of the Key class, call the\n"
    		"key() method on an instance of a Client class.\n",
    .tp_traverse		= 0,
    .tp_clear			= 0,
    .tp_richcompare		= 0,
    .tp_weaklistoffset	= 0,
    .tp_iter			= 0,
    .tp_iternext		= 0,
    .tp_methods			= AerospikeKey_Type_Methods,
    .tp_members			= 0,
    .tp_getset			= 0,
    .tp_base			= 0,
    .tp_dict			= 0,
    .tp_descr_get		= 0,
    .tp_descr_set		= 0,
    .tp_dictoffset		= 0,
    .tp_init			= (initproc) AerospikeKey_Type_Init,
    .tp_alloc			= 0,
    .tp_new				= AerospikeKey_Type_New
};

/*******************************************************************************
 * PUBLIC FUNCTIONS
 ******************************************************************************/

PyTypeObject * AerospikeKey_Ready()
{
	return PyType_Ready(&AerospikeKey_Type) == 0 ? &AerospikeKey_Type : NULL;
}

AerospikeKey * AerospikeKey_New(AerospikeClient * client, PyObject * args, PyObject * kwds)
{
	Py_INCREF(client);
	Py_INCREF(args);

    AerospikeKey * self = (AerospikeKey *) AerospikeKey_Type.tp_new(&AerospikeKey_Type, args, kwds);
    self->client = client;
	self->key = args;
    AerospikeKey_Type.tp_init((PyObject *) self, args, kwds);
	return self;
}
