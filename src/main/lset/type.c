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
#include <aerospike/as_policy.h>
#include <aerospike/as_ldt.h>

#include "client.h"
#include "conversions.h"
#include "exceptions.h"
#include "lset.h"

/*******************************************************************************
 * PYTHON TYPE METHODS
 ******************************************************************************/

static PyMethodDef AerospikeLSet_Type_Methods[] = {

	// LSET OPERATIONS

	{"add",
		(PyCFunction) AerospikeLSet_Add, METH_VARARGS | METH_KEYWORDS,
		"Adds a value to the LSet."},
	{"add_many",
		(PyCFunction) AerospikeLSet_Add_Many, METH_VARARGS | METH_KEYWORDS,
		"Adds multiple values to the LSet."},
	{"remove",
		(PyCFunction) AerospikeLSet_Remove, METH_VARARGS | METH_KEYWORDS,
		"Find and remove the element matching the given value from the LSet."},
	{"exists",
		(PyCFunction) AerospikeLSet_Exists, METH_VARARGS | METH_KEYWORDS,
		"Check whether the element exists in the LSet."},
	{"get",
		(PyCFunction) AerospikeLSet_Get, METH_VARARGS | METH_KEYWORDS,
		"Get an object from the set."},
	{"filter",
		(PyCFunction) AerospikeLSet_Filter, METH_VARARGS | METH_KEYWORDS,
		"scan the set and apply a predicate filter."},
	{"destroy",
		(PyCFunction) AerospikeLSet_Destroy, METH_VARARGS | METH_KEYWORDS,
		"Delete the entire set (LDT Remove)."},
	{"size",
		(PyCFunction) AerospikeLSet_Size, METH_VARARGS | METH_KEYWORDS,
		"Get the current item count of the set."},
	{"config",
		(PyCFunction) AerospikeLSet_Config, METH_VARARGS | METH_KEYWORDS,
		"Get the configuration parameters of the set."},

	{NULL}
};

/*******************************************************************************
 * PYTHON TYPE HOOKS
 ******************************************************************************/

static PyObject * AerospikeLSet_Type_New(PyTypeObject * type, PyObject * args, PyObject * kwds)
{
	AerospikeLSet * self = NULL;

	self = (AerospikeLSet *) type->tp_alloc(type, 0);

	if ( self == NULL ) {
		return NULL;
	}

	return (PyObject *) self;
}

static int AerospikeLSet_Type_Init(AerospikeLSet * self, PyObject * args, PyObject * kwds)
{
	PyObject * py_key = NULL;
	char* bin_name = NULL;
	char* module = NULL;

	static char * kwlist[] = {"key", "bin", "module", NULL};

	if ( PyArg_ParseTupleAndKeywords(args, kwds, "Os|s:lset", kwlist, &py_key,
				&bin_name, &module) == false ) {
		return -1;
	}

	/*
	 * Convert pyobject to as_key type.
	 */
	as_error error;
	as_error_init(&error);

	pyobject_to_key(&error, py_key, &self->key);
	if (error.code != AEROSPIKE_OK) {
		return -1;
	}

	int bin_name_len = strlen(bin_name);
	if ((bin_name_len == 0) || (bin_name_len > AS_BIN_NAME_MAX_LEN)) {
		return -1;
	}

	strcpy(self->bin_name, bin_name);

	/*
	 * LDT Initialization
	 */
	initialize_ldt(&error, &self->lset, self->bin_name, AS_LDT_LSET, module);
	if (error.code != AEROSPIKE_OK) {
		return -1;
	}

	return 0;
}

static void AerospikeLSet_Type_Dealloc(PyObject * self)
{
	self->ob_type->tp_free((PyObject *) self);
}

/*******************************************************************************
 * PYTHON TYPE DESCRIPTOR
 ******************************************************************************/

static PyTypeObject AerospikeLSet_Type = {
	PyObject_HEAD_INIT(NULL)

		.ob_size			= 0,
	.tp_name			= "aerospike.LSet",
	.tp_basicsize		= sizeof(AerospikeLSet),
	.tp_itemsize		= 0,
	.tp_dealloc			= (destructor) AerospikeLSet_Type_Dealloc,
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
		"The LSet class assists in populating the parameters of a LSet.\n",
	.tp_traverse		= 0,
	.tp_clear			= 0,
	.tp_richcompare		= 0,
	.tp_weaklistoffset	= 0,
	.tp_iter			= 0,
	.tp_iternext		= 0,
	.tp_methods			= AerospikeLSet_Type_Methods,
	.tp_members			= 0,
	.tp_getset			= 0,
	.tp_base			= 0,
	.tp_dict			= 0,
	.tp_descr_get		= 0,
	.tp_descr_set		= 0,
	.tp_dictoffset		= 0,
	.tp_init			= (initproc) AerospikeLSet_Type_Init,
	.tp_alloc			= 0,
	.tp_new				= AerospikeLSet_Type_New
};

/*******************************************************************************
 * PUBLIC FUNCTIONS
 ******************************************************************************/

PyTypeObject * AerospikeLSet_Ready()
{
	return PyType_Ready(&AerospikeLSet_Type) == 0 ? &AerospikeLSet_Type : NULL;
}

AerospikeLSet * AerospikeLSet_New(AerospikeClient * client, PyObject * args, PyObject * kwds)
{
	AerospikeLSet * self = (AerospikeLSet *) AerospikeLSet_Type.tp_new(&AerospikeLSet_Type, args, kwds);
	self->client = client;
	Py_INCREF(client);

	if (AerospikeLSet_Type.tp_init((PyObject *)self, args, kwds) == 0) {
		return self;
	} else {
		as_error err;
		as_error_init(&err);
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Parameters are incorrect");
		PyObject * py_err = NULL, *py_key = NULL;
		PyObject *exception_type = raise_exception(&err);
		error_to_pyobject(&err, &py_err);
		if(PyObject_HasAttrString(exception_type, "key")) {
			if(&self->key) {
				key_to_pyobject(&err, &self->key, &py_key);
				PyObject_SetAttrString(exception_type, "key", py_key);
				Py_DECREF(py_key);
			} else {
				PyObject_SetAttrString(exception_type, "key", Py_None);
			}
		} 
		if(PyObject_HasAttrString(exception_type, "bin")) {
			if(&self->bin_name) {
				PyObject *py_bins = PyString_FromString((char *)&self->bin_name);
				PyObject_SetAttrString(exception_type, "bin", py_bins);
				Py_DECREF(py_bins);
			} else {
				PyObject_SetAttrString(exception_type, "bin", Py_None);
			}
		}
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}
}
