/*******************************************************************************
 * Copyright 2013-2016 Aerospike, Inc.
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
#include "llist.h"

/*******************************************************************************
 * PYTHON TYPE METHODS
 ******************************************************************************/

static PyMethodDef AerospikeLList_Type_Methods[] = {

	// LLIST OPERATIONS

	{"add",
		(PyCFunction) AerospikeLList_Add, METH_VARARGS | METH_KEYWORDS,
		"Adds a value to the LList."},
	{"add_many",
		(PyCFunction) AerospikeLList_Add_Many, METH_VARARGS | METH_KEYWORDS,
		"Adds multiple values to the LList."},
	{"remove",
		(PyCFunction) AerospikeLList_Remove, METH_VARARGS | METH_KEYWORDS,
		"Find and remove the element matching the given value from the LList."},
	{"get",
		(PyCFunction) AerospikeLList_Get, METH_VARARGS | METH_KEYWORDS,
		"Get an object from the list."},
	{"filter",
		(PyCFunction) AerospikeLList_Filter, METH_VARARGS | METH_KEYWORDS,
		"scan the list and apply a predicate filter."},
	{"destroy",
		(PyCFunction) AerospikeLList_Destroy, METH_VARARGS | METH_KEYWORDS,
		"Delete the entire list (LDT Remove)."},
	{"size",
		(PyCFunction) AerospikeLList_Size, METH_VARARGS | METH_KEYWORDS,
		"Get the current item count of the list."},
	{"find_first",
		(PyCFunction) AerospikeLList_Find_First, METH_VARARGS | METH_KEYWORDS,
		"Select values from the beginning of list up to a maximum count"},
	{"find_first_filter",
		(PyCFunction) AerospikeLList_Find_First_Filter, METH_VARARGS | METH_KEYWORDS,
		"Select values from the beginning of list up to a maximum count applying a predicate filter"},
	{"find_last",
		(PyCFunction) AerospikeLList_Find_Last, METH_VARARGS | METH_KEYWORDS,
		"Select values from the end of list up to a maximum count"},
	{"find_last_filter",
		(PyCFunction) AerospikeLList_Find_Last_Filter, METH_VARARGS | METH_KEYWORDS,
		"Select values from the end of list up to a maximum count applying a predicate filter"},
	{"find_from",
		(PyCFunction) AerospikeLList_Find_From, METH_VARARGS | METH_KEYWORDS,
		"Select values from a begin key up to a maximum count"},
	{"find_from_filter",
		(PyCFunction) AerospikeLList_Find_From_Filter, METH_VARARGS | METH_KEYWORDS,
		"Select values from a begin key up to a maximum count applying the lua filter"},
	{"range_limit",
		(PyCFunction) AerospikeLList_Range_Limit, METH_VARARGS | METH_KEYWORDS,
		"Select values from a begin key up to a end key with a maximum count applying the lua filter"},
	{"set_page_size",
		(PyCFunction) AerospikeLList_Set_Page_Size, METH_VARARGS | METH_KEYWORDS,
		"Set page size of lua bin"},
	{"config",
		(PyCFunction) AerospikeLList_Config, METH_VARARGS | METH_KEYWORDS,
		"Get the configuration parameters of the list."},

	{NULL}
};

/*******************************************************************************
 * PYTHON TYPE HOOKS
 ******************************************************************************/

static PyObject * AerospikeLList_Type_New(PyTypeObject * type, PyObject * args, PyObject * kwds)
{
	AerospikeLList * self = NULL;

	self = (AerospikeLList *) type->tp_alloc(type, 0);

	return (PyObject *) self;
}

static int AerospikeLList_Type_Init(AerospikeLList * self, PyObject * args, PyObject * kwds)
{
	PyObject * py_key = NULL;
	char* bin_name = NULL;
	char* module = NULL;

	static char * kwlist[] = {"key", "bin", "module", NULL};

	if (PyArg_ParseTupleAndKeywords(args, kwds, "Os|s:llist", kwlist, &py_key,
				&bin_name, &module) == false) {
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
	initialize_ldt(&error, &self->llist, self->bin_name, AS_LDT_LLIST, module);
	if (error.code != AEROSPIKE_OK) {
		return -1;
	}

	return 0;
}

static void AerospikeLList_Type_Dealloc(PyObject * self)
{
	Py_TYPE(self)->tp_free((PyObject *) self);
}

/*******************************************************************************
 * PYTHON TYPE DESCRIPTOR
 ******************************************************************************/
static PyTypeObject AerospikeLList_Type = {
	PyVarObject_HEAD_INIT(NULL, 0)
	"aerospike.LList",                  // tp_name
	sizeof(AerospikeLList),             // tp_basicsize
	0,                                  // tp_itemsize
	(destructor) AerospikeLList_Type_Dealloc,
	                                    // tp_dealloc
	0,                                  // tp_print
	0,                                  // tp_getattr
	0,                                  // tp_setattr
	0,                                  // tp_compare
	0,                                  // tp_repr
	0,                                  // tp_as_number
	0,                                  // tp_as_sequence
	0,                                  // tp_as_mapping
	0,                                  // tp_hash
	0,                                  // tp_call
	0,                                  // tp_str
	0,                                  // tp_getattro
	0,                                  // tp_setattro
	0,                                  // tp_as_buffer
	Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
	                                    // tp_flags
	"The LList class assists in populating the parameters of a LList.\n",
	                                    // tp_doc
	0,                                  // tp_traverse
	0,                                  // tp_clear
	0,                                  // tp_richcompare
	0,                                  // tp_weaklistoffset
	0,                                  // tp_iter
	0,                                  // tp_iternext
	AerospikeLList_Type_Methods,        // tp_methods
	0,                                  // tp_members
	0,                                  // tp_getset
	0,                                  // tp_base
	0,                                  // tp_dict
	0,                                  // tp_descr_get
	0,                                  // tp_descr_set
	0,                                  // tp_dictoffset
	(initproc) AerospikeLList_Type_Init,
	                                    // tp_init
	0,                                  // tp_alloc
	AerospikeLList_Type_New,      		// tp_new
	0,                                  // tp_free
	0,                                  // tp_is_gc
	0                                   // tp_bases
};

/*******************************************************************************
 * PUBLIC FUNCTIONS
 ******************************************************************************/

PyTypeObject * AerospikeLList_Ready()
{
	return PyType_Ready(&AerospikeLList_Type) == 0 ? &AerospikeLList_Type : NULL;
}

AerospikeLList * AerospikeLList_New(AerospikeClient * client, PyObject * args, PyObject * kwds)
{
	AerospikeLList * self = (AerospikeLList *) AerospikeLList_Type.tp_new(&AerospikeLList_Type, args, kwds);
	self->client = client;
	Py_INCREF(client);

	if (AerospikeLList_Type.tp_init((PyObject *)self, args, kwds) == 0) {
		return self;
	} else {
		as_error err;
		as_error_init(&err);
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Parameters are incorrect");
		PyObject * py_err = NULL;
		PyObject * py_key = NULL;
		PyObject *exception_type = raise_exception(&err);
		error_to_pyobject(&err, &py_err);
		if (PyObject_HasAttrString(exception_type, "key")) {
			if (&self->key) {
				key_to_pyobject(&err, &self->key, &py_key);
				PyObject_SetAttrString(exception_type, "key", py_key);
				Py_DECREF(py_key);
			} else {
				PyObject_SetAttrString(exception_type, "key", Py_None);
			}
		} 
		if (PyObject_HasAttrString(exception_type, "bin")) {
			if (&self->bin_name) {
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
