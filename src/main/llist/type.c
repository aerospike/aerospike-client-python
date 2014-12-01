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
#include <aerospike/as_ldt.h>

#include "client.h"
#include "conversions.h"
#include "llist.h"

/*******************************************************************************
 * PYTHON TYPE METHODS
 ******************************************************************************/

static PyMethodDef AerospikeLList_Type_Methods[] = {

	// LLIST OPERATIONS

	{"add",
		(PyCFunction) AerospikeLList_Add, METH_VARARGS | METH_KEYWORDS,
		"Adds a value to the LList."},
	{"add_all",
		(PyCFunction) AerospikeLList_Add_All, METH_VARARGS | METH_KEYWORDS,
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

	if ( self == NULL ) {
		return NULL;
	}

	return (PyObject *) self;
}

static int AerospikeLList_Type_Init(AerospikeLList * self, PyObject * args, PyObject * kwds)
{
	PyObject * py_key = NULL;
    char* bin_name = NULL;
    char* module = NULL;

	static char * kwlist[] = {"key", "bin", "module", NULL};

	if ( PyArg_ParseTupleAndKeywords(args, kwds, "Os|s:llist", kwlist, &py_key,
                &bin_name, &module) == false ) {
		return -1;
	}

    /*
     * Convert pyobject to as_key type.
     */
    as_error error;
    as_error_init(&error);
    as_key key;
    as_ldt llist;

    pyobject_to_key(&error, py_key, &key);
    if (error.code != AEROSPIKE_OK) {
        return -1;
    }

    int bin_name_len = strlen(bin_name);
    if ((bin_name_len == 0) || (bin_name_len > AS_BIN_NAME_MAX_LEN)) {
        return -1;
    }

    self->key = key;
    strcpy(self->bin_name, bin_name);

    /*
     * LDT Initialization
     */
    initialize_ldt(&error, &llist, self->bin_name, AS_LDT_LLIST, module);
    if (error.code != AEROSPIKE_OK) {
        return -1;
    }

    self->llist = llist;

	return 0;
}

static void AerospikeLList_Type_Dealloc(PyObject * self)
{
	self->ob_type->tp_free((PyObject *) self);
}

/*******************************************************************************
 * PYTHON TYPE DESCRIPTOR
 ******************************************************************************/

static PyTypeObject AerospikeLList_Type = {
	PyObject_HEAD_INIT(NULL)

		.ob_size			= 0,
	.tp_name			= "aerospike.LList",
	.tp_basicsize		= sizeof(AerospikeLList),
	.tp_itemsize		= 0,
	.tp_dealloc			= (destructor) AerospikeLList_Type_Dealloc,
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
		"The LList class assists in populating the parameters of a LList.\n",
	.tp_traverse		= 0,
	.tp_clear			= 0,
	.tp_richcompare		= 0,
	.tp_weaklistoffset	= 0,
	.tp_iter			= 0,
	.tp_iternext		= 0,
	.tp_methods			= AerospikeLList_Type_Methods,
	.tp_members			= 0,
	.tp_getset			= 0,
	.tp_base			= 0,
	.tp_dict			= 0,
	.tp_descr_get		= 0,
	.tp_descr_set		= 0,
	.tp_dictoffset		= 0,
	.tp_init			= (initproc) AerospikeLList_Type_Init,
	.tp_alloc			= 0,
	.tp_new				= AerospikeLList_Type_New
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
    AerospikeLList_Type.tp_init((PyObject *) self, args, kwds);
    return self;

}
