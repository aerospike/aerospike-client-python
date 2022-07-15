/*******************************************************************************
 * Copyright 2013-2021 Aerospike, Inc.
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
#include <unistd.h>

#include "nullobject.h"

static PyObject *AerospikeNullObject_Type_New(PyTypeObject *parent,
											  PyObject *args, PyObject *kwds);

static void AerospikeNullObject_Type_Dealloc(AerospikeNullObject *self)
{
	PyObject_Del(self);
}

/*******************************************************************************
 * PYTHON TYPE DESCRIPTOR
 ******************************************************************************/

static PyTypeObject AerospikeNullObject_Type = {
	PyVarObject_HEAD_INIT(NULL, 0) "aerospike.null", // tp_name
	sizeof(AerospikeNullObject),					 // tp_basicsize
	0,												 // tp_itemsize
	(destructor)AerospikeNullObject_Type_Dealloc,
	// tp_dealloc
	0, // tp_print
	0, // tp_getattr
	0, // tp_setattr
	0, // tp_compare
	0, // tp_repr
	0, // tp_as_number
	0, // tp_as_sequence
	0, // tp_as_mapping
	0, // tp_hash
	0, // tp_call
	0, // tp_str
	0, // tp_getattro
	0, // tp_setattro
	0, // tp_as_buffer
	Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
	// tp_flags
	"The nullobject when used with put() works as a removebin()\n",
	// tp_doc
	0,							  // tp_traverse
	0,							  // tp_clear
	0,							  // tp_richcompare
	0,							  // tp_weaklistoffset
	0,							  // tp_iter
	0,							  // tp_iternext
	0,							  // tp_methods
	0,							  // tp_members
	0,							  // tp_getset
	0,							  // tp_base
	0,							  // tp_dict
	0,							  // tp_descr_get
	0,							  // tp_descr_set
	0,							  // tp_dictoffset
	0,							  // tp_init
	0,							  // tp_alloc
	AerospikeNullObject_Type_New, // tp_new
	0,							  // tp_free
	0,							  // tp_is_gc
	0							  // tp_bases
};

static PyObject *AerospikeNullObject_Type_New(PyTypeObject *parent,
											  PyObject *args, PyObject *kwds)
{
	return (PyObject *)PyObject_New(AerospikeNullObject, parent);
}

PyObject *AerospikeNullObject_New()
{
	return AerospikeNullObject_Type_New(&AerospikeNullObject_Type, Py_None,
										Py_None);
}

PyTypeObject *AerospikeNullObject_Ready()
{
	return PyType_Ready(&AerospikeNullObject_Type) == 0
			   ? &AerospikeNullObject_Type
			   : NULL;
}
