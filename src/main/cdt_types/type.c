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
#include "types.h"
#include "cdt_types.h"

static PyObject *AerospikeWildCardType_New(PyTypeObject *parent, PyObject *args,
										   PyObject *kwds);
static PyObject *AerospikeInfiniteType_New(PyTypeObject *parent, PyObject *args,
										   PyObject *kwds);

/*******************************************************************************
 * PYTHON TYPE DESCRIPTOR
 ******************************************************************************/

static PyTypeObject AerospikeCDTWildcard_Type = {
	PyVarObject_HEAD_INIT(NULL, 0) AS_CDT_WILDCARD_NAME, // tp_name
	sizeof(AerospikeCDTWildcardObject),					 // tp_basicsize
	0,													 // tp_itemsize
	0,													 // tp_dealloc
	0,													 // tp_print
	0,													 // tp_getattr
	0,													 // tp_setattr
	0,													 // tp_compare
	0,													 // tp_repr
	0,													 // tp_as_number
	0,													 // tp_as_sequence
	0,													 // tp_as_mapping
	0,													 // tp_hash
	0,													 // tp_call
	0,													 // tp_str
	0,													 // tp_getattro
	0,													 // tp_setattro
	0,													 // tp_as_buffer
	Py_TPFLAGS_DEFAULT,									 // tp_flags
	"A type used to match anything when used in a Map or list "
	"comparison.\n",		  //tp_doc
	0,						  // tp_traverse
	0,						  // tp_clear
	0,						  // tp_richcompare
	0,						  // tp_weaklistoffset
	0,						  // tp_iter
	0,						  // tp_iternext
	0,						  // tp_methods
	0,						  // tp_members
	0,						  // tp_getset
	0,						  // tp_base
	0,						  // tp_dict
	0,						  // tp_descr_get
	0,						  // tp_descr_set
	0,						  // tp_dictoffset
	0,						  // tp_init
	0,						  // tp_alloc
	AerospikeWildCardType_New // tp_new
};

PyTypeObject *AerospikeWildcardObject_Ready()
{
	return PyType_Ready(&AerospikeCDTWildcard_Type) == 0
			   ? &AerospikeCDTWildcard_Type
			   : NULL;
}

static PyTypeObject AerospikeCDTInfinite_Type = {
	PyVarObject_HEAD_INIT(NULL, 0) AS_CDT_INFINITE_NAME, // tp_name
	sizeof(AerospikeCDTInfObject),						 // tp_basicsize
	0,													 // tp_itemsize
	0,													 // tp_dealloc
	0,													 // tp_print
	0,													 // tp_getattr
	0,													 // tp_setattr
	0,													 // tp_compare
	0,													 // tp_repr
	0,													 // tp_as_number
	0,													 // tp_as_sequence
	0,													 // tp_as_mapping
	0,													 // tp_hash
	0,													 // tp_call
	0,													 // tp_str
	0,													 // tp_getattro
	0,													 // tp_setattro
	0,													 // tp_as_buffer
	Py_TPFLAGS_DEFAULT,									 // tp_flags
	"A type used to match anything when used in a Map or list "
	"comparison.\n",		  //tp_doc
	0,						  // tp_traverse
	0,						  // tp_clear
	0,						  // tp_richcompare
	0,						  // tp_weaklistoffset
	0,						  // tp_iter
	0,						  // tp_iternext
	0,						  // tp_methods
	0,						  // tp_members
	0,						  // tp_getset
	0,						  // tp_base
	0,						  // tp_dict
	0,						  // tp_descr_get
	0,						  // tp_descr_set
	0,						  // tp_dictoffset
	0,						  // tp_init
	0,						  // tp_alloc
	AerospikeInfiniteType_New // tp_new
};

PyTypeObject *AerospikeInfiniteObject_Ready()
{
	return PyType_Ready(&AerospikeCDTInfinite_Type) == 0
			   ? &AerospikeCDTInfinite_Type
			   : NULL;
}

static PyObject *AerospikeWildCardType_New(PyTypeObject *parent, PyObject *args,
										   PyObject *kwds)
{
	return (PyObject *)PyObject_New(AerospikeCDTWildcardObject, parent);
}

static PyObject *AerospikeInfiniteType_New(PyTypeObject *parent, PyObject *args,
										   PyObject *kwds)
{
	return (PyObject *)PyObject_New(AerospikeCDTInfObject, parent);
}